import type {Env,Params} from './types';
import type {Outcome,IssueGuard} from './operations';
import {dueReceiptCleanup,receiptGuardValid} from './receipts';
import {deliveryRetryGuardValid} from './delivery-retry';
import {broadcastGuardValid} from './broadcast-guard';
const allowed = new Set(['getMe','getChat','getChatMember','getUserProfilePhotos','createForumTopic','editForumTopic','sendMessage','sendPhoto','sendMediaGroup','copyMessage','editMessageText','editMessageCaption','editMessageMedia','editMessageReplyMarkup','pinChatMessage','unpinChatMessage','setMessageReaction','deleteMessage','answerCallbackQuery','setMyCommands']);
const nonSending = new Set(['editMessageText','editMessageCaption','editMessageMedia','editMessageReplyMarkup','deleteMessage','unpinChatMessage']);
const priorityEdits=new Set(['editMessageText','editMessageCaption','editMessageMedia','editMessageReplyMarkup']);
const localRateDueHeader='X-Bot-Local-Rate-Due';
const maxLocalWaitMs=1000;
interface RequestBody {key:string;method:string;params:Params;guard?:IssueGuard}
interface Journal {phase:'issued'|'done';outcome?:Outcome;apiMs?:number}
/** Bot-wide serialization, durable result journal and conservative shared-chat limits. */
export class BotCoordinator {
 private tail:Promise<unknown>=Promise.resolve();
 private callbackTail:Promise<unknown>=Promise.resolve();
 private cardControlTail:Promise<unknown>=Promise.resolve();
 private broadcastTail:Promise<unknown>=Promise.resolve();
 private controlRequests=new Map<string,Promise<unknown>>();
 private reservationTail:Promise<unknown>=Promise.resolve();
 constructor(private state:DurableObjectState,private env:Env){}
 async fetch(request:Request):Promise<Response> {
  const journal=new URL(request.url).pathname==='/journal';
  const body=journal?null:await request.clone().json<RequestBody>();
  const callback=body?.method==='answerCallbackQuery';
  const cardControl=body?.guard?.priority==='moderation-card'&&priorityEdits.has(body.method)&&Number.isSafeInteger(body.guard.threadVersion?.moderationVersion);
  const broadcast=!!body?.guard?.broadcast;
  let queuedMs=0,localWaitMs=0,scheduledMs=0,waits=0;
  for(;;) {
   const queuedAt=Date.now();
   // Callback acknowledgements do not send a chat message. Their own durable
   // lane can return an already-committed result while a business API request
   // is still waiting for Telegram, without relaxing send/mutation pacing.
   const result=(callback?this.callbackTail:cardControl?this.cardControlTail:broadcast?this.broadcastTail:this.tail).then(async()=>{
    queuedMs+=Math.max(0,Date.now()-queuedAt);
    const attempt=request.clone();
    // A clear can recover a broadcast operation through the ordinary lane.
    // Wait for its active request before reading/sealing the response journal.
    if(!journal&&!callback&&!cardControl&&!broadcast)await this.controlRequests.get(body!.key)?.catch(()=>{});
    return journal?this.journal(attempt):this.execute(attempt);
   });
   if(callback||cardControl||broadcast){
    if(callback)this.callbackTail=result.catch(()=>{});else if(cardControl)this.cardControlTail=result.catch(()=>{});else this.broadcastTail=result.catch(()=>{});
    this.controlRequests.set(body!.key,result);
   }
   else this.tail=result.catch(()=>{});
   let response:Response;
   try {response=await result;}
   finally {if((callback||cardControl||broadcast)&&this.controlRequests.get(body!.key)===result)this.controlRequests.delete(body!.key);}
   const localDue=response.headers.get(localRateDueHeader);
   response.headers.delete(localRateDueHeader);
   const delay=localDue===null?NaN:Math.max(0,Number(localDue)-Date.now());
   if(!Number.isFinite(delay)||scheduledMs+delay>maxLocalWaitMs||waits>=4) {
    response.headers.set('X-Bot-Wait-Ms',String(queuedMs+localWaitMs));
    return response;
   }
   // Only explicit local rate deadlines reach here. Release the execution
   // tail while waiting, then re-enter through journal and all final guards.
   // Bound scheduled time and attempts even if a clock fails to advance.
   scheduledMs+=delay;waits++;
   const waitStarted=Date.now();
   if(delay>0)await new Promise<void>(resolve=>setTimeout(resolve,delay));
   localWaitMs+=Math.max(0,Date.now()-waitStarted);
  }
 }
 /** Maintenance reads share the execution tail, so an active request finishes
  * before it can be reconciled. This route never invokes Telegram. */
 private async journal(request:Request):Promise<Response> {
  const {key}=await request.json<{key?:unknown}>();
  if(typeof key!=='string'||!key||key.length>4096)return new Response('invalid',{status:400});
  // A paused reconciliation must wait for any active acknowledgement before
  // deciding whether its durable result is actually unknown.
  await this.controlRequests.get(key)?.catch(()=>{});
  const journalKey=`op:${key}`;
  const old=await this.state.storage.get<Journal>(journalKey);
  if(old?.phase==='done'&&old.outcome)return this.result(old.outcome,old.apiMs);
  const outcome:Outcome={status:'unknown',reason:old?.phase==='issued'?'发生未知情况：已发出请求的持久结果无法恢复；禁止自动重发':'发生未知情况：已标记发出的请求缺少持久响应；禁止自动重发'};
  // Seal missing records too: an original request delayed in transit must read
  // this terminal outcome instead of creating an external side effect later.
  await this.state.storage.put(journalKey,{phase:'done',outcome});
  return Response.json(outcome);
 }
 private async execute(request:Request):Promise<Response> {
  const {key,method,params,guard}=await request.json<RequestBody>();
  if(!key.startsWith(`${this.env.RECOVERY_EPOCH}:`)||!allowed.has(method)) return new Response('invalid',{status:400});
  const journalKey=`op:${key}`;
  const old=await this.state.storage.get<Journal>(journalKey);
  if(old?.phase==='done'&&old.outcome&&!(method.startsWith('get')&&old.outcome.status==='unknown')) return this.result(old.outcome,old.apiMs);
  if(old?.phase==='issued'&&!method.startsWith('get')) {
   const outcome:Outcome={status:'unknown',reason:'发生未知情况：请求已发出，无法确认结果'};
   await this.state.storage.put(journalKey,{phase:'done',outcome});return Response.json(outcome);
  }
  // Callback network requests may overlap ordinary Telegram requests. Keep
  // their short final rate/guard/reservation section atomic with any newly
  // received platform backoff; release it before awaiting Telegram itself.
  const reserved=await this.withReservation(async()=>{
  const chat=String(params.chat_id??'');
  const exempt=method.startsWith('get')||method==='answerCallbackQuery';
  // Edits, deletes and unpins do not send a new message. Keep one conservative
  // mutation lane beside sends and reactions; all share global/backoff limits.
  const reaction=method==='setMessageReaction';
  const chatRateKey=reaction?`rate:reaction:${chat}`:nonSending.has(method)?`rate:mutation:${chat}`:`rate:${chat}`;
  const globalAt=await this.state.storage.get<number>('rate:global')??0;
  const backoffAt=await this.state.storage.get<number>('rate:backoff')??0;
  const chatAt=chat?await this.state.storage.get<number>(chatRateKey)??0:0;
  const broadcastAt=guard?.broadcast?await this.state.storage.get<number>('rate:broadcast')??0:0;
  const due=exempt?backoffAt:Math.max(globalAt,chatAt,backoffAt,broadcastAt);
  const rateCheckedAt=Date.now();
  if(rateCheckedAt<due) return Response.json({due},{status:429,headers:!exempt&&backoffAt<=rateCheckedAt&&due-rateCheckedAt<=maxLocalWaitMs?{[localRateDueHeader]:String(due)}:{}});
  const checks:D1PreparedStatement[]=[];
  const hiddenIndex=guard?.messageIds?.length?checks.push(this.env.DB.prepare(`SELECT id FROM messages WHERE id IN (${guard.messageIds.map(()=>'?').join(',')}) AND tombstone=1 LIMIT 1`).bind(...guard.messageIds))-1:-1;
  const bindingIndex=guard?.threadVersion?checks.push(this.env.DB.prepare('SELECT binding_version,thread_id,blocked,moderation_version FROM users WHERE tgid=?').bind(guard.threadVersion.userId))-1:-1;
  const policyIndex=guard?.unblockedUser?(guard.unblockedUser===guard.threadVersion?.userId?bindingIndex:checks.push(this.env.DB.prepare('SELECT blocked FROM users WHERE tgid=?').bind(guard.unblockedUser))-1):-1;
  // Complex guards perform additional reads. Keep pause after those reads so
  // it remains the final safety check, while ordinary sends use one D1 batch.
  const deferPause=!!(guard?.receipt||guard?.deliveryRetry||guard?.broadcast);
  const pauseIndex=!deferPause&&this.env.PAUSED!=='true'?checks.push(this.env.DB.prepare("SELECT key FROM settings WHERE key='paused' AND value='1'"))-1:-1;
  const checked=checks.length?await this.env.DB.batch(checks):[];
  if(guard) {
   const hidden=hiddenIndex>=0&&checked[hiddenIndex].results.length>0;
   const binding=bindingIndex>=0?checked[bindingIndex].results[0] as {binding_version:number;thread_id:number;blocked:number;moderation_version:number}|undefined:undefined;
   const policy=policyIndex>=0?checked[policyIndex].results[0] as {blocked:number}|undefined:undefined;
   if(hidden||(guard.unblockedUser&&policy?.blocked!==0)||(guard.threadVersion&&(binding?.binding_version!==guard.threadVersion.version||(guard.threadVersion.threadId!==undefined&&binding?.thread_id!==guard.threadVersion.threadId)||(guard.threadVersion.moderationVersion!==undefined&&binding?.moderation_version!==guard.threadVersion.moderationVersion)))||(guard.receipt&&!await receiptGuardValid(this.env,guard.receipt))||(guard.deliveryRetry&&!await deliveryRetryGuardValid(this.env,guard.deliveryRetry))||(guard.broadcast&&!await broadcastGuardValid(this.env,guard.broadcast,method,params))) {
    const outcome:Outcome={status:'failed',reason:'cancelled_by_state',code:409};
    await this.state.storage.put(journalKey,{phase:'done',outcome});return Response.json(outcome);
   }
  }
  const paused=this.env.PAUSED==='true'||(deferPause?await this.env.DB.prepare("SELECT key FROM settings WHERE key='paused' AND value='1'").first():pauseIndex>=0&&checked[pauseIndex].results.length>0);
  if(paused)return Response.json({due:Date.now()+60000},{status:429});
  if(reaction && guard?.receipt && !guard.receipt.clear && guard.receipt.emoji==='👍' && await dueReceiptCleanup(this.env,Date.now()))return Response.json({due:Date.now()+100},{status:429});
  const messageCount=method==='sendMediaGroup'&&Array.isArray(params.media)?Math.max(1,params.media.length):1;
  const reservation=(at:number)=>exempt?{}:{'rate:global':at+40*messageCount,...(guard?.broadcast?{'rate:broadcast':at+1050}:{}),...(chat?{[chatRateKey]:at+(reaction?1050:chat.startsWith('-')?3100:1050)*messageCount}:{})};
  await this.state.storage.put({[journalKey]:{phase:'issued'},...reservation(Date.now())});
  return {reservation};
  });
  if(reserved instanceof Response)return reserved;
  const {reservation}=reserved;
  const reaction=method==='setMessageReaction';
  let outcome:Outcome;
  const apiStartedAt=Date.now();
  try {
   const response=await fetch(`https://api.telegram.org/bot${this.env.TELEGRAM_BOT_TOKEN}/${method}`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(params),signal:AbortSignal.timeout(20000)});
   const body=await response.json<{ok?:boolean;result?:unknown;error_code?:number;description?:string;parameters?:{retry_after?:number}}>();
   if(body.ok===true&&Object.hasOwn(body,'result')) outcome={status:'success',result:body.result,...(reaction?{confirmedAt:Date.now()}: {})};
   else if(body.ok===false&&body.error_code===429) {
    const retry=Number(body.parameters?.retry_after);
    const retryAt=Date.now()+Math.max(1000,Number.isFinite(retry)?retry*1000:60000);
    await this.state.storage.delete(journalKey);
    const due=await this.raiseBackoff(retryAt,reservation(apiStartedAt));
    return this.result({due},Date.now()-apiStartedAt,429);
   } else if(body.ok===false&&typeof body.error_code==='number'&&body.error_code>=400&&body.error_code<500) {
    const reason=(body.description??'Telegram 拒绝请求').replaceAll(this.env.TELEGRAM_BOT_TOKEN,'[redacted]').replaceAll(this.env.TELEGRAM_WEBHOOK_SECRET,'[redacted]').slice(0,240);
    // Telegram's no-change response confirms the desired edit already exists.
    if(method.startsWith('edit')&&reason.includes('message is not modified')) outcome={status:'success',result:true};
    else outcome={status:'failed',code:body.error_code,reason};
   } else outcome={status:'unknown',reason:'发生未知情况：Telegram 未返回可确认结果'};
  } catch {outcome={status:'unknown',reason:'发生未知情况：网络中断、超时或响应格式错误'};}
  const apiMs=Math.max(0,Date.now()-apiStartedAt);
  if(method.startsWith('get')&&outcome.status==='unknown'){await this.state.storage.delete(journalKey);return this.result({due:Date.now()+5000},apiMs,429);}
  // A slow durable reservation can finish after its first deadline. Persist
  // the actual API-start anchor with the result before the next request runs.
  await this.withReservation(async()=>{
   const rates=await this.mergeRates(reservation(apiStartedAt));
   await this.state.storage.put({[journalKey]:{phase:'done',outcome,apiMs},...rates});
  });
  return this.result(outcome,apiMs);
 }
 private withReservation<T>(work:()=>Promise<T>):Promise<T> {
  const result=this.reservationTail.then(work);
  this.reservationTail=result.catch(()=>{});return result;
 }
 private raiseBackoff(due:number,reservation:Record<string,number>):Promise<number> {
  return this.withReservation(async()=>{
   const rates=await this.mergeRates(reservation);
   const effective=Math.max(due,await this.state.storage.get<number>('rate:backoff')??0);
   const global=Math.max(effective,await this.state.storage.get<number>('rate:global')??0,rates['rate:global']??0);
   await this.state.storage.put({...rates,'rate:backoff':effective,'rate:global':global});
   return effective;
  });
 }
 private async mergeRates(reservation:Record<string,number>):Promise<Record<string,number>> {
  const merged:Record<string,number>={};
  for(const [key,due]of Object.entries(reservation))merged[key]=Math.max(due,await this.state.storage.get<number>(key)??0);
  return merged;
 }
 private result(value:unknown,apiMs?:number,status=200):Response {
  return Response.json(value,{status,headers:apiMs===undefined?{}:{'X-Bot-Api-Ms':String(apiMs)}});
 }
}
