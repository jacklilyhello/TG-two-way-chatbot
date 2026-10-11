import type {Env,Params} from './types';
import type {Outcome,IssueGuard} from './operations';
import {receiptGuardValid} from './receipts';
const allowed = new Set(['getMe','getChat','getChatMember','getUserProfilePhotos','createForumTopic','editForumTopic','sendMessage','sendPhoto','sendMediaGroup','copyMessage','editMessageText','editMessageCaption','editMessageMedia','editMessageReplyMarkup','pinChatMessage','unpinChatMessage','setMessageReaction','deleteMessage','answerCallbackQuery','setMyCommands']);
interface RequestBody {key:string;method:string;params:Params;guard?:IssueGuard}
interface Journal {phase:'issued'|'done';outcome?:Outcome;apiMs?:number}
/** Bot-wide serialization, durable result journal and conservative shared-chat limits. */
export class BotCoordinator {
 private tail:Promise<unknown>=Promise.resolve();
 constructor(private state:DurableObjectState,private env:Env){}
 fetch(request:Request):Promise<Response> {
  const queuedAt=Date.now();
  const result=this.tail.then(async()=>{
   const startedAt=Date.now();
   const response=await (new URL(request.url).pathname==='/journal'?this.journal(request):this.execute(request));
   response.headers.set('X-Bot-Wait-Ms',String(Math.max(0,startedAt-queuedAt)));
   return response;
  });this.tail=result.catch(()=>{});return result;
 }
 /** Maintenance reads share the execution tail, so an active request finishes
  * before it can be reconciled. This route never invokes Telegram. */
 private async journal(request:Request):Promise<Response> {
  const {key}=await request.json<{key?:unknown}>();
  if(typeof key!=='string'||!key||key.length>4096)return new Response('invalid',{status:400});
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
  const time=Date.now();
  const chat=String(params.chat_id??'');
  const exempt=method.startsWith('get')||method==='answerCallbackQuery';
  // Reactions change the bot's own mark; they do not send a group message.
  // Keep their own conservative per-chat pace, while preserving the shared
  // 20-message/minute group-send budget and all Telegram retry_after pauses.
  const reaction=method==='setMessageReaction';
  const chatRateKey=reaction?`rate:reaction:${chat}`:`rate:${chat}`;
  const globalAt=await this.state.storage.get<number>('rate:global')??0;
  const backoffAt=await this.state.storage.get<number>('rate:backoff')??0;
  const chatAt=chat?await this.state.storage.get<number>(chatRateKey)??0:0;
  const due=exempt?backoffAt:Math.max(globalAt,chatAt,backoffAt);
  if(time<due) return Response.json({due},{status:429});
  if(guard) {
   const hidden=guard.messageIds?.length?await this.env.DB.prepare(`SELECT id FROM messages WHERE id IN (${guard.messageIds.map(()=>'?').join(',')}) AND tombstone=1 LIMIT 1`).bind(...guard.messageIds).first():null;
   const binding=guard.threadVersion?await this.env.DB.prepare('SELECT binding_version,thread_id FROM users WHERE tgid=?').bind(guard.threadVersion.userId).first<{binding_version:number;thread_id:number}>():null;
   if(hidden||(guard.threadVersion&&(binding?.binding_version!==guard.threadVersion.version||(guard.threadVersion.threadId!==undefined&&binding?.thread_id!==guard.threadVersion.threadId)))||(guard.receipt&&!await receiptGuardValid(this.env,guard.receipt))) {
    const outcome:Outcome={status:'failed',reason:'cancelled_by_state',code:409};
    await this.state.storage.put(journalKey,{phase:'done',outcome});return Response.json(outcome);
   }
  }
  if(this.env.PAUSED==='true'||await this.env.DB.prepare("SELECT key FROM settings WHERE key='paused' AND value='1'").first())return Response.json({due:time+60000},{status:429});
  if(!exempt) {
   const messageCount=method==='sendMediaGroup'&&Array.isArray(params.media)?Math.max(1,params.media.length):1;
   await this.state.storage.put({'rate:global':time+40*messageCount,...(chat?{[chatRateKey]:time+(reaction?1050:chat.startsWith('-')?3100:1050)*messageCount}:{})});
  }
  await this.state.storage.put(journalKey,{phase:'issued'});
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
    await this.state.storage.put({'rate:backoff':Math.max(backoffAt,retryAt),'rate:global':Math.max(globalAt,retryAt)});
    return this.result({due:retryAt},Date.now()-apiStartedAt,429);
   } else if(body.ok===false&&typeof body.error_code==='number'&&body.error_code>=400&&body.error_code<500) {
    const reason=(body.description??'Telegram 拒绝请求').replaceAll(this.env.TELEGRAM_BOT_TOKEN,'[redacted]').replaceAll(this.env.TELEGRAM_WEBHOOK_SECRET,'[redacted]').slice(0,240);
    // Telegram's no-change response confirms the desired edit already exists.
    if(method.startsWith('edit')&&reason.includes('message is not modified')) outcome={status:'success',result:true};
    else outcome={status:'failed',code:body.error_code,reason};
   } else outcome={status:'unknown',reason:'发生未知情况：Telegram 未返回可确认结果'};
  } catch {outcome={status:'unknown',reason:'发生未知情况：网络中断、超时或响应格式错误'};}
  const apiMs=Math.max(0,Date.now()-apiStartedAt);
  if(method.startsWith('get')&&outcome.status==='unknown'){await this.state.storage.delete(journalKey);return this.result({due:Date.now()+5000},apiMs,429);}
  await this.state.storage.put(journalKey,{phase:'done',outcome,apiMs});
  return this.result(outcome,apiMs);
 }
 private result(value:unknown,apiMs?:number,status=200):Response {
  return Response.json(value,{status,headers:apiMs===undefined?{}:{'X-Bot-Api-Ms':String(apiMs)}});
 }
}
