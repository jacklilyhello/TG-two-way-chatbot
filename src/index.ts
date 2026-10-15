import type {Env,TGUpdate} from './types';
import {validUpdate,accept} from './ingress';
import {all,run,setting,setSetting,now,audit,enqueue,armWork,stmt} from './db';
import {versionEvent} from './logs';
import {reconcileIssued} from './operations';
import {recoverPreparedReceipts} from './receipts';
export {BotCoordinator} from './bot';
export {ConversationCoordinator} from './coordinator';
function secretMatches(a:string,b:string) {if(!a||!b)return false;let diff=a.length^b.length;for(let i=0;i<Math.max(a.length,b.length);i++)diff|=(a.charCodeAt(i)||0)^(b.charCodeAt(i)||0);return diff===0;}
async function wake(e:Env,key:string) {return e.CONVERSATIONS.get(e.CONVERSATIONS.idFromName(key)).fetch('https://conversation/wake',{method:'POST',body:JSON.stringify({key})});}
export async function recover(e:Env) {
 await run(e,"UPDATE jobs SET state='quarantined',error='恢复代次不匹配；禁止重放' WHERE epoch!=? AND state NOT IN ('done','quarantined')",e.RECOVERY_EPOCH);
 await recoverPreparedReceipts(e);
 // Upgrade every existing card once, including users who never send another
 // message. The job ledger spreads API work across ordinary recovery wakes;
 // the marker and inserts commit together and preserve cards and bindings.
 const cardUpgrade=`profile-ui-v2:${e.RECOVERY_EPOCH}`,time=now();
 await e.DB.batch([
  stmt(e,"INSERT OR IGNORE INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) SELECT ?||':'||tgid||':'||binding_version,'card:'||tgid,'card',json_object('userId',tgid,'version',binding_version,'threadId',thread_id,'urgent',json('true')),'pending',?,?,? FROM users WHERE thread_id IS NOT NULL AND card_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM settings WHERE key=?)",cardUpgrade,time,e.RECOVERY_EPOCH,time,cardUpgrade),
  stmt(e,"INSERT OR IGNORE INTO settings(key,value) VALUES(?,'queued')",cardUpgrade)
 ]);
 // A manually recovered binding can unblock originals whose first topic setup failed.
 const stranded=await all<{id:number;user_id:string;payload:string;binding_version:number}>(e,"SELECT m.id,m.user_id,m.payload,u.binding_version FROM messages m JOIN users u ON u.tgid=m.user_id WHERE m.epoch=? AND m.direction='in' AND m.business=1 AND m.delivery='pending' AND m.tombstone=0 AND u.thread_id IS NOT NULL ORDER BY m.id LIMIT 40",e.RECOVERY_EPOCH);
 for(const m of stranded)await enqueue(e,m.user_id,'update',{update:{update_id:0,message:JSON.parse(m.payload)}},`route:${m.id}:${m.binding_version}`);
 const keys=await all<{user_key:string}>(e,"SELECT DISTINCT user_key FROM jobs WHERE state IN ('pending','retry','running') AND due_at<=? AND epoch=? ORDER BY id LIMIT 40",now(),e.RECOVERY_EPOCH);
 await Promise.allSettled(keys.map(x=>e.JOBS.send({key:x.user_key})));
 await versionEvent(e);
}
export default {
 async fetch(request:Request,e:Env,ctx?:ExecutionContext):Promise<Response> {
  const path=new URL(request.url).pathname;
  if(path==='/health'&&request.method==='GET') {try{await e.DB.prepare('SELECT 1').first();return Response.json({ok:true,version:e.VERSION});}catch{return Response.json({ok:false},{status:503});}}
  if(!['/webhook','/internal/maintenance'].includes(path))return new Response('Not found',{status:404});
  if(request.method!=='POST')return new Response('Method not allowed',{status:405});
  if(!secretMatches(request.headers.get('X-Telegram-Bot-Api-Secret-Token')??'',e.TELEGRAM_WEBHOOK_SECRET))return new Response('Forbidden',{status:403});
  if(!(request.headers.get('Content-Type')??'').toLowerCase().startsWith('application/json'))return new Response('JSON required',{status:415});
  if(Number(request.headers.get('Content-Length'))>262144)return new Response('Too large',{status:413});
  try {
   const reader=request.body?.getReader();if(!reader)return new Response('Bad request',{status:400});
   const chunks:Uint8Array[]=[];let size=0;
   for(;;){const r=await reader.read();if(r.done)break;size+=r.value.length;if(size>262144){await reader.cancel();return new Response('Too large',{status:413});}chunks.push(r.value);}
   const data=new Uint8Array(size);let cursor=0;for(const c of chunks){data.set(c,cursor);cursor+=c.length;}
   let body:unknown;try{body=JSON.parse(new TextDecoder().decode(data));}catch{return new Response('Invalid JSON',{status:400});}
   if(path==='/internal/maintenance') {
    const action=(body as {action?:string}).action;
    if(action==='pause'||action==='resume') {await setSetting(e,'paused',action==='pause'?'1':'0');await audit(e,'维护状态',{action});}
    else if(action==='recover')await recover(e);
    else if(action!=='status')return new Response('Invalid action',{status:400});
    if((action==='pause'||action==='status')&&(e.PAUSED==='true'||await setting(e,'paused')==='1'))await reconcileIssued(e);
    const counts=await all<{state:string;count:number}>(e,'SELECT state,COUNT(*) count FROM jobs GROUP BY state');
    const issued=await e.DB.prepare("SELECT COUNT(*) n FROM operations WHERE phase='issued'").first<{n:number}>();
    return Response.json({ok:true,version:e.VERSION,epoch:e.RECOVERY_EPOCH,paused:e.PAUSED==='true'||await setting(e,'paused')==='1',jobs:counts,issued:issued?.n??0});
   }
   if(!validUpdate(body))return new Response('Invalid update',{status:400});
   const update=body as TGUpdate,started=now();
   const message=update.message??update.edited_message;
   // Correlate authenticated ingress with its durable outcome without logging
   // identities, content, media identifiers, credentials or exception text.
   const observation={event:'webhook_commit',version:e.VERSION,update_id:update.update_id,
    kind:update.message?'message':update.edited_message?'edited_message':update.callback_query?'callback_query':'other',
    media:message?.photo?'photo':message?.video?'video':message?.audio?'audio':message?.voice?'voice':message?.document?'document':message?.sticker?'sticker':message?.text?'text':'other',
    album:!!message?.media_group_id};
   console.info({...observation,phase:'received',at:started});
   let key:string;
   try {key=await accept(e,update);}
   catch {const failed=now();console.info({...observation,phase:'failed',at:failed,elapsed_ms:Math.max(0,failed-started)});throw new Error('Ingress commit failed');}
   const committed=now();
   console.info({...observation,phase:'committed',at:committed,elapsed_ms:Math.max(0,committed-started)});
   // The durable commit precedes both independent wake hints. Keep the
   // Webhook response separate from potentially long conversation draining.
   ctx?.waitUntil(armWork(e,key).catch(()=>{}));
   try{await e.JOBS.send({key});}catch{/* durable ledger + cron recover queue notification failure */}
   return new Response('ok');
  } catch {return new Response('Temporary unavailable',{status:503});}
 },
 async queue(batch:MessageBatch<{key:string}>,e:Env) {
  // Queue notifications are hints. A single wake drains a key's durable jobs;
  // unrelated conversations must not wait behind another key in this batch.
  const groups=new Map<string,(typeof batch.messages)[number][]>();
  for(const message of batch.messages){const group=groups.get(message.body.key)??[];group.push(message);groups.set(message.body.key,group);}
  await Promise.allSettled([...groups].map(async([key,messages])=>{
   try{const r=await wake(e,key);if(!r.ok){await r.body?.cancel();throw new Error();}await r.text();for(const message of messages)message.ack();}
   catch{for(const message of messages)message.retry({delaySeconds:10});}
  }));
 },
 async scheduled(_controller:ScheduledController,e:Env) {await recover(e);}
} satisfies ExportedHandler<Env, {key:string}>;
