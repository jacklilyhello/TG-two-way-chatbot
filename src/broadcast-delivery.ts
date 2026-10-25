import type {Env,Job,Params,MessageRow} from './types';
import {all,now,one,run,stmt,wakeWork} from './db';
import {call,notify,RetryLater,type Outcome} from './operations';

export interface BroadcastContent {text?:string;photo?:string|null}
export type BroadcastAudience={mode:'all'}|{mode:'directed';ids:string[]};
export const MAX_BROADCAST_IDS=200;
const invalidAudience=()=>new Error('invalid_broadcast_audience');
const validAudienceId=(id:unknown):id is string=>typeof id==='string'&&/^[1-9]\d{0,15}$/.test(id)&&Number(id)<=4503599627370495;
/** Canonical values make the requested range immutable across durable replays. */
export function normalizeBroadcastAudience(input?:BroadcastAudience):BroadcastAudience {
 if(input===undefined)return {mode:'all'};
 if(!input||typeof input!=='object'||Array.isArray(input))throw invalidAudience();
 if(input.mode==='all'){if(Object.keys(input).some(key=>key!=='mode'))throw invalidAudience();return {mode:'all'};}
 if(input.mode!=='directed'||Object.keys(input).some(key=>key!=='mode'&&key!=='ids')||!Array.isArray(input.ids)||!input.ids.length||Array.from(input.ids).some(id=>!validAudienceId(id)))throw invalidAudience();
 const ids=[...new Set(input.ids)].sort((a,b)=>Number(a)-Number(b));
 if(ids.length>MAX_BROADCAST_IDS)throw invalidAudience();
 return {mode:'directed',ids};
}
export function parseBroadcastIds(text:string):string[] {
 if(typeof text!=='string')throw invalidAudience();
 const audience=normalizeBroadcastAudience({mode:'directed',ids:text.split(/[\s,，]+/u).filter(Boolean)});
 return (audience as {mode:'directed';ids:string[]}).ids;
}
export interface BroadcastRow {id:string;instance_id:string;bot_id:string;actor_id:string;epoch:string;text:string;photo:string|null;scope_json:string;state:'draft'|'confirmed'|'sending'|'done'|'canceled'|'interrupted';total:number;snapshot_ready:number;created_at:number;started_at:number|null;completed_at:number|null;next_at:number;expected:number}
interface Recipient {broadcast_id:string;user_id:string;state:'pending'|'success'|'failed'|'unknown'|'skipped';part_index:number;success_parts:number;reason:string|null;code:number|null}
export interface BroadcastStats {id:string;state:BroadcastRow['state'];total:number;expected:number;success:number;failed:number;unsent:number;unknown:number;partial:number;pending:number}
export interface BroadcastGuard {id:string;userId:string;part:number}
export const BROADCAST_INTERVAL=1100;
const authorized=(e:Env,actor:string)=>e.ADMIN_IDS.split(/[\s,]+/).includes(actor);

/** The preview and recipient delivery use exactly the same unadorned body.
 * Longer text is a separate part because Telegram photo captions allow 1024
 * characters, while a plain message can preserve the full 3500-character text. */
export function bodyParts(content:BroadcastContent):{method:'sendPhoto'|'sendMessage';params:Params}[] {
 const text=content.text??'',photo=content.photo;
 if(photo&&text.length<=1024)return [{method:'sendPhoto',params:{photo,...(text?{caption:text}:{})}}];
 return [...(photo?[{method:'sendPhoto' as const,params:{photo}}]:[]),...(text?[{method:'sendMessage' as const,params:{text}}]:[])];
}
export const broadcastParts=(text:string,photo:string|null)=>bodyParts({text,photo});
/** Only isolated test instances honor this private allowlist; absent scope is closed. */
export function broadcastTestRecipients(e:Env):string[]|null {
 if(e.INSTANCE_ID!=='test-a'&&e.INSTANCE_ID!=='test-b')return null;
 const entries=(e.BROADCAST_TEST_RECIPIENT_IDS??'').split(/[\s,]+/).filter(Boolean);
 if(!entries.length||entries.length>20||new Set(entries).size!==entries.length||entries.some(id=>!/^\d{1,16}$/.test(id)||!Number.isSafeInteger(Number(id))||Number(id)<=0||String(Number(id))!==id))throw new Error('broadcast_test_scope_missing');
 return [...new Set(entries)];
}
export async function getBroadcast(e:Env,id:string):Promise<BroadcastRow|null> {
 const row=await one<BroadcastRow>(e,'SELECT *,total expected FROM broadcasts WHERE id=? AND instance_id=? AND bot_id=?',id,e.INSTANCE_ID,e.EXPECTED_BOT_ID);
 return row;
}
export async function prepareBroadcast(e:Env,input:BroadcastContent&{id:string;actor:string;audience?:BroadcastAudience}):Promise<BroadcastRow> {
 const text=input.text??'',photo=input.photo??null,time=now(),scope=broadcastTestRecipients(e),scopeJSON=scope?JSON.stringify(scope):null;
 const audience=normalizeBroadcastAudience(input.audience),audienceJSON=JSON.stringify(audience),directedJSON=audience.mode==='directed'?JSON.stringify(audience.ids):null;
 if(!authorized(e,input.actor)||!input.id||input.id.length>160||(!text&&!photo)||text.length>3500||photo!==null&&(typeof photo!=='string'||!photo||photo.length>1000))throw new Error('invalid_broadcast');
 await e.DB.batch([
  stmt(e,"INSERT OR IGNORE INTO broadcasts(id,instance_id,bot_id,actor_id,epoch,text,photo,scope_json,created_at) VALUES(?,?,?,?,?,?,?,?,?)",input.id,e.INSTANCE_ID,e.EXPECTED_BOT_ID,input.actor,e.RECOVERY_EPOCH,text,photo,audienceJSON,time),
  stmt(e,"INSERT OR IGNORE INTO broadcast_recipients(broadcast_id,user_id,updated_at) SELECT b.id,u.tgid,? FROM broadcasts b CROSS JOIN users u WHERE b.id=? AND b.instance_id=? AND b.bot_id=? AND b.actor_id=? AND b.epoch=? AND b.text=? AND b.photo IS ? AND b.scope_json=? AND b.state='draft' AND b.snapshot_ready=0 AND u.blocked=0 AND u.thread_id IS NOT NULL AND (? IS NULL OR u.tgid IN (SELECT value FROM json_each(?))) AND (? IS NULL OR u.tgid IN (SELECT value FROM json_each(?)))",time,input.id,e.INSTANCE_ID,e.EXPECTED_BOT_ID,input.actor,e.RECOVERY_EPOCH,text,photo,audienceJSON,scopeJSON,scopeJSON,directedJSON,directedJSON),
  stmt(e,"UPDATE broadcasts SET total=(SELECT COUNT(*) FROM broadcast_recipients WHERE broadcast_id=broadcasts.id),snapshot_ready=1 WHERE id=? AND instance_id=? AND bot_id=? AND actor_id=? AND epoch=? AND text=? AND photo IS ? AND scope_json=? AND state='draft' AND snapshot_ready=0",input.id,e.INSTANCE_ID,e.EXPECTED_BOT_ID,input.actor,e.RECOVERY_EPOCH,text,photo,audienceJSON)
 ]);
 const row=await getBroadcast(e,input.id);
 if(!row||row.actor_id!==input.actor||row.epoch!==e.RECOVERY_EPOCH||row.text!==text||row.photo!==photo||row.scope_json!==audienceJSON||!row.snapshot_ready)throw new Error('broadcast_snapshot_mismatch');
 return row;
}
export async function cancelBroadcast(e:Env,id:string,actor:string):Promise<void> {
 if(!authorized(e,actor))return;
 await run(e,"UPDATE broadcasts SET state='canceled',completed_at=? WHERE id=? AND instance_id=? AND actor_id=? AND epoch=? AND state IN ('draft','confirmed')",now(),id,e.INSTANCE_ID,actor,e.RECOVERY_EPOCH);
}
/** The UI alone may persist 'confirmed', in the same transaction as its second
 * independent, actor/revision-bound confirmation. This method cannot confirm. */
export async function launchBroadcast(e:Env,id:string):Promise<void> {
 let validScope=true;try{broadcastTestRecipients(e);}catch{validScope=false;}
 const row=await getBroadcast(e,id);
 if(!validScope){if(row?.epoch===e.RECOVERY_EPOCH)await run(e,"UPDATE broadcasts SET state='interrupted',completed_at=? WHERE id=? AND instance_id=? AND epoch=? AND state='confirmed'",now(),id,e.INSTANCE_ID,e.RECOVERY_EPOCH);return;}
 if(!row||row.epoch!==e.RECOVERY_EPOCH||!authorized(e,row.actor_id))return;
 const time=now(),key=`broadcast:${id}`;
 await e.DB.batch([
  stmt(e,"UPDATE broadcasts SET state='sending',started_at=COALESCE(started_at,?),next_at=? WHERE id=? AND instance_id=? AND bot_id=? AND epoch=? AND state='confirmed' AND snapshot_ready=1",time,time,id,e.INSTANCE_ID,e.EXPECTED_BOT_ID,e.RECOVERY_EPOCH),
  stmt(e,"INSERT OR IGNORE INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) SELECT ?,?,'broadcast',json_object('broadcastId',id),'pending',next_at,epoch,? FROM broadcasts WHERE id=? AND instance_id=? AND bot_id=? AND epoch=? AND state='sending'",key,key,time,id,e.INSTANCE_ID,e.EXPECTED_BOT_ID,e.RECOVERY_EPOCH)
 ]);
 await wakeWork(e,key);
}
export const startBroadcast=launchBroadcast;
export async function broadcastStats(e:Env,id:string):Promise<BroadcastStats|null> {
 const row=await getBroadcast(e,id);if(!row)return null;
 const counts=await one<{success:number;failed:number;unsent:number;unknown:number;partial:number;pending:number}>(e,`SELECT COALESCE(SUM(state='success'),0) success,COALESCE(SUM(state='failed'),0) failed,COALESCE(SUM(state IN ('pending','skipped')),0) unsent,COALESCE(SUM(state='unknown'),0) unknown,COALESCE(SUM(success_parts>0 AND state!='success'),0) partial,COALESCE(SUM(state='pending'),0) pending FROM broadcast_recipients WHERE broadcast_id=?`,id);
 return {id:row.id,state:row.state,total:row.total,expected:row.total,...counts!};
}
export const broadcastStatus=broadcastStats;
export async function listBroadcasts(e:Env,actor:string,limit=5):Promise<BroadcastRow[]> {
 if(!authorized(e,actor))return [];
 return all(e,'SELECT *,total expected FROM broadcasts WHERE instance_id=? AND bot_id=? AND actor_id=? ORDER BY created_at DESC,id DESC LIMIT ?',e.INSTANCE_ID,e.EXPECTED_BOT_ID,actor,Math.min(10,Math.max(1,limit)));
}
async function privatePart(e:Env,b:BroadcastRow,r:Recipient,part:number,content:{method:string;params:Params}):Promise<MessageRow> {
 const source=`broadcast:${b.id}:${r.user_id}`;
 await run(e,"INSERT OR IGNORE INTO messages(user_id,direction,source_chat,source_id,business,received_at,payload,epoch) VALUES(?,'broadcast',?,?,0,?,?,?)",r.user_id,source,part,now(),JSON.stringify({broadcastId:b.id,partIndex:part,method:content.method,...content.params}),b.epoch);
 return (await one<MessageRow>(e,'SELECT * FROM messages WHERE source_chat=? AND source_id=?',source,part))!;
}
async function savePart(e:Env,b:BroadcastRow,r:Recipient,m:MessageRow,outcome:Outcome,partCount:number,jobId:number) {
 const success=outcome.status==='success',id=success?(outcome.result as {message_id?:unknown})?.message_id:null;
 // Missing message identifiers cannot establish a confirmed private message.
 const normalized:Outcome=success&&(!Number.isSafeInteger(id)||Number(id)<=0)?{status:'unknown',reason:'Telegram 成功响应缺少消息标识'}:outcome;
 const sent=normalized.status==='success';
 const state=sent?(r.part_index+1===partCount?'success':'pending'):normalized.status==='failed'&&normalized.code===409?'skipped':normalized.status;
 await e.DB.batch([
  stmt(e,"UPDATE jobs SET attempts=0 WHERE id=? AND kind='broadcast' AND epoch=?",jobId,e.RECOVERY_EPOCH),
  stmt(e,'UPDATE messages SET delivery=?,private_id=COALESCE(?,private_id) WHERE id=?',sent?'success':normalized.status,sent?id:null,m.id),
  stmt(e,"UPDATE broadcast_recipients SET state=?,part_index=part_index+?,success_parts=success_parts+?,reason=?,code=?,updated_at=? WHERE broadcast_id=? AND user_id=? AND state='pending' AND part_index=?",state,sent?1:0,sent?1:0,sent?null:normalized.reason,sent?null:normalized.code??null,now(),b.id,r.user_id,r.part_index)
 ]);
 return {state,sent};
}
export function broadcastSummary(stats:BroadcastStats):string {
 return `预计接收：${stats.total}\n已确认成功：${stats.success}\n明确失败：${stats.failed}\n未发送：${stats.unsent}\n结果未知：${stats.unknown}${stats.partial?`\n部分内容已确认发送：${stats.partial}（已计入失败、未知或未发送）`:''}`;
}
async function finish(e:Env,b:BroadcastRow) {
 await run(e,"UPDATE broadcasts SET state='done',completed_at=COALESCE(completed_at,?) WHERE id=? AND state='sending' AND NOT EXISTS(SELECT 1 FROM broadcast_recipients WHERE broadcast_id=? AND state='pending')",now(),b.id,b.id);
 const stats=await broadcastStats(e,b.id);
 if(stats?.state==='done') {
  let audienceLabel='范围异常';
  try {const audience=normalizeBroadcastAudience(JSON.parse(b.scope_json));audienceLabel=audience.mode==='all'?'全体用户':`定向（指定 ${audience.ids.length} 个 ID）`;}catch{/* A corrupt range cannot become all-users or prevent the result report. */}
  await notify(e,`broadcast-report:${b.id}`,null,`广播 ${b.id.slice(0,8)} 已结束\n目标机器人：${b.instance_id}\n范围：${audienceLabel}\n${broadcastSummary(stats)}\n成功仅表示 Telegram API 已确认，不代表已读。未知结果保留，不自动重发。`);
 }
}
/** One recipient at a time, with a persistent interval between recipients.
 * RetryLater releases the dedicated broadcast coordinator instead of occupying
 * a chat/admin lane. Every part uses a stable external-operation journal key. */
export async function handleBroadcast(e:Env,j:Job):Promise<void> {
 const id=(JSON.parse(j.payload) as {broadcastId?:string}).broadcastId;
 if(!id||j.epoch!==e.RECOVERY_EPOCH||j.user_key!==`broadcast:${id}`)return;
 let b=await getBroadcast(e,id);
 if(!b||b.epoch!==e.RECOVERY_EPOCH)return;
 if(b.state==='done'){await finish(e,b);return;}
 if(b.state!=='sending')return;
 if(!authorized(e,b.actor_id)) {
  await run(e,"UPDATE broadcasts SET state='interrupted',completed_at=? WHERE id=? AND state='sending'",now(),id);return;
 }
 if(b.next_at>now()){await run(e,"UPDATE jobs SET attempts=0 WHERE id=? AND kind='broadcast' AND epoch=?",j.id,e.RECOVERY_EPOCH);throw new RetryLater(b.next_at);}
 let recipient=await one<Recipient>(e,"SELECT * FROM broadcast_recipients WHERE broadcast_id=? AND state='pending' ORDER BY user_id LIMIT 1",id);
 if(!recipient){await finish(e,b);return;}
 const parts=bodyParts(b);
 for(let part=recipient.part_index;part<parts.length;part++) {
  const m=await privatePart(e,b,recipient,part,parts[part]);
  // call() first recovers any terminal outcome before re-evaluating guards;
  // a later ban must not hide a prior confirmed/unknown external operation.
  let outcome:Outcome;
  try{outcome=await call(e,`broadcast:${id}:${recipient.user_id}:${part}`,parts[part].method,{chat_id:recipient.user_id,...parts[part].params},{messageIds:[m.id],broadcast:{id,userId:recipient.user_id,part}});}
  catch(error){if(error instanceof RetryLater)await run(e,"UPDATE jobs SET attempts=0 WHERE id=? AND kind='broadcast' AND epoch=?",j.id,e.RECOVERY_EPOCH);throw error;}
  const saved=await savePart(e,b,recipient,m,outcome,parts.length,j.id);
  if(!saved.sent)break;
  recipient={...recipient,part_index:part+1,success_parts:recipient.success_parts+1};
 }
 await run(e,"UPDATE broadcasts SET next_at=? WHERE id=? AND state='sending'",now()+BROADCAST_INTERVAL,id);
 if(await one(e,"SELECT user_id FROM broadcast_recipients WHERE broadcast_id=? AND state='pending' LIMIT 1",id))throw new RetryLater(now()+BROADCAST_INTERVAL);
 b=(await getBroadcast(e,id))!;await finish(e,b);
}
/** A same-epoch deployment resumes the existing job. A database restoration
 * cannot prove past external effects; seal its uncompleted recipients unknown. */
export async function recoverBroadcasts(e:Env):Promise<void> {
 await e.DB.batch([
  stmt(e,"UPDATE broadcast_recipients SET state='unknown',reason='恢复代次已变化；无法证明旧发送结果，禁止重发',updated_at=? WHERE state='pending' AND broadcast_id IN (SELECT id FROM broadcasts WHERE instance_id=? AND epoch!=? AND state='sending')",now(),e.INSTANCE_ID,e.RECOVERY_EPOCH),
  stmt(e,"UPDATE broadcasts SET state='interrupted',completed_at=? WHERE instance_id=? AND epoch!=? AND state IN ('confirmed','sending')",now(),e.INSTANCE_ID,e.RECOVERY_EPOCH)
 ]);
 const rows=await all<{id:string}>(e,"SELECT id FROM broadcasts WHERE instance_id=? AND bot_id=? AND epoch=? AND state='confirmed' ORDER BY created_at LIMIT 10",e.INSTANCE_ID,e.EXPECTED_BOT_ID,e.RECOVERY_EPOCH);
 for(const row of rows)await launchBroadcast(e,row.id);
}
