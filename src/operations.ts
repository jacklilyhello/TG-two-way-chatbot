import type {Env, Params} from './types';
import {all,one,run,now} from './db';
import type {ReceiptGuard} from './receipts';
import type {DeliveryRetryGuard} from './delivery-retry';
import type {BroadcastGuard} from './broadcast-guard';
export type Outcome<T = any> = {status: 'success'; result: T; confirmedAt?: number} | {status: 'failed' | 'unknown'; reason: string; code?: number};
export class RetryLater extends Error { constructor(public due: number) {super('retry_later');} }
interface Ledger {method: string;phase: string;result: string | null;reason: string | null;code: number | null;updated_at:number}
export interface IssueGuard {priority?:'moderation-card';messageIds?:number[];threadVersion?:{userId:string;version:number;threadId?:number;moderationVersion?:number};unblockedUser?:string;receipt?:ReceiptGuard;deliveryRetry?:DeliveryRetryGuard;broadcast?:BroadcastGuard}
interface Timing {transportMs:number;botWaitMs:number;apiMs:number|null}
function responseTiming(response:Response,started:number):Timing {
 const number=(name:string)=>{const text=response.headers.get(name);const value=Number(text);return text!==null&&Number.isFinite(value)&&value>=0?Math.round(value):null;};
 return {transportMs:Math.max(0,now()-started),botWaitMs:number('X-Bot-Wait-Ms')??0,apiMs:number('X-Bot-Api-Ms')};
}
async function persistOutcome(e:Env,opKey:string,method:string,outcome:Outcome,issuedOnly=false,timing?:Timing) {
 await e.DB.batch([
  e.DB.prepare(`UPDATE operations SET phase=?,result=?,reason=?,code=?,updated_at=?,call_finished_at=?,transport_ms=transport_ms+?,bot_wait_ms=bot_wait_ms+?,api_ms=COALESCE(?,api_ms) WHERE op_key=?${issuedOnly?" AND phase='issued'":''}`).bind(outcome.status,outcome.status==='success'?JSON.stringify(outcome.result):null,outcome.status==='success'?null:outcome.reason,outcome.status==='success'?null:outcome.code??null,outcome.status==='success'&&method==='setMessageReaction'?outcome.confirmedAt??now():now(),now(),timing?.transportMs??0,timing?.botWaitMs??0,timing?.apiMs??null,opKey),
  e.DB.prepare('INSERT INTO delivery_events(op_key,method,status,code,created_at) SELECT ?,?,?,?,? WHERE EXISTS(SELECT 1 FROM operations WHERE op_key=? AND phase=?) AND NOT EXISTS (SELECT 1 FROM delivery_events WHERE op_key=? AND status=?)').bind(opKey,method,outcome.status,outcome.status==='success'?null:outcome.code??null,now(),opKey,outcome.status,opKey,outcome.status)
 ]);
}
/** Reconcile only response journals while paused; no business jobs or external
 * requests are executed, including for rows from a previous recovery epoch. */
export async function reconcileIssued(e:Env):Promise<number> {
 if(e.PAUSED!=='true'&&!await one(e,"SELECT key FROM settings WHERE key='paused' AND value='1'"))return 0;
 const rows=await all<{op_key:string;method:string}>(e,"SELECT op_key,method FROM operations WHERE phase='issued' ORDER BY updated_at,op_key LIMIT 40");
 if(!rows.length)return 0;
 const stub=e.BOT.get(e.BOT.idFromName(e.INSTANCE_ID));
 for(const row of rows) {
  const started=now();
  const response=await stub.fetch('https://coordinator/journal',{method:'POST',body:JSON.stringify({key:row.op_key})});
  if(!response.ok)throw new Error('journal_reconciliation_unavailable');
  const outcome=await response.json<Outcome>();
  if(!['success','failed','unknown'].includes(outcome.status))throw new Error('journal_reconciliation_invalid');
  await persistOutcome(e,row.op_key,row.method,outcome,true,responseTiming(response,started));
 }
 return rows.length;
}
export async function call<T = any>(e: Env,key: string,method: string,params: Params,guard?:IssueGuard): Promise<Outcome<T>> {
 const entered=now();
 const opKey = `${e.RECOVERY_EPOCH}:${key}`;
 // These statements share one D1 transaction and round trip. Read the prior
 // result before inserting; terminal outcomes still return before pause/issue.
 const [prior,,pause]=await e.DB.batch([
  e.DB.prepare('SELECT * FROM operations WHERE op_key=?').bind(opKey),
  e.DB.prepare("INSERT OR IGNORE INTO operations(op_key,method,phase,created_at,updated_at) VALUES(?,?,'prepared',?,?)").bind(opKey,method,entered,entered),
  e.DB.prepare("SELECT key FROM settings WHERE key='paused' AND value='1'")
 ]);
 const old=prior.results[0] as unknown as Ledger|undefined;
 if(old?.phase==='success') return {status:'success',result:JSON.parse(old.result!),...(method==='setMessageReaction'?{confirmedAt:old.updated_at}:{})};
 const retryRead=old?.phase==='unknown'&&old.method===method&&method.startsWith('get');
 if(old?.phase==='failed'||(old?.phase==='unknown'&&!retryRead)) return {status:old.phase,reason:old.reason||'发生未知情况',code:old.code??undefined};
 if(e.PAUSED==='true'||pause.results.length) throw new RetryLater(now()+60000);
 const started=now();
 await run(e,"UPDATE operations SET phase='issued',updated_at=?,call_started_at=COALESCE(call_started_at,?),call_attempts=call_attempts+1,prepare_ms=prepare_ms+? WHERE op_key=?",started,started,Math.max(0,started-entered),opKey);
 // The Bot DO keeps an independent response journal across D1 write failure.
 const stub=e.BOT.get(e.BOT.idFromName(e.INSTANCE_ID));
 const rpcStarted=now();
 let response: Response;
 try {response=await stub.fetch('https://coordinator/execute',{method:'POST',body:JSON.stringify({key:opKey,method,params,guard})});}
 catch {throw new RetryLater(now()+5000);}
 if(response.status===429) {
  const b=await response.json<{due:number}>(),timing=responseTiming(response,rpcStarted);
  await run(e,"UPDATE operations SET phase='prepared',updated_at=?,transport_ms=transport_ms+?,bot_wait_ms=bot_wait_ms+?,api_ms=COALESCE(?,api_ms),rate_wait_ms=rate_wait_ms+? WHERE op_key=?",now(),timing.transportMs,timing.botWaitMs,timing.apiMs,Math.max(0,b.due-now()),opKey);
  throw new RetryLater(b.due);
 }
 if(!response.ok) throw new RetryLater(now()+5000);
 const outcome=await response.json<Outcome<T>>();
 await persistOutcome(e,opKey,method,outcome,false,responseTiming(response,rpcStarted));
 return outcome;
}
export async function notify(e: Env,key: string,thread: number | null,text: string,extra: Params = {}): Promise<Outcome> {
 return call(e,key,'sendMessage',{chat_id:e.ADMIN_CHAT_ID,...(thread?{message_thread_id:thread}:{}),text,...extra});
}
