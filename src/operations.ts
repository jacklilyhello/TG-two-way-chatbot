import type {Env, Params} from './types';
import {all,one,run,now} from './db';
export type Outcome<T = any> = {status: 'success'; result: T} | {status: 'failed' | 'unknown'; reason: string; code?: number};
export class RetryLater extends Error { constructor(public due: number) {super('retry_later');} }
interface Ledger {method: string;phase: string;result: string | null;reason: string | null;code: number | null}
export interface IssueGuard {messageIds?:number[];threadVersion?:{userId:string;version:number;threadId?:number}}
async function persistOutcome(e:Env,opKey:string,method:string,outcome:Outcome,issuedOnly=false) {
 await e.DB.batch([
  e.DB.prepare(`UPDATE operations SET phase=?,result=?,reason=?,code=?,updated_at=? WHERE op_key=?${issuedOnly?" AND phase='issued'":''}`).bind(outcome.status,outcome.status==='success'?JSON.stringify(outcome.result):null,outcome.status==='success'?null:outcome.reason,outcome.status==='success'?null:outcome.code??null,now(),opKey),
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
  const response=await stub.fetch('https://coordinator/journal',{method:'POST',body:JSON.stringify({key:row.op_key})});
  if(!response.ok)throw new Error('journal_reconciliation_unavailable');
  const outcome=await response.json<Outcome>();
  if(!['success','failed','unknown'].includes(outcome.status))throw new Error('journal_reconciliation_invalid');
  await persistOutcome(e,row.op_key,row.method,outcome,true);
 }
 return rows.length;
}
export async function call<T = any>(e: Env,key: string,method: string,params: Params,guard?:IssueGuard): Promise<Outcome<T>> {
 const opKey = `${e.RECOVERY_EPOCH}:${key}`;
 const old=await one<Ledger>(e,'SELECT * FROM operations WHERE op_key=?',opKey);
 if(old?.phase==='success') return {status:'success',result:JSON.parse(old.result!)};
 const retryRead=old?.phase==='unknown'&&old.method===method&&method.startsWith('get');
 if(old?.phase==='failed'||(old?.phase==='unknown'&&!retryRead)) return {status:old.phase,reason:old.reason||'发生未知情况',code:old.code??undefined};
 await run(e,"INSERT OR IGNORE INTO operations(op_key,method,phase,created_at,updated_at) VALUES(?,?,'prepared',?,?)",opKey,method,now(),now());
 if(e.PAUSED==='true'||await one(e,"SELECT key FROM settings WHERE key='paused' AND value='1'")) throw new RetryLater(now()+60000);
 await run(e,"UPDATE operations SET phase='issued',updated_at=? WHERE op_key=?",now(),opKey);
 // The Bot DO keeps an independent response journal across D1 write failure.
 const stub=e.BOT.get(e.BOT.idFromName(e.INSTANCE_ID));
 let response: Response;
 try {response=await stub.fetch('https://coordinator/execute',{method:'POST',body:JSON.stringify({key:opKey,method,params,guard})});}
 catch {throw new RetryLater(now()+5000);}
 if(response.status===429) {const b=await response.json<{due:number}>(); await run(e,"UPDATE operations SET phase='prepared',updated_at=? WHERE op_key=?",now(),opKey); throw new RetryLater(b.due);}
 if(!response.ok) throw new RetryLater(now()+5000);
 const outcome=await response.json<Outcome<T>>();
 await persistOutcome(e,opKey,method,outcome);
 return outcome;
}
export async function notify(e: Env,key: string,thread: number | null,text: string,extra: Params = {}): Promise<Outcome> {
 return call(e,key,'sendMessage',{chat_id:e.ADMIN_CHAT_ID,...(thread?{message_thread_id:thread}:{}),text,...extra});
}
