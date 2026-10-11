import type {Env, Job, UserRow, Params} from './types';
export const now = () => Date.now();
export const stmt = (e: Env, sql: string, ...args: unknown[]) => e.DB.prepare(sql).bind(...args);
export async function one<T>(e: Env, sql: string, ...args: unknown[]): Promise<T | null> { return stmt(e, sql, ...args).first<T>(); }
export async function all<T>(e: Env, sql: string, ...args: unknown[]): Promise<T[]> { return (await stmt(e, sql, ...args).all<T>()).results; }
export async function run(e: Env, sql: string, ...args: unknown[]) {return stmt(e, sql, ...args).run();}
export async function setting(e: Env, key: string, fallback = '') {return (await one<{value: string}>(e, 'SELECT value FROM settings WHERE key=?', key))?.value ?? fallback;}
export async function setSetting(e: Env, key: string, value: string) {await run(e, 'INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',key,value);}
export async function user(e: Env, id: string) {return one<UserRow>(e,'SELECT * FROM users WHERE tgid=?',id);}
export async function enqueue(e: Env, key: string, kind: string, payload: Params, dedupe: string, delay = 0) {
 await run(e, 'INSERT OR IGNORE INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES(?,?,?,?,?,?,?,?)', dedupe,key,kind,JSON.stringify(payload),'pending',now()+delay,e.RECOVERY_EPOCH,now());
}
export const bootstrapKey = (e: Env) => `bootstrap:${e.RECOVERY_EPOCH}:${e.VERSION}`;
/** A restored snapshot cannot prove which old logs were sent after its bookmark,
 * even if no operation row exists yet. Seal the existing backlog for manual
 * reconciliation before inserting any audit from the new recovery epoch. The
 * initial same-epoch upgrade preserves its backlog; legacy cross-epoch history
 * without a marker must use the conservative boundary instead. */
export function logEpochStatements(e: Env): D1PreparedStatement[] {
 return [
  stmt(e,"UPDATE audit_logs SET reported=2 WHERE reported=0 AND (EXISTS(SELECT 1 FROM settings WHERE key='log_outbox_epoch' AND value!=?) OR (NOT EXISTS(SELECT 1 FROM settings WHERE key='log_outbox_epoch') AND (EXISTS(SELECT 1 FROM jobs WHERE epoch!=?) OR EXISTS(SELECT 1 FROM audit_logs WHERE event='部署版本' AND CASE WHEN json_valid(detail) THEN json_extract(detail,'$.epoch') END!=?))))",e.RECOVERY_EPOCH,e.RECOVERY_EPOCH,e.RECOVERY_EPOCH),
  stmt(e,"INSERT INTO settings(key,value) VALUES('log_outbox_epoch',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE settings.value!=excluded.value",e.RECOVERY_EPOCH)
 ];
}
/** Append to the transaction that creates an audit. One outstanding batch per
 * epoch, and at most one new batch per minute, including explicit failures. */
export function logOutboxStatements(e: Env, time = now()): D1PreparedStatement[] {
 const minute = Math.floor(time/60000);
 return [
  stmt(e,"INSERT OR IGNORE INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) SELECT ?,'system','bootstrap','{}','pending',?,?,? WHERE EXISTS(SELECT 1 FROM audit_logs WHERE reported=0)",bootstrapKey(e),time,e.RECOVERY_EPOCH,time),
  stmt(e,"INSERT OR IGNORE INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) SELECT 'logs:'||?||':'||slot,'system','logs','{}','pending',MAX(?,slot*60000),?,? FROM (SELECT MAX(?,COALESCE(MAX(CAST(MAX(due_at,created_at)/60000 AS INTEGER))+1,?)) slot FROM jobs WHERE user_key='system' AND kind='logs' AND epoch=?) WHERE EXISTS(SELECT 1 FROM audit_logs WHERE reported=0) AND NOT EXISTS(SELECT 1 FROM jobs WHERE user_key='system' AND kind='logs' AND epoch=? AND state IN ('pending','retry','running','attention'))",e.RECOVERY_EPOCH,time,e.RECOVERY_EPOCH,time,minute,minute,e.RECOVERY_EPOCH,e.RECOVERY_EPOCH)
 ];
}
/** Notifications are hints only. Failure is deliberately not audited here: the
 * committed outbox and the other wake path remain, without an alert loop. */
export async function wakeLogs(e: Env) {
 await Promise.allSettled([
  Promise.resolve().then(()=>e.JOBS.send({key:'system'})),
  Promise.resolve().then(async()=>{
   const response=await e.CONVERSATIONS.get(e.CONVERSATIONS.idFromName('system')).fetch('https://conversation/logs/arm',{method:'POST',body:JSON.stringify({epoch:e.RECOVERY_EPOCH})});
   await response.body?.cancel();
  })
 ]);
}
export async function scheduleLogs(e: Env) {
 const results=await e.DB.batch([...logEpochStatements(e),...logOutboxStatements(e)]);
 return results.some(result=>result.meta.changes>0);
}
export async function audit(e: Env, event: string, detail: Params = {}) {
 const time=now();
 const epochGate=logEpochStatements(e);
 const results=await e.DB.batch([...epochGate,stmt(e,'INSERT INTO audit_logs(event,detail,created_at) VALUES(?,?,?)',event,JSON.stringify(detail),time),...logOutboxStatements(e,time)]);
 if(results.slice(epochGate.length+1).some(result=>result.meta.changes>0))await wakeLogs(e);
}
export async function nextJob(e: Env, key: string) {return one<Job>(e, "SELECT * FROM jobs WHERE user_key=? AND state IN ('pending','retry','running') AND epoch=? ORDER BY CASE WHEN user_key='system' AND kind='bootstrap' THEN 0 ELSE 1 END,id LIMIT 1",key,e.RECOVERY_EPOCH);}
