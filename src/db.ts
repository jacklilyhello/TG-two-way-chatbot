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
export async function audit(e: Env, event: string, detail: Params = {}) {
 await run(e,'INSERT INTO audit_logs(event,detail,created_at) VALUES(?,?,?)',event,JSON.stringify(detail),now());
}
export async function nextJob(e: Env, key: string) {return one<Job>(e, "SELECT * FROM jobs WHERE user_key=? AND state IN ('pending','retry','running') AND epoch=? ORDER BY id LIMIT 1",key,e.RECOVERY_EPOCH);}
