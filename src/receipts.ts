import type {Env, Job, MessageRow} from './types';
import {all, audit, now, one, run, setSetting, setting, stmt, wakeWork} from './db';
import {call, RetryLater} from './operations';
import {latestDelivery} from './delivery-retry';

export const SUCCESS_RECEIPT_MS = 10000;
export const FAILURE_EMOJI = '😢';
type Emoji = '👍' | '👎' | typeof FAILURE_EMOJI | null;
interface Receipt {
  admin_id: number; row_id: number; version: number; signature: string; emoji: Emoji;
  phase: string; shown_at: number | null; clear_after: number | null; cleared_at: number | null;
  epoch: string;
}
export interface ReceiptGuard {adminId: number; version: number; signature: string; emoji: Emoji; clear: boolean}
interface ReceiptJob {adminId: number; version: number; action: 'apply' | 'clear'; attempt: number}

async function currentState(e: Env, rowId: number) {
  const row = await one<MessageRow>(e, 'SELECT * FROM messages WHERE id=?', rowId);
  if (!row) return null;
  const rows = row.media_group ? await all<MessageRow>(e, 'SELECT * FROM messages WHERE user_id=? AND direction=? AND source_chat=? AND media_group=? ORDER BY source_id,id', row.user_id, row.direction, row.source_chat, row.media_group) : [row];
  const anchor = rows.find(r => r.admin_id !== null);
  if (!anchor?.admin_id) return null;
  const deleted = rows.every(r => r.deletion === 'deleted');
  const failed = row.direction === 'out' && rows.some(r => r.delivery === 'failed');
  const delivered = row.direction === 'out' && rows.every(r => r.delivery === 'success' && !r.tombstone && r.deletion === 'none');
  const emoji: Emoji = deleted ? '👎' : failed ? FAILURE_EMOJI : delivered ? '👍' : null;
  // Edits have a separate ledger; only delivery/deletion changes supersede a
  // receipt. Including every album member prevents partial-group success.
  const signature = JSON.stringify(rows.map(r => [r.id,r.admin_id,r.delivery,r.deletion,r.tombstone,r.epoch]));
  return {anchor, emoji, signature};
}

function jobStatement(e: Env, receipt: Receipt, action: ReceiptJob['action'], attempt: number, due: number) {
  const payload: ReceiptJob = {adminId: receipt.admin_id, version: receipt.version, action, attempt};
  return stmt(e, "INSERT OR IGNORE INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES(?,?,'reaction',?,'pending',?,?,?)", `receipt:${receipt.admin_id}:${receipt.version}:${action}:${attempt}`, `receipt:${receipt.admin_id}`, JSON.stringify(payload), due, e.RECOVERY_EPOCH, now());
}

/** Persist marker intent separately from content delivery. Each original has
 * its own lane: a future ten-second cleanup cannot hold up any business FIFO. */
export async function syncReceipt(e: Env, rowId: number, clearUntracked = false, waitUntil?:(promise:Promise<unknown>)=>void): Promise<void> {
  const state = await currentState(e, rowId);
  if (!state) return;
  const previous = await one<Receipt>(e, 'SELECT * FROM receipts WHERE admin_id=?', state.anchor.admin_id);
  if (!previous && !state.emoji && !clearUntracked) return;
  await e.DB.batch([
    stmt(e, `INSERT INTO receipts(admin_id,row_id,signature,emoji,epoch,updated_at) VALUES(?,?,?,?,?,?)
      ON CONFLICT(admin_id) DO UPDATE SET row_id=excluded.row_id,version=receipts.version+1,signature=excluded.signature,emoji=excluded.emoji,phase='pending',shown_at=NULL,clear_after=NULL,cleared_at=NULL,last_error=NULL,epoch=excluded.epoch,updated_at=excluded.updated_at
      WHERE receipts.signature!=excluded.signature OR receipts.emoji IS NOT excluded.emoji OR receipts.epoch!=excluded.epoch`, state.anchor.admin_id,state.anchor.id,state.signature,state.emoji,e.RECOVERY_EPOCH,now()),
    // Retire obsolete delayed cleanups before the replacement job joins this
    // lane, otherwise its FIFO would delay a new failure/deletion marker.
    stmt(e, "UPDATE jobs SET state='done' WHERE user_key=? AND kind='reaction' AND state IN ('pending','retry','running') AND (epoch!=? OR json_extract(payload,'$.version')!=(SELECT version FROM receipts WHERE admin_id=?))", `receipt:${state.anchor.admin_id}`,e.RECOVERY_EPOCH,state.anchor.admin_id),
    stmt(e, "INSERT OR IGNORE INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) SELECT 'receipt:'||admin_id||':'||version||':apply:1','receipt:'||admin_id,'reaction',json_object('adminId',admin_id,'version',version,'action','apply','attempt',1),'pending',?,?,? FROM receipts WHERE admin_id=? AND phase='pending'", now(),e.RECOVERY_EPOCH,now(),state.anchor.admin_id),
  ]);
  // Only detach hints after durable intent is committed. Business coordinators
  // can keep them alive without waiting; Queue/Alarm/Cron retain recovery.
  const wake = wakeWork(e, `receipt:${state.anchor.admin_id}`);
  if (waitUntil) waitUntil(wake); else await wake;
}

async function confirmedDelivery(e: Env, row: MessageRow): Promise<boolean> {
  const {key,method,members} = await latestDelivery(e,row);
  const ledger = await one<{method:string;phase:string;result:string|null;reason:string|null}>(e,'SELECT method,phase,result,reason FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:${key}`);
  if (!ledger || ledger.method !== method || ledger.phase !== row.delivery) return false;
  if (row.delivery === 'failed') return ledger.reason !== 'cancelled_by_state';
  if (row.delivery !== 'success' || !row.private_id || !ledger.result) return false;
  try {
    const result = JSON.parse(ledger.result);
    if (!members) return result?.message_id === row.private_id;
    return Array.isArray(result) && result.length === members.length && result[members.indexOf(row.id)]?.message_id === row.private_id;
  } catch {return false;}
}

/** Upgrade recovery is intentionally narrow: a current-epoch unfinished old
 * marker plus confirmed content ledger, never every historical success. Old
 * epochs, deletion targets and rows already under this receipt state machine
 * are excluded. No Telegram content operation is called by this scan. */
export async function recoverPreparedReceipts(e: Env): Promise<number> {
  const prefix = `${e.RECOVERY_EPOCH}:`;
  const savedCursor = await setting(e,'receipt_recovery_cursor');
  const cursor = savedCursor.startsWith(prefix) ? savedCursor : '';
  const candidates = await all<{op_key:string}>(e,"SELECT op_key FROM operations WHERE method='setMessageReaction' AND phase='prepared' AND substr(op_key,1,?)=? AND (substr(op_key,? ,14)='sent-reaction:' OR substr(op_key,?,6)='album:') AND op_key>? ORDER BY op_key LIMIT 20",prefix.length,prefix,prefix.length+1,prefix.length+1,cursor);
  let recovered = 0;
  for (const candidate of candidates) {
    const key = candidate.op_key.slice(prefix.length);
    const single = /^sent-reaction:(\d+)$/.exec(key);
    const album = /^album:([^:]+):(.+):album-reaction:(\d+)$/.exec(key);
    const rows = single ? await all<MessageRow>(e,'SELECT * FROM messages WHERE id=?',Number(single[1])) : album && album[1] === e.ADMIN_CHAT_ID ? await all<MessageRow>(e,'SELECT * FROM messages WHERE source_chat=? AND media_group=? AND direction=\'out\' ORDER BY source_id',album[1],album[2]) : [];
    if (!rows.length || single && rows[0].media_group || rows.some(row=>row.epoch!==e.RECOVERY_EPOCH || row.direction!=='out' || row.source_chat!==e.ADMIN_CHAT_ID || row.business!==1 || row.tombstone || row.deletion!=='none' || !['success','failed'].includes(row.delivery))) continue;
    const state = await currentState(e,rows[0].id);
    if (!state?.emoji || await one(e,'SELECT admin_id FROM receipts WHERE admin_id=?',state.anchor.admin_id)) continue;
    if (!(await Promise.all(rows.map(row=>confirmedDelivery(e,row)))).every(Boolean)) continue;
    await syncReceipt(e,state.anchor.id);
    // This explicitly supersedes an unissued legacy marker. It does not turn
    // an old request into success or alter its original delivery ledger.
    await run(e,"UPDATE operations SET phase='failed',reason='superseded_by_receipt',code=NULL,updated_at=? WHERE op_key=? AND phase='prepared' AND EXISTS(SELECT 1 FROM receipts WHERE admin_id=? AND epoch=?)",now(),candidate.op_key,state.anchor.admin_id,e.RECOVERY_EPOCH);
    recovered++;
  }
  // Ineligible historical markers stay unchanged and cannot permanently fill
  // the first page. Wrap on an empty page so later evidence is reconsidered.
  await setSetting(e,'receipt_recovery_cursor',candidates.at(-1)?.op_key ?? '');
  return recovered;
}

/** Run again inside the Bot DO immediately before Telegram. In-flight Bot
 * operations are serialized, so an old clear cannot overtake a newer marker. */
export async function receiptGuardValid(e: Env, guard: ReceiptGuard): Promise<boolean> {
  const receipt = await one<Receipt>(e, 'SELECT * FROM receipts WHERE admin_id=?', guard.adminId);
  if (!receipt || receipt.epoch !== e.RECOVERY_EPOCH || receipt.version !== guard.version || receipt.signature !== guard.signature || receipt.emoji !== guard.emoji) return false;
  const state = await currentState(e, receipt.row_id);
  if (!state || state.signature !== receipt.signature || state.emoji !== receipt.emoji) return false;
  return !guard.clear || receipt.emoji === '👍' && receipt.phase === 'visible' && receipt.clear_after !== null && receipt.clear_after <= now();
}

/** Ready cleanups get the next reaction slot before a fresh success marker.
 * The five-second readiness window is a priority lease, not cancellation: a
 * stalled/reconstructed lane cannot indefinitely starve every new receipt. */
export async function dueReceiptCleanup(e:Env,time:number):Promise<boolean> {
  const rows = await all<Receipt>(e,"SELECT r.* FROM receipts r WHERE r.epoch=? AND r.emoji='👍' AND r.phase='visible' AND r.clear_after<=? AND EXISTS(SELECT 1 FROM jobs j WHERE j.user_key='receipt:'||r.admin_id AND j.kind='reaction' AND j.epoch=r.epoch AND j.state IN ('pending','retry','running') AND j.due_at<=? AND j.due_at>=? AND json_extract(j.payload,'$.version')=r.version AND json_extract(j.payload,'$.action')='clear') ORDER BY r.clear_after,r.admin_id LIMIT 12",e.RECOVERY_EPOCH,time,time,time-5000);
  for (const row of rows) if (await receiptGuardValid(e,{adminId:row.admin_id,version:row.version,signature:row.signature,emoji:row.emoji,clear:true})) return true;
  return false;
}

/** Marker-only retries may converge an unknown reaction to the same desired
 * state, with a fresh version guard. They never replay a content operation. */
export async function handleReaction(e: Env, job: Job): Promise<void> {
  const payload = JSON.parse(job.payload) as Partial<ReceiptJob> & {rowId?: number};
  // Pending jobs created before this migration must derive current intent,
  // never blindly apply their saved emoji.
  if (payload.rowId) {await syncReceipt(e, payload.rowId, true); return;}
  const p = payload as ReceiptJob;
  if (!Number.isSafeInteger(p.adminId) || !Number.isSafeInteger(p.version) || !Number.isInteger(p.attempt) || p.attempt < 1 || p.attempt > 3 || !['apply','clear'].includes(p.action)) return;
  const receipt = await one<Receipt>(e, 'SELECT * FROM receipts WHERE admin_id=?', p.adminId);
  if (!receipt || receipt.epoch !== e.RECOVERY_EPOCH || receipt.version !== p.version) return;
  if (p.action === 'apply' && receipt.phase !== 'pending' || p.action === 'clear' && receipt.phase !== 'visible') return;
  if (p.action === 'clear' && receipt.clear_after !== null && receipt.clear_after > now()) throw new RetryLater(receipt.clear_after);
  const guard: ReceiptGuard = {adminId: receipt.admin_id, version: receipt.version, signature: receipt.signature, emoji: receipt.emoji, clear: p.action === 'clear'};
  if (!await receiptGuardValid(e, guard)) {await syncReceipt(e, receipt.row_id); return;}
  const emoji = p.action === 'clear' ? null : receipt.emoji;
  const outcome = await call(e, `receipt:${p.adminId}:${p.version}:${p.action}:${p.attempt}`, 'setMessageReaction', {chat_id: e.ADMIN_CHAT_ID,message_id: p.adminId,reaction: emoji ? [{type: 'emoji',emoji}] : []}, {receipt: guard});
  if (outcome.status === 'failed' && outcome.reason === 'cancelled_by_state') {await syncReceipt(e, receipt.row_id); return;}
  const success = outcome.status === 'success' && outcome.result === true;
  const status = outcome.status === 'success' && !success ? 'unknown' : outcome.status;
  const confirmedAt = success ? outcome.confirmedAt ?? now() : null;
  const statements = [stmt(e, 'INSERT OR IGNORE INTO receipt_events(admin_id,version,action,attempt,status,confirmed_at,created_at) VALUES(?,?,?,?,?,?,?)',p.adminId,p.version,p.action,p.attempt,status,confirmedAt,now())];
  if (success) {
    if (p.action === 'apply') {
      statements.push(stmt(e, "UPDATE receipts SET phase='visible',shown_at=?,clear_after=?,last_error=NULL,updated_at=? WHERE admin_id=? AND version=? AND epoch=?",confirmedAt,emoji === '👍' ? confirmedAt! + SUCCESS_RECEIPT_MS : null,now(),p.adminId,p.version,e.RECOVERY_EPOCH));
      if (emoji === '👍') statements.push(jobStatement(e, receipt, 'clear', 1, confirmedAt! + SUCCESS_RECEIPT_MS));
    } else statements.push(stmt(e, "UPDATE receipts SET phase='cleared',cleared_at=?,last_error=NULL,updated_at=? WHERE admin_id=? AND version=? AND epoch=?",confirmedAt,now(),p.adminId,p.version,e.RECOVERY_EPOCH));
  } else {
    statements.push(stmt(e, "UPDATE receipts SET phase=CASE WHEN ?=3 THEN 'attention' ELSE phase END,last_error=?,updated_at=? WHERE admin_id=? AND version=? AND epoch=?",p.attempt,status,now(),p.adminId,p.version,e.RECOVERY_EPOCH));
    if (p.attempt < 3) statements.push(jobStatement(e, receipt, p.action, p.attempt + 1, now() + 3000 * 2 ** (p.attempt - 1)));
  }
  await e.DB.batch(statements);
  if (!success) await audit(e, 'reaction_exception', {message: receipt.row_id,version: p.version,action: p.action,attempt: p.attempt,status,...(outcome.status !== 'success' ? {code: outcome.code} : {})});
}
