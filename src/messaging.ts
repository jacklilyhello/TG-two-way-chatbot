import type {Env, Job, MessageRow, Params, TGMessage, TGUpdate, UserRow} from './types';
import {all, audit, enqueue, now, one, run, setting, stmt, user, wakeWork} from './db';
import {call, notify, RetryLater, type Outcome} from './operations';
import {ensureThread, ensureUser, scheduleCard} from './admin';
import {DEFAULT_FIXED_TEXT} from './settings';
import {syncReceipt} from './receipts';
import {internalForward, admittedBindingMatches, albumMemberBindingMatches} from './security';
import {bindRetryNotice, deliveryRetryButton, latestDelivery, type DeliveryAttempt} from './delivery-retry';
export {handleReaction} from './receipts';

type WaitUntil = (promise: Promise<unknown>) => void;
const CLEAR_LIMIT = 2;
const CLEAR_BUDGET_MS = 2000;
const ALBUM_QUIET_MS = 1200;
const authorised = (e: Env, m: TGMessage) => String(m.chat.id) === e.ADMIN_CHAT_ID && !m.sender_chat && !m.from?.is_bot && e.ADMIN_IDS.split(',').map(x => x.trim()).includes(String(m.from?.id));
const groupKey = (m: TGMessage) => `${m.chat.id}:${m.media_group_id}`;
const sourceVersion = (job: Job) => {
  const payload = JSON.parse(job.payload) as {update?: TGUpdate; update_id?: number};
  return payload.update?.update_id ?? payload.update_id ?? job.id;
};

function media(m: TGMessage): Params | null {
  const file = m.photo?.at(-1)?.file_id;
  const entry = file ? ['photo', file] : m.video ? ['video', m.video.file_id] : m.audio ? ['audio', m.audio.file_id] : m.document ? ['document', m.document.file_id] : m.animation ? ['animation', m.animation.file_id] : null;
  if (!entry) return null;
  return {type: entry[0], media: entry[1], ...(m.caption !== undefined ? {caption: m.caption} : {}), ...(m.caption_entities ? {caption_entities: m.caption_entities} : {})};
}

function knownMessageId(result: unknown): number | null {
  if (!result || typeof result !== 'object') return null;
  const id = (result as {message_id?: unknown}).message_id;
  return typeof id === 'number' && Number.isSafeInteger(id) && id > 0 ? id : null;
}

async function recordOutcome(e: Env, row: MessageRow, outcome: Outcome, thread: number | null, id: number | null = null) {
  const status = outcome.status === 'success' && id === null ? 'unknown' : outcome.status === 'failed' && outcome.code === 409 && outcome.reason === 'cancelled_by_state' ? 'suppressed' : outcome.status;
  await run(e, 'UPDATE messages SET delivery=?,admin_id=COALESCE(?,admin_id),private_id=COALESCE(?,private_id),thread_id=? WHERE id=?', status, row.direction === 'in' ? id : null, row.direction === 'out' ? id : null, thread, row.id);
}

async function deliveryNotice(e: Env, row: MessageRow, outcome: Outcome, waitUntil?: WaitUntil) {
  if (outcome.status === 'success') return;
  if (outcome.status === 'failed' && outcome.reason === 'cancelled_by_state') return;
  const delivery = await latestDelivery(e,row);
  const opKey = `${e.RECOVERY_EPOCH}:${delivery.key}`;
  if (!await one(e, "SELECT id FROM audit_logs WHERE event=? AND json_extract(detail,'$.message')=? AND (json_extract(detail,'$.opKey')=? OR (?=0 AND json_extract(detail,'$.opKey') IS NULL)) LIMIT 1",`delivery_${outcome.status}`,row.id,opKey,delivery.attempt?.attempt??0)) await audit(e, `delivery_${outcome.status}`, {message: row.id, direction: row.direction, code: outcome.code, reason: outcome.reason,opKey});
  if (row.direction !== 'out') return;
  const text = outcome.status === 'unknown' ? '发生未知情况：投递结果未知，内容不会自动重发。' : /bot was blocked by the user/i.test(outcome.reason) ? '对方已拉黑机器人' : `未送达，请重试。${outcome.reason ? `原因：${outcome.reason}` : ''}`;
  const noticeKey = `delivery-notice:${row.id}${delivery.attempt?`:attempt:${delivery.attempt.attempt}`:''}`;
  await enqueue(e, `notice:${row.id}`, 'delivery-notice', {rowId:row.id,status:outcome.status,text,opKey}, noticeKey);
  const wake = wakeWork(e, `notice:${row.id}`);
  if (waitUntil) waitUntil(wake); else await wake;
}

/** A notification retry must not run the original content job again. */
export async function handleDeliveryNotice(e: Env, job: Job): Promise<void> {
  const p = JSON.parse(job.payload) as {rowId:number;status:string;text:string;opKey?:string};
  const row = await one<MessageRow>(e, 'SELECT * FROM messages WHERE id=?',p.rowId);
  if (!row || row.direction !== 'out' || row.epoch !== e.RECOVERY_EPOCH || row.delivery !== p.status || !['failed','unknown'].includes(p.status) || typeof p.text !== 'string') return;
  const delivery = await latestDelivery(e,row);
  if (!p.opKey && delivery.attempt) return;
  if (p.opKey && p.opKey!==`${e.RECOVERY_EPOCH}:${delivery.key}`) return;
  const button = p.status==='failed' ? await deliveryRetryButton(e,row) : null;
  const outcome = await notify(e, job.dedupe, row.thread_id, p.text,button?{reply_markup:button.reply_markup}:{});
  if (button && outcome.status==='success') await bindRetryNotice(e,button.token,knownMessageId(outcome.result)??0);
  if (outcome.status !== 'success') await audit(e, 'delivery_notice_exception', {message:row.id,status:outcome.status,code:outcome.code});
}

/** Recover a crash between recording a terminal delivery and queuing its
 * administrator notice. Read the ledger only; no content request is issued. */
async function restoreDeliveryNotice(e: Env, row: MessageRow, waitUntil?: WaitUntil) {
  if (row.direction !== 'out' || !['failed','unknown'].includes(row.delivery)) return;
  const delivery = await latestDelivery(e,row);
  if (await one(e,'SELECT id FROM jobs WHERE dedupe=?',`delivery-notice:${row.id}${delivery.attempt?`:attempt:${delivery.attempt.attempt}`:''}`)) return;
  const ledger = await one<{phase:string;reason:string|null;code:number|null}>(e,'SELECT phase,reason,code FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:${delivery.key}`);
  if (delivery.attempt && ledger?.phase!==row.delivery) return;
  await deliveryNotice(e,row,{status:row.delivery as 'failed'|'unknown',reason:ledger?.reason??'请查看持久操作记录',code:ledger?.code??undefined},waitUntil);
}

async function replyParameters(e: Env, row: MessageRow, m: TGMessage): Promise<Params> {
  if (!m.reply_to_message) return {};
  if (String(m.reply_to_message.chat.id) !== row.source_chat) return {};
  const column = row.direction === 'in' ? 'private_id' : 'admin_id';
  const target = await one<MessageRow>(e, `SELECT * FROM messages WHERE user_id=? AND ${column}=? ORDER BY id DESC LIMIT 1`, row.user_id, m.reply_to_message.message_id);
  const targetId = row.direction === 'in' ? target?.admin_id : target?.private_id;
  if (!target || !targetId || target.tombstone || target.deletion !== 'none' || target.thread_id !== row.thread_id) {
    await audit(e, 'reply_fallback', {message: row.id, reason: '映射缺失、目标已删除或不属于当前话题'});
    return {};
  }
  return {reply_parameters: {message_id: targetId, allow_sending_without_reply: true}};
}

async function copyOne(e: Env, row: MessageRow, m: TGMessage, u: UserRow, retry?:DeliveryAttempt, waitUntil?: WaitUntil) {
  const current = await one<MessageRow>(e, 'SELECT * FROM messages WHERE id=?', row.id);
  if (!current || current.tombstone || current.deletion !== 'none') return;
  const chatId = row.direction === 'in' ? e.ADMIN_CHAT_ID : u.tgid;
  const targetThread = row.thread_id ?? u.thread_id;
  // Save routing before issuing a request so a returned result can be recovered
  // against the actual destination even after a concurrent binding switch.
  if (row.thread_id === null) await run(e, 'UPDATE messages SET thread_id=? WHERE id=?', targetThread, row.id);
  const params: Params = {chat_id: chatId, from_chat_id: row.source_chat, message_id: row.source_id};
  if (row.direction === 'in') params.message_thread_id = targetThread;
  Object.assign(params, await replyParameters(e, {...row, thread_id: targetThread}, m));
  const guard = {messageIds: [row.id], threadVersion: {userId: u.tgid, version: retry?.binding_version??u.binding_version, threadId: targetThread ?? undefined},...(retry?{deliveryRetry:{messageId:row.id,attempt:retry.attempt,actorId:retry.actor_id,userId:retry.user_id,threadId:retry.thread_id,bindingVersion:retry.binding_version,priorOpKey:retry.prior_op_key}}:{})};
  const baseKey = retry?.op_key??`send:${row.id}`;
  // A previously issued no-reply fallback remains the authoritative attempt
  // even if the quoted target disappeared before this job resumed.
  const fallback = await one(e,'SELECT op_key FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:${baseKey}:without-reply`);
  const sendKey = fallback?`${baseKey}:without-reply`:baseKey;
  if (fallback) delete params.reply_parameters;
  let outcome = await call(e, sendKey, 'copyMessage', params, guard);
  // Reply targets can disappear after the mapping check. Telegram explicitly rejected
  // this request, so a single fallback without a reply cannot duplicate delivery.
  if (outcome.status === 'failed' && /reply.*(?:not found|deleted)|message to be replied not found/i.test(outcome.reason) && params.reply_parameters) {
    delete params.reply_parameters;
    await audit(e, 'reply_fallback', {message: row.id, reason: 'Telegram 拒绝引用目标'});
    outcome = await call(e, `${baseKey}:without-reply`, 'copyMessage', params, guard);
  }
  const id = outcome.status === 'success' ? knownMessageId(outcome.result) : null;
  if (outcome.status === 'success' && id === null) outcome = {status: 'unknown', reason: 'Telegram 返回缺少有效消息 ID'};
  await recordOutcome(e, row, outcome, targetThread, id);
  if (retry) await run(e,'UPDATE delivery_attempts SET state=? WHERE message_id=? AND attempt=?',outcome.status==='failed'&&outcome.reason==='cancelled_by_state'?'suppressed':outcome.status,row.id,retry.attempt);
  if (row.direction === 'out') await syncReceipt(e, row.id, false, waitUntil);
  await deliveryNotice(e, {...row, thread_id: targetThread}, outcome, waitUntil);
}

/** The new attempt runs in the same user's ordered business lane. A stale
 * reservation can recover its journal, but final guards forbid a new send. */
export async function handleDeliveryRetry(e:Env,job:Job,waitUntil?:WaitUntil):Promise<void> {
  const p = JSON.parse(job.payload) as {rowId:number;attempt:number};
  const attempt = await one<DeliveryAttempt>(e,'SELECT * FROM delivery_attempts WHERE message_id=? AND attempt=? AND epoch=?',p.rowId,p.attempt,e.RECOVERY_EPOCH);
  const row = attempt ? await one<MessageRow>(e,'SELECT * FROM messages WHERE id=?',attempt.message_id) : null;
  if (!attempt || !row || job.epoch!==e.RECOVERY_EPOCH || job.user_key!==attempt.user_id || row.user_id!==attempt.user_id || row.direction!=='out' || row.media_group || row.epoch!==e.RECOVERY_EPOCH) return;
  if ((await latestDelivery(e,row)).attempt?.attempt!==attempt.attempt) return;
  if (row.tombstone || row.deletion!=='none') {
    await recoverPrivateTarget(e,row);
    const delivery=await latestDelivery(e,row);
    const ledger=await one<{phase:string;reason:string|null}>(e,'SELECT phase,reason FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:${delivery.key}`);
    const state=ledger?.phase==='success'?'success':ledger?.phase==='unknown'?'unknown':ledger?.phase==='failed'&&ledger.reason!=='cancelled_by_state'?'failed':'suppressed';
    await run(e,'UPDATE delivery_attempts SET state=? WHERE message_id=? AND attempt=?',state,row.id,attempt.attempt);
    return;
  }
  const u = await user(e,row.user_id);
  if (!u) return;
  await copyOne(e,row,JSON.parse(row.payload) as TGMessage,u,attempt,waitUntil);
}

async function albumStatus(e: Env, row: MessageRow, deleting: boolean, scope: string, waitUntil?: WaitUntil) {
  const rows = await all<MessageRow>(e, 'SELECT * FROM messages WHERE user_id=? AND direction=? AND source_chat=? AND media_group=? ORDER BY source_id', row.user_id, row.direction, row.source_chat, row.media_group);
  const anchor = rows.find(r => r.admin_id !== null);
  if (!anchor?.admin_id || !rows.length) return;
  if (!deleting) for (const member of rows) await restoreDeliveryNotice(e,member,waitUntil);
  const success = rows.filter(r => deleting ? r.deletion === 'deleted' : r.delivery === 'success').length;
  const pending = rows.some(r => deleting ? r.deletion === 'none' : r.delivery === 'pending');
  if (success === rows.length) {
    if (deleting || row.direction === 'out') await syncReceipt(e, anchor.id, false, waitUntil);
  } else if (!pending || deleting) {
    // Incoming albums never receive a delivery marker. Outgoing albums may
    // retain a legacy successful mark from before receipt tracking existed.
    if (deleting || row.direction === 'out') await syncReceipt(e, anchor.id, deleting && row.direction === 'out', waitUntil);
    const unknown = rows.filter(r => deleting ? r.deletion === 'unknown' : r.delivery === 'unknown').length;
    await notify(e, `${scope}:album-summary:${success}:${unknown}:${rows.length}`, anchor.thread_id, `相册${deleting ? '删除' : '投递'}：已确认${success}/${rows.length}，未知${unknown}，其余${rows.length - success - unknown}。${deleting ? '仅处理选定条目，群内原件和历史保留。' : ''}`);
  }
}

async function album(e: Env, job: Job, row: MessageRow, m: TGMessage, u: UserRow, waitUntil?: WaitUntil) {
  const key = groupKey(m);
  await run(e, 'INSERT OR IGNORE INTO media_groups(group_key,user_id,direction,due_at) VALUES(?,?,?,?)', key, u.tgid, row.direction, row.received_at + ALBUM_QUIET_MS);
  const group = await one<{due_at: number}>(e, 'SELECT due_at FROM media_groups WHERE group_key=?', key);
  if (group && group.due_at > now()) throw new RetryLater(group.due_at);
  const savedBatch = await one<{batch_key: string; members: string; thread_id: number | null}>(e, "SELECT batch_key,members,thread_id FROM media_batches WHERE group_key=? AND state='pending' ORDER BY rowid LIMIT 1", key);
  let members = savedBatch ? await all<MessageRow>(e, 'SELECT * FROM messages WHERE id IN (SELECT value FROM json_each(?)) ORDER BY source_id', savedBatch.members) : await all<MessageRow>(e, "SELECT * FROM messages WHERE user_id=? AND direction=? AND source_chat=? AND media_group=? AND delivery='pending' AND tombstone=0 AND deletion='none' ORDER BY source_id LIMIT 10", u.tgid, row.direction, row.source_chat, m.media_group_id);
  let admitted = true;
  if (row.direction === 'out') {
    const matches = await Promise.all(members.map(member => albumMemberBindingMatches(e, member, u)));
    admitted = matches.every(Boolean);
    if (!savedBatch) {
      for (const [index,member] of members.entries()) if (!matches[index]) await run(e, "UPDATE messages SET delivery='suppressed' WHERE id=? AND delivery='pending'",member.id);
      members = members.filter((_member,index) => matches[index]);
      admitted = true;
    }
  }
  if (!members.length) {
    await albumStatus(e, row, false, `album:${key}`, waitUntil);
    return;
  }
  const mediaItems = members.map(r => media(JSON.parse(r.payload) as TGMessage));
  const types = mediaItems.map(x => x?.type);
  const compatible = mediaItems.every(x => x !== null) && (types.every(t => t === 'photo' || t === 'video') || types.every(t => t === 'audio') || types.every(t => t === 'document'));
  if (members.length < 2 || !compatible) {
    for (const member of members) await copyOne(e, member, JSON.parse(member.payload) as TGMessage, u, undefined, waitUntil);
  } else {
    const batchKey = savedBatch?.batch_key ?? `album-send:${key}:${members.map(r => r.id).join(',')}`;
    const targetThread = savedBatch?.thread_id ?? u.thread_id;
    if (!savedBatch) await run(e, 'INSERT OR IGNORE INTO media_batches(batch_key,group_key,members,thread_id) VALUES(?,?,?,?)', batchKey, key, JSON.stringify(members.map(r => r.id)), targetThread);
    const params: Params = {chat_id: row.direction === 'in' ? e.ADMIN_CHAT_ID : u.tgid, media: mediaItems};
    if (row.direction === 'in') params.message_thread_id = targetThread;
    Object.assign(params, await replyParameters(e, {...members[0], thread_id: targetThread}, JSON.parse(members[0].payload) as TGMessage));
    // A frozen batch must keep its original membership for journal recovery.
    // An invalid admission version forbids new side effects, while call() and
    // the Bot journal can still return an already confirmed batch mapping.
    let outcome = await call(e, batchKey, 'sendMediaGroup', params, {messageIds: members.map(r => r.id), threadVersion: {userId: u.tgid, version: admitted ? u.binding_version : -1, threadId: targetThread ?? undefined}});
    const ids = outcome.status === 'success' && Array.isArray(outcome.result) ? outcome.result.map(knownMessageId) : [];
    if (outcome.status === 'success' && (ids.length !== members.length || ids.some(id => id === null))) outcome = {status: 'unknown', reason: '相册返回数量或消息 ID 无法对应，禁止猜测映射或重发'};
    await e.DB.batch([
      ...members.map((member, i) => stmt(e, 'UPDATE messages SET delivery=?,admin_id=COALESCE(?,admin_id),private_id=COALESCE(?,private_id),thread_id=? WHERE id=?', outcome.status === 'failed' && outcome.reason === 'cancelled_by_state' ? 'suppressed' : outcome.status, row.direction === 'in' && outcome.status === 'success' ? ids[i] : null, row.direction === 'out' && outcome.status === 'success' ? ids[i] : null, targetThread, member.id)),
      stmt(e, "UPDATE media_batches SET state='done' WHERE batch_key=?", batchKey),
    ]);
    if (row.direction === 'out') await syncReceipt(e, row.id, false, waitUntil);
    for (let i = 0; i < members.length; i++) {
      await deliveryNotice(e, {...members[i], thread_id: targetThread}, outcome, waitUntil);
    }
  }
  await albumStatus(e, row, false, `album:${key}`, waitUntil);
}

async function edit(e: Env, job: Job, row: MessageRow | null, m: TGMessage, u: UserRow) {
  if (!row || row.delivery === 'pending') {
    await run(e, "UPDATE jobs SET state='waiting',due_at=? WHERE id=?", now() + 1000, job.id);
    return;
  }
  if (row.tombstone || row.deletion !== 'none' || row.delivery !== 'success' || row.thread_id !== u.thread_id || (row.direction === 'in' && u.blocked)) return;
  const previous = JSON.parse(row.payload) as TGMessage;
  const updateId = sourceVersion(job);
  const editDate = m.edit_date ?? m.date;
  const previousDate = previous.edit_date ?? previous.date;
  if (editDate < previousDate || (editDate === previousDate && updateId < row.version)) return;
  await run(e, 'UPDATE messages SET version=?,payload=? WHERE id=?', updateId, JSON.stringify(m), row.id);
  const params: Params = {chat_id: row.direction === 'in' ? e.ADMIN_CHAT_ID : u.tgid, message_id: row.direction === 'in' ? row.admin_id : row.private_id};
  let method: string;
  if (m.text !== undefined) {method = 'editMessageText'; params.text = m.text; if (m.entities) params.entities = m.entities;}
  else {
    const inputMedia = media(m);
    if (inputMedia) {method = 'editMessageMedia'; params.media = inputMedia;}
    else if (m.caption !== undefined) {method = 'editMessageCaption'; params.caption = m.caption; if (m.caption_entities) params.caption_entities = m.caption_entities;}
    else {await audit(e, 'edit_unsupported', {message: row.id}); await notify(e, `edit:${row.id}:${updateId}:unsupported`, row.thread_id, '该消息类型不支持编辑同步；保留已有副本。'); return;}
  }
  const outcome = await call(e, `edit:${row.id}:${updateId}`, method, params, {messageIds: [row.id], threadVersion: {userId: u.tgid, version: u.binding_version, threadId: row.thread_id ?? undefined}});
  if (outcome.status === 'failed' && outcome.reason === 'cancelled_by_state') return;
  if (outcome.status === 'failed' && /message is not modified/i.test(outcome.reason)) return;
  if (outcome.status !== 'success') {
    await audit(e, `edit_${outcome.status}`, {message: row.id, code: outcome.code});
    await notify(e, `edit:${row.id}:${updateId}:notice`, row.thread_id, outcome.status === 'unknown' ? '发生未知情况：编辑同步结果未知，不会重发原消息。' : `编辑未同步：${outcome.reason}`);
  }
}

async function fixedReply(e: Env, row: MessageRow, m: TGMessage, command: string) {
  const template = await setting(e, command === 'start' ? 'start_text' : 'help_text', DEFAULT_FIXED_TEXT[command === 'start' ? 'start' : 'help']);
  const name = [m.from?.first_name, m.from?.last_name].filter(Boolean).join(' ') || row.user_id;
  const text = template.replace(/\{(?:name|first_name|last_name|username|tgid)\}/g, token => ({'{name}': name, '{first_name}': m.from?.first_name ?? '', '{last_name}': m.from?.last_name ?? '', '{username}': m.from?.username ? `@${m.from.username}` : '未设置', '{tgid}': row.user_id}[token] ?? token));
  // Reserve the reply before sending. A concurrent clear captures this row, or
  // attaches it to the already captured command's batch before any API call.
  await e.DB.batch([
    stmt(e, "INSERT OR IGNORE INTO messages(user_id,direction,source_chat,source_id,business,received_at,payload,epoch) VALUES(?,?,?, ?,0,?,?,?)", row.user_id, 'system', row.user_id, -row.id, now(), JSON.stringify({text}), row.epoch),
    stmt(e, 'UPDATE messages SET tombstone=(SELECT tombstone FROM messages WHERE id=?) WHERE source_chat=? AND source_id=?', row.id, row.user_id, -row.id),
    stmt(e, 'INSERT OR IGNORE INTO deletion_items(batch_id,message_id) SELECT d.batch_id,m.id FROM deletion_items d JOIN messages m ON m.source_chat=? AND m.source_id=? WHERE d.message_id=?', row.user_id, -row.id, row.id),
  ]);
  const reply = await one<MessageRow>(e, 'SELECT * FROM messages WHERE source_chat=? AND source_id=?', row.user_id, -row.id);
  if (!reply || reply.tombstone) return;
  const outcome = await call(e, `fixed:${row.id}`, 'sendMessage', {chat_id: row.user_id, text}, {messageIds: [row.id, reply.id]});
  if (outcome.status === 'success') {
    const id = knownMessageId(outcome.result);
    if (id) await run(e, "UPDATE messages SET private_id=?,delivery='success' WHERE id=?", id, reply.id);
    else {await run(e, "UPDATE messages SET delivery='unknown' WHERE id=?", reply.id); await audit(e, 'fixed_reply_unknown', {message: row.id});}
  } else {await run(e, 'UPDATE messages SET delivery=? WHERE id=?', outcome.status, reply.id); await audit(e, `fixed_reply_${outcome.status}`, {message: row.id, code: outcome.code});}
  await run(e, "UPDATE messages SET business=0,delivery='ignored' WHERE id=?", row.id);
}

/** Handles only persisted business messages. Administrative commands are routed first. */
export async function handleMessage(e: Env, job: Job, m: TGMessage, edited: boolean, waitUntil?:(promise:Promise<unknown>)=>void): Promise<void> {
  if (internalForward(e, m)) return;
  const privateChat = m.chat.type === 'private' && String(m.chat.id) === String(m.from?.id) && !m.from?.is_bot;
  if (!privateChat && !authorised(e, m)) return;
  let row = await one<MessageRow>(e, 'SELECT * FROM messages WHERE source_chat=? AND source_id=?', String(m.chat.id), m.message_id);
  let u: UserRow | null = privateChat ? await ensureUser(e, String(m.chat.id), m.from, row?.received_at ?? job.created_at) : row ? await user(e, row.user_id) : null;
  if (!u) return;
  if (!privateChat && !await admittedBindingMatches(e, job, m, u)) {
    if (row) await run(e, "UPDATE messages SET delivery='suppressed' WHERE id=? AND delivery='pending'", row.id);
    return;
  }
  if (privateChat && u.blocked) {
    if (row && !edited && row.delivery === 'pending') await run(e, "UPDATE messages SET delivery='blocked' WHERE id=?", row.id);
    return;
  }
  if (edited) {await edit(e, job, row, m, u); return;}
  if (!row || row.tombstone || row.deletion !== 'none') return;
  if (!privateChat && (!u.thread_id || u.thread_id !== m.message_thread_id || row.thread_id !== u.thread_id)) {
    await run(e, "UPDATE messages SET delivery='suppressed' WHERE id=? AND delivery='pending'", row.id);
    await notify(e, `old-thread:${row.id}`, m.message_thread_id ?? null, '此话题已不是当前绑定，消息未转发。请在当前有效用户话题发送。');
    return;
  }
  const command = m.text?.match(/^\/(start|help)(?:@[A-Za-z0-9_]+)?(?:\s|$)/)?.[1];
  if (privateChat && command) {await fixedReply(e, row, m, command); return;}
  if (m.text?.startsWith('/')) {await run(e, "UPDATE messages SET delivery='ignored',business=0 WHERE id=?", row.id); return;}
  if (!(m.text || m.photo || m.video || m.audio || m.voice || m.document || m.sticker || m.animation)) {
    await run(e, "UPDATE messages SET delivery='ignored',business=0 WHERE id=?", row.id);
    await audit(e, 'unsupported_message', {message: row.id});
    return;
  }
  if (privateChat) {
    // ensureUser just returned the current binding. The Bot's final guard
    // rejects a later binding change before any new external side effect.
    if (!u.thread_id) u = await ensureThread(e, job, u.tgid, false);
    if (!u.thread_id) {await audit(e, 'forward_missing_thread', {message: row.id}); return;}
    await scheduleCard(e, job, u, false, waitUntil);
  }
  if (m.media_group_id) {
    row = await one<MessageRow>(e, 'SELECT * FROM messages WHERE id=?', row.id);
    if (!row || row.tombstone || row.deletion !== 'none') return;
    await album(e, job, row, m, u, waitUntil); return;
  }
  // copyOne rechecks the current row before preparing its request. Album
  // batches and terminal receipt recovery retain their own state reads.
  if (row.delivery === 'pending') await copyOne(e, row, m, u, undefined, waitUntil);
  else if (row.direction === 'out') {await syncReceipt(e, row.id, false, waitUntil);await restoreDeliveryNotice(e,row,waitUntil);}
}

async function recoverPrivateTarget(e: Env, row: MessageRow): Promise<void> {
  if (row.private_id || row.direction === 'in') return;
  const {key,method,members:memberIds} = await latestDelivery(e,row);
  if (!await one(e, 'SELECT op_key FROM operations WHERE op_key=?', `${e.RECOVERY_EPOCH}:${key}`)) return;
  // The row is tombstoned before this call. Existing DO/D1 journal results can
  // be read, while the guard forbids issuing any new Telegram content request.
  const outcome = await call(e, key, method, {}, {messageIds: [row.id]});
  let id: number | null = null;
  if (outcome.status === 'success') {
    if (memberIds) {
      const results = Array.isArray(outcome.result) ? outcome.result.map(knownMessageId) : [];
      if (results.length === memberIds.length && results.every(x => x !== null)) id = results[memberIds.indexOf(row.id)] ?? null;
    } else id = knownMessageId(outcome.result);
  }
  if (id !== null) {row.private_id = id; row.delivery = 'success'; await run(e, "UPDATE messages SET private_id=?,delivery='success' WHERE id=?", id, row.id);}
  else if (outcome.status === 'unknown' || outcome.status === 'success') {row.delivery = 'unknown'; await run(e, "UPDATE messages SET delivery='unknown' WHERE id=?", row.id);}
}

async function deleteRow(e: Env, row: MessageRow, opKey: string): Promise<string> {
  if (row.deletion === 'deleted' || row.deletion === 'unknown') return row.deletion;
  await run(e, 'UPDATE messages SET tombstone=1 WHERE id=?', row.id);
  await recoverPrivateTarget(e, row);
  if (!row.private_id) {
    const result = row.delivery === 'unknown' || row.delivery === 'success' ? 'unknown' : 'unavailable';
    await run(e, 'UPDATE messages SET deletion=? WHERE id=?', result, row.id);
    return result;
  }
  const outcome = await call(e, opKey, 'deleteMessage', {chat_id: row.user_id, message_id: row.private_id});
  const result = outcome.status === 'success' && outcome.result === true ? 'deleted' : outcome.status === 'unknown' || outcome.status === 'success' ? 'unknown' : /can't be deleted|cannot be deleted|message to delete not found|not enough rights/i.test(outcome.reason) ? 'unavailable' : 'failed';
  await run(e, 'UPDATE messages SET deletion=? WHERE id=?', result, row.id);
  if (result !== 'deleted') await audit(e, 'deletion_exception', {message: row.id, result, ...(outcome.status !== 'success' ? {code: outcome.code, reason: outcome.reason} : {})});
  return result;
}

export async function deleteSingle(e: Env, job: Job, m: TGMessage, u: UserRow): Promise<void> {
  if (!authorised(e, m) || m.message_thread_id !== u.thread_id || !m.reply_to_message || !await admittedBindingMatches(e, job, m, u)) return;
  if (String(m.reply_to_message.chat.id) !== e.ADMIN_CHAT_ID || m.reply_to_message.message_thread_id !== undefined && m.reply_to_message.message_thread_id !== u.thread_id) return;
  const row = await one<MessageRow>(e, 'SELECT * FROM messages WHERE user_id=? AND admin_id=? AND thread_id=? ORDER BY id DESC LIMIT 1', u.tgid, m.reply_to_message.message_id, u.thread_id);
  if (!row) {await notify(e, `sc:${job.id}:missing`, u.thread_id, '无法删除：新系统没有这条消息的用户端映射。'); return;}
  const result = await deleteRow(e, row, `delete:${row.id}:${job.id}`);
  if (row.media_group) await albumStatus(e, row, true, `delete:${job.id}`);
  else if (result === 'deleted' && row.admin_id) await syncReceipt(e, row.id);
  if (result !== 'deleted') await notify(e, `sc:${job.id}:result`, u.thread_id, result === 'unknown' ? '发生未知情况：删除结果未知，未标记为成功。' : result === 'unavailable' ? '未删除：消息不存在、超过限制、权限不足或没有已确认的私聊目标。' : '未删除：Telegram 明确拒绝，请查看异常记录。');
}

/** Only an ingress-captured, tombstoned snapshot may be cleared. */
export async function clearConversation(e: Env, job: Job, u: UserRow, batchId?: string): Promise<void> {
  const started = now();
  if (!batchId) return;
  const batch = await one<{user_id: string; state: string}>(e, 'SELECT user_id,state FROM deletion_batches WHERE id=?', batchId);
  if (!batch || batch.user_id !== u.tgid || batch.state === 'done') return;
  const items = await all<{message_id: number}>(e, "SELECT message_id FROM deletion_items WHERE batch_id=? AND result='pending' ORDER BY message_id LIMIT ?", batchId, CLEAR_LIMIT);
  let processedThrough: number | undefined;
  for (const item of items) {
    const row = await one<MessageRow>(e, 'SELECT * FROM messages WHERE id=? AND user_id=?', item.message_id, u.tgid);
    const result = row ? await deleteRow(e, row, `clear:${batchId}:${row.id}`) : 'unavailable';
    await run(e, 'UPDATE deletion_items SET result=? WHERE batch_id=? AND message_id=?', result, batchId, item.message_id);
    if (row?.admin_id && !row.media_group && result === 'deleted') await syncReceipt(e, row.id);
    processedThrough = item.message_id;
    // Yield only after the external result and this item are durable. Never
    // interrupt an in-flight deletion or reuse the unprocessed selection tail.
    if (now() - started >= CLEAR_BUDGET_MS) break;
  }
  const pending = await one<{n: number}>(e, "SELECT COUNT(*) n FROM deletion_items WHERE batch_id=? AND result='pending'", batchId);
  if (pending?.n) {
    await enqueue(e, u.tgid, 'clear', {batchId}, `clear:${batchId}:after:${processedThrough ?? job.id}`);
    return;
  }
  // Recalculate all affected albums once the batch has a final result, including
  // albums that crossed the bounded continuation between jobs.
  const albumRows = await all<MessageRow>(e, 'SELECT m.* FROM messages m JOIN deletion_items d ON d.message_id=m.id WHERE d.batch_id=? AND m.media_group IS NOT NULL GROUP BY m.source_chat,m.media_group', batchId);
  for (const row of albumRows) await albumStatus(e, row, true, `clear:${batchId}`);
  const counts = await all<{result: string; n: number}>(e, 'SELECT result,COUNT(*) n FROM deletion_items WHERE batch_id=? GROUP BY result', batchId);
  const count = (result: string) => counts.find(r => r.result === result)?.n ?? 0;
  await notify(e, `clear:${batchId}:summary`, u.thread_id, `清空已处理：确认删除 ${count('deleted')}；明确失败 ${count('failed')}；不可删除/未发送 ${count('unavailable')}；结果未知 ${count('unknown')}。确认后新消息不在本次范围内。旧系统未记录历史无法追溯，数量未知。管理员历史和话题保留。`);
  await run(e, "UPDATE deletion_batches SET state='done' WHERE id=?", batchId);
}
