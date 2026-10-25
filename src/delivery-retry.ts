import type {Env, Job, MessageRow, TGMessage, TGUpdate, UserRow} from './types';
import {now, one, run, stmt, user, wakeWork} from './db';
import {albumMemberBindingMatches,internalForward} from './security';

export interface RetryAction {token:string;kind:string;user_id:string;actor_id:string;thread_id:number|null;version:number;payload:string;expires_at:number;used:number}
export interface DeliveryAttempt {message_id:number;attempt:number;op_key:string;prior_op_key:string;action_token:string;actor_id:string;user_id:string;thread_id:number;binding_version:number;epoch:string;state:string}
export interface DeliveryRetryGuard {messageId:number;attempt:number;actorId:string;userId:string;threadId:number;bindingVersion:number;priorOpKey:string}
const authorized = (e:Env,actor:string) => e.ADMIN_IDS.split(/[\s,]+/).includes(actor);

/** Every recovery path must resolve the latest reserved attempt, including
 * when its external success has not yet reached messages.private_id. */
export async function latestDelivery(e:Env,row:MessageRow) {
  if(row.direction==='broadcast') {
    const content=JSON.parse(row.payload) as {broadcastId:string;partIndex:number;method:string};
    return {key:`broadcast:${content.broadcastId}:${row.user_id}:${content.partIndex}`,method:content.method,members:null as number[]|null,attempt:null};
  }
  const attempt = await one<DeliveryAttempt>(e,'SELECT * FROM delivery_attempts WHERE message_id=? AND epoch=? ORDER BY attempt DESC LIMIT 1',row.id,e.RECOVERY_EPOCH);
  if (attempt) {
    const fallback = await one(e,'SELECT op_key FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:${attempt.op_key}:without-reply`);
    return {key:attempt.op_key+(fallback?':without-reply':''),method:'copyMessage',members:null as number[]|null,attempt};
  }
  let key = row.direction === 'system' ? `fixed:${-row.source_id}` : `send:${row.id}`;
  let method = row.direction === 'system' ? 'sendMessage' : 'copyMessage';
  let members:number[]|null = null;
  if (row.media_group) {
    const batch = await one<{batch_key:string;members:string}>(e,'SELECT batch_key,members FROM media_batches WHERE EXISTS(SELECT 1 FROM json_each(members) WHERE value=?) ORDER BY rowid DESC LIMIT 1',row.id);
    if (batch) {key=batch.batch_key;method='sendMediaGroup';members=JSON.parse(batch.members) as number[];}
  }
  if (method==='copyMessage' && await one(e,'SELECT op_key FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:${key}:without-reply`)) key += ':without-reply';
  return {key,method,members,attempt:null};
}

async function eligible(e:Env,row:MessageRow,expectedKey?:string) {
  if (row.direction!=='out' || row.business!==1 || row.epoch!==e.RECOVERY_EPOCH || row.media_group || row.delivery!=='failed' || row.private_id!==null || row.tombstone || row.deletion!=='none' || row.source_chat!==e.ADMIN_CHAT_ID || row.admin_id!==row.source_id) return null;
  const u = await user(e,row.user_id);
  if (!u || !u.thread_id || row.thread_id!==u.thread_id || !await albumMemberBindingMatches(e,row,u)) return null;
  const original = JSON.parse(row.payload) as TGMessage;
  const actor = String(original.from?.id);
  if (!authorized(e,actor) || original.from?.is_bot || original.sender_chat || String(original.chat?.id)!==e.ADMIN_CHAT_ID || original.message_id!==row.source_id || original.message_thread_id!==row.thread_id || internalForward(e,original)) return null;
  const delivery = await latestDelivery(e,row);
  const priorOpKey = `${e.RECOVERY_EPOCH}:${delivery.key}`;
  if (delivery.method!=='copyMessage' || expectedKey && expectedKey!==priorOpKey) return null;
  const ledger = await one<{phase:string;method:string;code:number|null;reason:string|null;result:string|null}>(e,'SELECT phase,method,code,reason,result FROM operations WHERE op_key=?',priorOpKey);
  // Only Telegram's explicit 4xx rejection proves no content was delivered.
  // Synthetic state cancellation, 429, issued and unknown are never eligible.
  if (!ledger || ledger.method!=='copyMessage' || ledger.phase!=='failed' || ledger.result!==null || ledger.code===null || ledger.code<400 || ledger.code>=500 || ledger.code===409 || ledger.code===429 || ledger.reason==='cancelled_by_state') return null;
  return {u,actor,priorOpKey,delivery};
}

export async function deliveryRetryButton(e:Env,row:MessageRow) {
  const target = await eligible(e,row);
  if (!target) return null;
  const context = `delivery-retry:${row.id}:${target.priorOpKey}`;
  let action = await one<RetryAction>(e,"SELECT * FROM callback_actions WHERE kind='delivery_retry' AND user_id=? AND actor_id=? AND version=? AND json_extract(payload,'$.context')=? ORDER BY rowid LIMIT 1",row.user_id,target.actor,target.u.binding_version,context);
  if (!action) {
    const token = crypto.randomUUID().replaceAll('-','');
    const payload = {instance:e.INSTANCE_ID,epoch:e.RECOVERY_EPOCH,rowId:row.id,sourceId:row.source_id,priorOpKey:target.priorOpKey,context};
    await run(e,"INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) VALUES(?,'delivery_retry',?,?,?,?,?,?)",token,row.user_id,target.actor,row.thread_id,target.u.binding_version,JSON.stringify(payload),now()+15*60000);
    action = (await one<RetryAction>(e,'SELECT * FROM callback_actions WHERE token=?',token))!;
  }
  if (action.used!==0 || action.expires_at<=now()) return null;
  return {token:action.token,reply_markup:{inline_keyboard:[[{text:'重试此条',callback_data:`a:${action.token}`}]]}};
}

export async function bindRetryNotice(e:Env,token:string,messageId:number) {
  if (!Number.isSafeInteger(messageId) || messageId<=0) return;
  await run(e,"UPDATE callback_actions SET payload=json_set(payload,'$.messageId',?) WHERE token=? AND kind='delivery_retry' AND (json_extract(payload,'$.messageId') IS NULL OR json_extract(payload,'$.messageId')=?)",messageId,token,messageId);
}

/** The callback transaction reserves exactly one successor for this failed
 * operation and persists its user-FIFO job before acknowledging acceptance. */
export async function reserveDeliveryRetry(e:Env,job:Job,update:TGUpdate,action:RetryAction,_user?:UserRow):Promise<string> {
  const cb = update.callback_query, m = cb?.message, stamp = update.update_id+1;
  if (!cb || !m || action.kind!=='delivery_retry' || job.epoch!==e.RECOVERY_EPOCH || !authorized(e,String(cb.from.id)) || cb.from.is_bot || String(m.chat.id)!==e.ADMIN_CHAT_ID || String(m.from?.id)!==e.EXPECTED_BOT_ID || m.sender_chat || !m.is_topic_message || !m.message_thread_id || action.actor_id!==String(cb.from.id) || action.thread_id!==m.message_thread_id || action.expires_at<=now() || action.used!==0 && action.used!==stamp) return '按钮已失效或无权重试。';
  const p = JSON.parse(action.payload) as {instance:string;epoch:string;rowId:number;sourceId:number;priorOpKey:string;messageId:number};
  if (p.instance!==e.INSTANCE_ID || p.epoch!==e.RECOVERY_EPOCH || p.messageId!==m.message_id || !Number.isSafeInteger(p.rowId)) return '重试目标不匹配。';
  const existing = await one<DeliveryAttempt>(e,'SELECT * FROM delivery_attempts WHERE action_token=?',action.token);
  if (existing && action.used===stamp) return '此重试已保存；不会重复发送。';
  const row = await one<MessageRow>(e,'SELECT * FROM messages WHERE id=?',p.rowId);
  const target = row ? await eligible(e,row,p.priorOpKey) : null;
  if (!row || !target || row.user_id!==action.user_id || row.source_id!==p.sourceId || target.actor!==String(cb.from.id) || target.u.binding_version!==action.version || target.u.thread_id!==action.thread_id) return '当前状态不能安全重试；未知结果、相册、已删除或绑定变化的消息不会重发。';
  const previous = target.delivery.attempt?.attempt ?? 0;
  const attempt = previous+1;
  const opKey = `send-retry:${row.id}:${attempt}`;
  const time = now();
  await e.DB.batch([
    stmt(e,`INSERT OR IGNORE INTO delivery_attempts(message_id,attempt,op_key,prior_op_key,action_token,actor_id,user_id,thread_id,binding_version,epoch,created_at)
      SELECT m.id,?,?,?,?,?,?,?, ?,?,? FROM messages m JOIN users u ON u.tgid=m.user_id JOIN callback_actions a ON a.token=? JOIN operations o ON o.op_key=?
      WHERE m.id=? AND m.epoch=? AND m.delivery='failed' AND m.private_id IS NULL AND m.tombstone=0 AND m.deletion='none' AND m.media_group IS NULL AND m.user_id=? AND m.thread_id=? AND u.thread_id=? AND u.binding_version=? AND a.kind='delivery_retry' AND json_extract(a.payload,'$.rowId')=m.id AND json_extract(a.payload,'$.sourceId')=m.source_id AND json_extract(a.payload,'$.priorOpKey')=o.op_key AND a.used=0 AND a.expires_at>? AND a.actor_id=? AND a.user_id=m.user_id AND a.version=u.binding_version AND a.thread_id=u.thread_id AND json_extract(a.payload,'$.messageId')=? AND json_extract(a.payload,'$.epoch')=? AND json_extract(a.payload,'$.instance')=? AND o.method='copyMessage' AND o.phase='failed' AND o.result IS NULL AND o.code>=400 AND o.code<500 AND o.code NOT IN (409,429) AND COALESCE(o.reason,'')!='cancelled_by_state'
      AND m.direction='out' AND m.business=1 AND m.admin_id=m.source_id AND m.source_chat=? AND CAST(json_extract(m.payload,'$.from.id') AS TEXT)=a.actor_id
      AND COALESCE((SELECT MAX(d.attempt) FROM delivery_attempts d WHERE d.message_id=m.id AND d.epoch=?),0)=?`,attempt,opKey,p.priorOpKey,action.token,String(cb.from.id),row.user_id,row.thread_id,action.version,e.RECOVERY_EPOCH,time,action.token,p.priorOpKey,row.id,e.RECOVERY_EPOCH,row.user_id,row.thread_id,row.thread_id,action.version,time,String(cb.from.id),m.message_id,e.RECOVERY_EPOCH,e.INSTANCE_ID,e.ADMIN_CHAT_ID,e.RECOVERY_EPOCH,previous),
    stmt(e,"UPDATE callback_actions SET used=? WHERE token=? AND used=0 AND EXISTS(SELECT 1 FROM delivery_attempts WHERE action_token=? AND message_id=? AND attempt=?)",stamp,action.token,action.token,row.id,attempt),
    stmt(e,"INSERT OR IGNORE INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) SELECT ?,user_id,'delivery-retry',json_object('rowId',message_id,'attempt',attempt),'pending',?,epoch,? FROM delivery_attempts WHERE action_token=? AND EXISTS(SELECT 1 FROM callback_actions WHERE token=? AND used=?)",`delivery-retry:${row.id}:${attempt}`,time,time,action.token,action.token,stamp),
  ]);
  const reserved = await one<DeliveryAttempt>(e,'SELECT * FROM delivery_attempts WHERE action_token=?',action.token);
  if (!reserved) return '该失败已被处理或状态已变化，不会重复重试。';
  await wakeWork(e,reserved.user_id);
  return '已保存安全重试；确认成功后更新原消息回执。';
}

export async function deliveryRetryGuardValid(e:Env,g:DeliveryRetryGuard):Promise<boolean> {
  const attempt = await one<DeliveryAttempt>(e,'SELECT * FROM delivery_attempts WHERE message_id=? AND epoch=? ORDER BY attempt DESC LIMIT 1',g.messageId,e.RECOVERY_EPOCH);
  if (!attempt || attempt.attempt!==g.attempt || attempt.state!=='reserved' || attempt.actor_id!==g.actorId || attempt.user_id!==g.userId || attempt.thread_id!==g.threadId || attempt.binding_version!==g.bindingVersion || attempt.prior_op_key!==g.priorOpKey || !authorized(e,g.actorId)) return false;
  const row = await one<MessageRow>(e,'SELECT * FROM messages WHERE id=?',g.messageId),u = await user(e,g.userId);
  if (!row || !u || row.epoch!==e.RECOVERY_EPOCH || row.direction!=='out' || row.business!==1 || row.media_group || row.source_chat!==e.ADMIN_CHAT_ID || row.admin_id!==row.source_id || row.user_id!==g.userId || row.thread_id!==g.threadId || row.delivery!=='failed' || row.private_id!==null || row.tombstone || row.deletion!=='none' || u.thread_id!==g.threadId || u.binding_version!==g.bindingVersion) return false;
  const original = JSON.parse(row.payload) as TGMessage;
  if (String(original.from?.id)!==g.actorId || original.from?.is_bot || original.sender_chat || original.message_id!==row.source_id || String(original.chat?.id)!==e.ADMIN_CHAT_ID || original.message_thread_id!==g.threadId || internalForward(e,original)) return false;
  const prior = await one<{method:string;phase:string;result:string|null;code:number|null;reason:string|null}>(e,'SELECT method,phase,result,code,reason FROM operations WHERE op_key=?',g.priorOpKey);
  return !!prior && prior.method==='copyMessage' && prior.phase==='failed' && prior.result===null && prior.code!==null && prior.code>=400 && prior.code<500 && prior.code!==409 && prior.code!==429 && prior.reason!=='cancelled_by_state';
}
