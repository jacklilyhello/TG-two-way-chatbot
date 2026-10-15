import type {Env,Job} from './types';
import {audit,logEpochStatements,logOutboxStatements,now,one,stmt,user,wakeLogs,wakeWork} from './db';
import {call,RetryLater} from './operations';
import {ensureInitialCardReady,ensureThread,scheduleCard} from './admin';

export interface MatchedBanRule {id:number;kind:'keyword'|'regex';pattern:string;version:number}
export interface BanAction {token:string;stamp:number;version:number;threadId:number;cardId:number}
export interface BanOptions {source:'manual'|'rule';actorId?:string;rule?:MatchedBanRule;action?:BanAction;messageId?:number}
interface BanEvent {
 id:number;user_id:string;blocked:number;changed:number;source:'manual'|'rule';
 rule_id:number|null;rule_kind:'keyword'|'regex'|null;rule_pattern:string|null;rule_version:number|null;
 committed:number;notice_status:'none'|'pending'|'success'|'failed'|'unknown';
}
export interface BanResult {applied:boolean;changed:boolean;eventId:number|null}
const noticeLane=(userId:string)=>`ban:${userId}`;

/** State, action consumption, audit and notice outbox share one transaction.
 * The immutable event key also seals a no-op, so replaying a previously handled
 * advertisement or callback after an unblock cannot silently block again. */
export async function setBlocked(e:Env,job:Job,userId:string,blocked:boolean,options:BanOptions,waitUntil?:(promise:Promise<unknown>)=>void):Promise<BanResult> {
 const desired=Number(blocked),time=now(),action=options.action;
 if(options.source==='rule'&&(!blocked||!options.rule))throw new Error('invalid_rule_ban');
 const eventKey=`${e.RECOVERY_EPOCH}:ban:${options.source}:${action?.token??job.id}`;
 const actionGate=action?' AND binding_version=? AND thread_id=? AND card_id=? AND EXISTS(SELECT 1 FROM callback_actions WHERE token=? AND used=0 AND expires_at>=?)':'';
 const gateArgs=action?[action.version,action.threadId,action.cardId,action.token,time]:[];
 const epochGate=logEpochStatements(e);
 const statements=[
  ...epochGate,
  stmt(e,`INSERT OR IGNORE INTO ban_events(event_key,user_id,blocked,changed,source,actor_id,rule_id,rule_kind,rule_pattern,rule_version,notice_status,created_at) SELECT ?,tgid,?,blocked!=?, ?,?,?,?,?,?,CASE WHEN blocked!=? AND ?=1 THEN 'pending' ELSE 'none' END,? FROM users WHERE tgid=?${actionGate}`,eventKey,desired,desired,options.source,options.actorId??null,options.rule?.id??null,options.rule?.kind??null,options.rule?.pattern??null,options.rule?.version??null,desired,desired,time,userId,...gateArgs),
  stmt(e,"INSERT INTO audit_logs(event,detail,created_at) SELECT ?,?,? FROM ban_events WHERE event_key=? AND committed=0 AND changed=1",blocked?'user_blocked':'user_unblocked',JSON.stringify({user:userId,source:options.source,...(options.actorId?{actor:options.actorId}:{}),...(options.rule?{rule:options.rule.id,kind:options.rule.kind}:{})}),time,eventKey),
  stmt(e,'UPDATE users SET blocked=?,moderation_version=moderation_version+(blocked!=?) WHERE tgid=? AND EXISTS(SELECT 1 FROM ban_events WHERE event_key=? AND committed=0)',desired,desired,userId,eventKey),
  // Content waiting when a ban becomes effective must never reappear after a
  // quick unblock. In-flight requests still use the Bot's final blocked guard.
  ...(blocked?[stmt(e,"UPDATE messages SET delivery='blocked' WHERE user_id=? AND direction='in' AND delivery='pending' AND EXISTS(SELECT 1 FROM ban_events WHERE event_key=? AND committed=0 AND changed=1)",userId,eventKey)]:[]),
  ...(options.source==='rule'?[
   stmt(e,"UPDATE messages SET delivery='blocked' WHERE id=? AND user_id=? AND direction='in' AND delivery='pending' AND EXISTS(SELECT 1 FROM ban_events WHERE event_key=? AND committed=0)",options.messageId??-1,userId,eventKey),
   stmt(e,"UPDATE jobs SET payload=json_set(payload,'$.moderationBlocked',json('true')) WHERE id=? AND EXISTS(SELECT 1 FROM ban_events WHERE event_key=? AND committed=0)",job.id,eventKey)
  ]:[]),
  ...(action?[stmt(e,'UPDATE callback_actions SET used=? WHERE token=? AND used=0 AND EXISTS(SELECT 1 FROM ban_events WHERE event_key=? AND committed=0)',action.stamp,action.token,eventKey)]:[]),
  stmt(e,'UPDATE ban_events SET committed=1 WHERE event_key=? AND committed=0',eventKey),
  stmt(e,"INSERT OR IGNORE INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) SELECT 'ban-notice:'||?||':'||id,?,'ban-notice',json_object('eventId',id),'pending',?,?,? FROM ban_events WHERE event_key=? AND committed=1 AND notice_status='pending'",e.RECOVERY_EPOCH,noticeLane(userId),time,e.RECOVERY_EPOCH,time,eventKey),
  ...logOutboxStatements(e,time),
  stmt(e,'SELECT id,changed,committed FROM ban_events WHERE event_key=?',eventKey)
 ];
 const results=await e.DB.batch(statements);
 const event=results.at(-1)!.results[0] as unknown as Pick<BanEvent,'id'|'changed'|'committed'>|undefined;
 // Hints are detached when called from a coordinator; committed work survives
 // either wake failing and is also discovered by normal recovery scheduling.
 if(event) {
  const wakes=Promise.allSettled([wakeLogs(e),...(blocked&&event.changed?[wakeWork(e,noticeLane(userId))]:[])]);
  if(waitUntil)waitUntil(wakes);else await wakes;
 }
 return {applied:!!event?.committed,changed:!!event?.changed,eventId:event?.id??null};
}

/** Notice text is confined to the administrator group. No source message body
 * or rule pattern is copied into public logs or sent to the user's chat. */
export async function handleBanNotice(e:Env,job:Job,waitUntil?:(promise:Promise<unknown>)=>void):Promise<void> {
 const payload=JSON.parse(job.payload) as {eventId?:number};
 if(!Number.isSafeInteger(payload.eventId)||Number(payload.eventId)<=0)return;
 const event=await one<BanEvent>(e,"SELECT * FROM ban_events WHERE id=? AND committed=1 AND blocked=1 AND changed=1",payload.eventId);
 if(!event||event.notice_status!=='pending'||job.user_key!==noticeLane(event.user_id))return;
 let u=await ensureThread(e,job,event.user_id,false,waitUntil);
 if(!u.thread_id)throw new Error('ban_notice_thread_not_confirmed');
 if(!await ensureInitialCardReady(e,job,u.tgid))throw new RetryLater(now()+5000);
 u=(await user(e,event.user_id))!;
 if(!u.thread_id||!u.card_id)throw new RetryLater(now()+5000);
 await scheduleCard(e,job,u,false,waitUntil);
 const text=event.source==='rule'
  ?`🚫 该用户因命中拦截规则已被自动封禁。\n触发规则：#${event.rule_id} · ${event.rule_kind==='regex'?'正则表达式':'关键词'}\n规则内容：${event.rule_pattern??''}`
  :'🚫 该用户已被封禁。';
 const result=await call<{message_id:number}>(e,`ban-notice:${event.id}`,'sendMessage',{
  chat_id:e.ADMIN_CHAT_ID,message_thread_id:u.thread_id,
  text:text+(u.blocked?'':'\n当前状态：已解除封禁。'),disable_notification:true
 },{threadVersion:{userId:u.tgid,version:u.binding_version,threadId:u.thread_id}});
 const valid=result.status==='success'&&Number.isSafeInteger(result.result?.message_id)&&result.result.message_id>0;
 const status=valid?'success':result.status==='success'?'unknown':result.status;
 const messageId=valid?(result as {status:'success';result:{message_id:number}}).result.message_id:null;
 const changed=await stmt(e,"UPDATE ban_events SET notice_status=?,notice_message_id=?,notice_thread_id=?,notice_binding_version=? WHERE id=? AND notice_status='pending'",status,messageId,u.thread_id,u.binding_version,event.id).run();
 if(changed.meta.changes&&status!=='success')await audit(e,'ban_notice_unavailable',{event:event.id,user:u.tgid,status,...(result.status!=='success'&&result.code?{code:result.code}:{})},waitUntil);
}
