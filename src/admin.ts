import type {Env, Job, Params, TGMessage, TGUpdate, TGUser, UserRow} from './types';
import {all, audit, enqueue, now, one, run, setSetting, setting, stmt, user, logOutboxStatements, wakeLogs, wakeWork, logEpochStatements} from './db';
import {call, notify, RetryLater, type IssueGuard, type Outcome} from './operations';
import {deleteSingle} from './messaging';
import {DEFAULT_FIXED_TEXT} from './settings';
import {admittedBindingMatches,internalForward} from './security';
import {reserveDeliveryRetry} from './delivery-retry';
import {rulesPanel,handleRuleCommand,handleRuleAction,handleRuleInput} from './block-rules';
import {closeRuleSession} from './rule-workflow';
import {broadcastPanel,closeBroadcastSession,handleBroadcastInput,handleBroadcastAction} from './broadcast-workflow';
import {setBlocked} from './moderation';
import {profileLinks,profileButtons,withProfileFallback,profileFallbackDetails,PROFILE_ENTRIES,type ProfileCallbacks,type ProfileEntry} from './profile-links';
export {profileLinks} from './profile-links';

interface Action {
 token: string; kind: string; user_id: string; actor_id: string; thread_id: number | null;
 version: number; payload: string; expires_at: number; used: number;
}
type Button = {text: string; callback_data?: string; url?: string};
type Range = 'today' | '7' | '30' | 'all';
const tokenPattern = /^a:([a-f0-9]{32})$/;
const escape = (value: string) => value.replace(/[&<>\"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]!));
export const authorized = (e: Env, actor: string) => e.ADMIN_IDS.split(/[\s,]+/).includes(actor);
export const isGeneral = (m: TGMessage) => !m.is_topic_message || !m.message_thread_id || m.message_thread_id === 1;
const threadOf = (m: TGMessage) => isGeneral(m) ? null : m.message_thread_id!;
export function validUserId(value: string) {return /^[1-9]\d{0,15}$/.test(value) && Number(value) <= 4503599627370495;}
const displayName = (u: UserRow) => [u.first_name,u.last_name].filter(Boolean).join(' ') || u.tgid;
const actionPayload = (a: Action): Params => JSON.parse(a.payload) as Params;

export function topicLink(e: Env, thread: number, message = thread) {
 return `https://t.me/c/${e.ADMIN_CHAT_ID.replace(/^-100/,'')}/${thread}/${message}`;
}
export async function ensureUser(e: Env, id: string, profile?: TGUser, contactAt = now()): Promise<UserRow> {
 if (!validUserId(id)) throw new Error('invalid_user_id');
 const queries=[stmt(e,"INSERT OR IGNORE INTO users(tgid,first_contact) VALUES(?,MIN(?,COALESCE((SELECT MIN(received_at) FROM messages WHERE user_id=? AND direction='in'),?)))",id,contactAt,id,contactAt)];
 if (profile && String(profile.id) === id) {
  queries.push(stmt(e,'UPDATE users SET first_name=?,last_name=?,username=? WHERE tgid=?',profile.first_name ?? '',profile.last_name ?? '',profile.username ?? null,id));
 }
 queries.push(stmt(e,'SELECT * FROM users WHERE tgid=?',id));
 const results=await e.DB.batch<UserRow>(queries);
 return results.at(-1)!.results[0];
}

async function createAction(e: Env, kind: string, u: UserRow | null, actor: string, thread: number | null, payload: Params = {}, expires = now()+15*60_000, context?: string) {
 const token = crypto.randomUUID().replace(/-/g,'');
 if (context) {
  const where="kind=? AND user_id=? AND actor_id=? AND thread_id IS ? AND version=? AND json_extract(payload,'$.context')=? AND used=0";
  const identity=[kind,u?.tgid ?? '',actor,thread,u?.binding_version ?? 0,context];
  const [,selected]=await e.DB.batch<{token:string}>([
   stmt(e,`INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) SELECT ?,?,?,?,?,?,?,? WHERE NOT EXISTS(SELECT 1 FROM callback_actions WHERE ${where})`,token,kind,u?.tgid ?? '',actor,thread,u?.binding_version ?? 0,JSON.stringify({...payload,instance:e.INSTANCE_ID,context}),expires,...identity),
   stmt(e,`SELECT token FROM callback_actions WHERE ${where}`, ...identity)
  ]);
  return selected.results[0].token;
 }
 await run(e,'INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) VALUES(?,?,?,?,?,?,?,?)',token,kind,u?.tgid ?? '',actor,thread,u?.binding_version ?? 0,JSON.stringify({...payload,instance:e.INSTANCE_ID,...(context ? {context} : {})}),expires);
 return token;
}
async function bind(e: Env, u: UserRow, thread: number, actor: string, action?: Action, stamp?: number,waitUntil?:(promise:Promise<unknown>)=>void,initialCard=false) {
 const logThread = Number(await setting(e,'log_thread_id','0'));
 if (thread <= 1 || thread === logThread) return false;
 const queries: D1PreparedStatement[] = [];
 if (action) queries.push(stmt(e,'UPDATE callback_actions SET used=? WHERE token=? AND used=0 AND expires_at>?',stamp,action.token,now()));
 const gate = action ? ' AND EXISTS(SELECT 1 FROM callback_actions WHERE token=? AND used=?)' : '';
 queries.push(stmt(e,`UPDATE users SET thread_id=?,binding_version=binding_version+1,card_id=NULL WHERE tgid=? AND binding_version=? AND thread_id IS ? AND NOT EXISTS(SELECT 1 FROM users WHERE thread_id=? AND tgid!=?) AND ?!=COALESCE((SELECT CAST(value AS INTEGER) FROM settings WHERE key='log_thread_id'),0)${gate}`,thread,u.tgid,u.binding_version,u.thread_id,thread,u.tgid,thread,...(action ? [action.token,stamp] : [])));
 queries.push(stmt(e,'INSERT INTO thread_history(user_id,thread_id,version,created_at) SELECT tgid,thread_id,binding_version,? FROM users WHERE tgid=? AND thread_id=? AND binding_version=? AND NOT EXISTS(SELECT 1 FROM thread_history WHERE user_id=? AND version=?)',now(),u.tgid,thread,u.binding_version+1,u.tgid,u.binding_version+1));
 if(initialCard)queries.push(stmt(e,"INSERT OR IGNORE INTO settings(key,value) SELECT ?,? WHERE EXISTS(SELECT 1 FROM users WHERE tgid=? AND thread_id=? AND binding_version=?)",`card_initial:${u.tgid}:${u.binding_version+1}`,`pending:${thread}`,u.tgid,thread,u.binding_version+1));
 await e.DB.batch(queries);
 const current = await user(e,u.tgid);
 const success = current?.thread_id === thread && current.binding_version === u.binding_version+1;
 if (success) await audit(e,'binding_changed',{user:u.tgid,from:u.thread_id,to:thread,version:current.binding_version,actor},waitUntil);
 return success;
}
async function retryKnownCreationFailures(e: Env, job: Job, u: UserRow) {
 const candidates = [
  {setting:`thread_create_key:${u.tgid}`,base:`thread:create:${u.tgid}`,needed:!u.thread_id},
  {setting:`card_create_key:${u.tgid}:${u.binding_version}`,base:`card:${u.tgid}:${u.binding_version}:create`,needed:!!u.thread_id&&!u.card_id},
  {setting:`card_initial_pin_key:${u.tgid}:${u.binding_version}`,base:`card:${u.tgid}:${u.binding_version}:initial:pin:${u.card_id}`,needed:!!u.card_id&&(await setting(e,`card_initial:${u.tgid}:${u.binding_version}`)).startsWith('pending:')}
 ];
 for (const candidate of candidates) {
  if (!candidate.needed) continue;
  const key = await setting(e,candidate.setting,candidate.base);
  const previous = await one<{phase:string}>(e,'SELECT phase FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:${key}`);
  // An explicit /tj can repair an API rejection after permissions were fixed.
  // Unknown operations deliberately keep their original key forever.
  if (previous?.phase==='failed') await setSetting(e,candidate.setting,`${candidate.base}:retry:${job.id}`);
 }
}

export async function ensureThread(e: Env, job: Job, userId: string, prepareCard = true,waitUntil?:(promise:Promise<unknown>)=>void): Promise<UserRow> {
 let u = await ensureUser(e,userId,undefined,job.created_at);
 if (u.thread_id) {
  if(prepareCard)await ensureInitialCardReady(e,job,u);
  return (await user(e,userId))!;
 }
 const profile = await call<TGUser>(e,`job:${job.id}:profile:${userId}`,'getChat',{chat_id:userId});
 if (profile.status === 'success') u = await ensureUser(e,userId,profile.result,job.created_at);
 // A stable key is essential: a lost createForumTopic response must never
 // create a second topic when another incoming message wakes this user.
 const creationKey = await setting(e,`thread_create_key:${userId}`,`thread:create:${userId}`);
 const result = await call<{message_thread_id:number}>(e,creationKey,'createForumTopic',{chat_id:e.ADMIN_CHAT_ID,name:displayName(u).slice(0,110)});
 if (result.status !== 'success' || !Number.isSafeInteger(result.result.message_thread_id) || result.result.message_thread_id <= 1) {
  await audit(e,'thread_create_unavailable',{user:userId,status:result.status},waitUntil);
  return u;
 }
 if (!await bind(e,u,result.result.message_thread_id,'system',undefined,undefined,waitUntil,true)) {
  await audit(e,'thread_create_binding_conflict',{user:userId,thread:result.result.message_thread_id},waitUntil);
  return (await user(e,userId))!;
 }
 if(prepareCard) {
  await ensureInitialCardReady(e,job,userId);
  await refreshCard(e,job,userId,false,waitUntil);
 }
 return (await user(e,userId))!;
}

/** Existing-card maintenance has its own durable lane. Newly created topics
 * cross a persisted initial-card gate before any business delivery. */
export async function scheduleCard(e:Env,job:Job,u:UserRow,force=false,waitUntil?:(promise:Promise<unknown>)=>void) {
 if(!u.thread_id)return;
 const key=`card:${u.tgid}`,time=now();
 await e.DB.batch([
  stmt(e,"INSERT OR IGNORE INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) SELECT ?,?,'card',?,'pending',?,?,? WHERE NOT EXISTS(SELECT 1 FROM jobs WHERE user_key=? AND kind='card' AND epoch=? AND state IN ('pending','retry') AND json_extract(payload,'$.version')=?)",`${key}:${u.binding_version}:${job.id}`,key,JSON.stringify({userId:u.tgid,version:u.binding_version,threadId:u.thread_id,force}),time,e.RECOVERY_EPOCH,time,key,e.RECOVERY_EPOCH,u.binding_version),
  ...(force?[stmt(e,"UPDATE jobs SET payload=json_set(payload,'$.force',json('true')) WHERE user_key=? AND kind='card' AND epoch=? AND state IN ('pending','retry') AND json_extract(payload,'$.version')=?",key,e.RECOVERY_EPOCH,u.binding_version)]:[])
 ]);
 // The committed job is recoverable even if either hint fails. A conversation
 // coordinator can keep the hints alive without making content wait for them.
 const wake=wakeWork(e,key);
 if(waitUntil)waitUntil(wake);else await wake;
}

export async function handleCard(e:Env,job:Job) {
 const p=JSON.parse(job.payload) as {userId:string;version:number;threadId:number;force?:boolean;urgent?:boolean};
 const u=await user(e,p.userId);
 if(!u||u.binding_version!==p.version||u.thread_id!==p.threadId)return;
 if(!p.urgent&&await one(e,"SELECT id FROM jobs WHERE user_key=? AND kind IN ('update','clear') AND epoch=? AND state IN ('pending','retry','running') LIMIT 1",u.tgid,e.RECOVERY_EPOCH))throw new RetryLater(now()+1000);
 await refreshCardNow(e,job,u.tgid,p.force===true,p.urgent===true);
}

export async function refreshCard(e:Env,job:Job,userId:string,force=false,waitUntil?:(promise:Promise<unknown>)=>void):Promise<void> {
 const u=await user(e,userId);if(u?.thread_id)await scheduleCard(e,job,u,force,waitUntil);
}

function cardText(u: UserRow, timezone: string, noticeWarning='') {
 const date = new Intl.DateTimeFormat('zh-CN',{timeZone:timezone,dateStyle:'medium',timeStyle:'short'}).format(new Date(u.first_contact));
 return `姓名：${escape(displayName(u))}\nTGID：<code>${u.tgid}</code>\n用户名：${u.username ? '@'+escape(u.username) : '未设置'}\n首次联系：${date} (${escape(timezone)})\n状态：${u.blocked ? '已封禁' : '正常'}\n资料入口受 Telegram 隐私设置与客户端支持限制。${noticeWarning?'\n'+escape(noticeWarning):''}`;
}
// The moderation generation distinguishes normal -> blocked -> normal cycles.
// A retried render must not recover a keyboard whose actions were consumed in
// an earlier occurrence of the same visible state.
const cardSignature = (u: UserRow, timezone: string) => JSON.stringify(['profile-v2',u.card_id,u.first_name,u.last_name,u.username,u.first_contact,u.photo_id,u.blocked,u.moderation_version ?? 0,timezone]);
const cardDigest = async(value:string)=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value)))).map(byte=>byte.toString(16).padStart(2,'0')).join('');
async function keyboard(e: Env, job: Job, u: UserRow, context = `card:${job.id}`) {
 const blockKind=u.blocked?'card_unblock':'card_block';
 const kinds=[blockKind,'card_clear',...PROFILE_ENTRIES.map(({entry})=>`card_profile_${entry}`)];
 const where="kind=? AND user_id=? AND actor_id='*' AND thread_id IS ? AND version=? AND json_extract(payload,'$.context')=? AND used=0";
 // All five controls belong to the same render. Create and recover them in
 // one transaction instead of delaying a state edit with five D1 round trips.
 const results=await e.DB.batch([
  ...kinds.map(kind=>stmt(e,`INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) SELECT ?,?,?,'*',?,?,?,? WHERE NOT EXISTS(SELECT 1 FROM callback_actions WHERE ${where})`,crypto.randomUUID().replace(/-/g,''),kind,u.tgid,u.thread_id,u.binding_version,JSON.stringify({instance:e.INSTANCE_ID,context}),Number.MAX_SAFE_INTEGER,kind,u.tgid,u.thread_id,u.binding_version,context)),
  stmt(e,`SELECT token,kind FROM callback_actions WHERE user_id=? AND actor_id='*' AND thread_id IS ? AND version=? AND json_extract(payload,'$.context')=? AND used=0 AND kind IN (${kinds.map(()=>'?').join(',')}) ORDER BY rowid`,u.tgid,u.thread_id,u.binding_version,context,...kinds)
 ]);
 const tokens=new Map<string,string>();
 for(const row of results.at(-1)!.results as unknown as {token:string;kind:string}[])if(!tokens.has(row.kind))tokens.set(row.kind,row.token);
 const block=tokens.get(blockKind)!,clear=tokens.get('card_clear')!;
 const callbacks={} as ProfileCallbacks;
 const profileTokens:string[]=[];
 for(const {entry} of PROFILE_ENTRIES) {
  const token=tokens.get(`card_profile_${entry}`)!;
  callbacks[entry]='a:'+token;profileTokens.push(token);
 }
 const links = profileLinks(u);
 const markup={inline_keyboard:[
  profileButtons(u),
  [{text:u.blocked ? '解除封禁' : '封禁',callback_data:'a:'+block},{text:'直达对方主页',url:links[0]}],
  [{text:'清空此对话',callback_data:'a:'+clear}]
 ]};
 return {tokens:[block,clear,...profileTokens],callbacks,markup:await setting(e,`card_no_id_links:${u.tgid}`)==='1'?withProfileFallback(markup,callbacks):markup};
}
async function cardCall(e:Env,key:string,method:string,params:Params,guard:IssueGuard,u:UserRow,callbacks:ProfileCallbacks):Promise<Outcome<any>> {
 const result=await call(e,key,method,params,guard);
 // Telegram explicitly rejected these buttons, so this specific correction
 // cannot duplicate a delivered card. Unknown sends remain terminal.
 if(result.status==='failed'&&result.code===400&&/BUTTON_USER_PRIVACY_RESTRICTED/.test(result.reason)&&params.reply_markup) {
  await setSetting(e,`card_no_id_links:${u.tgid}`,'1');
  // Keep the previous release's operation key: it may already identify a
  // delivered card whose D1 card_id write was interrupted. New controls are
  // repaired in place after recovering that known result.
  return call(e,`${key}:without-private-links`,method,{...params,reply_markup:withProfileFallback(params.reply_markup as {inline_keyboard:Button[][]},callbacks)},guard);
 }
 return result;
}
async function recordProblem(e: Env, event: string, u: UserRow, result: Outcome) {
 if (result.status !== 'success') await audit(e,event,{user:u.tgid,thread:u.thread_id,status:result.status,code:result.code ?? null});
}
/** A marker is created atomically with a new binding. The stable send/pin keys
 * also coordinate a recovered business job with the independent card lane.
 * Legacy bindings have no marker and do not pay this initialization cost. */
export async function ensureInitialCardReady(e:Env,job:Job,target:string|UserRow):Promise<boolean> {
 const userId=typeof target==='string'?target:target.tgid;
 let u=typeof target==='string'?await user(e,target):target;
 if(!u?.thread_id)return false;
 const marker=`card_initial:${u.tgid}:${u.binding_version}`;
 const initial=await setting(e,marker);
 if(!initial)return true;
 if(initial===`ready:${u.thread_id}:${u.card_id}`)return true;
 const base=`card:${u.tgid}:${u.binding_version}`;
 const guard:IssueGuard={threadVersion:{userId:u.tgid,version:u.binding_version,threadId:u.thread_id}};
 const fail=async(stage:string,result:Outcome):Promise<never>=>{
  const logged=await run(e,'INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)',`${marker}:problem:${stage}`,result.status);
  if(logged.meta.changes)await recordProblem(e,`initial_card_${stage}_unavailable`,u!,result);
  // A terminal external result must keep business behind this gate. Ordinary
  // coordinator error handling puts it in attention instead of retrying sends.
  throw new Error('initial_card_not_confirmed');
 };
 let cardId=u.card_id;
 if(!cardId) {
  const timezone=await setting(e,'timezone','Asia/Singapore');
  const controls=await keyboard(e,job,u,`${base}:initial`);
  const creationKey=await setting(e,`card_create_key:${u.tgid}:${u.binding_version}`,`${base}:create`);
  const result=await cardCall(e,creationKey,'sendMessage',{chat_id:e.ADMIN_CHAT_ID,message_thread_id:u.thread_id,text:cardText(u,timezone),parse_mode:'HTML',reply_markup:controls.markup},guard,u,controls.callbacks);
  if(result.status!=='success')return fail('create',result);
  if(!Number.isSafeInteger(result.result.message_id)||result.result.message_id<=0)return fail('create',{status:'unknown',reason:'Telegram 返回缺少有效资料卡消息 ID'});
  cardId=result.result.message_id as number;
  await e.DB.batch([
   stmt(e,'UPDATE users SET card_id=? WHERE tgid=? AND binding_version=? AND thread_id=? AND card_id IS NULL',cardId,u.tgid,u.binding_version,u.thread_id),
   stmt(e,"INSERT OR IGNORE INTO settings(key,value) VALUES(?,'text')",`card_media:${u.tgid}:${u.binding_version}`)
  ]);
 }
 const pinKey=await setting(e,`card_initial_pin_key:${u.tgid}:${u.binding_version}`,`${base}:initial:pin:${cardId}`);
 const pin=await call(e,pinKey,'pinChatMessage',{chat_id:e.ADMIN_CHAT_ID,message_id:cardId,disable_notification:true},guard);
 if(pin.status!=='success')return fail('pin',pin);
 await run(e,'UPDATE settings SET value=? WHERE key=? AND EXISTS(SELECT 1 FROM users WHERE tgid=? AND binding_version=? AND thread_id=? AND card_id=?)',`ready:${u.thread_id}:${cardId}`,marker,u.tgid,u.binding_version,u.thread_id,cardId);
 u=await user(e,userId);
 return !!u&&await setting(e,marker)===`ready:${u.thread_id}:${u.card_id}`;
}
/** Only the card coordinator calls this in production. All administrator and
 * incoming-message paths enqueue work on that same lane. */
export async function refreshCardNow(e: Env, job: Job, userId: string, force = false, urgent = false): Promise<void> {
 let u = await user(e,userId);
 if (!u?.thread_id) return;
 if(!await ensureInitialCardReady(e,job,u))return;
 u=await user(e,userId);
 if(!u?.thread_id)return;
 const boundVersion=u.binding_version,boundThread=u.thread_id;
 const guard:IssueGuard={threadVersion:{userId:u.tgid,version:boundVersion,threadId:boundThread,moderationVersion:u.moderation_version??0},...(urgent?{priority:'moderation-card' as const}:{})};
 const photoChecked=urgent?0:Number(await setting(e,`profile_checked:${u.tgid}`,'0'));
 if(!urgent&&(force||!photoChecked||now()-photoChecked>=60000)) {
  const photos = await call<{photos:{file_id:string}[][]}>(e,`job:${job.id}:avatar:${userId}`,'getUserProfilePhotos',{user_id:userId,limit:1});
  if (photos.status === 'success') {
   const photo = photos.result.photos?.[0]?.at(-1)?.file_id ?? null;
   await e.DB.batch([
    stmt(e,'UPDATE users SET photo_id=? WHERE tgid=?',photo,userId),
    stmt(e,'INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',`profile_checked:${u.tgid}`,String(now()))
   ]);
  }
 }
 u = await user(e,userId);
 if(!u||u.binding_version!==boundVersion||u.thread_id!==boundThread)return;
 const timezone = await setting(e,'timezone','Asia/Singapore');
 const failedNotice=await one<{id:number;notice_status:string}>(e,"SELECT id,notice_status FROM ban_events WHERE user_id=? AND committed=1 AND notice_status IN ('failed','unknown') ORDER BY id DESC LIMIT 1",userId);
 const noticeWarning=failedNotice?`⚠ 封禁记录 #${failedNotice.id} 的话题通知${failedNotice.notice_status==='unknown'?'发送结果未知，未自动重发':'发送失败'}；请查看系统日志${failedNotice.notice_status==='unknown'?'或 General 的 /mb unknown':''}核查。当前封禁状态以上方为准。`:'';
 const stateKey = `card_state:${u.tgid}:${u.binding_version}`;
 const signature=(row:UserRow)=>JSON.stringify([cardSignature(row,timezone),noticeWarning]);
 if (!force && u.card_id && await setting(e,stateKey)===signature(u)) return;
 const text = cardText(u,timezone,noticeWarning);
 const signatureDigest=await cardDigest(signature(u));
 // Tokens are immutable and bind to the card and binding, so an old keyboard
 // cannot control a later card even if Telegram kept its message in history.
 const controls = await keyboard(e,job,u,`card:${job.id}:${signatureDigest}`);
 // The same profile can return after an intervening render retired its
 // controls. Fresh tokens require a fresh edit journal even for identical text.
 const render=await cardDigest(JSON.stringify([signatureDigest,controls.tokens]));
 let reply_markup = controls.markup;
 const mediaKey = `card_media:${u.tgid}:${u.binding_version}`;
 const existingMedia = await setting(e,mediaKey,'text');
 const base = `card:${u.tgid}:${u.binding_version}`;
 let cardId = u.card_id;
 let updated = true;
 // New topics deliberately keep the original text card. Converting it to a
 // photo would move the current pinned panel behind the first business item.
 const initialText=existingMedia==='text'&&!!await setting(e,`card_initial:${u.tgid}:${u.binding_version}`);
 const replace = !urgent&&cardId && ((!initialText&&existingMedia === 'text' && u.photo_id) || (existingMedia !== 'text' && !u.photo_id));
 if (!cardId || replace) {
  const creationKey = replace ? `${base}:replace:${cardId}:${u.photo_id ?? 'text'}` : await setting(e,`card_create_key:${u.tgid}:${u.binding_version}`,`${base}:create`);
  const result = u.photo_id
   ? await cardCall(e,creationKey,'sendPhoto',{chat_id:e.ADMIN_CHAT_ID,message_thread_id:u.thread_id,photo:u.photo_id,caption:text,parse_mode:'HTML',reply_markup},guard,u,controls.callbacks)
   : await cardCall(e,creationKey,'sendMessage',{chat_id:e.ADMIN_CHAT_ID,message_thread_id:u.thread_id,text,parse_mode:'HTML',reply_markup},guard,u,controls.callbacks);
  if (result.status !== 'success' || !Number.isSafeInteger(result.result.message_id) || result.result.message_id <= 0) {
   await recordProblem(e,'card_create_unavailable',u,result);
   return;
  }
  cardId = result.result.message_id;
  await e.DB.batch([
   stmt(e,'UPDATE users SET card_id=? WHERE tgid=? AND binding_version=? AND thread_id=?',cardId,u.tgid,u.binding_version,u.thread_id),
   stmt(e,'INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',mediaKey,u.photo_id ?? 'text')
  ]);
  if (u.card_id && u.card_id !== cardId) {
   const unpin = await call(e,`${base}:unpin:${u.card_id}`,'unpinChatMessage',{chat_id:e.ADMIN_CHAT_ID,message_id:u.card_id},guard);
   await recordProblem(e,'card_unpin_failed',u,unpin);
  }
 } else {
  const result = existingMedia !== 'text'
   ? u.photo_id && u.photo_id !== existingMedia
    ? await cardCall(e,`job:${job.id}:card:${render}:media:${cardId}`,'editMessageMedia',{chat_id:e.ADMIN_CHAT_ID,message_id:cardId,media:{type:'photo',media:u.photo_id,caption:text,parse_mode:'HTML'},reply_markup},guard,u,controls.callbacks)
    : await cardCall(e,`job:${job.id}:card:${render}:caption:${cardId}`,'editMessageCaption',{chat_id:e.ADMIN_CHAT_ID,message_id:cardId,caption:text,parse_mode:'HTML',reply_markup},guard,u,controls.callbacks)
   : await cardCall(e,`job:${job.id}:card:${render}:text:${cardId}`,'editMessageText',{chat_id:e.ADMIN_CHAT_ID,message_id:cardId,text,parse_mode:'HTML',reply_markup},guard,u,controls.callbacks);
  await recordProblem(e,'card_update_failed',u,result);
  updated = result.status==='success';
  if (result.status === 'success' && u.photo_id && existingMedia !== 'text') await setSetting(e,mediaKey,u.photo_id);
 }
 // A recovered create may return an already-created card with another job's
 // keyboard. Establish this generation before retiring older controls.
 if(await setting(e,`card_no_id_links:${u.tgid}`)==='1')reply_markup=withProfileFallback(reply_markup,controls.callbacks);
 // Existing-card moderation edits already update text and controls together.
 // Avoid a second edit and pin request delaying the visible result. Creation
 // and ordinary profile repair retain their full keyboard/pinning checks.
 const fastEdit=urgent&&!!u.card_id&&!replace&&updated;
 const markup:Outcome=fastEdit?{status:'success',result:true}:await cardCall(e,`job:${job.id}:card:${render}:buttons:${cardId}`,'editMessageReplyMarkup',{chat_id:e.ADMIN_CHAT_ID,message_id:cardId,reply_markup},guard,u,controls.callbacks);
 await recordProblem(e,'card_buttons_failed',u,markup);
 if (markup.status==='success') await run(e,`UPDATE callback_actions SET used=-1 WHERE user_id=? AND version=? AND kind LIKE 'card_%' AND used=0 AND token NOT IN (${controls.tokens.map(()=>'?').join(',')})`,u.tgid,u.binding_version,...controls.tokens);
 const pin:Outcome=fastEdit?{status:'success',result:true}:await call(e,`job:${job.id}:card:${render}:pin:${cardId}`,'pinChatMessage',{chat_id:e.ADMIN_CHAT_ID,message_id:cardId,disable_notification:true},guard);
 await recordProblem(e,'card_pin_failed',u,pin);
 if (updated && markup.status==='success' && pin.status==='success') {
  const current=await user(e,userId);
  if(!current||current.binding_version!==boundVersion||current.thread_id!==boundThread)return;
  if(signature(current)!==signature({...u,card_id:cardId}))throw new RetryLater(now()+100);
  await setSetting(e,stateKey,signature(current));
 }
 const latest=await user(e,userId);
 if(latest&&latest.binding_version===boundVersion&&latest.thread_id===boundThread&&latest.moderation_version!==u.moderation_version)throw new RetryLater(now()+100);
}

function partsAt(timestamp: number, timezone: string) {
 const parts = new Intl.DateTimeFormat('en-CA',{timeZone:timezone,year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'}).formatToParts(timestamp);
 return Object.fromEntries(parts.filter(p=>p.type!=='literal').map(p=>[p.type,Number(p.value)]));
}
/** Inclusive local calendar days, including today; values stored in D1 are UTC. */
export function rangeStart(timezone: string, range: Range, timestamp = now()) {
 if (range === 'all') return 0;
 const local = partsAt(timestamp,timezone);
 const days = range === 'today' ? 0 : Number(range)-1;
 const calendar = new Date(Date.UTC(local.year,local.month-1,local.day-days));
 const desired = calendar.getTime();
 // Find the first instant in the requested calendar day. Some timezones move
 // clocks forward at midnight, so local 00:00 may not exist at all.
 const formatter = new Intl.DateTimeFormat('en-CA',{timeZone:timezone,year:'numeric',month:'2-digit',day:'2-digit'});
 let left = desired-36*60*60_000;
 let right = desired+36*60*60_000;
 while (left<right) {
  const middle = Math.floor((left+right)/2);
  const p = Object.fromEntries(formatter.formatToParts(middle).filter(p=>p.type!=='literal').map(p=>[p.type,Number(p.value)]));
  if (Date.UTC(p.year,p.month-1,p.day)<desired) left=middle+1;
  else right=middle;
 }
 return left;
}
export async function statistics(e: Env, range: Range, timestamp = now()) {
 const timezone = await setting(e,'timezone','Asia/Singapore');
 const start = rangeStart(timezone,range,timestamp);
 const users = (await one<{total:number;blocked:number}>(e,'SELECT COUNT(*) total,COALESCE(SUM(blocked),0) blocked FROM users'))!;
 const messages = (await one<{total:number;success:number;failed:number;pending:number;unknown:number;active:number}>(e,`SELECT COUNT(*) total,
 COALESCE(SUM(direction='out' AND delivery='success'),0) success,
 COALESCE(SUM(direction='out' AND delivery='failed'),0) failed,
 COALESCE(SUM(direction='out' AND delivery IN ('pending','deferred','blocked','suppressed')),0) pending,
 COALESCE(SUM(direction='out' AND delivery='unknown'),0) unknown,
 COUNT(DISTINCT CASE WHEN direction='in' THEN user_id END) active
 FROM messages WHERE business=1 AND received_at>=? AND received_at<=?`,start,timestamp))!;
 const today = (await one<{count:number}>(e,'SELECT COUNT(*) count FROM messages WHERE business=1 AND received_at>=? AND received_at<=?',rangeStart(timezone,'today',timestamp),timestamp))!.count;
 const anomalies = (await one<{count:number}>(e,"SELECT COUNT(*) count FROM operations WHERE phase IN ('failed','unknown') AND method NOT IN ('copyMessage','sendMessage','sendPhoto','sendVideo','sendAudio','sendVoice','sendDocument','sendSticker','sendAnimation','sendMediaGroup') AND created_at>=? AND created_at<=?",start,timestamp))!.count;
 const denominator = messages.success+messages.failed;
 return {timezone,start,users,messages,today,anomalies,rate:denominator ? (messages.success/denominator*100).toFixed(1)+'%' : '暂无已确认投递'};
}
async function panelButton(e: Env, job:Job, label: string, actor: string, payload: Params): Promise<Button> {
 return {text:label,callback_data:'a:'+await createAction(e,'panel',null,actor,null,payload,now()+15*60_000,`panel:${job.id}:${JSON.stringify(payload)}`)};
}
async function sendPanel(e:Env,key:string,text:string,buttons:Button[][]) {
 const sent=await notify(e,key,null,text,{reply_markup:{inline_keyboard:buttons}});
 if(sent.status==='success') {
  const tokens=buttons.flat().flatMap(b=>b.callback_data?[b.callback_data.slice(2)]:[]);
  if(tokens.length)await e.DB.batch(tokens.map(token=>stmt(e,"UPDATE callback_actions SET payload=json_set(payload,'$.messageId',?) WHERE token=?",sent.result.message_id,token)));
 }
}
async function panel(e: Env, job: Job, actor: string, range: Range) {
 const s = await statistics(e,range);
 const label = {today:'今日','7':'最近 7 天（含今天）','30':'最近 30 天（含今天）',all:'全部'}[range];
 const text = `管理面板 · ${label}\n时区：${s.timezone}\n总注册用户：${s.users.total}\n当前封禁：${s.users.blocked}\n业务消息量：${s.messages.total}\n今日业务消息：${s.today}\n管理员发送成功：${s.messages.success}\n明确失败 / 未送达：${s.messages.failed}\n成功率：${s.rate}\n待处理 / 已阻止：${s.messages.pending}\n结果未知：${s.messages.unknown}\n其他操作异常：${s.anomalies}\n活跃用户：${s.messages.active}\n统计仅含新系统业务原消息；成功表示 API 确认送达，不表示已读。`;
 const ranges: Range[] = ['today','7','30','all'];
 const buttons: Button[] = [];
 for (const r of ranges) buttons.push(await panelButton(e,job,{today:'今日','7':'7 天','30':'30 天',all:'全部'}[r],actor,{view:'stats',range:r}));
 await sendPanel(e,`job:${job.id}:panel:${range}`,text,[buttons,[await panelButton(e,job,'用户列表',actor,{view:'users',page:0}),await panelButton(e,job,'系统设置',actor,{view:'settings'})],[await panelButton(e,job,'拦截规则',actor,{view:'rules'}),await panelButton(e,job,'管理员广播',actor,{view:'broadcast'})]]);
}
async function usersPage(e: Env, job: Job, actor: string, page: number) {
 const total = (await one<{count:number}>(e,'SELECT COUNT(*) count FROM users'))!.count;
 const maxPage = Math.max(0,Math.ceil(total/8)-1);
 page = Math.max(0,Math.min(maxPage,Number.isFinite(page) ? Math.floor(page) : 0));
 const rows = await all<UserRow>(e,'SELECT * FROM users ORDER BY first_contact DESC,tgid LIMIT 8 OFFSET ?',page*8);
 const buttons: Button[][] = [];
 for (const u of rows) buttons.push([await panelButton(e,job,`${u.blocked?'🚫 ':''}${displayName(u).slice(0,35)} · ${u.tgid}`,actor,{view:'user',id:u.tgid})]);
 const navigation: Button[] = [];
 if (page>0) navigation.push(await panelButton(e,job,'上一页',actor,{view:'users',page:page-1}));
 if (page<maxPage) navigation.push(await panelButton(e,job,'下一页',actor,{view:'users',page:page+1}));
 if (navigation.length) buttons.push(navigation);
 await sendPanel(e,`job:${job.id}:users:${page}`,`用户列表 · ${page+1}/${maxPage+1} · 共 ${total} 人\n按 TGID 查找：/mb search TGID`,buttons);
}
async function userDetail(e: Env, job: Job, id: string) {
 const u = await user(e,id);
 if (!u) {await notify(e,`job:${job.id}:user:missing`,null,'未找到该 TGID。');return;}
 const counts = (await one<{incoming:number;outgoing:number;success:number;failed:number;unknown:number}>(e,`SELECT COALESCE(SUM(direction='in'),0) incoming,COALESCE(SUM(direction='out'),0) outgoing,COALESCE(SUM(direction='out' AND delivery='success'),0) success,COALESCE(SUM(direction='out' AND delivery='failed'),0) failed,COALESCE(SUM(direction='out' AND delivery='unknown'),0) unknown FROM messages WHERE user_id=? AND business=1`,id))!;
 const histories = await all<{thread_id:number;version:number}>(e,'SELECT thread_id,version FROM thread_history WHERE user_id=? ORDER BY version DESC LIMIT 10',id);
 const text = `${cardText(u,await setting(e,'timezone','Asia/Singapore'))}\n当前绑定版本：${u.binding_version}\n当前话题：${u.thread_id ?? '未绑定'}\n用户消息：${counts.incoming}\n管理员消息：${counts.outgoing}\n成功 / 明确失败 / 未知：${counts.success} / ${counts.failed} / ${counts.unknown}\n最近绑定历史：${histories.map(h=>`${h.thread_id}(v${h.version})`).join('、') || '无'}`;
 await notify(e,`job:${job.id}:user:${id}`,null,text,{parse_mode:'HTML',reply_markup:{inline_keyboard:u.thread_id ? [[{text:'跳转当前话题',url:topicLink(e,u.thread_id,u.card_id ?? u.thread_id)}]] : []}});
}
async function settingsPanel(e: Env, job: Job) {
 await notify(e,`job:${job.id}:settings`,null,`系统设置\n时区：${await setting(e,'timezone','Asia/Singapore')}\n日志话题：${await setting(e,'log_thread_id','尚未就绪')}\n\n修改：\n/mb timezone Asia/Singapore\n/mb start 新欢迎文案\n/mb help 新帮助文案\n/mb logtopic 专用日志话题ID\n/mb unknown 查看结果未知操作（不自动重发）\n文案变量：{name}、{first_name}、{last_name}、{username}、{tgid}。无用户名时 {username} 显示“未设置”。`);
 await notify(e,`job:${job.id}:settings:start`,null,`欢迎文案：\n${await setting(e,'start_text',DEFAULT_FIXED_TEXT.start)}`);
 await notify(e,`job:${job.id}:settings:help`,null,`帮助文案：\n${await setting(e,'help_text',DEFAULT_FIXED_TEXT.help)}`);
}
async function adminSettingApplied(e:Env,job:Job,key:string) {
 if(job.epoch!==e.RECOVERY_EPOCH)throw new Error('admin_setting_epoch_mismatch');
 const saved=await one<{applied:string|null}>(e,"SELECT json_extract(payload,'$.adminSettingApplied') applied FROM jobs WHERE id=? AND epoch=?",job.id,e.RECOVERY_EPOCH);
 if(!saved)throw new Error('admin_setting_job_unavailable');
 if(saved.applied!==null&&saved.applied!==key)throw new Error('admin_setting_job_mismatch');
 return saved.applied===key;
}
/** The mutation, its audit and the original job's completion marker commit
 * together. A deferred confirmation can never reapply an old setting. */
async function applyAdminSetting(e:Env,job:Job,key:string,value:string,event:string,detail:Params,thread?:number,waitUntil?:(promise:Promise<unknown>)=>void) {
 const epochGate=logEpochStatements(e);
 const pending="EXISTS(SELECT 1 FROM jobs WHERE id=? AND epoch=? AND json_extract(payload,'$.adminSettingApplied') IS NULL)";
 const available=thread===undefined?'':" AND NOT EXISTS(SELECT 1 FROM users WHERE thread_id=?) AND NOT EXISTS(SELECT 1 FROM thread_history WHERE thread_id=?)";
 const args=[job.id,e.RECOVERY_EPOCH,...(thread===undefined?[]:[thread,thread])];
 const results=await e.DB.batch([
  ...epochGate,
  stmt(e,`INSERT INTO settings(key,value) SELECT ?,? WHERE ${pending}${available} ON CONFLICT(key) DO UPDATE SET value=excluded.value`,key,value,...args),
  stmt(e,`INSERT INTO audit_logs(event,detail,created_at) SELECT ?,?,? WHERE ${pending}${available} AND EXISTS(SELECT 1 FROM settings WHERE key=? AND value=?)`,event,JSON.stringify(detail),now(),...args,key,value),
  stmt(e,`UPDATE jobs SET payload=json_set(payload,'$.adminSettingApplied',?) WHERE id=? AND epoch=? AND json_extract(payload,'$.adminSettingApplied') IS NULL${available} AND EXISTS(SELECT 1 FROM settings WHERE key=? AND value=?)`,key,...args,key,value),
  ...logOutboxStatements(e)
 ]);
 if(results.slice(epochGate.length+3).some(result=>result.meta.changes>0)) {
  const wake=wakeLogs(e);
  if(waitUntil)waitUntil(wake);else await wake;
 }
 return adminSettingApplied(e,job,key);
}
async function unknownOperations(e: Env, job: Job) {
 const rows = await all<{op_key:string;method:string;created_at:number}>(e,"SELECT op_key,method,created_at FROM operations WHERE phase='unknown' ORDER BY created_at DESC LIMIT 20");
 const timezone = await setting(e,'timezone','Asia/Singapore');
 const formatter = new Intl.DateTimeFormat('zh-CN',{timeZone:timezone,dateStyle:'short',timeStyle:'short'});
 const lines = rows.map(r=>`${r.method}\n${r.op_key.length>100?r.op_key.slice(0,100)+'…':r.op_key}\n${formatter.format(r.created_at)}`);
 await notify(e,`job:${job.id}:unknown`,null,`最近结果未知的操作（最多 20 项）\n${lines.join('\n\n') || '无'}\n\n结果未知不能视为未发送。请在 Telegram 核实实际状态，保留操作编号并按恢复手册处理；此入口不会重发。`);
}

async function confirmation(e: Env, job: Job, u: UserRow, actor: string, thread: number, kind: 'clear_confirm'|'bind_confirm', text: string, payload: Params) {
 const context = `confirmation:${job.id}:${kind}`;
 const confirm = await createAction(e,kind,u,actor,thread,payload,now()+15*60_000,context);
 const cancel = await createAction(e,'cancel',u,actor,thread,{target:confirm},now()+15*60_000,context);
 const sent = await notify(e,`job:${job.id}:confirmation:${kind}`,thread,text,{reply_markup:{inline_keyboard:[[{text:'确认',callback_data:'a:'+confirm},{text:'取消',callback_data:'a:'+cancel}]]}});
 if (sent.status === 'success') {
  await e.DB.batch([
   stmt(e,'UPDATE callback_actions SET payload=? WHERE token=?',JSON.stringify({...payload,instance:e.INSTANCE_ID,context,messageId:sent.result.message_id}),confirm),
   stmt(e,'UPDATE callback_actions SET payload=? WHERE token=?',JSON.stringify({target:confirm,instance:e.INSTANCE_ID,context,messageId:sent.result.message_id}),cancel)
  ]);
 }
}
async function answer(e: Env, job: Job, id: string, text: string,showAlert=true) {
 await call(e,`job:${job.id}:answer:${id}`,'answerCallbackQuery',{callback_query_id:id,text,show_alert:showAlert});
}
async function callbacks(e: Env, job: Job, update: TGUpdate, waitUntil?:(promise:Promise<unknown>)=>void) {
 const cb = update.callback_query!;
 const m = cb.message;
 if (!m || String(m.chat.id)!==e.ADMIN_CHAT_ID || !authorized(e,String(cb.from.id)) || String(m.from?.id)!==e.EXPECTED_BOT_ID) return;
 const match = tokenPattern.exec(cb.data ?? '');
 const a = match ? await one<Action>(e,'SELECT * FROM callback_actions WHERE token=?',match[1]) : null;
 const stamp = update.update_id+1;
 const completedBan=a&&a.used>0&&(a.kind==='card_block'||a.kind==='card_unblock');
 if (!a || (a.expires_at<now() && !(a.kind==='clear_confirm' && a.used===stamp)) || (a.actor_id!=='*' && a.actor_id!==String(cb.from.id)) || a.thread_id!==threadOf(m) || (a.used!==0 && a.used!==stamp&&!completedBan)) {await answer(e,job,cb.id,'按钮已失效或无权操作。');return;}
 const payload = actionPayload(a);
 if (payload.instance!==e.INSTANCE_ID) {await answer(e,job,cb.id,'实例不匹配。');return;}
 if (a.kind==='panel') {
  if (!isGeneral(m) || payload.messageId!==m.message_id) {await answer(e,job,cb.id,'管理面板仅限原 General 消息。');return;}
  await closeRuleSession(e,String(cb.from.id));
  await closeBroadcastSession(e,String(cb.from.id));
  await run(e,'UPDATE callback_actions SET used=? WHERE token=? AND used=0',stamp,a.token);
  if (payload.view==='stats' && ['today','7','30','all'].includes(String(payload.range))) await panel(e,job,String(cb.from.id),payload.range as Range);
  else if (payload.view==='users') await usersPage(e,job,String(cb.from.id),Number(payload.page)||0);
  else if (payload.view==='user' && validUserId(String(payload.id))) await userDetail(e,job,String(payload.id));
  else if (payload.view==='settings') await settingsPanel(e,job);
  else if (payload.view==='rules') await rulesPanel(e,job,String(cb.from.id));
  else if (payload.view==='broadcast') await broadcastPanel(e,job,String(cb.from.id));
  await answer(e,job,cb.id,'已更新。',false);return;
 }
 if(a.kind.startsWith('broadcast_')) {
  if(!isGeneral(m)||payload.messageId!==m.message_id) {await answer(e,job,cb.id,'广播管理仅限原 General 消息。');return;}
  let answered=false;
  const text=await handleBroadcastAction(e,job,a,payload,stamp,String(cb.from.id),waitUntil,()=>panel(e,job,String(cb.from.id),'today'),async message=>{await answer(e,job,cb.id,message,false);answered=true;});
  if(!answered)await answer(e,job,cb.id,text,false);return;
 }
 if(a.kind.startsWith('rule_')) {
  if(!isGeneral(m)||payload.messageId!==m.message_id) {await answer(e,job,cb.id,'规则管理仅限原 General 消息。');return;}
  await closeBroadcastSession(e,String(cb.from.id));
  let answered=false;
  const text=await handleRuleAction(e,job,a,payload,stamp,String(cb.from.id),waitUntil,()=>panel(e,job,String(cb.from.id),'today'),async message=>{await answer(e,job,cb.id,message,false);answered=true;});
  if(!answered)await answer(e,job,cb.id,text,false);return;
 }
 const u = await user(e,a.user_id);
 if (a.kind==='clear_confirm') {
  const batch = await one<{id:string}>(e,'SELECT id FROM deletion_batches WHERE id=? AND user_id=?',a.token,a.user_id);
  // The webhook transaction, not deferred callback processing, captures the
  // boundary. Taking a later snapshot here would delete post-confirm traffic.
  // An already accepted snapshot survives a later binding switch or expiry.
  if (a.used!==stamp || !batch || payload.messageId!==m.message_id) {await answer(e,job,cb.id,'确认已过期或范围未保存，请重新发起。');return;}
  await enqueue(e,a.user_id,'clear',{batchId:a.token},`clear:${a.token}`);
  await answer(e,job,cb.id,'已保存清空范围，将逐条删除并报告实际结果。');return;
 }
 if (!u || u.binding_version!==a.version) {
  // Replaying the same bind-confirm job after its transaction is safe; all
  // external operations still use the original job's ledger keys.
  if (!(a.kind==='bind_confirm' && a.used===stamp && u?.binding_version===a.version+1 && u.thread_id===Number(payload.to))) {await answer(e,job,cb.id,'绑定已变化，请使用当前资料卡。');return;}
 }
 if (!u) return;
 if (a.kind.startsWith('card_')) {
  if (u.thread_id!==a.thread_id || u.card_id!==m.message_id) {await answer(e,job,cb.id,'旧资料卡已失效，请使用当前话题。');return;}
 } else if (payload.messageId!==m.message_id) {await answer(e,job,cb.id,'确认消息不匹配。');return;}
 if(completedBan&&a.used!==stamp) {
  await answer(e,job,cb.id,`此操作已处理；当前${u.blocked?'已封禁':'已解除封禁'}，请使用最新资料卡。`,false);return;
 }
 if (a.kind==='delivery_retry') {
  await answer(e,job,cb.id,await reserveDeliveryRetry(e,job,update,a,u));return;
 }
 if (a.kind==='card_block' || a.kind==='card_unblock') {
  const blocked = a.kind==='card_block' ? 1 : 0;
  if (a.used===0) {
   const result=await setBlocked(e,job,u.tgid,blocked===1,{source:'manual',actorId:String(cb.from.id),action:{token:a.token,stamp,version:a.version,threadId:a.thread_id!,cardId:m.message_id}},waitUntil);
   if(!result.applied) {await answer(e,job,cb.id,'绑定或资料卡已变化，请使用当前资料卡。');return;}
  } else if (u.blocked!==blocked) {
   await answer(e,job,cb.id,'操作已处理，请使用当前资料卡。');return;
  }
  await answer(e,job,cb.id,blocked ? '已封禁；用户端保持静默。' : '已解除封禁。');
 } else if(a.kind.startsWith('card_profile_')) {
  const entry=a.kind.slice('card_profile_'.length) as ProfileEntry;
  if(!PROFILE_ENTRIES.some(item=>item.entry===entry)) {await answer(e,job,cb.id,'资料入口已失效。');return;}
  const details=profileFallbackDetails(u,entry);
  const result=await notify(e,`job:${job.id}:profile-help`,u.thread_id,details.text,{reply_markup:details.reply_markup});
  await answer(e,job,cb.id,result.status==='success'?'兼容说明已记录在本话题，可复制 TGID 核对目标。':result.status==='unknown'?'兼容说明发送结果未知，请查看话题；未自动重发。':'兼容说明发送失败，请核对机器人话题权限。');
 } else if (a.kind==='card_clear') {
  await confirmation(e,job,u,String(cb.from.id),a.thread_id!,'clear_confirm','确认清空此对话？仅删除新系统已知的用户私聊消息。管理员历史与话题保留；旧系统未记录历史无法追溯、数量未知。确认后新消息不在本次范围。',{});
  await answer(e,job,cb.id,'请在确认消息中选择。');
 } else if (a.kind==='bind_confirm') {
  const to = Number(payload.to);
  if (a.used!==stamp || u.binding_version===a.version) {
   if (!await bind(e,u,to,String(cb.from.id),a,stamp,waitUntil)) {await answer(e,job,cb.id,'绑定状态已变化或目标话题已被占用，请重新 /tj。');return;}
  }
  await refreshCard(e,job,u.tgid,false,waitUntil);
  await answer(e,job,cb.id,'已切换；原话题与消息映射已保留。');
 } else if (a.kind==='cancel') {
  const target = await one<{used:number}>(e,'SELECT used FROM callback_actions WHERE token=?',payload.target);
  if (target && target.used>0) {await answer(e,job,cb.id,'操作已经确认，无法再取消。');return;}
  await e.DB.batch([
   stmt(e,'UPDATE callback_actions SET used=? WHERE token=? AND used=0',stamp,a.token),
   stmt(e,'UPDATE callback_actions SET used=-1 WHERE token=? AND actor_id=? AND used=0',payload.target,String(cb.from.id))
  ]);
  const after = await one<{used:number}>(e,'SELECT used FROM callback_actions WHERE token=?',payload.target);
  await answer(e,job,cb.id,after?.used===-1 ? '已取消。' : after && after.used>0 ? '操作已经确认，无法再取消。' : '确认按钮已失效。');
 } else await answer(e,job,cb.id,'按钮已失效。');
}

async function register(e: Env, job: Job, m: TGMessage, id: string,waitUntil?:(promise:Promise<unknown>)=>void) {
 const thread = threadOf(m);
 const reply = (text: string, extra: Params = {}) => notify(e,`job:${job.id}:register:reply`,thread,text,extra);
 if (!validUserId(id)) {await reply('用法：/tj TGID（正整数用户 ID）。');return;}
 if (thread===Number(await setting(e,'log_thread_id','0'))) {await reply('系统日志话题不能绑定用户。');return;}
 let u = await ensureUser(e,id,undefined,job.created_at);
 const profile = await call<TGUser>(e,`job:${job.id}:profile:${id}`,'getChat',{chat_id:id});
 if (profile.status==='success') u = await ensureUser(e,id,profile.result,job.created_at);
 await retryKnownCreationFailures(e,job,u);
 if (!thread) {
  u = await ensureThread(e,job,id,true,waitUntil);
  if (u.thread_id) {
   await refreshCard(e,job,id,true,waitUntil);
   await reply('已登记。首次联系与当前绑定已保留。注册不保证能够主动私信从未联系机器人的用户。',{reply_markup:{inline_keyboard:[[{text:'打开当前话题',url:topicLink(e,u.thread_id,u.card_id ?? u.thread_id)}]]}});
  } else await reply('话题未就绪。创建失败或结果未知已记账；不会盲目重复创建，请检查系统日志。');
  return;
 }
 const occupant = await one<{tgid:string}>(e,'SELECT tgid FROM users WHERE thread_id=?',thread);
 if (occupant && occupant.tgid!==id) {await reply('当前话题已属于另一用户，拒绝绑定。');return;}
 if (u.thread_id===thread) {
  await refreshCard(e,job,id,true,waitUntil);await reply('已绑定当前话题；无需重复登记。');return;
 }
 if (u.thread_id) {
  await confirmation(e,job,u,String(m.from!.id),thread,'bind_confirm',`TGID ${id} 当前绑定话题 ${u.thread_id}。确认切换到本话题？原话题与历史保留，切换后原话题停止普通转发。`,{to:thread,from:u.thread_id});return;
 }
 if (!await bind(e,u,thread,String(m.from!.id),undefined,undefined,waitUntil)) {await reply('绑定状态已变化或话题被占用，请重新 /tj。');return;}
 await refreshCard(e,job,id,false,waitUntil);await reply('已绑定当前话题，管理员历史已保留。');
}

/** All callback traffic and slash commands are management, never chat payload. */
export async function handleAdmin(e: Env, job: Job, update: TGUpdate, waitUntil?:(promise:Promise<unknown>)=>void): Promise<boolean> {
 if (update.callback_query) {await callbacks(e,job,update,waitUntil);return true;}
 const m = update.message ?? update.edited_message;
 if (!m || String(m.chat.id)!==e.ADMIN_CHAT_ID) return false;
 if (!authorized(e,String(m.from?.id)) || m.sender_chat || m.from?.is_bot) return true;
 if (internalForward(e,m)) return true;
 if(await handleBroadcastInput(e,job,update,waitUntil))return true;
 if(await handleRuleInput(e,job,update,waitUntil))return true;
 const text = m.text ?? '';
 const command = /^\/([a-zA-Z]+)(?:@([A-Za-z0-9_]+))?(?:\s+([\s\S]*))?$/.exec(text);
 if (!text.startsWith('/')) {
  const thread = threadOf(m);
  if (thread && !await one(e,'SELECT tgid FROM users WHERE thread_id=?',thread)) {
   const former = await one<UserRow>(e,'SELECT u.* FROM thread_history h JOIN users u ON u.tgid=h.user_id WHERE h.thread_id=? AND u.thread_id IS NOT ? ORDER BY h.id DESC LIMIT 1',thread,thread);
   if (former) {
    await run(e,"UPDATE messages SET delivery='suppressed' WHERE source_chat=? AND source_id=? AND delivery='pending'",String(m.chat.id),m.message_id);
    await notify(e,`job:${job.id}:retired`,thread,'本话题已停止普通转发，请前往当前有效话题。',{reply_markup:{inline_keyboard:former.thread_id ? [[{text:'打开当前话题',url:topicLink(e,former.thread_id,former.card_id ?? former.thread_id)}]] : []}});
    return true;
   }
  }
  return false;
 }
 if (!command || update.edited_message || m.forward_origin) return true;
 if (command[2]) {
  const botName = await setting(e,'bot_username','');
  if (!botName || command[2].toLowerCase()!==botName.toLowerCase()) return true;
 }
 const name = command[1].toLowerCase();
 const args = (command[3] ?? '').trim();
 const thread = threadOf(m);
 if (name==='tj') {await register(e,job,m,args,waitUntil);return true;}
 if (name==='sc') {
  const u = thread ? await one<UserRow>(e,'SELECT * FROM users WHERE thread_id=?',thread) : null;
  if (!u) {await notify(e,`job:${job.id}:sc:invalid`,thread,'请在当前有效用户话题中回复一条已知消息执行 /sc。');return true;}
  if (!await admittedBindingMatches(e,job,m,u)) {await notify(e,`job:${job.id}:sc:stale`,thread,'绑定已变化，旧删除命令未执行。请在当前话题重新确认目标。');return true;}
  await deleteSingle(e,job,m,u,waitUntil);
  return true;
 }
 if (name!=='mb') return true;
 if (!isGeneral(m)) {await notify(e,`job:${job.id}:mb:invalid`,thread,'/mb 仅限 General 主话题。');return true;}
 const [view,...rest] = args.split(/\s+/);
 const value = rest.join(' ');
 if (!view || ['today','7','30','all'].includes(view)) await panel(e,job,String(m.from!.id),(view || 'today') as Range);
 else if(view==='broadcast') await broadcastPanel(e,job,String(m.from!.id));
 else if(view==='rules') await handleRuleCommand(e,job,String(m.from!.id),args.slice('rules'.length).trim(),waitUntil);
 else if (view==='users') await usersPage(e,job,String(m.from!.id),Math.max(0,Number(rest[0] ?? 1)-1));
 else if ((view==='search' || view==='user') && validUserId(value)) await userDetail(e,job,value);
 else if (view==='settings') await settingsPanel(e,job);
 else if (view==='unknown') await unknownOperations(e,job);
 else if (view==='logtopic') {
  const destination = Number(value);
  const confirm=()=>notify(e,`job:${job.id}:logtopic:ok`,null,`系统日志话题已设为 ${destination}。`);
  if(await adminSettingApplied(e,job,'log_thread_id')) {await confirm();return true;}
  if (!/^\d+$/.test(value) || !Number.isSafeInteger(destination) || destination<=1 || await one(e,'SELECT tgid FROM users WHERE thread_id=?',destination) || await one(e,'SELECT user_id FROM thread_history WHERE thread_id=?',destination)) {
   await notify(e,`job:${job.id}:logtopic:invalid`,null,'请选择未用作用户话题的专用日志话题 ID（大于 1）。');return true;
  }
  const test = await notify(e,`job:${job.id}:logtopic:test`,destination,'管理员指定此话题作为系统日志话题。历史待报事件将继续补报。');
  if (test.status!=='success') {await notify(e,`job:${job.id}:logtopic:failed`,null,'日志话题验证未成功，配置保持原值。请检查话题与机器人权限。');return true;}
  if(!await applyAdminSetting(e,job,'log_thread_id',String(destination),'log_topic_changed',{thread:destination,actor:String(m.from!.id)},destination,waitUntil)) {await notify(e,`job:${job.id}:logtopic:conflict`,null,'该话题刚被用户绑定，日志配置保持原值。');return true;}
  await confirm();
 }
 else if (view==='timezone') {
  const confirm=()=>notify(e,`job:${job.id}:timezone:ok`,null,`时区已设为 ${value}。底层时间继续保存 UTC。`);
  if(await adminSettingApplied(e,job,'timezone')) {await confirm();return true;}
  try {new Intl.DateTimeFormat('en',{timeZone:value}).format();} catch {await notify(e,`job:${job.id}:timezone:invalid`,null,'时区无效，请使用 IANA 时区，例如 Asia/Singapore。');return true;}
  await applyAdminSetting(e,job,'timezone',value,'setting_changed',{key:'timezone',actor:String(m.from!.id)},undefined,waitUntil);
  await confirm();
 } else if (view==='start' || view==='help') {
  const confirm=()=>notify(e,`job:${job.id}:template:ok`,null,`${view==='start'?'欢迎':'帮助'}文案已更新。`);
  if(await adminSettingApplied(e,job,view+'_text')) {await confirm();return true;}
  const template = args.slice(view.length).trim();
  if (!template || template.length>3500 || /\{(?!name\}|first_name\}|last_name\}|username\}|tgid\})[^}]*\}/.test(template)) {
   await notify(e,`job:${job.id}:template:invalid`,null,'请输入 1–3500 字符文案。仅支持 {name}、{first_name}、{last_name}、{username}、{tgid}。');return true;
  }
  const maximum: Record<string,number> = {'{name}':257,'{first_name}':128,'{last_name}':128,'{username}':33,'{tgid}':16};
  let expandedLength = template.length;
  template.replace(/\{(?:name|first_name|last_name|username|tgid)\}/g,token=>{expandedLength+=maximum[token]-token.length;return token;});
  if (expandedLength>4096) {
   await notify(e,`job:${job.id}:template:too-long`,null,'文案变量替换后可能超过 Telegram 4096 字符限制，请减少文案长度或变量重复次数。');return true;
  }
  await applyAdminSetting(e,job,view+'_text',template,'setting_changed',{key:view+'_text',actor:String(m.from!.id)},undefined,waitUntil);
  await confirm();
 } else await notify(e,`job:${job.id}:mb:help`,null,'用法：/mb [today|7|30|all]、/mb users [页码]、/mb search TGID、/mb settings、/mb timezone 时区、/mb start 文案、/mb help 文案、/mb logtopic 话题ID、/mb unknown。');
 return true;
}
