import type {Env, Job, Params, TGMessage, TGUpdate, TGUser, UserRow} from './types';
import {all, audit, enqueue, now, one, run, setSetting, setting, stmt, user} from './db';
import {call, notify, type Outcome} from './operations';
import {deleteSingle} from './messaging';
import {DEFAULT_FIXED_TEXT} from './settings';

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

export function profileLinks(u: Pick<UserRow,'tgid'|'username'>): [string,string,string] {
 if (u.username && /^[A-Za-z0-9_]{5,32}$/.test(u.username)) {
  return [`https://t.me/${u.username}?profile`, `tg://resolve?domain=${u.username}&profile`, `tg://resolve?domain=${u.username}&profile`];
 }
 // The Bot API supports ID links in inline buttons. It does not guarantee that
 // a profile is accessible under the user's privacy settings on every client.
 const fallback = `tg://user?id=${u.tgid}`;
 return [fallback,fallback,fallback];
}
export function topicLink(e: Env, thread: number, message = thread) {
 return `https://t.me/c/${e.ADMIN_CHAT_ID.replace(/^-100/,'')}/${thread}/${message}`;
}
export async function ensureUser(e: Env, id: string, profile?: TGUser, contactAt = now()): Promise<UserRow> {
 if (!validUserId(id)) throw new Error('invalid_user_id');
 await run(e,"INSERT OR IGNORE INTO users(tgid,first_contact) VALUES(?,MIN(?,COALESCE((SELECT MIN(received_at) FROM messages WHERE user_id=? AND direction='in'),?)))",id,contactAt,id,contactAt);
 if (profile && String(profile.id) === id) {
  await run(e,'UPDATE users SET first_name=?,last_name=?,username=? WHERE tgid=?',profile.first_name ?? '',profile.last_name ?? '',profile.username ?? null,id);
 }
 return (await user(e,id))!;
}

async function createAction(e: Env, kind: string, u: UserRow | null, actor: string, thread: number | null, payload: Params = {}, expires = now()+15*60_000, context?: string) {
 if (context) {
  const existing = await one<{token:string}>(e,'SELECT token FROM callback_actions WHERE kind=? AND user_id=? AND actor_id=? AND thread_id IS ? AND version=? AND json_extract(payload,\'$.context\')=?',kind,u?.tgid ?? '',actor,thread,u?.binding_version ?? 0,context);
  if (existing) return existing.token;
 }
 const token = crypto.randomUUID().replace(/-/g,'');
 await run(e,'INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) VALUES(?,?,?,?,?,?,?,?)',token,kind,u?.tgid ?? '',actor,thread,u?.binding_version ?? 0,JSON.stringify({...payload,instance:e.INSTANCE_ID,...(context ? {context} : {})}),expires);
 return token;
}
async function bind(e: Env, u: UserRow, thread: number, actor: string, action?: Action, stamp?: number) {
 const logThread = Number(await setting(e,'log_thread_id','0'));
 if (thread <= 1 || thread === logThread) return false;
 const queries: D1PreparedStatement[] = [];
 if (action) queries.push(stmt(e,'UPDATE callback_actions SET used=? WHERE token=? AND used=0 AND expires_at>?',stamp,action.token,now()));
 const gate = action ? ' AND EXISTS(SELECT 1 FROM callback_actions WHERE token=? AND used=?)' : '';
 queries.push(stmt(e,`UPDATE users SET thread_id=?,binding_version=binding_version+1,card_id=NULL WHERE tgid=? AND binding_version=? AND thread_id IS ? AND NOT EXISTS(SELECT 1 FROM users WHERE thread_id=? AND tgid!=?) AND ?!=COALESCE((SELECT CAST(value AS INTEGER) FROM settings WHERE key='log_thread_id'),0)${gate}`,thread,u.tgid,u.binding_version,u.thread_id,thread,u.tgid,thread,...(action ? [action.token,stamp] : [])));
 queries.push(stmt(e,'INSERT INTO thread_history(user_id,thread_id,version,created_at) SELECT tgid,thread_id,binding_version,? FROM users WHERE tgid=? AND thread_id=? AND binding_version=? AND NOT EXISTS(SELECT 1 FROM thread_history WHERE user_id=? AND version=?)',now(),u.tgid,thread,u.binding_version+1,u.tgid,u.binding_version+1));
 await e.DB.batch(queries);
 const current = await user(e,u.tgid);
 const success = current?.thread_id === thread && current.binding_version === u.binding_version+1;
 if (success) await audit(e,'binding_changed',{user:u.tgid,from:u.thread_id,to:thread,version:current.binding_version,actor});
 return success;
}
async function retryKnownCreationFailures(e: Env, job: Job, u: UserRow) {
 const candidates = [
  {setting:`thread_create_key:${u.tgid}`,base:`thread:create:${u.tgid}`,needed:!u.thread_id},
  {setting:`card_create_key:${u.tgid}:${u.binding_version}`,base:`card:${u.tgid}:${u.binding_version}:create`,needed:!!u.thread_id&&!u.card_id}
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

export async function ensureThread(e: Env, job: Job, userId: string): Promise<UserRow> {
 let u = await ensureUser(e,userId,undefined,job.created_at);
 if (u.thread_id) return u;
 const profile = await call<TGUser>(e,`job:${job.id}:profile:${userId}`,'getChat',{chat_id:userId});
 if (profile.status === 'success') u = await ensureUser(e,userId,profile.result,job.created_at);
 // A stable key is essential: a lost createForumTopic response must never
 // create a second topic when another incoming message wakes this user.
 const creationKey = await setting(e,`thread_create_key:${userId}`,`thread:create:${userId}`);
 const result = await call<{message_thread_id:number}>(e,creationKey,'createForumTopic',{chat_id:e.ADMIN_CHAT_ID,name:displayName(u).slice(0,110)});
 if (result.status !== 'success' || !Number.isSafeInteger(result.result.message_thread_id) || result.result.message_thread_id <= 1) {
  await audit(e,'thread_create_unavailable',{user:userId,status:result.status});
  return u;
 }
 if (!await bind(e,u,result.result.message_thread_id,'system')) {
  await audit(e,'thread_create_binding_conflict',{user:userId,thread:result.result.message_thread_id});
  return (await user(e,userId))!;
 }
 await refreshCard(e,job,userId);
 return (await user(e,userId))!;
}

function cardText(u: UserRow, timezone: string) {
 const date = new Intl.DateTimeFormat('zh-CN',{timeZone:timezone,dateStyle:'medium',timeStyle:'short'}).format(new Date(u.first_contact));
 return `姓名：${escape(displayName(u))}\nTGID：<code>${u.tgid}</code>\n用户名：${u.username ? '@'+escape(u.username) : '未设置'}\n首次联系：${date} (${escape(timezone)})\n状态：${u.blocked ? '已封禁' : '正常'}\n资料入口受 Telegram 隐私设置与客户端支持限制。`;
}
const cardSignature = (u: UserRow, timezone: string) => JSON.stringify([u.card_id,u.first_name,u.last_name,u.username,u.first_contact,u.photo_id,u.blocked,timezone]);
async function keyboard(e: Env, job: Job, u: UserRow) {
 const context = `card:${job.id}`;
 const block = await createAction(e,u.blocked ? 'card_unblock' : 'card_block',u,'*',u.thread_id,{},Number.MAX_SAFE_INTEGER,context);
 const clear = await createAction(e,'card_clear',u,'*',u.thread_id,{},Number.MAX_SAFE_INTEGER,context);
 const links = profileLinks(u);
 return {tokens:[block,clear],markup:{inline_keyboard:[
  [{text:'通用资料',url:links[0]},{text:'苹果资料',url:links[1]},{text:'安卓资料',url:links[2]}],
  [{text:u.blocked ? '解除封禁' : '封禁',callback_data:'a:'+block},{text:'直达对方主页',url:links[0]}],
  [{text:'清空此对话',callback_data:'a:'+clear}]
 ]}};
}
async function recordProblem(e: Env, event: string, u: UserRow, result: Outcome) {
 if (result.status !== 'success') await audit(e,event,{user:u.tgid,thread:u.thread_id,status:result.status,code:result.code ?? null});
}
export async function refreshCard(e: Env, job: Job, userId: string, force = false): Promise<void> {
 let u = await user(e,userId);
 if (!u?.thread_id) return;
 const photos = await call<{photos:{file_id:string}[][]}>(e,`job:${job.id}:avatar:${userId}`,'getUserProfilePhotos',{user_id:userId,limit:1});
 if (photos.status === 'success') {
  const photo = photos.result.photos?.[0]?.at(-1)?.file_id ?? null;
  await run(e,'UPDATE users SET photo_id=? WHERE tgid=?',photo,userId);
  u = (await user(e,userId))!;
 }
 const timezone = await setting(e,'timezone','Asia/Singapore');
 const stateKey = `card_state:${u.tgid}:${u.binding_version}`;
 if (!force && u.card_id && await setting(e,stateKey)===cardSignature(u,timezone)) return;
 const text = cardText(u,timezone);
 // Tokens are immutable and bind to the card and binding, so an old keyboard
 // cannot control a later card even if Telegram kept its message in history.
 const controls = await keyboard(e,job,u);
 const reply_markup = controls.markup;
 const mediaKey = `card_media:${u.tgid}:${u.binding_version}`;
 const existingMedia = await setting(e,mediaKey,'text');
 const base = `card:${u.tgid}:${u.binding_version}`;
 let cardId = u.card_id;
 let updated = true;
 const replace = cardId && ((existingMedia === 'text' && u.photo_id) || (existingMedia !== 'text' && !u.photo_id));
 if (!cardId || replace) {
  const creationKey = replace ? `${base}:replace:${cardId}:${u.photo_id ?? 'text'}` : await setting(e,`card_create_key:${u.tgid}:${u.binding_version}`,`${base}:create`);
  const result = u.photo_id
   ? await call<TGMessage>(e,creationKey,'sendPhoto',{chat_id:e.ADMIN_CHAT_ID,message_thread_id:u.thread_id,photo:u.photo_id,caption:text,parse_mode:'HTML',reply_markup})
   : await notify(e,creationKey,u.thread_id,text,{parse_mode:'HTML',reply_markup});
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
   const unpin = await call(e,`${base}:unpin:${u.card_id}`,'unpinChatMessage',{chat_id:e.ADMIN_CHAT_ID,message_id:u.card_id});
   await recordProblem(e,'card_unpin_failed',u,unpin);
  }
 } else {
  const result = existingMedia !== 'text'
   ? u.photo_id && u.photo_id !== existingMedia
    ? await call(e,`job:${job.id}:card:media:${cardId}`,'editMessageMedia',{chat_id:e.ADMIN_CHAT_ID,message_id:cardId,media:{type:'photo',media:u.photo_id,caption:text,parse_mode:'HTML'},reply_markup})
    : await call(e,`job:${job.id}:card:caption:${cardId}`,'editMessageCaption',{chat_id:e.ADMIN_CHAT_ID,message_id:cardId,caption:text,parse_mode:'HTML',reply_markup})
   : await call(e,`job:${job.id}:card:text:${cardId}`,'editMessageText',{chat_id:e.ADMIN_CHAT_ID,message_id:cardId,text,parse_mode:'HTML',reply_markup});
  await recordProblem(e,'card_update_failed',u,result);
  updated = result.status==='success';
  if (result.status === 'success' && u.photo_id && existingMedia !== 'text') await setSetting(e,mediaKey,u.photo_id);
 }
 // A recovered create may return an already-created card with another job's
 // keyboard. Establish this generation before retiring older controls.
 const markup = await call(e,`job:${job.id}:card:buttons:${cardId}`,'editMessageReplyMarkup',{chat_id:e.ADMIN_CHAT_ID,message_id:cardId,reply_markup});
 await recordProblem(e,'card_buttons_failed',u,markup);
 if (markup.status==='success') await run(e,"UPDATE callback_actions SET used=-1 WHERE user_id=? AND version=? AND kind LIKE 'card_%' AND used=0 AND token NOT IN (?,?)",u.tgid,u.binding_version,...controls.tokens);
 const pin = await call(e,`job:${job.id}:card:pin:${cardId}`,'pinChatMessage',{chat_id:e.ADMIN_CHAT_ID,message_id:cardId,disable_notification:true});
 await recordProblem(e,'card_pin_failed',u,pin);
 if (updated && markup.status==='success' && pin.status==='success') await setSetting(e,stateKey,cardSignature({...u,card_id:cardId},timezone));
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
async function panelButton(e: Env, label: string, actor: string, payload: Params): Promise<Button> {
 return {text:label,callback_data:'a:'+await createAction(e,'panel',null,actor,null,payload)};
}
async function panel(e: Env, job: Job, actor: string, range: Range) {
 const s = await statistics(e,range);
 const label = {today:'今日','7':'最近 7 天（含今天）','30':'最近 30 天（含今天）',all:'全部'}[range];
 const text = `管理面板 · ${label}\n时区：${s.timezone}\n总注册用户：${s.users.total}\n当前封禁：${s.users.blocked}\n业务消息量：${s.messages.total}\n今日业务消息：${s.today}\n管理员发送成功：${s.messages.success}\n明确失败 / 未送达：${s.messages.failed}\n成功率：${s.rate}\n待处理 / 已阻止：${s.messages.pending}\n结果未知：${s.messages.unknown}\n其他操作异常：${s.anomalies}\n活跃用户：${s.messages.active}\n统计仅含新系统业务原消息；成功表示 API 确认送达，不表示已读。`;
 const ranges: Range[] = ['today','7','30','all'];
 const buttons: Button[] = [];
 for (const r of ranges) buttons.push(await panelButton(e,{today:'今日','7':'7 天','30':'30 天',all:'全部'}[r],actor,{view:'stats',range:r}));
 await notify(e,`job:${job.id}:panel:${range}`,null,text,{reply_markup:{inline_keyboard:[buttons,[await panelButton(e,'用户列表',actor,{view:'users',page:0}),await panelButton(e,'系统设置',actor,{view:'settings'})]]}});
}
async function usersPage(e: Env, job: Job, actor: string, page: number) {
 const total = (await one<{count:number}>(e,'SELECT COUNT(*) count FROM users'))!.count;
 const maxPage = Math.max(0,Math.ceil(total/8)-1);
 page = Math.max(0,Math.min(maxPage,Number.isFinite(page) ? Math.floor(page) : 0));
 const rows = await all<UserRow>(e,'SELECT * FROM users ORDER BY first_contact DESC,tgid LIMIT 8 OFFSET ?',page*8);
 const buttons: Button[][] = [];
 for (const u of rows) buttons.push([await panelButton(e,`${u.blocked?'🚫 ':''}${displayName(u).slice(0,35)} · ${u.tgid}`,actor,{view:'user',id:u.tgid})]);
 const navigation: Button[] = [];
 if (page>0) navigation.push(await panelButton(e,'上一页',actor,{view:'users',page:page-1}));
 if (page<maxPage) navigation.push(await panelButton(e,'下一页',actor,{view:'users',page:page+1}));
 if (navigation.length) buttons.push(navigation);
 await notify(e,`job:${job.id}:users:${page}`,null,`用户列表 · ${page+1}/${maxPage+1} · 共 ${total} 人\n按 TGID 查找：/mb search TGID`,{reply_markup:{inline_keyboard:buttons}});
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
async function answer(e: Env, job: Job, id: string, text: string) {
 await call(e,`job:${job.id}:answer:${id}`,'answerCallbackQuery',{callback_query_id:id,text,show_alert:true});
}
async function callbacks(e: Env, job: Job, update: TGUpdate) {
 const cb = update.callback_query!;
 const m = cb.message;
 if (!m || String(m.chat.id)!==e.ADMIN_CHAT_ID || !authorized(e,String(cb.from.id)) || String(m.from?.id)!==e.EXPECTED_BOT_ID) return;
 const match = tokenPattern.exec(cb.data ?? '');
 const a = match ? await one<Action>(e,'SELECT * FROM callback_actions WHERE token=?',match[1]) : null;
 const stamp = update.update_id+1;
 if (!a || (a.expires_at<now() && !(a.kind==='clear_confirm' && a.used===stamp)) || (a.actor_id!=='*' && a.actor_id!==String(cb.from.id)) || a.thread_id!==threadOf(m) || (a.used!==0 && a.used!==stamp)) {await answer(e,job,cb.id,'按钮已失效或无权操作。');return;}
 const payload = actionPayload(a);
 if (payload.instance!==e.INSTANCE_ID) {await answer(e,job,cb.id,'实例不匹配。');return;}
 if (a.kind==='panel') {
  if (!isGeneral(m)) {await answer(e,job,cb.id,'管理面板仅限 General。');return;}
  await run(e,'UPDATE callback_actions SET used=? WHERE token=? AND used=0',stamp,a.token);
  if (payload.view==='stats' && ['today','7','30','all'].includes(String(payload.range))) await panel(e,job,String(cb.from.id),payload.range as Range);
  else if (payload.view==='users') await usersPage(e,job,String(cb.from.id),Number(payload.page)||0);
  else if (payload.view==='user' && validUserId(String(payload.id))) await userDetail(e,job,String(payload.id));
  else if (payload.view==='settings') await settingsPanel(e,job);
  await answer(e,job,cb.id,'已更新。');return;
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
 if (a.kind==='card_block' || a.kind==='card_unblock') {
  const blocked = a.kind==='card_block' ? 1 : 0;
  await e.DB.batch([
   stmt(e,'UPDATE callback_actions SET used=? WHERE token=? AND used=0',stamp,a.token),
   stmt(e,'UPDATE users SET blocked=? WHERE tgid=? AND binding_version=? AND thread_id=? AND card_id=? AND EXISTS(SELECT 1 FROM callback_actions WHERE token=? AND used=?)',blocked,u.tgid,a.version,a.thread_id,m.message_id,a.token,stamp)
  ]);
  await audit(e,blocked ? 'user_blocked' : 'user_unblocked',{user:u.tgid,actor:String(cb.from.id)});
  await refreshCard(e,job,u.tgid);
  await answer(e,job,cb.id,blocked ? '已封禁；用户端保持静默。' : '已解除封禁。');
 } else if (a.kind==='card_clear') {
  await confirmation(e,job,u,String(cb.from.id),a.thread_id!,'clear_confirm','确认清空此对话？仅删除新系统已知的用户私聊消息。管理员历史与话题保留；旧系统未记录历史无法追溯、数量未知。确认后新消息不在本次范围。',{});
  await answer(e,job,cb.id,'请在确认消息中选择。');
 } else if (a.kind==='bind_confirm') {
  const to = Number(payload.to);
  if (a.used!==stamp || u.binding_version===a.version) {
   if (!await bind(e,u,to,String(cb.from.id),a,stamp)) {await answer(e,job,cb.id,'绑定状态已变化或目标话题已被占用，请重新 /tj。');return;}
  }
  await refreshCard(e,job,u.tgid);
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

async function register(e: Env, job: Job, m: TGMessage, id: string) {
 const thread = threadOf(m);
 const reply = (text: string, extra: Params = {}) => notify(e,`job:${job.id}:register:reply`,thread,text,extra);
 if (!validUserId(id)) {await reply('用法：/tj TGID（正整数用户 ID）。');return;}
 if (thread===Number(await setting(e,'log_thread_id','0'))) {await reply('系统日志话题不能绑定用户。');return;}
 let u = await ensureUser(e,id,undefined,job.created_at);
 const profile = await call<TGUser>(e,`job:${job.id}:profile:${id}`,'getChat',{chat_id:id});
 if (profile.status==='success') u = await ensureUser(e,id,profile.result,job.created_at);
 await retryKnownCreationFailures(e,job,u);
 if (!thread) {
  u = await ensureThread(e,job,id);
  if (u.thread_id) {
   await refreshCard(e,job,id,true);
   await reply('已登记。首次联系与当前绑定已保留。注册不保证能够主动私信从未联系机器人的用户。',{reply_markup:{inline_keyboard:[[{text:'打开当前话题',url:topicLink(e,u.thread_id,u.card_id ?? u.thread_id)}]]}});
  } else await reply('话题未就绪。创建失败或结果未知已记账；不会盲目重复创建，请检查系统日志。');
  return;
 }
 const occupant = await one<{tgid:string}>(e,'SELECT tgid FROM users WHERE thread_id=?',thread);
 if (occupant && occupant.tgid!==id) {await reply('当前话题已属于另一用户，拒绝绑定。');return;}
 if (u.thread_id===thread) {
  await refreshCard(e,job,id,true);await reply('已绑定当前话题；无需重复登记。');return;
 }
 if (u.thread_id) {
  await confirmation(e,job,u,String(m.from!.id),thread,'bind_confirm',`TGID ${id} 当前绑定话题 ${u.thread_id}。确认切换到本话题？原话题与历史保留，切换后原话题停止普通转发。`,{to:thread,from:u.thread_id});return;
 }
 if (!await bind(e,u,thread,String(m.from!.id))) {await reply('绑定状态已变化或话题被占用，请重新 /tj。');return;}
 await refreshCard(e,job,id);await reply('已绑定当前话题，管理员历史已保留。');
}

/** All callback traffic and slash commands are management, never chat payload. */
export async function handleAdmin(e: Env, job: Job, update: TGUpdate): Promise<boolean> {
 if (update.callback_query) {await callbacks(e,job,update);return true;}
 const m = update.message ?? update.edited_message;
 if (!m || String(m.chat.id)!==e.ADMIN_CHAT_ID) return false;
 if (!authorized(e,String(m.from?.id)) || m.sender_chat || m.from?.is_bot) return true;
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
 if (!command || update.edited_message) return true;
 if (command[2]) {
  const botName = await setting(e,'bot_username','');
  if (!botName || command[2].toLowerCase()!==botName.toLowerCase()) return true;
 }
 const name = command[1].toLowerCase();
 const args = (command[3] ?? '').trim();
 const thread = threadOf(m);
 if (name==='tj') {await register(e,job,m,args);return true;}
 if (name==='sc') {
  const u = thread ? await one<UserRow>(e,'SELECT * FROM users WHERE thread_id=?',thread) : null;
  if (!u) {await notify(e,`job:${job.id}:sc:invalid`,thread,'请在当前有效用户话题中回复一条已知消息执行 /sc。');return true;}
  await deleteSingle(e,job,m,u);
  return true;
 }
 if (name!=='mb') return true;
 if (!isGeneral(m)) {await notify(e,`job:${job.id}:mb:invalid`,thread,'/mb 仅限 General 主话题。');return true;}
 const [view,...rest] = args.split(/\s+/);
 const value = rest.join(' ');
 if (!view || ['today','7','30','all'].includes(view)) await panel(e,job,String(m.from!.id),(view || 'today') as Range);
 else if (view==='users') await usersPage(e,job,String(m.from!.id),Math.max(0,Number(rest[0] ?? 1)-1));
 else if ((view==='search' || view==='user') && validUserId(value)) await userDetail(e,job,value);
 else if (view==='settings') await settingsPanel(e,job);
 else if (view==='unknown') await unknownOperations(e,job);
 else if (view==='logtopic') {
  const destination = Number(value);
  if (!/^\d+$/.test(value) || !Number.isSafeInteger(destination) || destination<=1 || await one(e,'SELECT tgid FROM users WHERE thread_id=?',destination) || await one(e,'SELECT user_id FROM thread_history WHERE thread_id=?',destination)) {
   await notify(e,`job:${job.id}:logtopic:invalid`,null,'请选择未用作用户话题的专用日志话题 ID（大于 1）。');return true;
  }
  const test = await notify(e,`job:${job.id}:logtopic:test`,destination,'管理员指定此话题作为系统日志话题。历史待报事件将继续补报。');
  if (test.status!=='success') {await notify(e,`job:${job.id}:logtopic:failed`,null,'日志话题验证未成功，配置保持原值。请检查话题与机器人权限。');return true;}
  await run(e,"INSERT INTO settings(key,value) SELECT 'log_thread_id',? WHERE NOT EXISTS(SELECT 1 FROM users WHERE thread_id=?) AND NOT EXISTS(SELECT 1 FROM thread_history WHERE thread_id=?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",String(destination),destination,destination);
  if (await setting(e,'log_thread_id')!==String(destination)) {await notify(e,`job:${job.id}:logtopic:conflict`,null,'该话题刚被用户绑定，日志配置保持原值。');return true;}
  await audit(e,'log_topic_changed',{thread:destination,actor:String(m.from!.id)});
  await notify(e,`job:${job.id}:logtopic:ok`,null,`系统日志话题已设为 ${destination}。`);
 }
 else if (view==='timezone') {
  try {new Intl.DateTimeFormat('en',{timeZone:value}).format();} catch {await notify(e,`job:${job.id}:timezone:invalid`,null,'时区无效，请使用 IANA 时区，例如 Asia/Singapore。');return true;}
  await setSetting(e,'timezone',value);await audit(e,'setting_changed',{key:'timezone',actor:String(m.from!.id)});
  await notify(e,`job:${job.id}:timezone:ok`,null,`时区已设为 ${value}。底层时间继续保存 UTC。`);
 } else if (view==='start' || view==='help') {
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
  await setSetting(e,view+'_text',template);await audit(e,'setting_changed',{key:view+'_text',actor:String(m.from!.id)});
  await notify(e,`job:${job.id}:template:ok`,null,`${view==='start'?'欢迎':'帮助'}文案已更新。`);
 } else await notify(e,`job:${job.id}:mb:help`,null,'用法：/mb [today|7|30|all]、/mb users [页码]、/mb search TGID、/mb settings、/mb timezone 时区、/mb start 文案、/mb help 文案、/mb logtopic 话题ID、/mb unknown。');
 return true;
}
