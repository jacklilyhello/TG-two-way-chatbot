import type {Env,TGUpdate,TGMessage,UserRow} from './types';
import {one,stmt,now} from './db';
import {adminBinding,internalForward,type PrivateAdmission} from './security';
import {captureRuleInput} from './rule-workflow';
import {captureBroadcastInput} from './broadcast-workflow';
interface Action {token:string;kind:string;user_id:string;actor_id:string;thread_id:number;version:number;payload:string;expires_at:number;used:number}
export function authorized(e:Env,id?:number) {return !!id&&e.ADMIN_IDS.split(',').map(s=>s.trim()).includes(String(id));}
export function command(m:TGMessage) {return (m.text??'').startsWith('/');}
export function business(m:TGMessage) {return !!(m.text||m.photo||m.video||m.audio||m.voice||m.document||m.sticker||m.animation)&&!command(m);}
export function validUpdate(value:unknown):value is TGUpdate {
 if(!value||typeof value!=='object')return false;
 const u=value as TGUpdate;
 if(!Number.isSafeInteger(u.update_id)||u.update_id<0)return false;
 const m=u.message??u.edited_message??u.callback_query?.message;
 if(m&&(!Number.isSafeInteger(m.message_id)||!Number.isSafeInteger(m.chat?.id)||typeof m.chat?.type!=='string'))return false;
 if(u.callback_query&&(!Number.isSafeInteger(u.callback_query.from?.id)||typeof u.callback_query.id!=='string'))return false;
 return true;
}
export async function accept(e:Env,u:TGUpdate):Promise<string> {
 const time=now(),m=u.message??u.edited_message,cb=u.callback_query;
 let key='system',uid:string|undefined,dir='in';
 let action:Action|null=null;
 let current:UserRow|null=null;
 if(m?.chat.type==='private'&&m.from&&m.chat.id===m.from.id&&!m.from.is_bot) {uid=String(m.from.id);key=uid;current=await one<UserRow>(e,'SELECT * FROM users WHERE tgid=?',uid);}
 else if(m&&String(m.chat.id)===e.ADMIN_CHAT_ID&&authorized(e,m.from?.id)&&!m.sender_chat) {
  const target=m.text?.match(/^\/tj(?:@[\w]+)?\s+(\d{1,16})(?:\s|$)/i);
  current=await one<UserRow>(e,'SELECT * FROM users WHERE thread_id=?',m.message_thread_id??0);
  uid=target?.[1]??current?.tgid;key=uid??`admin:${m.from!.id}`;dir='out';
 } else if(cb&&String(cb.message?.chat.id)===e.ADMIN_CHAT_ID&&authorized(e,cb.from.id)) {
  const token=cb.data?.match(/^a:([a-f0-9]{32})$/)?.[1];
  if(token) action=await one<Action>(e,'SELECT * FROM callback_actions WHERE token=?',token);
  if(action) {uid=action.user_id;key=uid||`admin:${cb.from.id}`;current=uid?await one<UserRow>(e,'SELECT * FROM users WHERE tgid=?',uid):null;}
  else key=`admin:${cb.from.id}`;
  // A confirmed ban does not need to wait for an unrelated Telegram upload or
  // deletion in the business lane. Validate the whole card context before
  // admitting it to this user-specific moderation lane; callbacks still pass
  // the administrator handler's checks before changing any state.
  if(action&&current&&(action.kind==='card_block'||action.kind==='card_unblock')&&
   (action.actor_id==='*'||action.actor_id===String(cb.from.id))&&action.expires_at>=time&&
   current.binding_version===action.version&&current.thread_id===action.thread_id&&
   cb.message?.message_thread_id===action.thread_id&&cb.message.message_id===current.card_id&&
   String(cb.message.from?.id)===e.EXPECTED_BOT_ID) {
   try {if(JSON.parse(action.payload).instance===e.INSTANCE_ID)key=`moderation:${current.tgid}`;}catch{}
  }
 }
 const privateAdmission:PrivateAdmission|undefined=m&&uid&&dir==='in'?{instance:e.INSTANCE_ID,userId:uid,thread:current?.thread_id??null,version:current?.binding_version??0,blocked:current?.blocked===1}:undefined;
 const ruleInputAdmission=m&&dir==='out'?await captureRuleInput(e,m):null;
 const broadcastInputAdmission=m&&dir==='out'?await captureBroadcastInput(e,m):null;
 const p={update:u,...(privateAdmission?{privateAdmission}:{}),...(m&&dir==='out'?{ruleInputAdmission,broadcastInputAdmission}:{}),...(m&&dir==='out'&&(m.message_thread_id??0)>1?{adminBinding:adminBinding(e,m,current)}:{})};
 const sql:D1PreparedStatement[]=[
  stmt(e,'INSERT OR IGNORE INTO inbox_updates(update_id,payload,received_at) VALUES(?,?,?)',u.update_id,JSON.stringify(u),time),
  stmt(e,"INSERT OR IGNORE INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES(?,?,?,?,'pending',?,?,?)",`u:${u.update_id}`,key,'update',JSON.stringify(p),time,e.RECOVERY_EPOCH,time)
 ];
 if(m&&uid&&!u.edited_message&&(dir==='in'||(current&&current.thread_id===m.message_thread_id&&m.is_topic_message&&m.from&&!m.from.is_bot&&business(m)&&!internalForward(e,m)))) {
  sql.push(stmt(e,'INSERT OR IGNORE INTO messages(user_id,direction,source_chat,source_id,admin_id,private_id,thread_id,business,media_group,received_at,payload,epoch) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',uid,dir,String(m.chat.id),m.message_id,dir==='out'?m.message_id:null,dir==='in'?m.message_id:null,dir==='out'?m.message_thread_id:null,business(m)?1:0,m.media_group_id??null,time,JSON.stringify(m),e.RECOVERY_EPOCH));
  if(m.media_group_id) sql.push(stmt(e,'INSERT INTO media_groups(group_key,user_id,direction,due_at) SELECT ?,?,?,? WHERE changes()=1 ON CONFLICT(group_key) DO UPDATE SET due_at=MAX(due_at,excluded.due_at),version=version+1',`${m.chat.id}:${m.media_group_id}`,uid,dir,time+1200));
 }
 if(cb&&action?.kind==='cancel'&&current&&action.actor_id===String(cb.from.id)&&action.used===0&&action.expires_at>time&&current.binding_version===action.version&&cb.message?.message_thread_id===action.thread_id&&JSON.parse(action.payload).messageId===cb.message.message_id&&JSON.parse(action.payload).instance===e.INSTANCE_ID&&String(cb.message.from?.id)===e.EXPECTED_BOT_ID) {
  sql.push(stmt(e,'UPDATE callback_actions SET used=? WHERE token=? AND used=0',u.update_id+1,action.token));
  sql.push(stmt(e,"UPDATE callback_actions SET used=-1 WHERE token=? AND actor_id=? AND used=0 AND EXISTS(SELECT 1 FROM callback_actions WHERE token=? AND used=?)",JSON.parse(action.payload).target,action.actor_id,action.token,u.update_id+1));
 }
 if(cb&&action?.kind==='clear_confirm'&&current&&action.actor_id===String(cb.from.id)&&action.used===0&&action.expires_at>time&&current.binding_version===action.version&&current.thread_id===action.thread_id&&cb.message?.message_thread_id===action.thread_id&&JSON.parse(action.payload).messageId===cb.message.message_id&&JSON.parse(action.payload).instance===e.INSTANCE_ID&&String(cb.message.from?.id)===e.EXPECTED_BOT_ID) {
  sql.push(stmt(e,'UPDATE callback_actions SET used=? WHERE token=? AND used=0 AND expires_at>? AND EXISTS(SELECT 1 FROM users WHERE tgid=? AND binding_version=? AND thread_id=?)',u.update_id+1,action.token,time,uid,action.version,action.thread_id));
  sql.push(stmt(e,"INSERT OR IGNORE INTO deletion_batches(id,user_id,boundary,created_at) SELECT ?,?,COALESCE((SELECT MAX(id) FROM messages WHERE user_id=?),0),? WHERE EXISTS(SELECT 1 FROM callback_actions WHERE token=? AND used=?)",action.token,uid,uid,time,action.token,u.update_id+1));
  sql.push(stmt(e,'INSERT OR IGNORE INTO deletion_items(batch_id,message_id) SELECT ?,id FROM messages WHERE user_id=? AND id<=(SELECT boundary FROM deletion_batches WHERE id=?) AND EXISTS(SELECT 1 FROM callback_actions WHERE token=? AND used=?)',action.token,uid,action.token,action.token,u.update_id+1));
  sql.push(stmt(e,'UPDATE messages SET tombstone=1 WHERE user_id=? AND id<=(SELECT boundary FROM deletion_batches WHERE id=?) AND EXISTS(SELECT 1 FROM callback_actions WHERE token=? AND used=?)',uid,action.token,action.token,u.update_id+1));
 }
 await e.DB.batch(sql);
 return key;
}
