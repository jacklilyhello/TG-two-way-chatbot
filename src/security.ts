import type {Env,Job,MessageRow,TGMessage,TGUpdate,UserRow} from './types';
import {one} from './db';

export interface AdminBinding {
 instance:string;actor:string;chat:string;thread:number|null;userId:string|null;version:number|null;
}
export interface PrivateAdmission {instance:string;userId:string;thread:number|null;version:number;blocked:boolean}

/** Preserve the private sender's policy and binding at durable admission.
 * A first binding is permitted for messages received before initialization;
 * later generations cannot revive their old queued content. */
export async function privateAdmissionMatches(e:Env,job:Job,u:UserRow):Promise<boolean> {
 if(job.epoch!==e.RECOVERY_EPOCH)return false;
 const payload=JSON.parse(job.payload) as {privateAdmission?:PrivateAdmission;update?:TGUpdate};
 let snapshot=payload.privateAdmission,admittedAt=job.created_at;
 if(!snapshot) {
  // Cron route repair is a new job, never a new authorization. Recover its
  // original durable admission, keeping edit admissions separate from sends.
  const field=payload.update?.edited_message?'edited_message':'message';
  const message=payload.update?.edited_message??payload.update?.message;
  if(message) {
   const original=await one<Job>(e,`SELECT * FROM jobs WHERE kind='update' AND user_key=? AND epoch=? AND dedupe GLOB 'u:[0-9]*' AND CAST(json_extract(payload,'$.update.${field}.chat.id') AS TEXT)=? AND json_extract(payload,'$.update.${field}.message_id')=?${field==='edited_message'?' AND json_extract(payload,\'$.update.update_id\')=?':''} ORDER BY id LIMIT 1`,u.tgid,e.RECOVERY_EPOCH,String(message.chat.id),message.message_id,...(field==='edited_message'?[payload.update!.update_id]:[]));
   if(original) {snapshot=(JSON.parse(original.payload) as {privateAdmission?:PrivateAdmission}).privateAdmission;admittedAt=original.created_at;}
   else {
    const row=await one<{received_at:number}>(e,'SELECT received_at FROM messages WHERE user_id=? AND source_chat=? AND source_id=?',u.tgid,String(message.chat.id),message.message_id);
    if(row)admittedAt=row.received_at;
   }
  }
 }
 if(snapshot) {
  if(snapshot.instance!==e.INSTANCE_ID||snapshot.userId!==u.tgid||snapshot.blocked)return false;
  if(snapshot.thread!==null)return snapshot.thread===u.thread_id&&snapshot.version===u.binding_version;
  if(snapshot.version===u.binding_version&&u.thread_id===null)return true;
  return snapshot.version===0&&u.binding_version===1&&u.thread_id!==null&&!!await one(e,'SELECT id FROM thread_history WHERE user_id=? AND version=1 AND thread_id=? AND created_at>=? LIMIT 1',u.tgid,u.thread_id,admittedAt);
 }
 // Existing queued jobs predate admission snapshots. Ignore only the initial
 // binding needed by an uninitialized user's first message.
 return !await one(e,`SELECT id FROM thread_history WHERE user_id=? AND version>1 AND created_at>=?
  UNION ALL SELECT id FROM audit_logs WHERE event='user_blocked' AND id=(SELECT id FROM audit_logs WHERE event IN ('user_blocked','user_unblocked') AND json_extract(detail,'$.user')=? AND created_at<=? ORDER BY created_at DESC,id DESC LIMIT 1) LIMIT 1`,u.tgid,admittedAt,u.tgid,admittedAt);
}

/** Telegram supplies the forward origin. Never infer an origin or privilege
 * from message text, display names, or a forwarded sender's identity. */
export function internalForward(e:Env,m:TGMessage):boolean {
 if(String(m.chat.id)!==e.ADMIN_CHAT_ID)return false;
 const origin=m.forward_origin;
 return !!origin && (origin.type==='user' ? origin.sender_user.is_bot===true||e.ADMIN_IDS.split(/[\s,]+/).includes(String(origin.sender_user.id)) : origin.type==='chat' ? String(origin.sender_chat.id)===e.ADMIN_CHAT_ID : origin.type==='channel' ? String(origin.chat.id)===e.ADMIN_CHAT_ID : false);
}

export function adminBinding(e:Env,m:TGMessage,u:UserRow|null):AdminBinding {
 return {instance:e.INSTANCE_ID,actor:String(m.from?.id),chat:String(m.chat.id),thread:m.message_thread_id??null,userId:u?.tgid??null,version:u?.binding_version??null};
}

/** A topic returning to the same user is still a new binding. A delayed
 * command or outgoing message must not borrow that later authorization. */
export async function admittedBindingMatches(e:Env,job:Job,m:TGMessage,u:UserRow):Promise<boolean> {
 if(job.epoch!==e.RECOVERY_EPOCH)return false;
 const snapshot=(JSON.parse(job.payload) as {adminBinding?:AdminBinding}).adminBinding;
 if(snapshot)return snapshot.instance===e.INSTANCE_ID && snapshot.actor===String(m.from?.id) && snapshot.chat===e.ADMIN_CHAT_ID && snapshot.thread===m.message_thread_id && snapshot.userId===u.tgid && snapshot.version===u.binding_version && u.thread_id===snapshot.thread;
 // Upgrade compatibility for already queued jobs: history records can prove
 // that their original admission predates a binding change.
 return !await one(e,'SELECT id FROM thread_history WHERE user_id=? AND created_at>=? LIMIT 1',u.tgid,job.created_at);
}

/** Album aggregation must validate every original admission, not only the
 * update that happens to wake the batch after its collection window. */
export async function albumMemberBindingMatches(e:Env,row:MessageRow,u:UserRow):Promise<boolean> {
 if(row.user_id!==u.tgid||row.epoch!==e.RECOVERY_EPOCH||row.direction==='out'&&row.thread_id!==u.thread_id)return false;
 const job=await one<Job>(e,"SELECT * FROM jobs WHERE kind='update' AND user_key=? AND CAST(json_extract(payload,'$.update.message.chat.id') AS TEXT)=? AND json_extract(payload,'$.update.message.message_id')=? ORDER BY id LIMIT 1",row.user_id,row.source_chat,row.source_id);
 if(job)return row.direction==='in'?privateAdmissionMatches(e,job,u):admittedBindingMatches(e,job,JSON.parse(row.payload) as TGMessage,u);
 if(row.direction==='in')return privateAdmissionMatches(e,{id:0,dedupe:'',user_key:u.tgid,kind:'update',payload:JSON.stringify({update:{update_id:0,message:JSON.parse(row.payload)}}),state:'pending',due_at:row.received_at,attempts:0,epoch:row.epoch,created_at:row.received_at},u);
 return !await one(e,'SELECT id FROM thread_history WHERE user_id=? AND version>? AND created_at>=? LIMIT 1',u.tgid,row.direction==='in'?1:0,row.received_at);
}
