import type {Env,Job,MessageRow,TGMessage,UserRow} from './types';
import {one} from './db';

export interface AdminBinding {
 instance:string;actor:string;chat:string;thread:number|null;userId:string|null;version:number|null;
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
 return !await one(e,'SELECT id FROM thread_history WHERE user_id=? AND created_at>? LIMIT 1',u.tgid,job.created_at);
}

/** Album aggregation must validate every original admission, not only the
 * update that happens to wake the batch after its collection window. */
export async function albumMemberBindingMatches(e:Env,row:MessageRow,u:UserRow):Promise<boolean> {
 if(row.direction!=='out')return true;
 if(row.user_id!==u.tgid||row.thread_id!==u.thread_id||row.epoch!==e.RECOVERY_EPOCH)return false;
 const job=await one<Job>(e,"SELECT * FROM jobs WHERE kind='update' AND user_key=? AND CAST(json_extract(payload,'$.update.message.chat.id') AS TEXT)=? AND json_extract(payload,'$.update.message.message_id')=? ORDER BY id LIMIT 1",row.user_id,row.source_chat,row.source_id);
 if(job)return admittedBindingMatches(e,job,JSON.parse(row.payload) as TGMessage,u);
 return !await one(e,'SELECT id FROM thread_history WHERE user_id=? AND created_at>? LIMIT 1',u.tgid,row.received_at);
}
