import type {Env,Job,Params,TGMessage} from './types';
import {now,one,run} from './db';

export type RuleStage='type'|'pattern'|'confirm_add'|'delete_id'|'confirm_delete'|'done';
export interface RuleSession {instance_id:string;actor_id:string;epoch:string;session_id:string;stage:RuleStage;revision:number;payload:string;transition_key:string;expires_at:number}
export interface RuleInputAdmission {instance:string;actor:string;session:string;revision:number;stage:RuleStage;expiresAt:number;epoch:string}
export const RULE_SESSION_TTL=15*60_000;
const authorized=(e:Env,actor:string)=>e.ADMIN_IDS.split(/[\s,]+/).includes(actor);
export const ruleGeneral=(m:TGMessage)=>m.message_thread_id===undefined||m.message_thread_id===1;
export const ruleInputEligible=(e:Env,m:TGMessage)=>String(m.chat.id)===e.ADMIN_CHAT_ID&&ruleGeneral(m)&&authorized(e,String(m.from?.id))&&!m.from?.is_bot&&!m.sender_chat&&!m.forward_origin;
export async function ruleSession(e:Env,actor:string):Promise<RuleSession|null> {
 return one<RuleSession>(e,'SELECT * FROM rule_sessions WHERE instance_id=? AND actor_id=? AND epoch=?',e.INSTANCE_ID,actor,e.RECOVERY_EPOCH);
}
/** Capture at webhook admission, so delayed text cannot enter a later draft or
 * become a business message after its original management flow has ended. */
export async function captureRuleInput(e:Env,m:TGMessage):Promise<RuleInputAdmission|null> {
 if(!ruleInputEligible(e,m))return null;
 const session=await ruleSession(e,String(m.from!.id));
 if(!session||session.stage==='done'||session.expires_at<now())return null;
 return {instance:e.INSTANCE_ID,actor:session.actor_id,session:session.session_id,revision:session.revision,stage:session.stage,expiresAt:session.expires_at,epoch:e.RECOVERY_EPOCH};
}
export async function startRuleSession(e:Env,job:Job,actor:string,stage:RuleStage):Promise<RuleSession> {
 const key=`start:${job.id}:${stage}`,id=`${e.RECOVERY_EPOCH}:${job.id}:${stage}`;
 await run(e,`INSERT INTO rule_sessions(instance_id,actor_id,epoch,session_id,stage,payload,transition_key,expires_at) VALUES(?,?,?,?,?,'{}',?,?) ON CONFLICT(instance_id,actor_id) DO UPDATE SET epoch=excluded.epoch,session_id=excluded.session_id,stage=excluded.stage,revision=1,payload='{}',transition_key=excluded.transition_key,expires_at=excluded.expires_at WHERE rule_sessions.transition_key!=excluded.transition_key`,e.INSTANCE_ID,actor,e.RECOVERY_EPOCH,id,stage,key,now()+RULE_SESSION_TTL);
 return (await ruleSession(e,actor))!;
}
/** Optimistic transitions also recognize a replay of the same durable job. */
export async function moveRuleSession(e:Env,session:RuleSession,key:string,stage:RuleStage,payload:Params={}):Promise<RuleSession|null> {
 await run(e,`UPDATE rule_sessions SET stage=?,revision=revision+1,payload=?,transition_key=?,expires_at=? WHERE instance_id=? AND actor_id=? AND epoch=? AND session_id=? AND revision=? AND expires_at>=? AND transition_key!=?`,stage,JSON.stringify(payload),key,now()+RULE_SESSION_TTL,e.INSTANCE_ID,session.actor_id,e.RECOVERY_EPOCH,session.session_id,session.revision,now(),key);
 const current=await ruleSession(e,session.actor_id);
 return current?.session_id===session.session_id&&current.transition_key===key?current:null;
}
export async function closeRuleSession(e:Env,actor:string,completionKey?:string):Promise<void> {
 await run(e,"UPDATE rule_sessions SET stage='done',revision=revision+1,payload='{}',transition_key=COALESCE(?,transition_key) WHERE instance_id=? AND actor_id=? AND stage!='done'",completionKey??null,e.INSTANCE_ID,actor);
}
