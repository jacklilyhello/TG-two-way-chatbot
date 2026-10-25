import type {Env,Params} from './types';
import {one} from './db';
import {broadcastParts,broadcastTestRecipients,type BroadcastGuard} from './broadcast-delivery';
export type {BroadcastGuard} from './broadcast-delivery';

/** Recheck the immutable recipient/content and current eligibility immediately
 * before issuing Telegram, after rate waits and operation-journal recovery. */
export async function broadcastGuardValid(e:Env,g:BroadcastGuard,method:string,params:Params):Promise<boolean> {
 if(!g||typeof g.id!=='string'||typeof g.userId!=='string'||!Number.isSafeInteger(g.part)||g.part<0)return false;
 let scope:string[]|null;
 try {scope=broadcastTestRecipients(e);}catch{return false;}
 if(scope&&!scope.includes(g.userId))return false;
 const row=await one<{text:string;photo:string|null;actor_id:string}>(e,`SELECT b.text,b.photo,b.actor_id FROM broadcasts b
 JOIN broadcast_recipients r ON r.broadcast_id=b.id JOIN users u ON u.tgid=r.user_id
 WHERE b.id=? AND b.instance_id=? AND b.bot_id=? AND b.epoch=? AND b.state='sending' AND b.snapshot_ready=1
 AND r.user_id=? AND r.state='pending' AND r.part_index=? AND u.blocked=0 AND u.thread_id IS NOT NULL`,g.id,e.INSTANCE_ID,e.EXPECTED_BOT_ID,e.RECOVERY_EPOCH,g.userId,g.part);
 if(!row||!e.ADMIN_IDS.split(/[\s,]+/).includes(row.actor_id))return false;
 const part=broadcastParts(row.text,row.photo)[g.part];
 if(!part||part.method!==method||String(params.chat_id)!==g.userId)return false;
 const expected={chat_id:g.userId,...part.params};
 const keys=Object.keys(expected) as (keyof typeof expected)[];
 return Object.keys(params).length===keys.length&&keys.every(key=>params[key]===expected[key]);
}
