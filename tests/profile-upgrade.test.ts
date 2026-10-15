import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it,vi} from 'vitest';
import {handleCard} from '../src/admin';
import {BotCoordinator} from '../src/bot';
import {all,one,run,setSetting,user} from '../src/db';
import {recover} from '../src/index';
import {handleBanNotice,setBlocked} from '../src/moderation';
import {RetryLater,type IssueGuard} from '../src/operations';
import type {Env,Job,Params} from '../src/types';

const base=env as unknown as Env;
const api={requests:[] as {key:string;method:string;params:Params;guard?:IssueGuard}[],notifications:[] as {key:string}[],noticeStatus:null as null|'failed'|'unknown'};
const conversations={idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})};
const bot={idFromName:(name:string)=>name,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
 const body=JSON.parse(String(init.body)) as {key:string;method:string;params:Params;guard?:IssueGuard};api.requests.push(body);
 if(api.noticeStatus&&body.method==='sendMessage'&&body.key.includes(':ban-notice:'))return Response.json({status:api.noticeStatus,code:400,reason:'synthetic notice unavailable'});
 return Response.json({status:'success',result:body.method==='getUserProfilePhotos'?{photos:[]}:true});
}})};
const e={...base,BOT:bot,CONVERSATIONS:conversations,JOBS:{send:async(body:{key:string})=>{api.notifications.push(body);}}} as unknown as Env;
const job=(id:number,urgent=true):Job=>({id,dedupe:`upgrade-test:${id}`,user_key:'card:20001',kind:'card',payload:JSON.stringify({userId:'20001',version:2,threadId:40,urgent}),state:'running',due_at:0,attempts:0,epoch:e.RECOVERY_EPOCH,created_at:1});
async function registered(id='20001',thread:number|null=40,card:number|null=500) {
 await run(e,'INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES(?,1,?,2,?)',id,thread,card);
}
beforeEach(()=>{api.requests.length=0;api.notifications.length=0;api.noticeStatus=null;});

describe('durable existing-card upgrade',()=>{
 it('queues every bound existing card once while preserving bindings, cards, messages and history',async()=>{
  await registered();await registered('20002',41,501);await registered('20003',42,null);await registered('20004',null,502);
  await run(e,"INSERT INTO thread_history(user_id,thread_id,version,created_at) VALUES('20001',39,1,1)");
  await run(e,"INSERT INTO messages(user_id,direction,source_chat,source_id,admin_id,private_id,thread_id,received_at,payload,delivery) VALUES('20001','in','20001',10,600,10,39,1,'{}','success')");
  const before={users:await all(e,'SELECT * FROM users ORDER BY tgid'),history:await all(e,'SELECT * FROM thread_history'),messages:await all(e,'SELECT * FROM messages')};
  await recover(e);
  const queued=await all<{user_key:string;payload:string;epoch:string}>(e,"SELECT user_key,payload,epoch FROM jobs WHERE kind='card' ORDER BY id");
  expect(queued.map(row=>row.user_key)).toEqual(['card:20001','card:20002']);
  expect(queued.map(row=>JSON.parse(row.payload))).toEqual([{userId:'20001',version:2,threadId:40,urgent:true},{userId:'20002',version:2,threadId:41,urgent:true}]);
  expect(queued.every(row=>row.epoch===e.RECOVERY_EPOCH)).toBe(true);
  expect(api.notifications).toContainEqual({key:'card:20001'});expect(api.notifications).toContainEqual({key:'card:20002'});
  await run(e,"UPDATE jobs SET state='done' WHERE kind='card'");await recover(e);
  expect(await one(e,"SELECT COUNT(*) n FROM jobs WHERE kind='card'")).toEqual({n:2});
  expect({users:await all(e,'SELECT * FROM users ORDER BY tgid'),history:await all(e,'SELECT * FROM thread_history'),messages:await all(e,'SELECT * FROM messages')}).toEqual(before);
  expect(api.requests).toEqual([]);
 });
 it('keeps all upgrade work durable while bounding the recovery queue wake batch',async()=>{
  for(let index=0;index<45;index++)await registered(String(20001+index),40+index,500+index);
  await recover(e);
  expect(await one(e,"SELECT COUNT(*) n FROM jobs WHERE kind='card' AND state='pending'")).toEqual({n:45});
  expect(api.notifications.filter(notification=>notification.key.startsWith('card:'))).toHaveLength(40);
  await run(e,"UPDATE jobs SET state='done' WHERE kind='card' AND user_key IN (SELECT user_key FROM jobs WHERE kind='card' ORDER BY id LIMIT 40)");
  api.notifications.length=0;await recover(e);
  expect(api.notifications.filter(notification=>notification.key.startsWith('card:'))).toHaveLength(5);
  expect(await one(e,"SELECT COUNT(*) n FROM jobs WHERE kind='card'")).toEqual({n:45});
 });
 it('requeues the same cards safely after an epoch change and quarantines old pending work',async()=>{
  await registered();await recover(e);const before=await user(e,'20001');
  const restored={...e,RECOVERY_EPOCH:'restored-profile-epoch'};await recover(restored);await recover(restored);
  const rows=await all<{state:string;epoch:string;payload:string}>(e,"SELECT state,epoch,payload FROM jobs WHERE kind='card' ORDER BY id");
  expect(rows.map(row=>[row.state,row.epoch])).toEqual([['quarantined',e.RECOVERY_EPOCH],['pending',restored.RECOVERY_EPOCH]]);
  expect(JSON.parse(rows[1].payload)).toEqual({userId:'20001',version:2,threadId:40,urgent:true});
  expect(await user(e,'20001')).toEqual(before);
 });
});

describe('urgent profile state repairs',()=>{
 it.each(['text','photo'] as const)('bypasses business backlog and updates the existing %s card and controls with one edit',async media=>{
  await registered();await run(e,"UPDATE users SET blocked=1,moderation_version=4 WHERE tgid='20001'");
  await run(e,"INSERT INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES('pending-business','20001','update','{}','pending',0,?,1),('running-clear','20001','clear','{}','running',0,?,1)",e.RECOVERY_EPOCH,e.RECOVERY_EPOCH);
  if(media==='photo') {await setSetting(e,'card_media:20001:2','existing-photo');await run(e,"UPDATE users SET photo_id='existing-photo' WHERE tgid='20001'");}
  await handleCard(e,job(401));
  expect(api.requests.map(request=>request.method)).toEqual([media==='text'?'editMessageText':'editMessageCaption']);
  const request=api.requests[0];
  expect(request.params.message_id).toBe(500);expect(request.guard?.threadVersion).toEqual({userId:'20001',version:2,threadId:40,moderationVersion:4});
  expect(String(request.params.text??request.params.caption)).toContain('状态：已封禁');
  expect(JSON.stringify(request.params.reply_markup)).toContain('解除封禁');
  expect((await user(e,'20001'))!.card_id).toBe(500);
 });
 it('keeps ordinary profile maintenance behind the current business backlog',async()=>{
  await registered();await run(e,"INSERT INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES('business','20001','update','{}','pending',0,?,1)",e.RECOVERY_EPOCH);
  await expect(handleCard(e,job(402,false))).rejects.toBeInstanceOf(RetryLater);expect(api.requests).toEqual([]);
 });
 it.each(['failed','unknown'] as const)('shows a %s historical ban notification on the latest unblocked card without messaging the user',async status=>{
  await registered();
  const ban=await setBlocked(e,{...job(410),kind:'update'},'20001',true,{source:'manual'});
  const notice=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='ban-notice' ORDER BY id DESC LIMIT 1"))!;
  await setBlocked(e,{...job(411),kind:'update'},'20001',false,{source:'manual'});
  // Establish an already-current normal card, so only the later notice result
  // can cause another visible repair instead of being hidden by the cache.
  const initial=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='card' AND state='pending' ORDER BY id DESC LIMIT 1"))!;
  await handleCard(e,initial);await run(e,"UPDATE jobs SET state='done' WHERE kind='card'");
  api.requests.length=0;api.noticeStatus=status;await handleBanNotice(e,notice);
  const repair=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='card' AND state='pending' ORDER BY id DESC LIMIT 1"))!;
  expect(JSON.parse(repair.payload).urgent).toBe(true);await handleCard(e,repair);
  const edit=api.requests.findLast(request=>request.method==='editMessageText')!;
  expect(String(edit.params.text)).toContain('状态：正常');
  expect(String(edit.params.text)).toContain(`封禁记录 #${ban.eventId} 的话题通知${status==='failed'?'发送失败':'发送结果未知'}`);
  expect(String(edit.params.text)).toContain('当前封禁状态以上方为准');
  expect(String(edit.params.text)).not.toContain('状态：已封禁');
  expect(JSON.stringify(edit.params.reply_markup)).toContain('封禁');expect(JSON.stringify(edit.params.reply_markup)).not.toContain('解除封禁');
  expect(api.requests.every(request=>request.params.chat_id===e.ADMIN_CHAT_ID)).toBe(true);
  expect(await one(e,'SELECT blocked,notice_status FROM ban_events WHERE id=?',ban.eventId)).toEqual({blocked:1,notice_status:status});
  expect((await user(e,'20001'))!.blocked).toBe(0);
  await handleBanNotice(e,notice);expect(api.requests.filter(request=>request.method==='sendMessage')).toHaveLength(1);
 });
 it('cancels a stale moderation render before Telegram and retries with the latest confirmed state',async()=>{
  await registered();const values=new Map<string,unknown>();
  const storage={get:async(key:string)=>values.get(key),put:async(key:string|Record<string,unknown>,value?:unknown)=>{if(typeof key==='string')values.set(key,value);else for(const [name,item]of Object.entries(key))values.set(name,item);},delete:async(key:string)=>values.delete(key)};
  const coordinator=new BotCoordinator({storage} as unknown as DurableObjectState,e);
  let change=true;
  const checked={...e,BOT:{idFromName:(name:string)=>name,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
   const body=JSON.parse(String(init.body)) as {key:string;method:string;params:Params;guard?:IssueGuard};api.requests.push(body);
   if(change){change=false;await run(e,"UPDATE users SET blocked=1,moderation_version=1 WHERE tgid='20001'");}
   return coordinator.fetch(new Request('https://bot/execute',init));
  }})}} as unknown as Env;
  vi.mocked(fetch).mockResolvedValue(Response.json({ok:true,result:true}));
  await expect(handleCard(checked,job(403))).rejects.toBeInstanceOf(RetryLater);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();expect(await one(e,"SELECT value FROM settings WHERE key='card_state:20001:2'")).toBeNull();
  api.requests.length=0;await handleCard(checked,job(403));
  expect(vi.mocked(fetch)).toHaveBeenCalledOnce();
  expect(api.requests).toHaveLength(1);expect(api.requests[0].guard?.threadVersion?.moderationVersion).toBe(1);
  expect(String(api.requests[0].params.text)).toContain('状态：已封禁');
  expect(await one(e,"SELECT value FROM settings WHERE key='card_state:20001:2'")).not.toBeNull();
 });
});
