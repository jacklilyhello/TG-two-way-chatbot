import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it,vi} from 'vitest';
import type {Env,Job,TGMessage,TGUpdate} from '../src/types';
import {one,run} from '../src/db';
import {accept} from '../src/ingress';
import {ConversationCoordinator,execute} from '../src/coordinator';
import {BotCoordinator} from '../src/bot';
import {RetryLater} from '../src/operations';
import {recover} from '../src/index';
import {refreshCardNow} from '../src/admin';

let e:Env;
const incoming=(id:number,extra:Partial<TGMessage>={}):TGMessage=>({message_id:id,date:1,chat:{id:20001,type:'private'},from:{id:20001,first_name:'Synthetic'},text:'synthetic business text',...extra});
const outgoing=(id:number,text:string,extra:Partial<TGMessage>={}):TGMessage=>({message_id:id,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup'},from:{id:10001},message_thread_id:40,is_topic_message:true,text,...extra});
async function admit(update:TGUpdate) {
 await accept(e,update);
 return (await one<Job>(e,'SELECT * FROM jobs WHERE dedupe=?',`u:${update.update_id}`))!;
}
function botWithInterleaving(method:string,change:()=>Promise<unknown>) {
 const values=new Map<string,unknown>();let changed=false;
 const storage={get:async(key:string)=>values.get(key),put:async(key:string|Record<string,unknown>,value?:unknown)=>{if(typeof key==='string')values.set(key,value);else for(const [k,v] of Object.entries(key))values.set(k,v);},delete:async(key:string)=>values.delete(key)};
 const bot=new BotCoordinator({storage} as unknown as DurableObjectState,e);
 e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:async(url:string,init:RequestInit)=>{
  const request=JSON.parse(String(init.body)) as {method:string};
  if(!changed&&request.method===method){changed=true;await change();}
  return bot.fetch(new Request(url,init));
 }})} as unknown as DurableObjectNamespace;
 return {values,changed:()=>changed};
}
function apiMethods() {return vi.mocked(fetch).mock.calls.map(([url])=>String(url).split('/').at(-1));}
beforeEach(async()=>{
 e={...env} as unknown as Env;
 e.JOBS={send:async()=>{}} as unknown as Queue;
 e.CONVERSATIONS={idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})} as unknown as DurableObjectNamespace;
 await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES('20001',1,40,1,500)");
 vi.mocked(fetch).mockImplementation(async(url)=>Response.json({ok:true,result:String(url).endsWith('/sendMediaGroup')?[{message_id:901},{message_id:902}]:String(url).includes('/edit')||String(url).endsWith('/deleteMessage')?true:{message_id:900}}));
});

describe('goal regression: current policy at the final external request boundary',()=>{
 it('keeps a queued /start silent when a ban commits after handler admission but before its fixed reply API issue',async()=>{
  const job=await admit({update_id:5,message:incoming(5,{text:'/start'})});
  const interleaving=botWithInterleaving('sendMessage',()=>run(e,"UPDATE users SET blocked=1 WHERE tgid='20001'"));
  await execute(e,job);
  expect(interleaving.changed()).toBe(true);
  expect(apiMethods()).toEqual([]);
  expect(await one(e,"SELECT phase,reason FROM operations WHERE method='sendMessage'")).toEqual({phase:'failed',reason:'cancelled_by_state'});
  await run(e,"UPDATE users SET blocked=0 WHERE tgid='20001'");
  await execute(e,job);
  expect(apiMethods()).toEqual([]);
 });
 it.each(['text','album','edit'] as const)('does not forward an incoming %s when a ban commits after handler admission but before API issue',async kind=>{
  let job:Job,method:string;
  if(kind==='album') {
   const first=incoming(10,{text:undefined,media_group_id:'synthetic-race-album',photo:[{file_id:'synthetic-photo-one',file_unique_id:'one',width:1,height:1}]});
   job=await admit({update_id:10,message:first});
   await admit({update_id:11,message:incoming(11,{...first,message_id:11,photo:[{file_id:'synthetic-photo-two',file_unique_id:'two',width:1,height:1}]})});
   await run(e,'UPDATE media_groups SET due_at=0');
   await run(e,'UPDATE messages SET received_at=0');
   method='sendMediaGroup';
  } else {
   job=await admit({update_id:10,message:incoming(10)});
   if(kind==='edit') {
    await run(e,"UPDATE messages SET delivery='success',admin_id=900,thread_id=40 WHERE source_id=10");
    job=await admit({update_id:11,edited_message:incoming(10,{text:'synthetic edited business text',edit_date:2})});
   }
   method=kind==='edit'?'editMessageText':'copyMessage';
  }
  const interleaving=botWithInterleaving(method,()=>run(e,"UPDATE users SET blocked=1 WHERE tgid='20001'"));
  await execute(e,job);
  expect(interleaving.changed()).toBe(true);
  expect(apiMethods().filter(value=>value===method)).toEqual([]);
  expect(await one(e,'SELECT phase,reason FROM operations WHERE method=?',method)).toEqual({phase:'failed',reason:'cancelled_by_state'});
  // Unblocking cannot cause the cancelled admission to become a new delivery.
  await run(e,"UPDATE users SET blocked=0 WHERE tgid='20001'");
  await execute(e,job);
  expect(apiMethods().filter(value=>value===method)).toEqual([]);
 });
 it('does not synchronize an old queued incoming edit after leaving and returning to the same topic',async()=>{
  await admit({update_id:20,message:incoming(20)});
  await run(e,"UPDATE messages SET delivery='success',admin_id=920,thread_id=40 WHERE source_id=20");
  const job=await admit({update_id:21,edited_message:incoming(20,{text:'queued before binding change',edit_date:2})});
  await run(e,"UPDATE users SET thread_id=41,binding_version=2 WHERE tgid='20001'");
  await run(e,"INSERT INTO thread_history(user_id,thread_id,version,created_at) VALUES('20001',41,2,?)",job.created_at+1);
  await run(e,"UPDATE users SET thread_id=40,binding_version=3 WHERE tgid='20001'");
  await run(e,"INSERT INTO thread_history(user_id,thread_id,version,created_at) VALUES('20001',40,3,?)",job.created_at+2);
  botWithInterleaving('editMessageText',async()=>{});
  await execute(e,job);
  expect(apiMethods()).not.toContain('editMessageText');
  expect(await one(e,'SELECT payload FROM messages WHERE source_id=20')).toEqual({payload:JSON.stringify(incoming(20))});
 });
 it('revalidates a single-delete admission if the binding changes immediately before Telegram issue',async()=>{
  await run(e,"INSERT INTO messages(user_id,direction,source_chat,source_id,admin_id,private_id,thread_id,received_at,delivery,payload) VALUES('20001','out',?,30,30,930,40,1,'success','{}')",e.ADMIN_CHAT_ID);
  const job=await admit({update_id:31,message:outgoing(31,'/sc',{reply_to_message:outgoing(30,'synthetic target')})});
  const interleaving=botWithInterleaving('deleteMessage',()=>run(e,"UPDATE users SET binding_version=3 WHERE tgid='20001'"));
  await execute(e,job);
  expect(interleaving.changed()).toBe(true);
  expect(apiMethods()).not.toContain('deleteMessage');
  expect(apiMethods()).not.toContain('sendMessage');
  expect(await one(e,"SELECT phase,reason FROM operations WHERE method='deleteMessage'")).toEqual({phase:'failed',reason:'cancelled_by_state'});
  // Retain the safety lock without claiming an unissued API deletion failed.
  expect(await one(e,"SELECT tombstone,deletion FROM messages WHERE source_id=30")).toEqual({tombstone:1,deletion:'none'});
 });
 it.each(['text','edit'] as const)('does not revive an incoming %s admitted during a ban after the user is unblocked',async kind=>{
  if(kind==='edit') {
   await admit({update_id:40,message:incoming(40)});
   await run(e,"UPDATE messages SET delivery='success',admin_id=940,thread_id=40 WHERE source_id=40");
  }
  await run(e,"UPDATE users SET blocked=1 WHERE tgid='20001'");
  const job=await admit(kind==='edit'?{update_id:41,edited_message:incoming(40,{text:'edited during ban',edit_date:2})}:{update_id:41,message:incoming(41)});
  await run(e,"UPDATE users SET blocked=0 WHERE tgid='20001'");
  botWithInterleaving('copyMessage',async()=>{});
  await execute(e,job);
  expect(apiMethods()).toEqual([]);
  if(kind==='text')expect(await one(e,'SELECT delivery,private_id FROM messages WHERE source_id=41')).toEqual({delivery:'suppressed',private_id:41});
  else expect(await one(e,'SELECT payload FROM messages WHERE source_id=40')).toEqual({payload:JSON.stringify(incoming(40))});
 });
 it('filters a blocked album member while delivering a later member admitted after unblocking',async()=>{
  const photo=(id:number):TGMessage=>incoming(id,{text:undefined,media_group_id:'spanning-ban',photo:[{file_id:`synthetic-photo-${id}`}]});
  await run(e,"UPDATE users SET blocked=1 WHERE tgid='20001'");
  await admit({update_id:50,message:photo(50)});
  await run(e,"UPDATE users SET blocked=0 WHERE tgid='20001'");
  const job=await admit({update_id:51,message:photo(51)});
  await run(e,'UPDATE media_groups SET due_at=0');
  botWithInterleaving('copyMessage',async()=>{});
  let clock=Date.now();vi.spyOn(Date,'now').mockImplementation(()=>clock);
  // The explanatory administrator summary uses the same group-send lane as
  // the confirmed member, so resume only after its persisted rate deadline.
  const deferred=await execute(e,job).catch(error=>error);
  expect(deferred).toBeInstanceOf(RetryLater);
  clock=(deferred as RetryLater).due;
  await execute(e,job);
  expect(apiMethods().filter(method=>method==='copyMessage'||method==='sendMediaGroup')).toEqual(['copyMessage']);
  expect(vi.mocked(fetch).mock.calls.every(([,init])=>JSON.parse(String(init?.body)).chat_id===e.ADMIN_CHAT_ID)).toBe(true);
  expect(JSON.parse(String(vi.mocked(fetch).mock.calls[0][1]?.body)).message_id).toBe(51);
  expect(await one(e,'SELECT delivery FROM messages WHERE source_id=50')).toEqual({delivery:'suppressed'});
  expect(await one(e,'SELECT delivery FROM messages WHERE source_id=51')).toEqual({delivery:'success'});
 });
 it('keeps administrator replies available while only the private sender is manually blocked',async()=>{
  await run(e,"UPDATE users SET blocked=1 WHERE tgid='20001'");
  const job=await admit({update_id:60,message:outgoing(60,'administrator follow-up')});
  botWithInterleaving('copyMessage',async()=>{});
  await execute(e,job);
  expect(apiMethods()).toEqual(['copyMessage']);
  expect(await one(e,'SELECT delivery,private_id FROM messages WHERE source_id=60')).toEqual({delivery:'success',private_id:900});
 });
 it('allows two private admissions made before first initialization to use the first binding without resetting contact time',async()=>{
  await run(e,"DELETE FROM users WHERE tgid='20001'");
  const first=await admit({update_id:70,message:incoming(70)}),second=await admit({update_id:71,message:incoming(71)});
  const requests:{method:string;params:Record<string,unknown>}[]=[];
  e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
   const request=JSON.parse(String(init.body));requests.push(request);
   const result=request.method==='getChat'?{id:20001,first_name:'Synthetic'}:request.method==='createForumTopic'?{message_thread_id:40}:request.method==='sendMessage'?{message_id:700}:request.method==='pinChatMessage'?true:{message_id:1000+Number(request.params.message_id)};
   return Response.json({status:'success',result});
  }})} as unknown as DurableObjectNamespace;
  await execute(e,first);await execute(e,second);
  expect(requests.filter(request=>request.method==='createForumTopic')).toHaveLength(1);
  expect(requests.filter(request=>request.method==='copyMessage').map(request=>request.params.message_id)).toEqual([70,71]);
  expect(await one(e,"SELECT first_contact,thread_id,binding_version FROM users WHERE tgid='20001'")).toEqual({first_contact:first.created_at,thread_id:40,binding_version:1});
 });
 it('does not treat an arbitrary version-one row without a matching initial binding history as initialization',async()=>{
  await run(e,"UPDATE users SET thread_id=NULL,binding_version=0 WHERE tgid='20001'");
  const job=await admit({update_id:80,message:incoming(80)});
  await run(e,"UPDATE users SET thread_id=41,binding_version=1 WHERE tgid='20001'");
  botWithInterleaving('copyMessage',async()=>{});
  await execute(e,job);
  expect(apiMethods()).toEqual([]);
  expect(await one(e,'SELECT delivery FROM messages WHERE source_id=80')).toEqual({delivery:'suppressed'});
 });
 it.each([1,3])('handles a legacy private task without admission metadata at binding version %s',async version=>{
  const job=await admit({update_id:90,message:incoming(90)});
  job.payload=JSON.stringify({update:{update_id:90,message:incoming(90)}});
  await run(e,'UPDATE jobs SET payload=? WHERE id=?',job.payload,job.id);
  await run(e,"UPDATE users SET binding_version=? WHERE tgid='20001'",version);
  await run(e,"INSERT INTO thread_history(user_id,thread_id,version,created_at) VALUES('20001',40,?,?)",version,job.created_at+(version===1?-1:1));
  botWithInterleaving('copyMessage',async()=>{});
  await execute(e,job);
  expect(apiMethods()).toEqual(version===1?['copyMessage']:[]);
  expect(await one(e,'SELECT delivery FROM messages WHERE source_id=90')).toEqual({delivery:version===1?'success':'suppressed'});
 });
 it.each(['blocked-snapshot','blocked-legacy','binding-snapshot'] as const)('keeps %s authorization when Cron creates a new route-repair job',async reason=>{
  let clock=1000;vi.spyOn(Date,'now').mockImplementation(()=>clock);
  if(reason.startsWith('blocked')) {
   await run(e,"UPDATE users SET blocked=1 WHERE tgid='20001'");
   await run(e,"INSERT INTO audit_logs(event,detail,created_at) VALUES('user_blocked','{\"user\":\"20001\"}',900)");
  }
  const original=await admit({update_id:100,message:incoming(100)});
  if(reason==='blocked-legacy')await run(e,"UPDATE jobs SET payload=json_remove(payload,'$.privateAdmission') WHERE id=?",original.id);
  await run(e,"UPDATE jobs SET state='attention' WHERE id=?",original.id);
  clock=2000;
  if(reason.startsWith('blocked')) {
   await run(e,"UPDATE users SET blocked=0 WHERE tgid='20001'");
   await run(e,"INSERT INTO audit_logs(event,detail,created_at) VALUES('user_unblocked','{\"user\":\"20001\"}',1500)");
  } else {
   await run(e,"UPDATE users SET binding_version=3 WHERE tgid='20001'");
   await run(e,"INSERT INTO thread_history(user_id,thread_id,version,created_at) VALUES('20001',41,2,1400),('20001',40,3,1500)");
  }
  botWithInterleaving('copyMessage',async()=>{});
  await recover(e);
  const repaired=(await one<Job>(e,"SELECT * FROM jobs WHERE dedupe LIKE 'route:%'"))!;
  expect(repaired.created_at).toBe(2000);
  expect(JSON.parse(repaired.payload).privateAdmission).toBeUndefined();
  await execute(e,repaired);
  expect(apiMethods()).toEqual([]);
  expect(await one(e,'SELECT delivery FROM messages WHERE source_id=100')).toEqual({delivery:'suppressed'});
  expect(await one(e,'SELECT state FROM jobs WHERE id=?',original.id)).toEqual({state:'attention'});
 });
 it('answers a generated valid ban button and continues the user lane while durable system wake hints remain slow',async()=>{
  const methods:string[]=[];
  e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
   const request=JSON.parse(String(init.body));methods.push(request.method);
   return Response.json({status:'success',result:request.method==='getUserProfilePhotos'?{photos:[]}:request.method==='copyMessage'?{message_id:1102}:true});
  }})} as unknown as DurableObjectNamespace;
  const seed:Job={id:200,dedupe:'card-seed',user_key:'card:20001',kind:'card',payload:'{}',state:'running',due_at:0,attempts:0,epoch:e.RECOVERY_EPOCH,created_at:Date.now()};
  await refreshCardNow(e,seed,'20001');
  const action=(await one<{token:string}>(e,"SELECT token FROM callback_actions WHERE kind='card_block' AND used=0"))!;
  expect(action).not.toBeNull();methods.length=0;
  await admit({update_id:201,callback_query:{id:'synthetic-valid-ban',from:{id:10001},data:`a:${action.token}`,message:outgoing(500,'',{from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true}})}});
  await admit({update_id:202,message:outgoing(202,'administrator follow-up after ban')});
  let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});
  e.JOBS={send:async({key}:{key:string})=>{if(key==='system')await gate;}} as unknown as Queue;
  e.CONVERSATIONS={idFromName:(key:string)=>key,get:(key:string)=>({fetch:async()=>{if(key==='system')await gate;return new Response('ok');}})} as unknown as DurableObjectNamespace;
  const values=new Map<string,unknown>(),pending:Promise<unknown>[]=[];
  const state={storage:{get:async(key:string)=>values.get(key),put:async(key:string,value:unknown)=>{values.set(key,value);},setAlarm:async()=>{}},waitUntil:(promise:Promise<unknown>)=>{pending.push(promise);}} as unknown as DurableObjectState;
  const coordinator=new ConversationCoordinator(state,e);let completed=false;
  const processing=coordinator.fetch(new Request('https://conversation/wake',{method:'POST',body:JSON.stringify({key:'20001'})})).then(()=>{completed=true;});
  try {
   await vi.waitFor(()=>expect(completed).toBe(true));
   expect(methods).toEqual(['answerCallbackQuery','copyMessage']);
   expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:1});
   expect(await one(e,"SELECT event FROM audit_logs WHERE event='user_blocked'")).toEqual({event:'user_blocked'});
   expect(await one(e,"SELECT used FROM callback_actions WHERE token=?",action.token)).toEqual({used:202});
   expect(await one(e,"SELECT COUNT(*) n FROM jobs WHERE user_key='20001' AND state='done'")).toEqual({n:2});
  } finally {release();await processing;await Promise.all(pending);}
 });
});
