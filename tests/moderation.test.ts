import {env} from 'cloudflare:test';
import {describe,it,expect} from 'vitest';
import type {Env,Job,Params} from '../src/types';
import {all,one,run} from '../src/db';
import {handleBanNotice,setBlocked,type BanAction} from '../src/moderation';
import {RetryLater} from '../src/operations';

const base=env as unknown as Env;
const job=(id:number):Job=>({id,dedupe:`u:${id}`,user_key:'20001',kind:'update',payload:'{}',state:'running',due_at:0,attempts:0,epoch:base.RECOVERY_EPOCH,created_at:1});
async function setup(blocked=0) {
 await run(base,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id,blocked) VALUES('20001',1,40,1,500,?)",blocked);
 const sent:{key:string;method:string;params:Params;guard:unknown}[]=[];
 const e={...base,JOBS:{send:async()=>{}},CONVERSATIONS:{idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})},BOT:{idFromName:(name:string)=>name,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  const body=JSON.parse(String(init.body));sent.push(body);
  return Response.json({status:'success',result:{message_id:700+sent.length}});
 }})}} as unknown as Env;
 return {e,sent};
}
async function action(e:Env,index:number,kind='card_block'):Promise<BanAction> {
 const token=index.toString(16).padStart(32,'0');
 await run(e,"INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) VALUES(?,?,'20001','*',40,1,'{}',?)",token,kind,Number.MAX_SAFE_INTEGER);
 return {token,stamp:index+1,version:1,threadId:40,cardId:500};
}
const pendingNotice=(e:Env)=>one<Job>(e,"SELECT * FROM jobs WHERE kind='ban-notice' ORDER BY id DESC LIMIT 1");

describe('durable shared ban transitions and administrator notices',()=>{
 it('commits one state change, audit and persistent admin notice across repeated clicks and retries',async()=>{
  const {e,sent}=await setup();const first=await action(e,1);
  const result=await setBlocked(e,job(1),'20001',true,{source:'manual',actorId:'10001',action:first});
  expect(result).toMatchObject({applied:true,changed:true});
  await setBlocked(e,job(1),'20001',true,{source:'manual',actorId:'10001',action:first});
  await setBlocked(e,job(2),'20001',true,{source:'manual',actorId:'10001',action:await action(e,2)});
  expect(await one(e,"SELECT blocked,moderation_version FROM users WHERE tgid='20001'")).toEqual({blocked:1,moderation_version:1});
  expect(await all(e,"SELECT event FROM audit_logs WHERE event='user_blocked'")).toEqual([{event:'user_blocked'}]);
  expect(await one(e,"SELECT COUNT(*) n FROM jobs WHERE kind='ban-notice'")).toEqual({n:1});
  const notice=(await pendingNotice(e))!;await handleBanNotice(e,notice);await handleBanNotice(e,notice);
  expect(sent).toHaveLength(1);
  expect(sent[0].method).toBe('sendMessage');
  expect(sent[0].params).toMatchObject({chat_id:e.ADMIN_CHAT_ID,message_thread_id:40});
  expect(sent[0].params.text).toContain(`🚫 封禁操作记录 #${result.eventId}`);
  expect(sent[0].params.text).toContain('操作来源：管理员手动封禁');
  expect(await one(e,'SELECT notice_status,notice_message_id FROM ban_events WHERE id=?',result.eventId)).toEqual({notice_status:'success',notice_message_id:701});
 });
 it('does not reapply a consumed callback after unblock and records a later fresh ban',async()=>{
  const {e}=await setup();const first=await action(e,10);
  await setBlocked(e,job(10),'20001',true,{source:'manual',action:first});
  await setBlocked(e,job(11),'20001',false,{source:'manual',action:await action(e,11,'card_unblock')});
  await setBlocked(e,job(10),'20001',true,{source:'manual',action:first});
  expect(await one(e,"SELECT blocked,moderation_version FROM users WHERE tgid='20001'")).toEqual({blocked:0,moderation_version:2});
  expect(await one(e,"SELECT COUNT(*) n FROM jobs WHERE kind='ban-notice'")).toEqual({n:1});
  await setBlocked(e,job(12),'20001',true,{source:'manual',action:await action(e,12)});
  expect(await one(e,"SELECT COUNT(*) n FROM jobs WHERE kind='ban-notice'")).toEqual({n:2});
 });
 it('rolls back state, callback, audit, and notice outbox when the transaction fails',async()=>{
  const {e}=await setup();const a=await action(e,20);
  const broken={...e,DB:new Proxy(e.DB,{get(db,key){
   if(key==='batch')return (statements:D1PreparedStatement[])=>db.batch([...statements,db.prepare('INSERT INTO missing_moderation_drill VALUES(1)')]);
   const value=Reflect.get(db,key);return typeof value==='function'?value.bind(db):value;
  }})};
  await expect(setBlocked(broken,job(20),'20001',true,{source:'manual',action:a})).rejects.toThrow();
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:0});
  expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',a.token)).toEqual({used:0});
  expect(await all(e,'SELECT id FROM ban_events')).toEqual([]);
  expect(await all(e,"SELECT id FROM jobs WHERE kind='ban-notice'")).toEqual([]);
  expect(await all(e,'SELECT event FROM audit_logs')).toEqual([]);
 });
 it('rejects a card or binding that changed before the moderation transaction',async()=>{
  const {e}=await setup();const a=await action(e,30);
  await run(e,"UPDATE users SET binding_version=2,card_id=501 WHERE tgid='20001'");
  expect(await setBlocked(e,job(30),'20001',true,{source:'manual',action:a})).toEqual({applied:false,changed:false,eventId:null});
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:0});
  expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',a.token)).toEqual({used:0});
 });
 it('captures a rule snapshot, seals triggering work, and prevents replay after unblock',async()=>{
  const {e,sent}=await setup();
  await run(e,"INSERT INTO jobs(id,dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES(50,'u:50','20001','update','{}','running',0,?,1)",e.RECOVERY_EPOCH);
  await run(e,"INSERT INTO messages(user_id,direction,source_chat,source_id,received_at,payload,epoch) VALUES('20001','in','20001',1,1,'{}',?),('20001','in','20001',2,1,'{}',?)",e.RECOVERY_EPOCH,e.RECOVERY_EPOCH);
  const result=await setBlocked(e,job(50),'20001',true,{source:'rule',rule:{id:7,kind:'regex',pattern:'广告[0-9]+',version:2},messageId:1});
  expect(await all(e,'SELECT delivery FROM messages ORDER BY id')).toEqual([{delivery:'blocked'},{delivery:'blocked'}]);
  expect(await one(e,"SELECT json_extract(payload,'$.moderationBlocked') blocked FROM jobs WHERE id=50")).toEqual({blocked:1});
  await setBlocked(e,job(51),'20001',false,{source:'manual'});
  await setBlocked(e,job(50),'20001',true,{source:'rule',rule:{id:7,kind:'regex',pattern:'广告[0-9]+',version:2},messageId:1});
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:0});
  await handleBanNotice(e,(await pendingNotice(e))!);
  expect(sent).toHaveLength(1);
  expect(sent[0].params.text).toContain('操作来源：拦截规则自动封禁\n触发规则：#7 · 正则表达式\n规则内容：广告[0-9]+');
  expect(sent[0].params.text).toContain('这是历史操作记录，不代表当前状态；当前状态请查看置顶资料卡。');
  expect(sent[0].params.text).not.toContain('当前状态：已解除封禁');
  expect(sent[0].params.reply_markup).toMatchObject({inline_keyboard:[[{text:'查看当前资料卡'}]]});
  expect(await one(e,'SELECT rule_pattern,rule_version FROM ban_events WHERE id=?',result.eventId)).toEqual({rule_pattern:'广告[0-9]+',rule_version:2});
  expect(JSON.stringify(await all(e,'SELECT detail FROM audit_logs'))).not.toContain('广告');
 });
 it('waits for the initialized pinned panel before issuing the administrator notice',async()=>{
  const {e,sent}=await setup();let deferred=false;
  await run(e,"INSERT INTO settings(key,value) VALUES('card_initial:20001:1','pending')");
  const delayed={...e,BOT:{idFromName:(name:string)=>name,get:()=>({fetch:async(url:string,init:RequestInit)=>{
   const body=JSON.parse(String(init.body));
   if(body.method==='pinChatMessage'&&!deferred){deferred=true;return Response.json({due:Date.now()+1000},{status:429});}
   return e.BOT.get(e.BOT.idFromName(e.INSTANCE_ID)).fetch(url,init);
  }})}} as unknown as Env;
  await setBlocked(delayed,job(60),'20001',true,{source:'rule',rule:{id:1,kind:'keyword',pattern:'synthetic-ad',version:1}});
  const notice=(await pendingNotice(delayed))!;
  await expect(handleBanNotice(delayed,notice)).rejects.toBeInstanceOf(RetryLater);
  expect(sent).toEqual([]);
  expect(await one(e,"SELECT notice_status FROM ban_events WHERE notice_status='pending'")).toEqual({notice_status:'pending'});
  await handleBanNotice(delayed,notice);
  expect(sent.map(request=>request.method)).toEqual(['pinChatMessage','sendMessage']);
 });
 it('never blindly resends an unknown Telegram notice outcome',async()=>{
  const {e}=await setup();let sent=0;
  const unknown={...e,BOT:{idFromName:(name:string)=>name,get:()=>({fetch:async()=>{sent++;return Response.json({status:'unknown',reason:'network_outcome_unknown'});}})}} as unknown as Env;
  await setBlocked(unknown,job(70),'20001',true,{source:'manual'});
  const notice=(await pendingNotice(unknown))!;
  await handleBanNotice(unknown,notice);await handleBanNotice(unknown,notice);
  expect(sent).toBe(1);
  expect(await one(e,"SELECT notice_status FROM ban_events WHERE notice_status='unknown'")).toEqual({notice_status:'unknown'});
  expect(await one(e,"SELECT json_extract(payload,'$.urgent') urgent FROM jobs WHERE kind='card' AND state='pending'")).toEqual({urgent:1});
  expect(await all(e,"SELECT event FROM audit_logs WHERE event='ban_notice_unavailable'")).toEqual([{event:'ban_notice_unavailable'}]);
 });
 it('replays a confirmed operation after event persistence failed without a second send',async()=>{
  const {e,sent}=await setup();await setBlocked(e,job(80),'20001',true,{source:'manual'});
  const notice=(await pendingNotice(e))!;let fail=true;
  const broken={...e,DB:new Proxy(e.DB,{get(db,key){
   if(key==='prepare')return (sql:string)=>{
    if(fail&&sql.startsWith('UPDATE ban_events SET notice_status=')){fail=false;throw new Error('synthetic persistence interruption');}
    return db.prepare(sql);
   };
   const value=Reflect.get(db,key);return typeof value==='function'?value.bind(db):value;
  }})};
  await expect(handleBanNotice(broken,notice)).rejects.toThrow('synthetic persistence interruption');
  await handleBanNotice(e,notice);
  expect(sent).toHaveLength(1);
  expect(await one(e,"SELECT notice_status FROM ban_events WHERE notice_status='success'")).toEqual({notice_status:'success'});
 });
 it('does not dispatch a notice when its durable lane does not match the event owner',async()=>{
  const {e,sent}=await setup();await setBlocked(e,job(90),'20001',true,{source:'manual'});
  await handleBanNotice(e,{...(await pendingNotice(e))!,user_key:'ban:20002'});
  expect(sent).toEqual([]);
 });
 it('durably requests a visible card warning when Telegram explicitly rejects the topic notice',async()=>{
  const {e}=await setup();
  const rejected={...e,BOT:{idFromName:(name:string)=>name,get:()=>({fetch:async()=>Response.json({status:'failed',code:403,reason:'Synthetic notice rejection'})})}} as unknown as Env;
  await setBlocked(rejected,job(100),'20001',true,{source:'manual'});
  await run(e,"UPDATE jobs SET state='done' WHERE kind='card'");
  await handleBanNotice(rejected,(await pendingNotice(rejected))!);
  expect(await one(e,"SELECT notice_status FROM ban_events WHERE notice_status='failed'")).toEqual({notice_status:'failed'});
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:1});
  expect(await one(e,"SELECT json_extract(payload,'$.urgent') urgent FROM jobs WHERE kind='card' AND state='pending'")).toEqual({urgent:1});
  expect(await one(e,"SELECT phase,code FROM operations WHERE method='sendMessage'")).toEqual({phase:'failed',code:403});
 });
});
