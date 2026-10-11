import {env} from 'cloudflare:test';
import {describe,it,expect} from 'vitest';
import type {Env,Job,TGUpdate,Params} from '../src/types';
import {handleAdmin,handleCard} from '../src/admin';
import {accept} from '../src/ingress';
import {all,one,run} from '../src/db';
import {RetryLater} from '../src/operations';

const base=env as unknown as Env;
const token='b'.repeat(32),id=901;
const job:Job={id,dedupe:`u:${id}`,user_key:'20001',kind:'update',payload:'{}',state:'running',due_at:0,attempts:0,epoch:base.RECOVERY_EPOCH,created_at:1};
const callback:TGUpdate={update_id:id,callback_query:{id:'synthetic-block-callback',from:{id:10001},data:`a:${token}`,message:{message_id:500,date:1,chat:{id:Number(base.ADMIN_CHAT_ID),type:'supergroup'},message_thread_id:40,is_topic_message:true,from:{id:Number(base.EXPECTED_BOT_ID),is_bot:true}}}};
async function setup(kind:string,initial:number,defer=false):Promise<Env> {
 await run(base,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id,blocked) VALUES('20001',1,40,1,500,?)",initial);
 await run(base,"INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) VALUES(?,?,'20001','*',40,1,?,?)",token,kind,JSON.stringify({instance:base.INSTANCE_ID}),Number.MAX_SAFE_INTEGER);
 const deferred=new Set<string>();
 const bot={idFromName:(name:string)=>name,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  const body=JSON.parse(String(init.body)) as {key:string;method:string;params:Params};
  if(defer&&['editMessageText','pinChatMessage'].includes(body.method)&&!deferred.has(body.method)) {
   deferred.add(body.method);return Response.json({due:Date.now()+1000},{status:429});
  }
  return Response.json({status:'success',result:body.method==='getUserProfilePhotos'?{photos:[]}:true});
 }})};
 return {...base,BOT:bot} as unknown as Env;
}
describe('administrator block mutation retry boundaries',()=>{
 it.each([['card_block',0,1,'user_blocked'],['card_unblock',1,0,'user_unblocked']])('audits %s once across two deferred UI repairs',async(kind,initial,desired,event)=>{
  const e=await setup(String(kind),Number(initial),true);
  await accept(e,callback);await accept(e,callback);
  expect(await one(e,'SELECT COUNT(*) n FROM inbox_updates WHERE update_id=?',id)).toEqual({n:1});
  expect(await one(e,'SELECT COUNT(*) n FROM jobs WHERE dedupe=?',`u:${id}`)).toEqual({n:1});
  expect(await handleAdmin(e,job,callback)).toBe(true);
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:desired});
  expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',token)).toEqual({used:id+1});
  await run(e,"UPDATE jobs SET state='done' WHERE dedupe=?",`u:${id}`);
  const card=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='card'"))!;
  await expect(handleCard(e,card)).rejects.toBeInstanceOf(RetryLater);
  await expect(handleCard(e,card)).rejects.toBeInstanceOf(RetryLater);
  await handleCard(e,card);
  await handleAdmin(e,job,callback);
  expect(await all(e,"SELECT event FROM audit_logs WHERE event IN ('user_blocked','user_unblocked')")).toEqual([{event}]);
  expect(await one(e,"SELECT phase FROM operations WHERE op_key=?",`${e.RECOVERY_EPOCH}:job:${card.id}:card:pin:500`)).toEqual({phase:'success'});
  expect(await one(e,"SELECT phase FROM operations WHERE op_key=?",`${e.RECOVERY_EPOCH}:job:${id}:answer:synthetic-block-callback`)).toEqual({phase:'success'});
 });
 it('rolls back state, callback consumption and audit together when its D1 transaction fails',async()=>{
  const e=await setup('card_block',0);
  const broken={...e,DB:new Proxy(e.DB,{get(db,key){
   if(key==='batch')return (statements:D1PreparedStatement[])=>db.batch([...statements,db.prepare('INSERT INTO missing_drill_table VALUES(1)')]);
   const value=Reflect.get(db,key);return typeof value==='function'?value.bind(db):value;
  }})};
  await expect(handleAdmin(broken,job,callback)).rejects.toThrow();
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:0});
  expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',token)).toEqual({used:0});
  expect(await all(e,'SELECT event FROM audit_logs')).toEqual([]);
  await handleAdmin(e,job,callback);
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:1});
  expect(await all(e,'SELECT event FROM audit_logs')).toEqual([{event:'user_blocked'}]);
 });
 it('does not reapply a consumed callback after the user state subsequently changes',async()=>{
  const e=await setup('card_block',0);await handleAdmin(e,job,callback);
  await run(e,"UPDATE users SET blocked=0 WHERE tgid='20001'");
  await handleAdmin(e,job,callback);
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:0});
  expect(await all(e,'SELECT event FROM audit_logs')).toEqual([{event:'user_blocked'}]);
 });
 it('consumes a valid already-satisfied action without inventing another state change audit',async()=>{
  const e=await setup('card_block',1);await handleAdmin(e,job,callback);
  expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',token)).toEqual({used:id+1});
  expect(await all(e,'SELECT event FROM audit_logs')).toEqual([]);
 });
});
