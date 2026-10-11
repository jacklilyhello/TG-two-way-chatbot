import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it,vi} from 'vitest';
import type {Env,Job,TGUpdate,UserRow} from '../src/types';
import {one,run} from '../src/db';
import {ensureUser,scheduleCard} from '../src/admin';
import {accept} from '../src/ingress';
import {execute} from '../src/coordinator';
import {BotCoordinator} from '../src/bot';
import {RetryLater} from '../src/operations';

let e:Env;
beforeEach(async()=>{
 e={...env} as unknown as Env;
 e.JOBS={send:async()=>{}} as unknown as Queue;
 e.CONVERSATIONS={idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})} as unknown as DurableObjectNamespace;
 await run(e,"INSERT INTO users(tgid,first_name,first_contact,thread_id,binding_version,card_id) VALUES('20001','Original',1,40,1,500)");
});
async function admitted() {
 const update:TGUpdate={update_id:10,message:{message_id:10,date:1,chat:{id:20001,type:'private'},from:{id:20001,first_name:'Updated'},text:'private business message'}};
 await accept(e,update);
 return (await one<Job>(e,"SELECT * FROM jobs WHERE dedupe='u:10'"))!;
}
async function bindingConfirmation() {
 const token='a'.repeat(32);
 await run(e,"INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) VALUES(?,'bind_confirm','20001','10001',41,1,?,?)",token,JSON.stringify({instance:e.INSTANCE_ID,to:41,from:40,messageId:600}),Date.now()+60000);
 const update:TGUpdate={update_id:20,callback_query:{id:'binding-callback',from:{id:10001},data:`a:${token}`,message:{message_id:600,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup'},from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true},message_thread_id:41,is_topic_message:true}}};
 await accept(e,update);
 return (await one<Job>(e,"SELECT * FROM jobs WHERE dedupe='u:20'"))!;
}
function beforeBotIssue(change:()=>Promise<void>) {
 const values=new Map<string,unknown>();
 const storage={get:async(key:string)=>values.get(key),put:async(key:string|Record<string,unknown>,value?:unknown)=>{if(typeof key==='string')values.set(key,value);else for(const [k,v]of Object.entries(key))values.set(k,v);},delete:async(key:string)=>values.delete(key)};
 const bot=new BotCoordinator({storage} as unknown as DurableObjectState,e);
 e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:async(url:string,init:RequestInit)=>{await change();return bot.fetch(new Request(url,init));}})} as unknown as DurableObjectNamespace;
 return values;
}

describe('batched idle path preserves live security checks',()=>{
 it.each([
  ['binding',"UPDATE users SET thread_id=41,binding_version=2 WHERE tgid='20001'"],
  ['clear tombstone',"UPDATE messages SET tombstone=1 WHERE source_id=10"],
 ])('rechecks %s after the private-message handler has reused its user snapshot',async(_name,sql)=>{
  const job=await admitted();
  const values=beforeBotIssue(()=>run(e,sql).then(()=>{}));
  await execute(e,job);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  expect(values.has(`rate:${e.ADMIN_CHAT_ID}`)).toBe(false);
  expect(await one(e,'SELECT delivery,admin_id FROM messages WHERE source_id=10')).toEqual({delivery:'suppressed',admin_id:null});
  expect(await one(e,"SELECT phase,reason FROM operations WHERE method='copyMessage'")).toEqual({phase:'failed',reason:'cancelled_by_state'});
 });
 it('honors a persistent pause set after the operation preflight but before the final Bot check',async()=>{
  const job=await admitted();
  const values=beforeBotIssue(()=>run(e,"INSERT INTO settings(key,value) VALUES('paused','1') ON CONFLICT(key) DO UPDATE SET value='1'").then(()=>{}));
  await expect(execute(e,job)).rejects.toBeInstanceOf(RetryLater);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  expect(values.has('op:initial:send:1')).toBe(false);
  expect(await one(e,"SELECT phase FROM operations WHERE method='copyMessage'")).toEqual({phase:'prepared'});
  expect(await one(e,'SELECT delivery,admin_id FROM messages WHERE source_id=10')).toEqual({delivery:'pending',admin_id:null});
 });
 it.each(['new','existing'])('rolls back the %s user profile if the final batched snapshot read fails',async kind=>{
  const database=e.DB;
  e.DB=new Proxy(database,{get(target,key){
   if(key==='batch')return (statements:D1PreparedStatement[])=>target.batch([...statements.slice(0,-1),target.prepare('SELECT missing_snapshot_column FROM users')]);
   const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});
  const id=kind==='new'?'20002':'20001';
  await expect(ensureUser(e,id,{id:Number(id),first_name:'Should roll back'},100)).rejects.toThrow();
  expect(await one({...e,DB:database},'SELECT first_name,first_contact,thread_id,binding_version FROM users WHERE tgid=?',id)).toEqual(kind==='new'?null:{first_name:'Original',first_contact:1,thread_id:40,binding_version:1});
 });
 it('never schedules a background wake if durable card-job persistence fails',async()=>{
  const job=await admitted(),u=(await one<UserRow>(e,"SELECT * FROM users WHERE tgid='20001'"))!;
  const send=vi.fn(),wake=vi.fn(),waitUntil=vi.fn();
  e.JOBS={send} as unknown as Queue;
  e.CONVERSATIONS={idFromName:(name:string)=>name,get:()=>({fetch:wake})} as unknown as DurableObjectNamespace;
  const database=e.DB;
  e.DB=new Proxy(database,{get(target,key){if(key==='batch')return async()=>{throw new Error('card commit failed');};const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;}});
  await expect(scheduleCard(e,job,u,false,waitUntil)).rejects.toThrow('card commit failed');
  expect(send).not.toHaveBeenCalled();expect(wake).not.toHaveBeenCalled();expect(waitUntil).not.toHaveBeenCalled();
  expect(await one({...e,DB:database},"SELECT id FROM jobs WHERE kind='card'")).toBeNull();
 });
 it('lets a durable card wake run in the background while the private message is delivered',async()=>{
  const job=await admitted();let finish!:()=>void;
  const wakePending=new Promise<void>(resolve=>{finish=resolve;});
  let queueSawDurable=false,alarmSawDurable=false;
  e.JOBS={send:async()=>{queueSawDurable=!!await one(e,"SELECT id FROM jobs WHERE kind='card'");await wakePending;}} as unknown as Queue;
  e.CONVERSATIONS={idFromName:(name:string)=>name,get:()=>({fetch:async()=>{alarmSawDurable=!!await one(e,"SELECT id FROM jobs WHERE kind='card'");await wakePending;return new Response('ok');}})} as unknown as DurableObjectNamespace;
  const scheduled:Promise<unknown>[]=[];
  beforeBotIssue(async()=>{});
  vi.mocked(fetch).mockResolvedValue(Response.json({ok:true,result:{message_id:900}}));
  let complete=false;
  const processing=execute(e,job,promise=>{scheduled.push(promise);}).then(()=>{complete=true;});
  try {
   await vi.waitFor(()=>expect(complete).toBe(true));
   expect(queueSawDurable).toBe(true);expect(alarmSawDurable).toBe(true);
   expect(scheduled).toHaveLength(1);
   expect(await one(e,'SELECT delivery,admin_id FROM messages WHERE source_id=10')).toEqual({delivery:'success',admin_id:900});
  } finally {finish();await processing;await Promise.all(scheduled);}
 });
 it('answers a binding confirmation after durable binding and card scheduling while card wake hints remain pending',async()=>{
  const job=await bindingConfirmation();let finish!:()=>void;
  const pending=new Promise<void>(resolve=>{finish=resolve;});
  e.JOBS={send:async(body:{key:string})=>{if(body.key==='card:20001')await pending;}} as unknown as Queue;
  e.CONVERSATIONS={idFromName:(name:string)=>name,get:(key:string)=>({fetch:async()=>{if(key==='card:20001')await pending;return new Response('ok');}})} as unknown as DurableObjectNamespace;
  beforeBotIssue(async()=>{});
  let answerChecked=false;
  vi.mocked(fetch).mockImplementation(async(_url,init)=>{
   const params=JSON.parse(String(init?.body));
   expect(params.callback_query_id).toBe('binding-callback');
   expect(await one(e,"SELECT thread_id,binding_version FROM users WHERE tgid='20001'")).toEqual({thread_id:41,binding_version:2});
   expect(await one(e,"SELECT user_key FROM jobs WHERE kind='card'")).toEqual({user_key:'card:20001'});
   answerChecked=true;return Response.json({ok:true,result:true});
  });
  const scheduled:Promise<unknown>[]=[];let complete=false;
  const processing=execute(e,job,promise=>{scheduled.push(promise);}).then(()=>{complete=true;});
  try {
   await vi.waitFor(()=>expect(complete).toBe(true));
   expect(answerChecked).toBe(true);expect(scheduled).toHaveLength(1);
  } finally {finish();await processing;await Promise.all(scheduled);}
 });
 it('does not claim binding completion before card persistence, then safely resumes the same confirmation',async()=>{
  const job=await bindingConfirmation(),database=e.DB,cardStatements=new WeakSet<D1PreparedStatement>();
  e.DB=new Proxy(database,{get(target,key){
   if(key==='prepare')return (sql:string)=>{
    const statement=target.prepare(sql);
    if(!sql.includes("SELECT ?,?,'card'"))return statement;
    return new Proxy(statement,{get(prepared,property){
     if(property==='bind')return (...args:unknown[])=>{const bound=prepared.bind(...args);cardStatements.add(bound);return bound;};
     const value=Reflect.get(prepared,property);return typeof value==='function'?value.bind(prepared):value;
    }});
   };
   if(key==='batch')return (statements:D1PreparedStatement[])=>{if(statements.some(statement=>cardStatements.has(statement)))throw new Error('card commit failed');return target.batch(statements);};
   const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});
  beforeBotIssue(async()=>{});
  const scheduled:Promise<unknown>[]=[];
  await expect(execute(e,job,promise=>{scheduled.push(promise);})).rejects.toThrow('card commit failed');
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();expect(scheduled).toEqual([]);
  expect(await one(e,"SELECT thread_id,binding_version FROM users WHERE tgid='20001'")).toEqual({thread_id:41,binding_version:2});
  e.DB=database;
  vi.mocked(fetch).mockResolvedValue(Response.json({ok:true,result:true}));
  await execute(e,job,promise=>{scheduled.push(promise);});await Promise.all(scheduled);
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
  expect(await one(e,"SELECT thread_id,binding_version FROM users WHERE tgid='20001'")).toEqual({thread_id:41,binding_version:2});
  expect(await one(e,"SELECT COUNT(*) n FROM jobs WHERE kind='card'")).toEqual({n:1});
 });
});
