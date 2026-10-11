import {env,SELF,runInDurableObject,evictDurableObject} from 'cloudflare:test';
import {describe,it,expect,vi,beforeEach} from 'vitest';
import type {Env,TGUpdate} from '../src/types';
import {accept} from '../src/ingress';
import {call,RetryLater} from '../src/operations';
import {BotCoordinator} from '../src/bot';
import {failOperationCompletion} from './completion-fault';
import {one,run,all} from '../src/db';
const e=env as unknown as Env;
const update=(id:number):TGUpdate=>({update_id:id,message:{message_id:id,date:1791700000,chat:{id:20001,type:'private'},from:{id:20001,first_name:'Test'},text:'hello'}});
const webhook=(u:unknown,secret='test-webhook-secret')=>new Request('https://test/webhook',{method:'POST',headers:{'Content-Type':'application/json','X-Telegram-Bot-Api-Secret-Token':secret},body:JSON.stringify(u)});

describe('durable ingress',()=>{
 it('rejects wrong secrets and invalid bodies without acknowledging receipt',async()=>{
  expect((await SELF.fetch(webhook(update(1),'wrong'))).status).toBe(403);
  expect((await SELF.fetch(webhook({update_id:'bad'}))).status).toBe(400);
  expect(await one(e,'SELECT * FROM inbox_updates')).toBeNull();
 });
 it('commits update, task and blocked private originals exactly once',async()=>{
  await Promise.all([accept(e,update(2)),accept(e,update(2))]);
  expect((await all(e,'SELECT * FROM inbox_updates')).length).toBe(1);
  expect((await all(e,'SELECT * FROM jobs')).length).toBe(1);
  expect((await all(e,'SELECT * FROM messages')).length).toBe(1);
 });
 it('captures clear range at valid confirmation and tombstones backlog without new arrivals',async()=>{
  await accept(e,update(3));
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES('20001',1,22,1,500)");
  const token='a'.repeat(32);
  await run(e,"INSERT INTO callback_actions VALUES(?,'clear_confirm','20001','10001',22,1,?,?,0)",token,JSON.stringify({messageId:501,instance:e.INSTANCE_ID}),Date.now()+60000);
  const cb:TGUpdate={update_id:4,callback_query:{id:'c',from:{id:10001},data:`a:${token}`,message:{message_id:501,date:1,from:{id:123456,is_bot:true},chat:{id:-1001234567890,type:'supergroup'},message_thread_id:22,is_topic_message:true}}};
  await accept(e,cb);await accept(e,cb);await accept(e,update(5));
  const rows=await all<{source_id:number;tombstone:number}>(e,'SELECT source_id,tombstone FROM messages ORDER BY id');
  expect(rows).toEqual([{source_id:3,tombstone:1},{source_id:5,tombstone:0}]);
  expect((await all(e,'SELECT * FROM deletion_items')).length).toBe(1);
 });
 it('old confirmation cannot clear a subsequently rebound user',async()=>{
  await accept(e,update(6));
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,23,2)");
  const token='b'.repeat(32);
  await run(e,"INSERT INTO callback_actions VALUES(?,'clear_confirm','20001','10001',22,1,?,?,0)",token,JSON.stringify({messageId:501,instance:e.INSTANCE_ID}),Date.now()+60000);
  await accept(e,{update_id:7,callback_query:{id:'stale',from:{id:10001},data:`a:${token}`,message:{message_id:501,date:1,from:{id:123456,is_bot:true},chat:{id:-1001234567890,type:'supergroup'},message_thread_id:22}}});
  expect(await one(e,'SELECT * FROM deletion_batches')).toBeNull();
 });
});

describe('external request journal',()=>{
 it('recovers a confirmed external response after D1 state loss and DO eviction, with no new request',async()=>{
  const stub=e.BOT.get(e.BOT.idFromName(e.INSTANCE_ID));
  await runInDurableObject(stub,async(_obj,state)=>state.storage.put('op:initial:sent',{phase:'done',outcome:{status:'success',result:{message_id:777}}}));
  await evictDurableObject(stub);
  expect(await call(e,'sent','copyMessage',{chat_id:20001,from_chat_id:1,message_id:1})).toEqual({status:'success',result:{message_id:777}});
  expect((await one<{phase:string}>(e,"SELECT phase FROM operations WHERE op_key='initial:sent'"))?.phase).toBe('success');
 });
 it('preserves interrupted issued sends as unknown after reconstruction',async()=>{
  const stub=e.BOT.get(e.BOT.idFromName(e.INSTANCE_ID));
  await runInDurableObject(stub,async(_obj,state)=>state.storage.put('op:initial:interrupted',{phase:'issued'}));
  await evictDurableObject(stub);
  const outcome=await call(e,'interrupted','copyMessage',{chat_id:20001,from_chat_id:1,message_id:1});
  expect(outcome.status).toBe('unknown');
  expect((await call(e,'interrupted','copyMessage',{chat_id:20001,from_chat_id:1,message_id:1})).status).toBe('unknown');
 });
 it('does not issue queued sends after a clear tombstone or a binding switch',async()=>{
  await accept(e,update(8));
  await run(e,"UPDATE messages SET tombstone=1 WHERE source_id=8");
  const row=await one<{id:number}>(e,'SELECT id FROM messages WHERE source_id=8');
  expect(await call(e,'cancel','copyMessage',{chat_id:20001,from_chat_id:1,message_id:1},{messageIds:[row!.id]})).toMatchObject({status:'failed',code:409});
 });
 it('bot-wide limiter delays a second topic in the same administrator group',async()=>{
  const stub=e.BOT.get(e.BOT.idFromName(e.INSTANCE_ID));
  await runInDurableObject(stub,async(_obj,state)=>state.storage.put('rate:-1001234567890',Date.now()+30000));
  await expect(call(e,'limited','sendMessage',{chat_id:e.ADMIN_CHAT_ID,message_thread_id:20,text:'x'})).rejects.toBeInstanceOf(RetryLater);
 });
});

function directBotEnv():Env {
 const data=new Map<string,unknown>();
 const storage={get:async(k:string)=>data.get(k),put:async(k:string|Record<string,unknown>,v?:unknown)=>{if(typeof k==='string')data.set(k,v);else for(const [key,value] of Object.entries(k))data.set(key,value);},delete:async(k:string)=>data.delete(k)};
 const bot=new BotCoordinator({storage} as unknown as DurableObjectState,e);
 return {...e,BOT:{idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:(url:string,init:RequestInit)=>bot.fetch(new Request(url,init))})} as unknown as DurableObjectNamespace};
}

describe('Telegram request outcomes',()=>{
 let client:Env;
 beforeEach(()=>{client=directBotEnv();});
 it('honors explicit 429 without issuing duplicate content and keeps a safely prepared request',async()=>{
  vi.mocked(fetch).mockResolvedValueOnce(Response.json({ok:false,error_code:429,parameters:{retry_after:2}}));
  await expect(call(client,'429','sendMessage',{chat_id:20001,text:'x'})).rejects.toBeInstanceOf(RetryLater);
  expect((await one<{phase:string}>(e,"SELECT phase FROM operations WHERE op_key='initial:429'"))?.phase).toBe('prepared');
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
  await expect(call(client,'429','sendMessage',{chat_id:20001,text:'x'})).rejects.toBeInstanceOf(RetryLater);
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
 });
 it('keeps a timed out external send unknown on every subsequent wake',async()=>{
  const result=await call(client,'network-lost','copyMessage',{chat_id:20001,from_chat_id:e.ADMIN_CHAT_ID,message_id:5});
  expect(result.status).toBe('unknown');
  expect((await call(client,'network-lost','copyMessage',{chat_id:20001,from_chat_id:e.ADMIN_CHAT_ID,message_id:5})).status).toBe('unknown');
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
 });
 it('persists successful Telegram result through a D1 completion failure',async()=>{
  vi.mocked(fetch).mockResolvedValueOnce(Response.json({ok:true,result:{message_id:999}}));
  const broken={...client,DB:failOperationCompletion(e.DB,'D1 completion interrupted')};
  await expect(call(broken,'d1-lost','sendMessage',{chat_id:20001,text:'x'})).rejects.toThrow('D1 completion interrupted');
  expect(await one(e,"SELECT phase FROM operations WHERE op_key='initial:d1-lost'")).toEqual({phase:'issued'});
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
  expect(await call(client,'d1-lost','sendMessage',{chat_id:20001,text:'x'})).toEqual({status:'success',result:{message_id:999}});
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
 });
});

describe('webhook persistence failures',()=>{
 it('does not acknowledge a database commit failure',async()=>{
  const {default:worker}=await import('../src/index');
  const broken={...e,DB:{prepare:e.DB.prepare.bind(e.DB),batch:async()=>{throw new Error('D1 unavailable');}}} as Env;
  expect((await worker.fetch(webhook(update(20)),broken)).status).toBe(503);
  expect(await one(e,'SELECT * FROM inbox_updates')).toBeNull();
 });
 it('acknowledges durable receipt when queue notification fails, leaving a recoverable job',async()=>{
  const {default:worker}=await import('../src/index');
  const broken={...e,JOBS:{send:async()=>{throw new Error('queue unavailable');}}} as Env;
  expect((await worker.fetch(webhook(update(21)),broken)).status).toBe(200);
  expect(await one(e,"SELECT state FROM jobs WHERE dedupe='u:21'")).toEqual({state:'pending'});
 });
});

it('cancels a confirmation at ingress before delayed jobs can execute it',async()=>{
 await accept(e,update(30));
 await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,22,1)");
 const confirm='c'.repeat(32),cancel='d'.repeat(32),payload={messageId:501,instance:e.INSTANCE_ID};
 await run(e,"INSERT INTO callback_actions VALUES(?,'clear_confirm','20001','10001',22,1,?,?,0)",confirm,JSON.stringify(payload),Date.now()+60000);
 await run(e,"INSERT INTO callback_actions VALUES(?,'cancel','20001','10001',22,1,?,?,0)",cancel,JSON.stringify({...payload,target:confirm}),Date.now()+60000);
 const cb=(id:number,token:string):TGUpdate=>({update_id:id,callback_query:{id:`c${id}`,from:{id:10001},data:`a:${token}`,message:{message_id:501,date:1,from:{id:123456,is_bot:true},chat:{id:-1001234567890,type:'supergroup'},message_thread_id:22,is_topic_message:true}}});
 await accept(e,cb(31,cancel));await accept(e,cb(32,confirm));
 expect(await one(e,'SELECT * FROM deletion_batches')).toBeNull();
 expect((await one<{tombstone:number}>(e,'SELECT tombstone FROM messages WHERE source_id=30'))?.tombstone).toBe(0);
});

it('does not let duplicate webhook updates extend a persistent album window',async()=>{
 const u=update(40);u.message!.media_group_id='group';
 await accept(e,u);
 await run(e,"UPDATE media_groups SET due_at=1 WHERE group_key='20001:group'");
 await accept(e,u);
 expect(await one(e,"SELECT due_at,version FROM media_groups WHERE group_key='20001:group'")).toEqual({due_at:1,version:0});
});
