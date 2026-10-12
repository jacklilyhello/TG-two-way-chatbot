import {env} from 'cloudflare:test';
import {describe,expect,it,vi} from 'vitest';
import worker from '../src/index';
import {accept} from '../src/ingress';
import {statistics} from '../src/admin';
import {BotCoordinator} from '../src/bot';
import {ConversationCoordinator} from '../src/coordinator';
import {all,audit,one,run} from '../src/db';
import {handleReaction,syncReceipt} from '../src/receipts';
import {RetryLater} from '../src/operations';
import type {Env,Job,TGMessage,TGUpdate} from '../src/types';

const photo=(id:number,album?:string):TGUpdate=>({update_id:id,message:{message_id:id,date:1,chat:{id:20001,type:'private'},from:{id:20001},photo:[{file_id:`isolated-photo-${id}`}],...(album?{media_group_id:album}:{})}});
const webhook=(update:TGUpdate)=>new Request('https://test/webhook',{method:'POST',headers:{'Content-Type':'application/json','X-Telegram-Bot-Api-Secret-Token':'test-webhook-secret'},body:JSON.stringify(update)});
function quietEnv():Env {
 return {...env,JOBS:{send:async()=>{}},CONVERSATIONS:{idFromName:(key:string)=>key,get:()=>({fetch:async()=>new Response('ok')})}} as unknown as Env;
}

describe('goal ingress integrity and receipt confirmation',()=>{
 it('durably accepts every independent photo and album member once without confusing the two forms',async()=>{
  const e=quietEnv();
  const independent=Array.from({length:10},(_,i)=>photo(i+1));
  const album=Array.from({length:3},(_,i)=>photo(i+11,'isolated-album'));
  expect(await Promise.all([...independent,...album].map(async update=>(await worker.fetch(webhook(update),e)).status))).toEqual(Array(13).fill(200));
  await Promise.all([...independent,...album].map(update=>worker.fetch(webhook(update),e)));
  expect(await all(e,'SELECT update_id FROM inbox_updates ORDER BY update_id')).toEqual(Array.from({length:13},(_,i)=>({update_id:i+1})));
  expect(await all(e,'SELECT source_id,media_group FROM messages ORDER BY source_id')).toEqual([...independent,...album].map(update=>({source_id:update.message!.message_id,media_group:update.message!.media_group_id??null})));
  expect(await one(e,'SELECT COUNT(*) count FROM jobs')).toEqual({count:13});
  expect(await one(e,'SELECT group_key,version FROM media_groups')).toEqual({group_key:'20001:isolated-album',version:2});
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });

 it('returns 503 and rolls back an actual failing D1 batch before acknowledging a photo, then safely accepts its retry once',async()=>{
  const e=quietEnv(),database=e.DB;
  const failed={...e,DB:new Proxy(database,{get(target,key){
   if(key==='batch')return (statements:D1PreparedStatement[])=>target.batch([...statements,target.prepare("INSERT INTO users(tgid,first_contact,blocked) VALUES('isolated-rollback-probe',0,2)")]);
   const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }})};
  const update=photo(40,'isolated-rollback-album');
  expect((await worker.fetch(webhook(update),failed)).status).toBe(503);
  for(const table of ['inbox_updates','jobs','messages','media_groups'])expect(await one(e,`SELECT COUNT(*) count FROM ${table}`)).toEqual({count:0});
  expect((await worker.fetch(webhook(update),e)).status).toBe(200);
  expect((await worker.fetch(webhook(update),e)).status).toBe(200);
  for(const table of ['inbox_updates','jobs','messages','media_groups'])expect(await one(e,`SELECT COUNT(*) count FROM ${table}`)).toEqual({count:1});
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });

 it('starts the full ten seconds after a slow Telegram response and records cleanup confirmation separately',async()=>{
  const e=quietEnv();let clock=100000;
  vi.spyOn(Date,'now').mockImplementation(()=>clock);
  const values=new Map<string,unknown>();
  const storage={get:async(key:string)=>values.get(key),put:async(key:string|Record<string,unknown>,value?:unknown)=>{if(typeof key==='string')values.set(key,value);else for(const [name,item]of Object.entries(key))values.set(name,item);},delete:async(key:string)=>values.delete(key)};
  const bot=new BotCoordinator({storage} as unknown as DurableObjectState,e);
  e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:(url:string,init:RequestInit)=>bot.fetch(new Request(url,init))})} as unknown as DurableObjectNamespace;
  vi.mocked(fetch).mockImplementation(async(_url,init)=>{const {reaction}=JSON.parse(String(init?.body));clock+=reaction.length?3400:500;return Response.json({ok:true,result:true});});
  await run(e,"INSERT INTO messages(id,user_id,direction,source_chat,source_id,admin_id,private_id,thread_id,received_at,delivery,payload) VALUES(1,'20001','out',?,10,10,900,40,1,'success','{}')",e.ADMIN_CHAT_ID);
  await syncReceipt(e,1);
  const apply=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='reaction'"))!;
  await handleReaction(e,apply);
  expect(await one(e,'SELECT shown_at,clear_after FROM receipts')).toEqual({shown_at:103400,clear_after:113400});
  const clear=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='reaction' AND json_extract(payload,'$.action')='clear'"))!;
  clock=113399;
  await expect(handleReaction(e,clear)).rejects.toBeInstanceOf(RetryLater);
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
  clock=113400;await handleReaction(e,clear);
  expect(await one(e,'SELECT shown_at,clear_after,cleared_at,phase FROM receipts')).toEqual({shown_at:103400,clear_after:113400,cleared_at:113900,phase:'cleared'});
  expect(await all(e,'SELECT action,confirmed_at FROM receipt_events ORDER BY created_at')).toEqual([{action:'apply',confirmed_at:103400},{action:'clear',confirmed_at:113900}]);
  expect(vi.mocked(fetch).mock.calls.map(([,init])=>JSON.parse(String(init?.body)).reaction)).toEqual([[{type:'emoji',emoji:'👍'}],[]]);
 });

 it('counts mixed originals once across Singapore midnight when one delivery changes from failure to success',async()=>{
  const e=quietEnv();let clock=Date.parse('2026-10-11T15:59:59.999Z');
  vi.spyOn(Date,'now').mockImplementation(()=>clock);
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,40,1),('20002',1,41,1)");
  const incoming=(id:number,userId=20001):TGMessage=>({message_id:id,date:1,chat:{id:userId,type:'private'},from:{id:userId},text:'isolated business'});
  const outgoing=(id:number,extra:Partial<TGMessage>={}):TGMessage=>({message_id:id,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup'},from:{id:10001},message_thread_id:40,is_topic_message:true,text:'isolated business',...extra});
  await accept(e,{update_id:20,message:incoming(20)});
  clock+=2;
  await accept(e,{update_id:21,message:incoming(21)});await accept(e,{update_id:22,message:incoming(22,20002)});
  for(const id of [30,31,32,33,34])await accept(e,{update_id:id,message:outgoing(id,id===32||id===33?{text:undefined,photo:[{file_id:'isolated-album-file'}],media_group_id:'isolated-stat-album'}:{})});
  await accept(e,{update_id:35,message:{...incoming(35),text:'/help'}});
  await accept(e,{update_id:30,message:outgoing(30)});
  await accept(e,{update_id:36,edited_message:outgoing(30,{text:'isolated edited business',edit_date:2})});
  await run(e,"UPDATE messages SET delivery=CASE source_id WHEN 30 THEN 'failed' WHEN 31 THEN 'unknown' WHEN 34 THEN 'failed' ELSE 'success' END WHERE direction='out'");
  let stats=await statistics(e,'today',clock);
  expect(stats.messages).toMatchObject({total:7,success:2,failed:2,unknown:1,active:2});expect(stats.rate).toBe('50.0%');
  await run(e,"UPDATE messages SET delivery='success',private_id=930 WHERE direction='out' AND source_id=30");
  stats=await statistics(e,'today',clock);
  expect(stats.messages).toMatchObject({total:7,success:3,failed:1,unknown:1,active:2});expect(stats.rate).toBe('75.0%');expect(stats.today).toBe(7);
  expect((await statistics(e,'all',clock)).messages.total).toBe(8);
  expect((await statistics(e,'7',clock)).messages.total).toBe(8);
  expect((await statistics(e,'30',clock)).messages.total).toBe(8);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });

 it('continues business after confirmed /sc deletion while its durable receipt wake hints remain slow',async()=>{
  const e=quietEnv();
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,40,1)");
  await run(e,"INSERT INTO messages(id,user_id,direction,source_chat,source_id,admin_id,private_id,thread_id,received_at,delivery,payload) VALUES(1,'20001','out',?,100,100,900,40,1,'success','{}')",e.ADMIN_CHAT_ID);
  const original:TGMessage={message_id:100,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup'},from:{id:10001},message_thread_id:40,is_topic_message:true,text:'isolated original'};
  await accept(e,{update_id:101,message:{...original,message_id:101,text:'/sc',reply_to_message:original}});
  await accept(e,{update_id:102,message:{...original,message_id:102,text:'isolated after deletion'}});
  let release!:()=>void;
  const gate=new Promise<void>(resolve=>{release=resolve;});
  const pending:Promise<unknown>[]=[],methods:string[]=[];
  e.JOBS={send:async({key}:{key:string})=>{if(key.startsWith('receipt:'))await gate;}} as unknown as Queue;
  e.CONVERSATIONS={idFromName:(key:string)=>key,get:(key:string)=>({fetch:async()=>{if(key.startsWith('receipt:'))await gate;return new Response('ok');}})} as unknown as DurableObjectNamespace;
  e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
   const {method}=JSON.parse(String(init.body));methods.push(method);
   return Response.json({status:'success',result:method==='copyMessage'?{message_id:1002}:true});
  }})} as unknown as DurableObjectNamespace;
  const values=new Map<string,unknown>();
  const state={storage:{get:async(key:string)=>values.get(key),put:async(key:string,value:unknown)=>{values.set(key,value);},setAlarm:async()=>{}},waitUntil:(promise:Promise<unknown>)=>{pending.push(promise);}} as unknown as DurableObjectState;
  const coordinator=new ConversationCoordinator(state,e);
  let complete=false;
  const processing=coordinator.fetch(new Request('https://conversation/wake',{method:'POST',body:JSON.stringify({key:'20001'})})).then(response=>{expect(response.ok).toBe(true);complete=true;});
  try {
   await vi.waitFor(()=>expect(complete).toBe(true));
   expect(methods).toEqual(['deleteMessage','copyMessage']);
   expect(await one(e,'SELECT deletion FROM messages WHERE id=1')).toEqual({deletion:'deleted'});
   expect(await one(e,'SELECT delivery,private_id FROM messages WHERE source_id=102')).toEqual({delivery:'success',private_id:1002});
   expect(await all(e,"SELECT state FROM jobs WHERE user_key='20001' ORDER BY id")).toEqual([{state:'done'},{state:'done'}]);
   expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  } finally {release();await processing;await Promise.all(pending);}
 });

 it('continues business after a failed send while durable audit-log wake hints remain slow',async()=>{
  const e=quietEnv();
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,40,1)");
  for(const id of [201,202])await accept(e,{update_id:id,message:{message_id:id,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup'},from:{id:10001},message_thread_id:40,is_topic_message:true,text:'isolated audit scheduling'}});
  let release!:()=>void;
  const gate=new Promise<void>(resolve=>{release=resolve;});
  const pending:Promise<unknown>[]=[],copied:number[]=[];
  e.JOBS={send:async({key}:{key:string})=>{if(key==='system')await gate;}} as unknown as Queue;
  e.CONVERSATIONS={idFromName:(key:string)=>key,get:(key:string)=>({fetch:async()=>{if(key==='system')await gate;return new Response('ok');}})} as unknown as DurableObjectNamespace;
  e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
   const {method,params}=JSON.parse(String(init.body));expect(method).toBe('copyMessage');copied.push(params.message_id);
   return Response.json(params.message_id===201?{status:'failed',code:403,reason:'isolated explicit delivery rejection'}:{status:'success',result:{message_id:1202}});
  }})} as unknown as DurableObjectNamespace;
  const values=new Map<string,unknown>();
  const state={storage:{get:async(key:string)=>values.get(key),put:async(key:string,value:unknown)=>{values.set(key,value);},setAlarm:async()=>{}},waitUntil:(promise:Promise<unknown>)=>{pending.push(promise);}} as unknown as DurableObjectState;
  const coordinator=new ConversationCoordinator(state,e);
  let complete=false;
  const processing=coordinator.fetch(new Request('https://conversation/wake',{method:'POST',body:JSON.stringify({key:'20001'})})).then(response=>{expect(response.ok).toBe(true);complete=true;});
  try {
   await vi.waitFor(()=>expect(complete).toBe(true));
   expect(copied).toEqual([201,202]);
   expect(await all(e,'SELECT delivery,private_id FROM messages ORDER BY id')).toEqual([{delivery:'failed',private_id:null},{delivery:'success',private_id:1202}]);
   expect(await one(e,"SELECT event,reported FROM audit_logs WHERE event='delivery_failed'")).toEqual({event:'delivery_failed',reported:0});
   expect(await one(e,"SELECT state FROM jobs WHERE user_key='system' AND kind='logs'")).toEqual({state:'pending'});
   expect(await all(e,"SELECT state FROM jobs WHERE user_key='20001' ORDER BY id")).toEqual([{state:'done'},{state:'done'}]);
   expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  } finally {release();await processing;await Promise.all(pending);}
 });

 it('does not detach any audit wake before its durable transaction commits',async()=>{
  const e=quietEnv(),database=e.DB,send=vi.fn(),wake=vi.fn(),waitUntil=vi.fn();
  e.JOBS={send} as unknown as Queue;
  e.CONVERSATIONS={idFromName:(key:string)=>key,get:()=>({fetch:wake})} as unknown as DurableObjectNamespace;
  e.DB=new Proxy(database,{get(target,key){
   if(key==='batch')return async()=>{throw new Error('isolated audit transaction failure');};
   const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});
  await expect(audit(e,'isolated_audit',{},waitUntil)).rejects.toThrow('isolated audit transaction failure');
  expect(send).not.toHaveBeenCalled();expect(wake).not.toHaveBeenCalled();expect(waitUntil).not.toHaveBeenCalled();
  expect(await one({...e,DB:database},'SELECT COUNT(*) count FROM audit_logs')).toEqual({count:0});
  expect(await one({...e,DB:database},'SELECT COUNT(*) count FROM jobs')).toEqual({count:0});
 });
});
