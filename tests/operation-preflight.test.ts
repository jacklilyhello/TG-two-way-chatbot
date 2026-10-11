import {env} from 'cloudflare:test';
import {describe,expect,it,vi} from 'vitest';
import type {Env} from '../src/types';
import {BotCoordinator} from '../src/bot';
import {call,RetryLater} from '../src/operations';
import {one,run,setSetting} from '../src/db';

const e=env as unknown as Env;
function tracedDatabase() {
 const calls:string[][]=[];
 const statements=new WeakMap<D1PreparedStatement,{raw:D1PreparedStatement;sql:string}>();
 const wrap=(raw:D1PreparedStatement,sql:string):D1PreparedStatement=>{
  const value=new Proxy(raw,{get(target,key){
   if(key==='bind')return (...args:unknown[])=>wrap(target.bind(...args),sql);
   const member=Reflect.get(target,key);
   if(['first','all','run','raw'].includes(String(key)))return (...args:unknown[])=>{calls.push([sql]);return member.apply(target,args);};
   return typeof member==='function'?member.bind(target):member;
  }});
  statements.set(value,{raw,sql});return value;
 };
 const db={prepare:(sql:string)=>wrap(e.DB.prepare(sql),sql),batch:async(items:D1PreparedStatement[])=>{
  const entries=items.map(item=>statements.get(item)!);
  calls.push(entries.map(entry=>entry.sql));
  return e.DB.batch(entries.map(entry=>entry.raw));
 }} as D1Database;
 return {db,calls};
}

describe('batched external-operation preparation',()=>{
 it.each(['success','failed','unknown'])('preserves cached %s while paused without issuing or changing accounting',async phase=>{
  await run(e,'INSERT INTO operations(op_key,method,phase,result,reason,code,created_at,updated_at) VALUES(?,?,?,?,?,?,1,123)',`initial:cached-${phase}`,'copyMessage',phase,phase==='success'?JSON.stringify({message_id:7}):null,phase==='success'?null:'recorded outcome',phase==='failed'?403:null);
  await setSetting(e,'paused','1');
  const before=await one(e,'SELECT * FROM operations WHERE op_key=?',`initial:cached-${phase}`);
  const result=await call(e,`cached-${phase}`,'copyMessage',{chat_id:20001,from_chat_id:e.ADMIN_CHAT_ID,message_id:8});
  expect(result).toEqual(phase==='success'?{status:'success',result:{message_id:7}}:{status:phase,reason:'recorded outcome',code:phase==='failed'?403:undefined});
  expect(await one(e,'SELECT * FROM operations WHERE op_key=?',`initial:cached-${phase}`)).toEqual(before);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
 it('retains the actual successful reaction timestamp on cached reads',async()=>{
  await run(e,"INSERT INTO operations(op_key,method,phase,result,created_at,updated_at) VALUES('initial:reaction','setMessageReaction','success','true',1,123)");
  expect(await call(e,'reaction','setMessageReaction',{chat_id:e.ADMIN_CHAT_ID,message_id:8,reaction:[]})).toEqual({status:'success',result:true,confirmedAt:123});
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
 it.each(['persistent','environment'])('leaves a new request prepared when %s pause applies',async mode=>{
  if(mode==='persistent')await setSetting(e,'paused','1');
  await expect(call({...e,...(mode==='environment'?{PAUSED:'true'}:{})},'paused','copyMessage',{chat_id:20001,from_chat_id:e.ADMIN_CHAT_ID,message_id:8})).rejects.toBeInstanceOf(RetryLater);
  expect(await one(e,"SELECT phase,call_attempts,call_started_at FROM operations WHERE op_key='initial:paused'")).toEqual({phase:'prepared',call_attempts:0,call_started_at:null});
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
 it('uses three pre-API D1 round trips while durably marking both journals before sending',async()=>{
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,20,2)");
  await run(e,"INSERT INTO messages(user_id,direction,source_chat,source_id,thread_id,received_at,payload) VALUES('20001','out',?,8,20,1,'{}')",e.ADMIN_CHAT_ID);
  const message=(await one<{id:number}>(e,'SELECT id FROM messages WHERE source_id=8'))!;
  const {db,calls}=tracedDatabase(),values=new Map<string,unknown>();
  const storage={get:async(key:string)=>values.get(key),put:async(key:string|Record<string,unknown>,value?:unknown)=>{if(typeof key==='string')values.set(key,value);else for(const [k,v]of Object.entries(key))values.set(k,v);},delete:async(key:string)=>values.delete(key)};
  const client={...e,DB:db};
  const bot=new BotCoordinator({storage} as unknown as DurableObjectState,client);
  client.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:(url:string,init:RequestInit)=>bot.fetch(new Request(url,init))})} as unknown as DurableObjectNamespace;
  vi.mocked(fetch).mockImplementationOnce(async()=>{
   // Preparation + separate issued commit + final ordinary state checks.
   expect(calls.map(statements=>statements.length)).toEqual([3,1,3]);
   expect(await one(e,"SELECT phase FROM operations WHERE op_key='initial:measured'")).toEqual({phase:'issued'});
   expect(values.get('op:initial:measured')).toEqual({phase:'issued'});
   return Response.json({ok:true,result:{message_id:70}});
  });
  expect(await call(client,'measured','copyMessage',{chat_id:20001,from_chat_id:e.ADMIN_CHAT_ID,message_id:8},{messageIds:[message.id],threadVersion:{userId:'20001',version:2,threadId:20}})).toEqual({status:'success',result:{message_id:70}});
  expect(calls.map(statements=>statements.length)).toEqual([3,1,3,2]);
  expect(values.get('op:initial:measured')).toMatchObject({phase:'done',outcome:{status:'success',result:{message_id:70}}});
  expect(await one(e,"SELECT phase FROM operations WHERE op_key='initial:measured'")).toEqual({phase:'success'});
 });
});
