import {env} from 'cloudflare:test';
import {describe,expect,it,vi} from 'vitest';
import type {Env,Job,Params} from '../src/types';
import {all,one,run} from '../src/db';
import {handleReaction,syncReceipt} from '../src/receipts';
import {accept} from '../src/ingress';
import {ConversationCoordinator,execute} from '../src/coordinator';

async function delivered(e:Env) {
 await run(e,"INSERT INTO messages(id,user_id,direction,source_chat,source_id,admin_id,private_id,thread_id,received_at,delivery,payload) VALUES(1,'20001','out',?,10,10,900,40,1,'success','{}')",e.ADMIN_CHAT_ID);
}
function conversation(e:Env,pending:Promise<unknown>[]=[]) {
 const values=new Map<string,unknown>();
 const state={storage:{get:async(key:string)=>values.get(key),put:async(key:string,value:unknown)=>{values.set(key,value);},setAlarm:async()=>{}},waitUntil:(promise:Promise<unknown>)=>{pending.push(promise);}} as unknown as DurableObjectState;
 const coordinator=new ConversationCoordinator(state,e);
 return ()=>coordinator.fetch(new Request('https://conversation/wake',{method:'POST',body:JSON.stringify({key:'20001'})}));
}

describe('receipt wake hints after durable intent',()=>{
 it('drains consecutive administrator messages while their committed receipt hints remain pending',async()=>{
  const e={...env} as unknown as Env;
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,40,1)");
  for(const id of [11,12])await accept(e,{update_id:id,message:{message_id:id,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup'},from:{id:10001},message_thread_id:40,is_topic_message:true,text:`test ${id}`}});
  let release!:()=>void;
  const gate=new Promise<void>(resolve=>{release=resolve;});
  const pending:Promise<unknown>[]=[],copied:number[]=[];
  e.JOBS={send:async({key}:{key:string})=>{if(key.startsWith('receipt:'))await gate;}} as unknown as Queue;
  e.CONVERSATIONS={idFromName:(key:string)=>key,get:(key:string)=>({fetch:async()=>{if(key.startsWith('receipt:'))await gate;return new Response('ok');}})} as unknown as DurableObjectNamespace;
  e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
   const {method,params}=JSON.parse(String(init.body)) as {method:string;params:Params};
   expect(method).toBe('copyMessage');
   copied.push(Number(params.message_id));
   return Response.json({status:'success',result:{message_id:Number(params.message_id)+1000}});
  }})} as unknown as DurableObjectNamespace;
  const values=new Map<string,unknown>();
  const state={storage:{get:async(key:string)=>values.get(key),put:async(key:string,value:unknown)=>{values.set(key,value);},setAlarm:async()=>{}},waitUntil:(promise:Promise<unknown>)=>{pending.push(promise);}} as unknown as DurableObjectState;
  const coordinator=new ConversationCoordinator(state,e);
  let complete=false;
  const processing=coordinator.fetch(new Request('https://conversation/wake',{method:'POST',body:JSON.stringify({key:'20001'})})).then(response=>{expect(response.ok).toBe(true);complete=true;});
  try {
   await vi.waitFor(()=>expect(complete).toBe(true));
   expect(copied).toEqual([11,12]);
   expect(await all(e,"SELECT delivery,private_id FROM messages ORDER BY id")).toEqual([{delivery:'success',private_id:1011},{delivery:'success',private_id:1012}]);
   expect(await all(e,"SELECT state FROM jobs WHERE user_key='20001' ORDER BY id")).toEqual([{state:'done'},{state:'done'}]);
   expect(await all(e,"SELECT state FROM jobs WHERE kind='reaction' ORDER BY id")).toEqual([{state:'pending'},{state:'pending'}]);
   expect(pending).toHaveLength(2);
   expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  } finally {release();await processing;await Promise.all(pending);}
 });

 it.each(['failed','unknown'] as const)('does not delay the next administrator message behind %s notice hints or repeat the first send',async status=>{
  const e={...env} as unknown as Env;
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,40,1)");
  const updates=[11,12].map(id=>({update_id:id,message:{message_id:id,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup'},from:{id:10001},message_thread_id:40,is_topic_message:true,text:`test ${id}`}}));
  for(const update of updates)await accept(e,update);
  let release!:()=>void;
  const gate=new Promise<void>(resolve=>{release=resolve;});
  const pending:Promise<unknown>[]=[],copied:number[]=[];
  const delayed=(key:string)=>key.startsWith('receipt:')||key.startsWith('notice:');
  e.JOBS={send:async({key}:{key:string})=>{if(delayed(key))await gate;}} as unknown as Queue;
  e.CONVERSATIONS={idFromName:(key:string)=>key,get:(key:string)=>({fetch:async()=>{if(delayed(key))await gate;return new Response('ok');}})} as unknown as DurableObjectNamespace;
  e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
   const {method,params}=JSON.parse(String(init.body)) as {method:string;params:Params};
   expect(method).toBe('copyMessage');
   copied.push(Number(params.message_id));
   return Response.json(Number(params.message_id)===11?{status,reason:'Synthetic delivery result',...(status==='failed'?{code:403}:{})}:{status:'success',result:{message_id:1012}});
  }})} as unknown as DurableObjectNamespace;
  const wake=conversation(e,pending);
  let complete=false;
  const processing=wake().then(response=>{expect(response.ok).toBe(true);complete=true;});
  try {
   await vi.waitFor(()=>expect(complete).toBe(true));
   expect(copied).toEqual([11,12]);
   expect(await all(e,'SELECT delivery,private_id FROM messages ORDER BY id')).toEqual([{delivery:status,private_id:null},{delivery:'success',private_id:1012}]);
   expect(await one(e,"SELECT state FROM jobs WHERE user_key='notice:1'")).toEqual({state:'pending'});
   await accept(e,updates[0]);
   await wake();
   expect(copied).toEqual([11,12]);
   expect(await all(e,"SELECT phase FROM operations WHERE method='copyMessage' ORDER BY created_at,op_key")).toEqual([{phase:status},{phase:'success'}]);
   expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  } finally {release();await processing;await Promise.all(pending);}
 });

 it('finishes receipt scheduling while both wake hints remain pending',async()=>{
  const e={...env} as unknown as Env;
  await delivered(e);
  let release!:()=>void;
  const gate=new Promise<void>(resolve=>{release=resolve;});
  const scheduled:Promise<unknown>[]=[];
  let queueSawDurable=false,alarmSawDurable=false,complete=false;
  const committed=async()=>{
   expect(await one(e,"SELECT phase FROM receipts WHERE admin_id=10")).toEqual({phase:'pending'});
   expect(await one(e,"SELECT state FROM jobs WHERE user_key='receipt:10' AND kind='reaction'")).toEqual({state:'pending'});
  };
  e.JOBS={send:async()=>{await committed();queueSawDurable=true;await gate;}} as unknown as Queue;
  e.CONVERSATIONS={idFromName:(key:string)=>key,get:()=>({fetch:async()=>{await committed();alarmSawDurable=true;await gate;return new Response('ok');}})} as unknown as DurableObjectNamespace;
  const processing=syncReceipt(e,1,false,promise=>{scheduled.push(promise);}).then(()=>{complete=true;});
  try {
   await vi.waitFor(()=>{expect(queueSawDurable).toBe(true);expect(alarmSawDurable).toBe(true);});
   await vi.waitFor(()=>expect(complete).toBe(true));
   expect(scheduled).toHaveLength(1);
   expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  } finally {release();await processing;await Promise.all(scheduled);}
 });

 it('does not detach wake hints when the receipt transaction fails',async()=>{
  const e={...env} as unknown as Env;
  await delivered(e);
  const database=e.DB,send=vi.fn(),wake=vi.fn(),waitUntil=vi.fn();
  e.JOBS={send} as unknown as Queue;
  e.CONVERSATIONS={idFromName:(key:string)=>key,get:()=>({fetch:wake})} as unknown as DurableObjectNamespace;
  e.DB=new Proxy(database,{get(target,key){
   if(key==='batch')return async()=>{throw new Error('Synthetic receipt commit failure');};
   const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});
  await expect(syncReceipt(e,1,false,waitUntil)).rejects.toThrow('Synthetic receipt commit failure');
  expect(send).not.toHaveBeenCalled();expect(wake).not.toHaveBeenCalled();expect(waitUntil).not.toHaveBeenCalled();
  expect(await one({...e,DB:database},'SELECT admin_id FROM receipts')).toBeNull();
  expect(await one({...e,DB:database},"SELECT id FROM jobs WHERE kind='reaction'")).toBeNull();
 });

 it.each(['synchronous','asynchronous'])('keeps %s failed wake hints recoverable and rejects an obsolete cleanup',async mode=>{
  const e={...env} as unknown as Env;
  await delivered(e);
  const fail=()=>{if(mode==='synchronous')throw new Error('Synthetic wake failure');return Promise.reject(new Error('Synthetic wake failure'));};
  e.JOBS={send:fail} as unknown as Queue;
  e.CONVERSATIONS={idFromName:(key:string)=>key,get:()=>({fetch:fail})} as unknown as DurableObjectNamespace;
  const scheduled:Promise<unknown>[]=[];
  await syncReceipt(e,1,false,promise=>{scheduled.push(promise);});
  await Promise.all(scheduled);
  const job=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='reaction'"))!;
  expect(job.state).toBe('pending');
  const methods:string[]=[];
  e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
   methods.push(JSON.parse(String(init.body)).method);
   return Response.json({status:'success',result:true,confirmedAt:Date.now()});
  }})} as unknown as DurableObjectNamespace;
  await handleReaction(e,job);
  expect(await one(e,'SELECT phase FROM receipts WHERE admin_id=10')).toEqual({phase:'visible'});
  const cleanup=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='reaction' AND json_extract(payload,'$.action')='clear'"))!;
  vi.spyOn(Date,'now').mockReturnValue(cleanup.due_at);
  await run(e,"UPDATE messages SET tombstone=1,deletion='deleted' WHERE id=1");
  await handleReaction(e,cleanup);
  expect(await one(e,'SELECT version,emoji,phase FROM receipts WHERE admin_id=10')).toEqual({version:2,emoji:'👎',phase:'pending'});
  expect(await one(e,'SELECT state FROM jobs WHERE id=?',cleanup.id)).toEqual({state:'done'});
  expect(methods).toEqual(['setMessageReaction']);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
});

describe('first-contact delivery and asynchronous card ordering',()=>{
 it('pins the initial panel before the first business message and defers a hanging avatar query',async()=>{
  const e={...env} as unknown as Env;
  let clock=100000;
  vi.spyOn(Date,'now').mockImplementation(()=>clock);
  const update=(id:number)=>({update_id:id,message:{message_id:id,date:1,chat:{id:20001,type:'private'},from:{id:20001,first_name:'Synthetic'},text:`test ${id}`}});
  await accept(e,update(21));
  let release!:()=>void;
  const avatarGate=new Promise<void>(resolve=>{release=resolve;});
  const methods:string[]=[];
  e.JOBS={send:async()=>{}} as unknown as Queue;
  e.CONVERSATIONS={idFromName:(key:string)=>key,get:()=>({fetch:async()=>new Response('ok')})} as unknown as DurableObjectNamespace;
  e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
   const {method,params}=JSON.parse(String(init.body)) as {method:string;params:Params};
   methods.push(method);
   if(method==='getUserProfilePhotos')await avatarGate;
   const result=method==='getChat'?{id:20001,first_name:'Synthetic'}:method==='createForumTopic'?{message_thread_id:40}:method==='copyMessage'?{message_id:Number(params.message_id)+1000}:method==='getUserProfilePhotos'?{photos:[]}:method==='sendMessage'?{message_id:700}:true;
   return Response.json({status:'success',result});
  }})} as unknown as DurableObjectNamespace;
  const pending:Promise<unknown>[]=[],wake=conversation(e,pending);
  await wake();
  expect(methods).toEqual(['getChat','createForumTopic','sendMessage','pinChatMessage','copyMessage']);
  expect(await one(e,'SELECT first_contact,thread_id,binding_version,card_id FROM users WHERE tgid=\'20001\'')).toEqual({first_contact:100000,thread_id:40,binding_version:1,card_id:700});
  const card=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='card'"))!;
  let cardDone=false;
  const cardProcessing=execute(e,card).then(()=>{cardDone=true;});
  try {
   await vi.waitFor(()=>expect(methods).toContain('getUserProfilePhotos'));
   expect(cardDone).toBe(false);
   expect(await one(e,'SELECT delivery,admin_id FROM messages WHERE source_id=21')).toEqual({delivery:'success',admin_id:1021});
  } finally {release();await cardProcessing;await Promise.all(pending);}
  clock=200000;
  await accept(e,update(22));await wake();await Promise.all(pending);
  expect(await one(e,"SELECT first_contact,thread_id FROM users WHERE tgid='20001'")).toEqual({first_contact:100000,thread_id:40});
  expect(methods.filter(method=>method==='createForumTopic')).toHaveLength(1);
  expect(methods.filter(method=>method==='sendMessage')).toHaveLength(1);
  expect(methods.filter(method=>method==='copyMessage')).toHaveLength(2);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });

 it('keeps unknown first topic creation under one operation across later private messages',async()=>{
  const e={...env} as unknown as Env;
  let clock=100000;
  vi.spyOn(Date,'now').mockImplementation(()=>clock);
  e.JOBS={send:async()=>{}} as unknown as Queue;
  e.CONVERSATIONS={idFromName:(key:string)=>key,get:()=>({fetch:async()=>new Response('ok')})} as unknown as DurableObjectNamespace;
  const methods:string[]=[];
  e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
   const {method}=JSON.parse(String(init.body)) as {method:string};
   methods.push(method);
   return Response.json(method==='getChat'?{status:'success',result:{id:20001,first_name:'Synthetic'}}:{status:'unknown',reason:'Synthetic interrupted topic response'});
  }})} as unknown as DurableObjectNamespace;
  const pending:Promise<unknown>[]=[],wake=conversation(e,pending);
  for(const id of [31,32]) {
   await accept(e,{update_id:id,message:{message_id:id,date:1,chat:{id:20001,type:'private'},from:{id:20001},text:`test ${id}`}});
   await wake();clock+=1000;
  }
  await Promise.all(pending);
  expect(methods.filter(method=>method==='createForumTopic')).toHaveLength(1);
  expect(methods).not.toContain('copyMessage');
  expect(await one(e,"SELECT first_contact,thread_id FROM users WHERE tgid='20001'")).toEqual({first_contact:100000,thread_id:null});
  expect(await one(e,"SELECT phase FROM operations WHERE op_key='initial:thread:create:20001'")).toEqual({phase:'unknown'});
  expect(await all(e,'SELECT delivery FROM messages ORDER BY id')).toEqual([{delivery:'pending'},{delivery:'pending'}]);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
});
