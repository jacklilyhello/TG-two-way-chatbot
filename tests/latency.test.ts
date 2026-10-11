import {env,runInDurableObject} from 'cloudflare:test';
import {describe,it,expect,vi} from 'vitest';
import {BotCoordinator} from '../src/bot';
import worker from '../src/index';
import {call,RetryLater} from '../src/operations';
import {one,run,armWork} from '../src/db';
import type {Env} from '../src/types';
import {syncReceipt,type ReceiptGuard} from '../src/receipts';

const e=env as unknown as Env;
function directBot() {
 const values=new Map<string,unknown>();
 const storage={get:async(key:string)=>values.get(key),put:async(key:string|Record<string,unknown>,value?:unknown)=>{if(typeof key==='string')values.set(key,value);else for(const [k,v]of Object.entries(key))values.set(k,v);},delete:async(key:string)=>values.delete(key)};
 const bot=new BotCoordinator({storage} as unknown as DurableObjectState,e);
 const client={...e,BOT:{idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:(url:string,init:RequestInit)=>bot.fetch(new Request(url,init))})}} as unknown as Env;
 return {client,values};
}

describe('delivery pacing and measured stages',()=>{
 it('keeps group message pacing shared across topics while reactions use their own paced lane',async()=>{
  const {client,values}=directBot();let clock=100000;vi.spyOn(Date,'now').mockImplementation(()=>clock);
  vi.mocked(fetch).mockImplementation(async()=>Response.json({ok:true,result:true}));
  await call(client,'group-one','sendMessage',{chat_id:e.ADMIN_CHAT_ID,message_thread_id:20,text:'test'});
  clock+=50;
  const result=await call(client,'receipt','setMessageReaction',{chat_id:e.ADMIN_CHAT_ID,message_id:1,reaction:[{type:'emoji',emoji:'👍'}]});
  expect(result).toMatchObject({status:'success',confirmedAt:clock});
  expect(values.get(`rate:${e.ADMIN_CHAT_ID}`)).toBe(103100);
  clock+=50;
  await expect(call(client,'group-two','sendMessage',{chat_id:e.ADMIN_CHAT_ID,message_thread_id:30,text:'test'})).rejects.toBeInstanceOf(RetryLater);
  await expect(call(client,'receipt-two','setMessageReaction',{chat_id:e.ADMIN_CHAT_ID,message_id:2,reaction:[]})).rejects.toBeInstanceOf(RetryLater);
  clock=103100;
  await call(client,'group-two','sendMessage',{chat_id:e.ADMIN_CHAT_ID,message_thread_id:30,text:'test'});
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(3);
 });
 it('reserves the group send budget for every album member',async()=>{
  const {client,values}=directBot();vi.spyOn(Date,'now').mockReturnValue(100000);
  vi.mocked(fetch).mockResolvedValueOnce(Response.json({ok:true,result:[{message_id:1},{message_id:2}]}));
  await call(client,'album','sendMediaGroup',{chat_id:e.ADMIN_CHAT_ID,media:[{type:'photo',media:'one'},{type:'photo',media:'two'}]});
  expect(values.get(`rate:${e.ADMIN_CHAT_ID}`)).toBe(106200);
 });
 it('starts Telegram retry_after at the response and honors it for reads as well as reactions',async()=>{
  const {client,values}=directBot();let clock=100000;vi.spyOn(Date,'now').mockImplementation(()=>clock);
  vi.mocked(fetch).mockImplementationOnce(async()=>{clock+=2500;return Response.json({ok:false,error_code:429,parameters:{retry_after:2}});});
  await expect(call(client,'limited','setMessageReaction',{chat_id:e.ADMIN_CHAT_ID,message_id:1,reaction:[]})).rejects.toMatchObject({due:104500});
  expect(values.get('rate:backoff')).toBe(104500);
  clock=104499;
  await expect(call(client,'profile','getMe',{})).rejects.toBeInstanceOf(RetryLater);
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
  clock=104500;vi.mocked(fetch).mockResolvedValueOnce(Response.json({ok:true,result:{id:123456}}));
  await call(client,'profile','getMe',{});
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(2);
 });
 it('records API and operation durations without adding private payloads to timing metadata',async()=>{
  const {client}=directBot();let clock=100000;vi.spyOn(Date,'now').mockImplementation(()=>clock);
  vi.mocked(fetch).mockImplementationOnce(async()=>{clock+=321;return Response.json({ok:true,result:{message_id:1}});});
  await call(client,'measured','copyMessage',{chat_id:20001,from_chat_id:e.ADMIN_CHAT_ID,message_id:8});
  expect(await one(e,"SELECT call_started_at,call_finished_at,call_attempts,api_ms,transport_ms,bot_wait_ms,rate_wait_ms FROM operations WHERE op_key='initial:measured'")).toEqual({call_started_at:100000,call_finished_at:100321,call_attempts:1,api_ms:321,transport_ms:321,bot_wait_ms:0,rate_wait_ms:0});
  await call(client,'measured','copyMessage',{chat_id:20001,from_chat_id:e.ADMIN_CHAT_ID,message_id:8});
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
  expect(await one(e,"SELECT call_attempts FROM operations WHERE op_key='initial:measured'")).toEqual({call_attempts:1});
 });
 it('does not consume group send allowance for state-cancelled requests',async()=>{
  const {client,values}=directBot();vi.spyOn(Date,'now').mockReturnValue(100000);
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,20,2)");
  expect(await call(client,'old-binding','sendMessage',{chat_id:e.ADMIN_CHAT_ID,text:'test'},{threadVersion:{userId:'20001',version:1,threadId:20}})).toMatchObject({status:'failed',reason:'cancelled_by_state'});
  expect(values.has(`rate:${e.ADMIN_CHAT_ID}`)).toBe(false);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
 it('rejects a stale receipt at the final Bot guard before Telegram is called',async()=>{
  const {client}=directBot();
  await run(e,"INSERT INTO messages(user_id,direction,source_chat,source_id,admin_id,private_id,thread_id,received_at,payload,delivery) VALUES('20001','out',?,8,8,50,20,1,'{}','success')",e.ADMIN_CHAT_ID);
  const row=(await one<{id:number}>(e,'SELECT id FROM messages WHERE admin_id=8'))!;
  await syncReceipt({...e,JOBS:{send:async()=>{}}} as unknown as Env,row.id);
  const saved=(await one<{version:number;signature:string}>(e,'SELECT version,signature FROM receipts WHERE admin_id=8'))!;
  const receipt:ReceiptGuard={adminId:8,...saved,emoji:'👍',clear:false};
  await run(e,"UPDATE messages SET tombstone=1,deletion='deleted' WHERE id=?",row.id);
  expect(await call(client,'stale-receipt','setMessageReaction',{chat_id:e.ADMIN_CHAT_ID,message_id:8,reaction:[{type:'emoji',emoji:'👍'}]},{receipt})).toMatchObject({status:'failed',reason:'cancelled_by_state'});
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
});

describe('independent queue wake scheduling',()=>{
 it('arms a persisted alarm without executing business work or accepting the wrong coordinator key',async()=>{
  await run(e,"INSERT INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES('arm-test','20001','update','{}','pending',0,'initial',1)");
  await armWork(e,'20001');
  const stub=e.CONVERSATIONS.get(e.CONVERSATIONS.idFromName('20001'));
  await runInDurableObject(stub,async(_object,state)=>{
   expect(await state.storage.get('key')).toBe('20001');
   expect(await state.storage.getAlarm()).toBeGreaterThan(Date.now()-1000);
  });
  expect(await one(e,"SELECT state FROM jobs WHERE dedupe='arm-test'")).toEqual({state:'pending'});
  const wrong=await stub.fetch('https://conversation/arm',{method:'POST',body:JSON.stringify({key:'20002',epoch:e.RECOVERY_EPOCH})});
  expect(wrong.status).toBe(409);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
 it('commits Webhook receipt before scheduling the non-blocking arm even when Queue notification fails',async()=>{
  let finish!:()=>void;const pending:Promise<unknown>[]=[];let armed=false;
  const client={...e,JOBS:{send:async()=>{throw new Error('queue unavailable');}},CONVERSATIONS:{idFromName:(key:string)=>key,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
   expect(JSON.parse(String(init.body))).toEqual({key:'20001',epoch:e.RECOVERY_EPOCH});
   expect(await one(e,"SELECT state FROM jobs WHERE dedupe='u:901'")).toEqual({state:'pending'});
   armed=true;await new Promise<void>(resolve=>{finish=resolve;});return new Response('ok');
  }})}} as unknown as Env;
  const ctx={waitUntil:(promise:Promise<unknown>)=>{pending.push(promise);}} as ExecutionContext;
  const response=await worker.fetch(new Request('https://instance/webhook',{method:'POST',headers:{'Content-Type':'application/json','X-Telegram-Bot-Api-Secret-Token':e.TELEGRAM_WEBHOOK_SECRET},body:JSON.stringify({update_id:901,message:{message_id:901,date:1,chat:{id:20001,type:'private'},from:{id:20001},text:'test'}})}),client,ctx);
  expect(response.status).toBe(200);expect(pending).toHaveLength(1);
  await vi.waitFor(()=>expect(armed).toBe(true));
  finish();await Promise.all(pending);
 });
 it('coalesces duplicate keys and starts unrelated conversations without waiting for the first',async()=>{
  const started:string[]=[];let finish!:()=>void;
  const client={...e,CONVERSATIONS:{idFromName:(key:string)=>key,get:(key:string)=>({fetch:async()=>{started.push(key);if(key==='slow')await new Promise<void>(resolve=>{finish=resolve;});return new Response('ok');}})}} as unknown as Env;
  const message=(key:string)=>({body:{key},ack:vi.fn(),retry:vi.fn()});
  const messages=[message('slow'),message('fast'),message('slow')];
  const queued=worker.queue({messages} as unknown as MessageBatch<{key:string}>,client);
  await vi.waitFor(()=>expect(started).toEqual(['slow','fast']));
  await vi.waitFor(()=>expect(messages[1].ack).toHaveBeenCalledOnce());expect(messages[0].ack).not.toHaveBeenCalled();
  finish();await queued;
  for(const m of messages){expect(m.ack).toHaveBeenCalledOnce();expect(m.retry).not.toHaveBeenCalled();}
 });
 it('keeps retry and acknowledgement scoped to each key',async()=>{
  const client={...e,CONVERSATIONS:{idFromName:(key:string)=>key,get:(key:string)=>({fetch:async()=>new Response('',{status:key==='broken'?503:200})})}} as unknown as Env;
  const messages=['broken','ok','broken'].map(key=>({body:{key},ack:vi.fn(),retry:vi.fn()}));
  await worker.queue({messages} as unknown as MessageBatch<{key:string}>,client);
  expect(messages[1].ack).toHaveBeenCalledOnce();
  for(const m of [messages[0],messages[2]]){expect(m.ack).not.toHaveBeenCalled();expect(m.retry).toHaveBeenCalledWith({delaySeconds:10});}
 });
});
