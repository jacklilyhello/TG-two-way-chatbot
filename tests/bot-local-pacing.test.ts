import {env} from 'cloudflare:test';
import {describe,expect,it,vi} from 'vitest';
import type {Env,Params} from '../src/types';
import {BotCoordinator} from '../src/bot';
import {call} from '../src/operations';
import {one,run} from '../src/db';
import {deliveryRetryGuardValid} from '../src/delivery-retry';

const base=env as unknown as Env;
function directBot() {
 const values=new Map<string,unknown>();
 const storage={get:async(key:string)=>values.get(key),put:async(key:string|Record<string,unknown>,value?:unknown)=>{if(typeof key==='string')values.set(key,value);else for(const [name,item]of Object.entries(key))values.set(name,item);},delete:async(key:string)=>values.delete(key)};
 const e={...base};
 const bot=new BotCoordinator({storage} as unknown as DurableObjectState,e);
 const client={...e,BOT:{idFromName:()=>base.INSTANCE_ID,get:()=>({fetch:(url:string,init:RequestInit)=>bot.fetch(new Request(url,init))})}} as unknown as Env;
 const invoke=(key:string,method='copyMessage',params:Params={chat_id:base.ADMIN_CHAT_ID,message_id:1},guard?:unknown)=>bot.fetch(new Request('https://bot/execute',{method:'POST',body:JSON.stringify({key:`${base.RECOVERY_EPOCH}:${key}`,method,params,guard})}));
 const journal=(key:string)=>bot.fetch(new Request('https://bot/journal',{method:'POST',body:JSON.stringify({key:`${base.RECOVERY_EPOCH}:${key}`})}));
 return {values,e,client,invoke,journal};
}
function allowTelegram() {vi.mocked(fetch).mockImplementation(async()=>Response.json({ok:true,result:{message_id:900}}));}
// Wait for the actual pacing timer, then advance it only after the concurrent
// state change commits. CI speed cannot consume the guard-change window.
function pacingClock() {
 let clock=100000,timerId=0;
 const pending:{at:number;callback:()=>void}[]=[],listeners:(()=>void)[]=[];
 vi.spyOn(Date,'now').mockImplementation(()=>clock);
 const timers=vi.spyOn(globalThis,'setTimeout').mockImplementation(((callback:()=>void,delay=0)=>{
  pending.push({at:clock+delay,callback});
  for(const notify of listeners.splice(0))notify();
  return ++timerId;
 }) as typeof setTimeout);
 return {
  entered:async(count=1)=>{while(pending.length<count)await new Promise<void>(resolve=>listeners.push(resolve));},
  release:(count=1,advance=true)=>{
   const ready=pending.splice(0,count);expect(ready).toHaveLength(count);
   if(advance)clock=Math.max(clock,...ready.map(timer=>timer.at));
   for(const timer of ready)timer.callback();
  },
  scheduled:()=>timers.mock.calls.map(([,delay])=>delay),
 };
}

describe('bounded local pacing without full business preparation replay',()=>{
 it('finishes one operation RPC across a short local rate deadline',async()=>{
  const pacing=pacingClock(),{values,client}=directBot();allowTelegram();
  values.set(`rate:${base.ADMIN_CHAT_ID}`,Date.now()+100);
  const outcome=call(client,'waiting','copyMessage',{chat_id:base.ADMIN_CHAT_ID,message_id:1});
  await pacing.entered();expect(vi.mocked(fetch)).not.toHaveBeenCalled();pacing.release();
  expect((await outcome).status).toBe('success');
  expect(await one(base,"SELECT phase,call_attempts FROM operations WHERE op_key='initial:waiting'")).toEqual({phase:'success',call_attempts:1});
  expect((await one<{bot_wait_ms:number}>(base,"SELECT bot_wait_ms FROM operations WHERE op_key='initial:waiting'"))!.bot_wait_ms).toBe(100);
  expect(vi.mocked(fetch)).toHaveBeenCalledOnce();
 });

 it('returns longer local deadlines to durable scheduling without issuing',async()=>{
  const pacing=pacingClock(),{values,invoke}=directBot();allowTelegram();
  const due=Date.now()+2000;values.set(`rate:${base.ADMIN_CHAT_ID}`,due);
  const response=await invoke('waiting');
  expect(response.status).toBe(429);expect(await response.json()).toEqual({due});
  expect(response.headers.has('X-Bot-Local-Rate-Due')).toBe(false);
  expect(values.has('op:initial:waiting')).toBe(false);
  expect(pacing.scheduled()).toEqual([]);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });

 it('leaves the execution tail free for other chats, reads and journal reconciliation',async()=>{
  const pacing=pacingClock(),{values,invoke,journal}=directBot();allowTelegram();
  values.set(`rate:${base.ADMIN_CHAT_ID}`,Date.now()+400);
  let finished=false;
  const waiting=invoke('waiting').then(response=>{finished=true;return response;});
  await pacing.entered();expect(values.has('op:initial:waiting')).toBe(false);
  expect((await invoke('private','copyMessage',{chat_id:20001,message_id:2})).status).toBe(200);
  expect((await invoke('read','getMe',{})).status).toBe(200);
  expect((await journal('different-operation')).status).toBe(200);
  expect(finished).toBe(false);
  pacing.release();
  expect((await waiting).status).toBe(200);
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(3);
 });

 it.each(['binding','tombstone','pause'] as const)('rechecks %s after a short wait before issuing any external request',async change=>{
  const pacing=pacingClock(),{values,invoke}=directBot();allowTelegram();
  await run(base,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,40,1)");
  await run(base,"INSERT INTO messages(id,user_id,direction,source_chat,source_id,admin_id,thread_id,received_at,payload) VALUES(1,'20001','out',?,1,1,40,1,'{}')",base.ADMIN_CHAT_ID);
  values.set(`rate:${base.ADMIN_CHAT_ID}`,Date.now()+150);
  const waiting=invoke('waiting','copyMessage',undefined,{messageIds:[1],threadVersion:{userId:'20001',version:1,threadId:40}});
  await pacing.entered();expect(values.has('op:initial:waiting')).toBe(false);
  if(change==='binding')await run(base,"UPDATE users SET binding_version=2,thread_id=41 WHERE tgid='20001'");
  if(change==='tombstone')await run(base,'UPDATE messages SET tombstone=1 WHERE id=1');
  if(change==='pause')await run(base,"INSERT INTO settings(key,value) VALUES('paused','1')");
  pacing.release();
  const response=await waiting;
  if(change==='pause')expect(response.status).toBe(429);
  else expect(await response.json()).toMatchObject({status:'failed',reason:'cancelled_by_state'});
  expect(values.get('op:initial:waiting')).not.toEqual({phase:'issued'});
  expect(values.has('rate:global')).toBe(false);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });

 it('deduplicates concurrent same-key requests that wake at the same deadline',async()=>{
  const pacing=pacingClock(),{values,invoke}=directBot();allowTelegram();
  values.set(`rate:${base.ADMIN_CHAT_ID}`,Date.now()+100);
  const responses=Promise.all([invoke('waiting'),invoke('waiting')]);
  await pacing.entered(2);expect(vi.mocked(fetch)).not.toHaveBeenCalled();pacing.release(2);
  for(const response of await responses)expect(await response.json()).toMatchObject({status:'success'});
  expect(vi.mocked(fetch)).toHaveBeenCalledOnce();
 });

 it('honors journal sealing while a request waits outside the tail',async()=>{
  const pacing=pacingClock(),{values,invoke,journal}=directBot();allowTelegram();
  values.set(`rate:${base.ADMIN_CHAT_ID}`,Date.now()+150);
  const waiting=invoke('waiting');await pacing.entered();
  expect(await (await journal('waiting')).json()).toMatchObject({status:'unknown'});
  pacing.release();
  expect(await (await waiting).json()).toMatchObject({status:'unknown'});
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });

 it('observes a platform backoff introduced by another request while waiting',async()=>{
  const pacing=pacingClock(),{values,invoke}=directBot();allowTelegram();
  values.set(`rate:${base.ADMIN_CHAT_ID}`,Date.now()+150);
  const waiting=invoke('waiting');await pacing.entered();
  const due=Date.now()+2000;values.set('rate:backoff',due);
  pacing.release();
  const response=await waiting;
  expect(response.status).toBe(429);expect(await response.json()).toEqual({due});
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });

 it('rechecks a reserved delivery retry actor after waiting',async()=>{
  const pacing=pacingClock(),{values,e,invoke}=directBot();allowTelegram();
  await run(base,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,40,1)");
  const original={message_id:1,chat:{id:Number(base.ADMIN_CHAT_ID)},from:{id:10001},message_thread_id:40};
  await run(base,"INSERT INTO messages(id,user_id,direction,source_chat,source_id,admin_id,thread_id,received_at,delivery,payload) VALUES(1,'20001','out',?,1,1,40,1,'failed',?)",base.ADMIN_CHAT_ID,JSON.stringify(original));
  await run(base,"INSERT INTO operations(op_key,method,phase,code,reason,created_at,updated_at) VALUES('initial:send:1','copyMessage','failed',403,'Synthetic rejection',1,1)");
  await run(base,"INSERT INTO delivery_attempts(message_id,attempt,op_key,prior_op_key,action_token,actor_id,user_id,thread_id,binding_version,epoch,created_at) VALUES(1,1,'send-retry:1:1','initial:send:1','synthetic-action','10001','20001',40,1,'initial',1)");
  const guard={messageId:1,attempt:1,actorId:'10001',userId:'20001',threadId:40,bindingVersion:1,priorOpKey:'initial:send:1'};
  expect(await deliveryRetryGuardValid(e,guard)).toBe(true);
  values.set(`rate:${base.ADMIN_CHAT_ID}`,Date.now()+150);
  const waiting=invoke('waiting','copyMessage',undefined,{deliveryRetry:guard});await pacing.entered();
  e.ADMIN_IDS='';
  pacing.release();
  expect(await (await waiting).json()).toMatchObject({status:'failed',reason:'cancelled_by_state'});
  expect(values.has('rate:global')).toBe(false);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });

 it('returns even a short platform backoff directly to durable scheduling',async()=>{
  const pacing=pacingClock(),{values,invoke}=directBot();allowTelegram();
  const due=Date.now()+150;values.set('rate:backoff',due);
  const response=await invoke('waiting');
  expect(response.status).toBe(429);expect(await response.json()).toEqual({due});
  expect(pacing.scheduled()).toEqual([]);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });

 it('bounds scheduled sleep even when the clock never advances',async()=>{
  const pacing=pacingClock(),{values,invoke}=directBot();allowTelegram();
  values.set(`rate:${base.ADMIN_CHAT_ID}`,100600);
  const response=invoke('waiting');await pacing.entered();pacing.release(1,false);
  const limited=await response;
  expect(limited.status).toBe(429);expect(await limited.json()).toEqual({due:100600});
  expect(pacing.scheduled()).toEqual([600]);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });

 it('also caps repeated short deadline waits independently of the time budget',async()=>{
  const pacing=pacingClock(),{values,invoke}=directBot();allowTelegram();
  values.set(`rate:${base.ADMIN_CHAT_ID}`,100100);
  const reads=vi.spyOn(values,'get');
  const response=invoke('waiting');
  for(let i=0;i<4;i++){await pacing.entered();pacing.release(1,false);}
  const limited=await response;
  expect(limited.status).toBe(429);expect(await limited.json()).toEqual({due:100100});
  expect(pacing.scheduled()).toEqual([100,100,100,100]);
  expect(reads.mock.calls.filter(([key])=>key==='op:initial:waiting')).toHaveLength(5);
  expect(limited.headers.has('X-Bot-Local-Rate-Due')).toBe(false);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });

 it('keeps an unknown external result terminal after a short local wait',async()=>{
  const pacing=pacingClock(),{values,invoke}=directBot();
  vi.mocked(fetch).mockRejectedValue(new Error('Synthetic connection lost'));
  values.set(`rate:${base.ADMIN_CHAT_ID}`,Date.now()+100);
  const waiting=invoke('waiting');await pacing.entered();pacing.release();
  expect(await (await waiting).json()).toMatchObject({status:'unknown'});
  expect(await (await invoke('waiting')).json()).toMatchObject({status:'unknown'});
  expect(vi.mocked(fetch)).toHaveBeenCalledOnce();
 });

 it('returns real Telegram 429 and maintenance pause without an inline retry',async()=>{
  const pacing=pacingClock(),{values,invoke}=directBot();
  vi.mocked(fetch).mockResolvedValue(Response.json({ok:false,error_code:429,parameters:{retry_after:1}}));
  const limited=await invoke('platform');
  expect(limited.status).toBe(429);expect(pacing.scheduled()).toEqual([]);
  expect(values.has('op:initial:platform')).toBe(false);
  expect(vi.mocked(fetch)).toHaveBeenCalledOnce();
  await run(base,"INSERT INTO settings(key,value) VALUES('paused','1')");
  values.delete('rate:backoff');values.delete('rate:global');values.delete(`rate:${base.ADMIN_CHAT_ID}`);
  const paused=await invoke('paused');
  expect(paused.status).toBe(429);
  expect(vi.mocked(fetch)).toHaveBeenCalledOnce();
 });

 it('does not replay a previously issued operation after reconstruction',async()=>{
  const pacing=pacingClock(),{values,invoke}=directBot();allowTelegram();
  values.set('op:initial:waiting',{phase:'issued'});
  values.set(`rate:${base.ADMIN_CHAT_ID}`,Date.now()+100);
  expect(await (await invoke('waiting')).json()).toMatchObject({status:'unknown'});
  expect(pacing.scheduled()).toEqual([]);
  expect(await (await invoke('waiting')).json()).toMatchObject({status:'unknown'});
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
});
