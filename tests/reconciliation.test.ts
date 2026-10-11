import {env,evictDurableObject,runInDurableObject} from 'cloudflare:test';
import {describe,expect,it,vi} from 'vitest';
import type {Env} from '../src/types';
import {all,one,run,setSetting} from '../src/db';
import {call,reconcileIssued,RetryLater} from '../src/operations';
import {BotCoordinator} from '../src/bot';
import {failOperationCompletion} from './completion-fault';
import worker from '../src/index';

const e=env as unknown as Env;
const issued=(key:string)=>run(e,"INSERT INTO operations(op_key,method,phase,created_at,updated_at) VALUES(?,'copyMessage','issued',1,1)",key);
function directClient() {
 const data=new Map<string,unknown>();
 const storage={get:async(k:string)=>data.get(k),put:async(k:string|Record<string,unknown>,v?:unknown)=>{if(typeof k==='string')data.set(k,v);else for(const [key,value] of Object.entries(k))data.set(key,value);},delete:async(k:string)=>data.delete(k)};
 const bot=new BotCoordinator({storage} as unknown as DurableObjectState,e);
 return {...e,BOT:{idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:(url:string,init:RequestInit)=>bot.fetch(new Request(url,init))})} as unknown as DurableObjectNamespace};
}

describe('paused journal reconciliation',()=>{
 it('restores native DO results and seals orphan or missing journals, including prior epochs',async()=>{
  await setSetting(e,'paused','1');
  const stub=e.BOT.get(e.BOT.idFromName(e.INSTANCE_ID));
  await runInDurableObject(stub,async(_object,state)=>{
   await state.storage.put('op:initial:confirmed',{phase:'done',outcome:{status:'success',result:{message_id:777}}});
   await state.storage.put('op:initial:interrupted',{phase:'issued'});
  });
  await evictDurableObject(stub);
  for(const key of ['initial:confirmed','initial:interrupted','initial:delayed','previous:old'])await issued(key);
  expect(await reconcileIssued(e)).toBe(4);
  expect(await all(e,'SELECT op_key,phase FROM operations ORDER BY op_key')).toEqual([
   {op_key:'initial:confirmed',phase:'success'},{op_key:'initial:delayed',phase:'unknown'},
   {op_key:'initial:interrupted',phase:'unknown'},{op_key:'previous:old',phase:'unknown'}
  ]);
  expect(JSON.parse((await one<{result:string}>(e,"SELECT result FROM operations WHERE op_key='initial:confirmed'"))!.result)).toEqual({message_id:777});
  expect((await all(e,'SELECT * FROM delivery_events')).length).toBe(4);
  expect(await reconcileIssued(e)).toBe(0);
  const delayed=await stub.fetch('https://coordinator/execute',{method:'POST',body:JSON.stringify({key:'initial:delayed',method:'copyMessage',params:{chat_id:20001,from_chat_id:e.ADMIN_CHAT_ID,message_id:1}})});
  expect(delayed.status).toBe(200);
  expect(await delayed.json()).toMatchObject({status:'unknown'});
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
 it('recovers a successful response after D1 completion fails even when persistent pause is already set',async()=>{
  const client=directClient();
  vi.mocked(fetch).mockResolvedValueOnce(Response.json({ok:true,result:{message_id:999}}));
  const broken={...client,DB:failOperationCompletion(e.DB,'D1 completion failed')};
  await expect(call(broken,'d1-completion','copyMessage',{chat_id:20001,from_chat_id:e.ADMIN_CHAT_ID,message_id:1})).rejects.toThrow('D1 completion failed');
  expect(await one(e,"SELECT phase FROM operations WHERE op_key='initial:d1-completion'")).toEqual({phase:'issued'});
  await setSetting(e,'paused','1');
  expect(await reconcileIssued(client)).toBe(1);
  expect(await call(client,'d1-completion','copyMessage',{chat_id:20001,from_chat_id:e.ADMIN_CHAT_ID,message_id:1})).toEqual({status:'success',result:{message_id:999}});
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
 });
 it('waits behind an active request on the same Bot tail instead of prematurely changing it to unknown',async()=>{
  const client=directClient();
  let finish!: (response:Response)=>void;
  vi.mocked(fetch).mockImplementationOnce(()=>new Promise<Response>(resolve=>{finish=resolve;}));
  const active=call(client,'active','copyMessage',{chat_id:20001,from_chat_id:e.ADMIN_CHAT_ID,message_id:1});
  await vi.waitFor(()=>expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1));
  await setSetting(e,'paused','1');
  const reconciliation=reconcileIssued(client);
  finish(Response.json({ok:true,result:{message_id:888}}));
  expect(await active).toEqual({status:'success',result:{message_id:888}});
  await reconciliation;
  expect(await one(e,"SELECT phase FROM operations WHERE op_key='initial:active'")).toEqual({phase:'success'});
  expect((await all(e,"SELECT * FROM delivery_events WHERE op_key='initial:active'")).length).toBe(1);
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
 });
 it('bounds one maintenance scan to forty issued rows and leaves ordinary paused jobs untouched',async()=>{
  await setSetting(e,'paused','1');
  await e.DB.batch(Array.from({length:41},(_,i)=>e.DB.prepare("INSERT INTO operations(op_key,method,phase,created_at,updated_at) VALUES(?,'copyMessage','issued',1,1)").bind(`initial:batch:${i}`)));
  await run(e,"INSERT INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES('paused-job','20001','update','{}','pending',0,'initial',1)");
  expect(await reconcileIssued(e)).toBe(40);
  expect(await one(e,"SELECT COUNT(*) n FROM operations WHERE phase='issued'")).toEqual({n:1});
  expect(await reconcileIssued(e)).toBe(1);
  expect(await one(e,"SELECT state FROM jobs WHERE dedupe='paused-job'")).toEqual({state:'pending'});
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
 it('lets authenticated maintenance status drain journals without dispatching conversations or queues',async()=>{
  await setSetting(e,'paused','1');await issued('initial:maintenance');
  await run(e,"INSERT INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES('queued','20001','update','{}','pending',0,'initial',1)");
  const enqueue=vi.fn(),wake=vi.fn();
  const client={...e,JOBS:{send:enqueue},CONVERSATIONS:{idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:wake})}} as unknown as Env;
  const response=await worker.fetch(new Request('https://instance/internal/maintenance',{method:'POST',headers:{'Content-Type':'application/json','X-Telegram-Bot-Api-Secret-Token':e.TELEGRAM_WEBHOOK_SECRET},body:JSON.stringify({action:'status'})}),client);
  expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({ok:true,paused:true,issued:0,jobs:[{state:'pending',count:1}]});
  expect(enqueue).not.toHaveBeenCalled();expect(wake).not.toHaveBeenCalled();
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
 it('does not reconcile or seal planned operations while normal processing is enabled',async()=>{
  await issued('initial:not-paused');
  expect(await reconcileIssued(e)).toBe(0);
  expect(await one(e,"SELECT phase FROM operations WHERE op_key='initial:not-paused'")).toEqual({phase:'issued'});
  const stub=e.BOT.get(e.BOT.idFromName(e.INSTANCE_ID));
  await runInDurableObject(stub,async(_object,state)=>{expect(await state.storage.get('op:initial:not-paused')).toBeUndefined();});
 });
 it('retries a reconciled unknown read only after resume while mutating unknown results remain terminal',async()=>{
  const client=directClient();
  await run(e,"INSERT INTO operations(op_key,method,phase,created_at,updated_at) VALUES('initial:missing-identity','getMe','issued',1,1)");
  await issued('initial:missing-send');
  await setSetting(e,'paused','1');
  expect(await reconcileIssued(client)).toBe(2);
  await expect(call(client,'missing-identity','getMe',{})).rejects.toBeInstanceOf(RetryLater);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  await setSetting(e,'paused','0');
  vi.mocked(fetch).mockResolvedValueOnce(Response.json({ok:true,result:{id:123456,username:'test_bot'}}));
  expect(await call(client,'missing-identity','getMe',{})).toEqual({status:'success',result:{id:123456,username:'test_bot'}});
  expect(await call(client,'missing-identity','getMe',{})).toEqual({status:'success',result:{id:123456,username:'test_bot'}});
  expect((await call(client,'missing-send','copyMessage',{chat_id:20001,from_chat_id:e.ADMIN_CHAT_ID,message_id:1})).status).toBe('unknown');
  // Reusing a mutating operation key under a read method must not clear its
  // unknown accounting or allow an external request either.
  expect((await call(client,'missing-send','getChat',{chat_id:20001})).status).toBe('unknown');
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
 });
});
