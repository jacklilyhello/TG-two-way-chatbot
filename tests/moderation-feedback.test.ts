import {env} from 'cloudflare:test';
import {describe,it,expect,vi} from 'vitest';
import type {Env,Job,Params,TGUpdate} from '../src/types';
import {all,one,run} from '../src/db';
import {accept} from '../src/ingress';
import {setBlocked} from '../src/moderation';
import {BotCoordinator} from '../src/bot';

const base=env as unknown as Env;
const token='d'.repeat(32);
const j:Job={id:1,dedupe:'u:1',user_key:'moderation:20001',kind:'update',payload:'{}',state:'running',due_at:0,attempts:0,epoch:base.RECOVERY_EPOCH,created_at:1};
async function card() {
 await run(base,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES('20001',1,40,1,500)");
 await run(base,"INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) VALUES(?,'card_block','20001','*',40,1,?,?)",token,JSON.stringify({instance:base.INSTANCE_ID}),Number.MAX_SAFE_INTEGER);
}
function callback(id=1):TGUpdate {
 return {update_id:id,callback_query:{id:`callback-${id}`,from:{id:10001},data:`a:${token}`,message:{message_id:500,date:1,from:{id:Number(base.EXPECTED_BOT_ID),is_bot:true},chat:{id:Number(base.ADMIN_CHAT_ID),type:'supergroup'},message_thread_id:40,is_topic_message:true}}};
}
function bot(afterChecks?:()=>Promise<void>) {
 const values=new Map<string,unknown>();
 const storage={get:async(key:string)=>values.get(key),put:async(key:string|Record<string,unknown>,value?:unknown)=>{if(typeof key==='string')values.set(key,value);else for(const [name,item]of Object.entries(key))values.set(name,item);},delete:async(key:string)=>values.delete(key)};
 const e=afterChecks?{...base,DB:new Proxy(base.DB,{get(target,key){
  if(key==='batch')return async(statements:D1PreparedStatement[])=>{const result=await target.batch(statements);await afterChecks();return result;};
  const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
 }})}:base;
 const coordinator=new BotCoordinator({storage} as unknown as DurableObjectState,e);
 const invoke=(key:string,method:string,params:Params={},guard?:unknown)=>coordinator.fetch(new Request('https://bot/execute',{method:'POST',body:JSON.stringify({key:`${base.RECOVERY_EPOCH}:${key}`,method,params,guard})}));
 return {values,invoke,coordinator};
}
function deferred<T>() {
 let resolve!:(value:T)=>void;
 const promise=new Promise<T>(done=>{resolve=done;});return {promise,resolve};
}

describe('moderation ingress and atomic visible repair',()=>{
 it('admits valid card moderation to a separate user lane even when business is pending',async()=>{
  await card();
  await run(base,"INSERT INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES('business','20001','update','{}','running',0,?,1)",base.RECOVERY_EPOCH);
  expect(await accept(base,callback())).toBe('moderation:20001');
  expect(await one(base,"SELECT user_key FROM jobs WHERE dedupe='u:1'")).toEqual({user_key:'moderation:20001'});
 });
 it.each(['actor','instance','card','binding','bot','expired'] as const)('keeps a mismatched %s out of the moderation lane',async mismatch=>{
  await card();const update=callback();
  if(mismatch==='actor')update.callback_query!.from.id=20001;
  if(mismatch==='instance')await run(base,'UPDATE callback_actions SET payload=?',JSON.stringify({instance:'other-instance'}));
  if(mismatch==='card')update.callback_query!.message!.message_id=501;
  if(mismatch==='binding')await run(base,"UPDATE users SET binding_version=2 WHERE tgid='20001'");
  if(mismatch==='bot')update.callback_query!.message!.from!.id=99999;
  if(mismatch==='expired')await run(base,'UPDATE callback_actions SET expires_at=1');
  expect(await accept(base,update)).not.toBe('moderation:20001');
 });
 it('commits an urgent repair with a ban and promotes an already pending card without duplication',async()=>{
  await card();
  const e={...base,JOBS:{send:async()=>{throw Error('Synthetic queue unavailable');}},CONVERSATIONS:{idFromName:(name:string)=>name,get:()=>({fetch:async()=>{throw Error('Synthetic alarm unavailable');}})}} as unknown as Env;
  await run(base,"INSERT INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES('existing-card','card:20001','card',?,'pending',0,?,1)",JSON.stringify({userId:'20001',version:1,threadId:40}),base.RECOVERY_EPOCH);
  const result=await setBlocked(e,j,'20001',true,{source:'manual',actorId:'10001',action:{token,stamp:2,version:1,threadId:40,cardId:500}});
  expect(result).toMatchObject({applied:true,changed:true});
  expect(await one(base,"SELECT blocked,moderation_version FROM users WHERE tgid='20001'")).toEqual({blocked:1,moderation_version:1});
  expect(await all(base,"SELECT dedupe,json_extract(payload,'$.urgent') urgent FROM jobs WHERE kind='card'")).toEqual([{dedupe:'existing-card',urgent:1}]);
  expect(await one(base,"SELECT COUNT(*) n FROM jobs WHERE kind='ban-notice' AND state='pending'")).toEqual({n:1});
  await setBlocked(e,{...j,id:2},'20001',false,{source:'manual'});
  expect(await one(base,"SELECT blocked,moderation_version FROM users WHERE tgid='20001'")).toEqual({blocked:0,moderation_version:2});
  expect(await one(base,"SELECT COUNT(*) n FROM jobs WHERE kind='ban-notice'")).toEqual({n:1});
 });
 it('creates urgent card repair when a prior render is already running',async()=>{
  await card();
  await run(base,"INSERT INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES('running-card','card:20001','card',?,'running',0,?,1)",JSON.stringify({userId:'20001',version:1,threadId:40}),base.RECOVERY_EPOCH);
  const e={...base,JOBS:{send:async()=>{}},CONVERSATIONS:{idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})}} as unknown as Env;
  await setBlocked(e,j,'20001',true,{source:'manual'});
  expect(await one(base,"SELECT COUNT(*) n FROM jobs WHERE kind='card'")).toEqual({n:2});
  expect(await one(base,"SELECT json_extract(payload,'$.urgent') urgent FROM jobs WHERE kind='card' AND state='pending'")).toEqual({urgent:1});
 });
 it('repairs a stale card after an accepted no-op without creating a repeated ban notification',async()=>{
  await card();await run(base,"UPDATE users SET blocked=1 WHERE tgid='20001'");
  const e={...base,JOBS:{send:async()=>{}},CONVERSATIONS:{idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})}} as unknown as Env;
  expect(await setBlocked(e,j,'20001',true,{source:'manual'})).toMatchObject({applied:true,changed:false});
  expect(await one(base,"SELECT json_extract(payload,'$.urgent') urgent FROM jobs WHERE kind='card' AND state='pending'")).toEqual({urgent:1});
  expect(await one(base,"SELECT COUNT(*) n FROM jobs WHERE kind='ban-notice'")).toEqual({n:0});
 });
});

describe('callback acknowledgement bypasses an in-flight business request safely',()=>{
 it('acknowledges and deduplicates a confirmed moderation result before a slow Telegram copy finishes',async()=>{
  const {invoke}=bot(),started=deferred<void>(),release=deferred<Response>();
  vi.mocked(fetch).mockImplementation(async input=>{
   if(String(input).endsWith('/copyMessage')){started.resolve();return release.promise;}
   return Response.json({ok:true,result:true});
  });
  let businessFinished=false;
  const business=invoke('slow-copy','copyMessage',{chat_id:20001,message_id:1}).then(response=>{businessFinished=true;return response;});
  await started.promise;
  const answers=await Promise.all([invoke('answer','answerCallbackQuery',{callback_query_id:'confirmed',text:'已封禁。'}),invoke('answer','answerCallbackQuery',{callback_query_id:'confirmed',text:'已封禁。'})]);
  for(const answer of answers)expect(await answer.json()).toMatchObject({status:'success'});
  expect(businessFinished).toBe(false);
  expect(vi.mocked(fetch).mock.calls.filter(([url])=>String(url).endsWith('/answerCallbackQuery'))).toHaveLength(1);
  release.resolve(Response.json({ok:true,result:{message_id:700}}));await business;
 });
 it('never reduces a concurrent platform backoff and keeps acknowledgement failures durable',async()=>{
  vi.spyOn(Date,'now').mockReturnValue(100000);
  const {invoke,values}=bot(),started=deferred<void>(),release=deferred<Response>();
  vi.mocked(fetch).mockImplementation(async input=>{
   if(String(input).endsWith('/copyMessage')){started.resolve();return release.promise;}
   return Response.json({ok:false,error_code:429,parameters:{retry_after:10}});
  });
  const business=invoke('copy','copyMessage',{chat_id:20001,message_id:1});await started.promise;
  expect(await (await invoke('answer','answerCallbackQuery',{callback_query_id:'confirmed'})).json()).toEqual({due:110000});
  release.resolve(Response.json({ok:false,error_code:429,parameters:{retry_after:1}}));
  expect(await (await business).json()).toEqual({due:110000});
  expect(values.get('rate:backoff')).toBe(110000);
  expect((await invoke('answer-after','answerCallbackQuery',{callback_query_id:'next'})).status).toBe(429);
 });
 it('serializes a newly received callback backoff with a business request final reservation',async()=>{
  vi.spyOn(Date,'now').mockReturnValue(100000);
  const checked=deferred<void>(),releaseChecks=deferred<void>(),releaseCallback=deferred<void>(),callbackStarted=deferred<void>(),backoffStarted=deferred<void>();
  let checks=0;
  const {invoke,values,coordinator}=bot(async()=>{if(++checks===2){checked.resolve();await releaseChecks.promise;}});
  const backoffs:number[]=[];
  vi.mocked(fetch).mockImplementation(async input=>{
   if(String(input).endsWith('/answerCallbackQuery')){callbackStarted.resolve();await releaseCallback.promise;return Response.json({ok:false,error_code:429,parameters:{retry_after:10}});}
   backoffs.push(Number(values.get('rate:backoff')??0));return Response.json({ok:true,result:{message_id:700}});
  });
  const internals=coordinator as unknown as {raiseBackoff:(due:number,reservation:Record<string,number>)=>Promise<number>};
  const raise=internals.raiseBackoff.bind(coordinator);
  vi.spyOn(internals,'raiseBackoff').mockImplementation((due,reservation)=>{const result=raise(due,reservation);backoffStarted.resolve();return result;});
  const callback=invoke('answer-race','answerCallbackQuery',{callback_query_id:'confirmed'});await callbackStarted.promise;
  const business=invoke('copy-race','copyMessage',{chat_id:20001,message_id:1});await checked.promise;
  releaseCallback.resolve();await backoffStarted.promise;
  // Let all already-runnable promise continuations finish while the ordinary
  // request is deliberately held at its final D1 check. The old independent
  // backoff writer would commit here and the ordinary request would ignore it.
  await new Promise<void>(resolve=>setTimeout(resolve,0));
  releaseChecks.resolve();
  expect((await business).status).toBe(200);
  expect(await (await callback).json()).toEqual({due:110000});
  expect(backoffs).toEqual([0]);
  expect((await invoke('copy-after-race','copyMessage',{chat_id:20002,message_id:2})).status).toBe(429);
  expect(backoffs).toEqual([0]);
 });
 it.each(['pause','moderation'] as const)('still checks %s before any Telegram request',async condition=>{
  await card();const {invoke}=bot();
  if(condition==='pause')await run(base,"INSERT INTO settings(key,value) VALUES('paused','1')");
  else await run(base,"UPDATE users SET moderation_version=2 WHERE tgid='20001'");
  const result=await invoke('guarded',condition==='pause'?'answerCallbackQuery':'editMessageText',{chat_id:base.ADMIN_CHAT_ID,message_id:500},{threadVersion:{userId:'20001',version:1,threadId:40,moderationVersion:condition==='pause'?0:1}});
  if(condition==='pause')expect(result.status).toBe(429);
  else expect(await result.json()).toMatchObject({status:'failed',reason:'cancelled_by_state'});
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
 it.each(['failed','unknown'] as const)('keeps a %s acknowledgement terminal across retries',async status=>{
  const {invoke}=bot();
  if(status==='failed')vi.mocked(fetch).mockImplementation(async()=>Response.json({ok:false,error_code:400,description:'Synthetic callback expired'}));
  else vi.mocked(fetch).mockRejectedValue(Error('Synthetic response lost'));
  const first=await invoke('answer-terminal','answerCallbackQuery',{callback_query_id:'confirmed'});
  const repeat=await invoke('answer-terminal','answerCallbackQuery',{callback_query_id:'confirmed'});
  expect(await first.json()).toMatchObject({status});expect(await repeat.json()).toMatchObject({status});
  expect(vi.mocked(fetch)).toHaveBeenCalledOnce();
 });
});

describe('urgent card control requests share all final safety reservations',()=>{
 const guard={priority:'moderation-card',threadVersion:{userId:'20001',version:1,threadId:40,moderationVersion:0}};
 it('updates the state card while a copy is in flight and never moves the newer global deadline backwards',async()=>{
  await card();let clock=100000;vi.spyOn(Date,'now').mockImplementation(()=>clock);
  const {invoke,values}=bot(),started=deferred<void>(),release=deferred<Response>();
  vi.mocked(fetch).mockImplementation(async input=>{
   if(String(input).endsWith('/copyMessage')){started.resolve();return release.promise;}
   return Response.json({ok:true,result:true});
  });
  let copied=false;
  const business=invoke('slow-copy-control','copyMessage',{chat_id:20002,message_id:1}).then(response=>{copied=true;return response;});
  await started.promise;clock=100100;
  const edited=await invoke('urgent-card','editMessageText',{chat_id:base.ADMIN_CHAT_ID,message_id:500,text:'状态：已封禁'},guard);
  expect(await edited.json()).toMatchObject({status:'success'});expect(copied).toBe(false);
  expect(values.get('rate:global')).toBe(100140);
  expect(values.get(`rate:mutation:${base.ADMIN_CHAT_ID}`)).toBe(103200);
  clock=100110;release.resolve(Response.json({ok:true,result:{message_id:700}}));await business;
  expect(values.get('rate:global')).toBe(100140);
  const limited=await invoke('urgent-card-too-soon','editMessageCaption',{chat_id:base.ADMIN_CHAT_ID,message_id:500,caption:'状态：正常'},guard);
  expect(limited.status).toBe(429);expect(await limited.json()).toEqual({due:103200});
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(2);
 });
 it('does not let a slow ordinary mutation completion lower a later urgent mutation deadline',async()=>{
  await card();let clock=100000;vi.spyOn(Date,'now').mockImplementation(()=>clock);
  const {invoke,values}=bot(),started=deferred<void>(),release=deferred<Response>();
  vi.mocked(fetch).mockImplementation(async input=>{
   if(String(input).endsWith('/deleteMessage')){started.resolve();return release.promise;}
   return Response.json({ok:true,result:true});
  });
  const deletion=invoke('slow-delete-control','deleteMessage',{chat_id:base.ADMIN_CHAT_ID,message_id:501});await started.promise;
  clock=103100;
  expect((await invoke('urgent-after-delete','editMessageReplyMarkup',{chat_id:base.ADMIN_CHAT_ID,message_id:500,reply_markup:{inline_keyboard:[]}},guard)).status).toBe(200);
  expect(values.get(`rate:mutation:${base.ADMIN_CHAT_ID}`)).toBe(106200);
  clock=103200;release.resolve(Response.json({ok:true,result:true}));await deletion;
  expect(values.get(`rate:mutation:${base.ADMIN_CHAT_ID}`)).toBe(106200);
  expect((await invoke('next-delete','deleteMessage',{chat_id:base.ADMIN_CHAT_ID,message_id:502})).status).toBe(429);
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(2);
 });
 it.each(['backoff','pause','moderation'] as const)('keeps %s effective for the independent card control lane',async condition=>{
  await card();vi.spyOn(Date,'now').mockReturnValue(100000);
  const {invoke,values}=bot();
  if(condition==='backoff')values.set('rate:backoff',110000);
  if(condition==='pause')await run(base,"INSERT INTO settings(key,value) VALUES('paused','1')");
  if(condition==='moderation')await run(base,"UPDATE users SET moderation_version=1 WHERE tgid='20001'");
  const result=await invoke('unsafe-card','editMessageText',{chat_id:base.ADMIN_CHAT_ID,message_id:500,text:'状态'},guard);
  if(condition==='moderation')expect(await result.json()).toMatchObject({status:'failed',reason:'cancelled_by_state'});
  else expect(result.status).toBe(429);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
 it.each(['sending-method','missing-generation'] as const)('keeps %s on the ordinary request lane despite the priority hint',async condition=>{
  await card();let clock=100000;vi.spyOn(Date,'now').mockImplementation(()=>clock);
  const {invoke}=bot(),started=deferred<void>(),release=deferred<Response>();
  vi.mocked(fetch).mockImplementation(async input=>{
   if(String(input).endsWith('/copyMessage')){started.resolve();return release.promise;}
   return Response.json({ok:true,result:true});
  });
  const business=invoke('slow-copy-scope','copyMessage',{chat_id:20002,message_id:1});await started.promise;clock=100100;
  let finished=false;
  const gated=invoke('priority-scope',condition==='sending-method'?'sendMessage':'editMessageText',{chat_id:base.ADMIN_CHAT_ID,message_id:500,text:'test'},condition==='sending-method'?guard:{priority:'moderation-card',threadVersion:{userId:'20001',version:1,threadId:40}}).then(response=>{finished=true;return response;});
  await new Promise<void>(resolve=>setTimeout(resolve,0));expect(finished).toBe(false);expect(vi.mocked(fetch)).toHaveBeenCalledOnce();
  release.resolve(Response.json({ok:true,result:{message_id:700}}));await business;
  expect((await gated).status).toBe(200);
 });
});
