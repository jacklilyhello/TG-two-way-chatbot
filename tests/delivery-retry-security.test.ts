import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it,vi} from 'vitest';
import type {Env,Job,MessageRow,Params,TGMessage,TGUpdate} from '../src/types';
import {all,one,run} from '../src/db';
import {accept} from '../src/ingress';
import {execute} from '../src/coordinator';
import {handleDeliveryNotice} from '../src/messaging';
import type {Outcome} from '../src/operations';
import {BotCoordinator} from '../src/bot';

interface Request {key:string;method:string;params:Params}
interface Action {token:string;kind:string;user_id:string;actor_id:string;thread_id:number;version:number;payload:string;expires_at:number;used:number}
let e:Env,requests:Request[],sendOutcome:Outcome;
const original=(id=10):TGMessage=>({message_id:id,date:1,chat:{id:-1001234567890,type:'supergroup'},from:{id:10001},message_thread_id:40,is_topic_message:true,text:'test retry payload'});
async function updateJob(update:TGUpdate) {
 await accept(e,update);
 return (await one<Job>(e,'SELECT * FROM jobs WHERE dedupe=?',`u:${update.update_id}`))!;
}
async function failedNotice() {
 const update:TGUpdate={update_id:10,message:original()};
 await execute(e,await updateJob(update));
 const notice=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='delivery-notice'"))!;
 await handleDeliveryNotice(e,notice);
 const action=(await one<Action>(e,"SELECT * FROM callback_actions WHERE kind='delivery_retry'"))!;
 const row=(await one<MessageRow>(e,'SELECT * FROM messages WHERE source_id=10'))!;
 expect(action).not.toBeNull();
 expect(action).toMatchObject({actor_id:'10001',user_id:'20001',thread_id:40,version:1,used:0});
 const sent=requests.find(r=>r.method==='sendMessage'&&r.params.reply_markup)!;
 expect(JSON.stringify(sent.params.reply_markup)).toContain(`a:${action.token}`);
 return {action,row};
}
function callback(action:Action,id=20,overrides:Partial<TGMessage>={},actor=10001):TGUpdate {
 return {update_id:id,callback_query:{id:`cb:${id}`,from:{id:actor},data:`a:${action.token}`,message:{...original(JSON.parse(action.payload).messageId),from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true},...overrides}}};
}
async function submit(action:Action,id=20,overrides:Partial<TGMessage>={},actor=10001) {
 const update=callback(action,id,overrides,actor),job=await updateJob(update);
 await execute(e,job);return job;
}
async function retryJobs() {return all<Job>(e,"SELECT * FROM jobs WHERE kind='delivery-retry'");}
function guardedBot(beforeIssue:()=>Promise<void>) {
 const values=new Map<string,unknown>();
 const storage={get:async(key:string)=>values.get(key),put:async(key:string|Record<string,unknown>,value?:unknown)=>{if(typeof key==='string')values.set(key,value);else for(const [k,v]of Object.entries(key))values.set(k,v);},delete:async(key:string)=>values.delete(key)};
 const bot=new BotCoordinator({storage} as unknown as DurableObjectState,e);
 e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:async(url:string,init:RequestInit)=>{await beforeIssue();return bot.fetch(new Request(url,init));}})} as unknown as DurableObjectNamespace;
 return values;
}

beforeEach(async()=>{
 e={...env} as unknown as Env;requests=[];
 sendOutcome={status:'failed',code:403,reason:'Forbidden: bot was blocked by the user'};
 let sequence=1000;
 const journal=new Map<string,Outcome>();
 e.BOT={idFromName:(name:string)=>name,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  const request=JSON.parse(String(init.body)) as Request;
  if(!journal.has(request.key)) {
   requests.push(request);
   const outcome:Outcome=request.method==='copyMessage'?sendOutcome:{status:'success',result:request.method==='sendMessage'?{message_id:++sequence}:true};
   journal.set(request.key,outcome);
  }
  return Response.json(journal.get(request.key));
 }})} as unknown as DurableObjectNamespace;
 e.JOBS={send:async()=>{}} as unknown as Queue;
 e.CONVERSATIONS={idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})} as unknown as DurableObjectNamespace;
 await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES('20001',1,40,1,500),('20002',1,41,1,600)");
});

describe('delivery retry administrator callback boundary',()=>{
 it('reserves one attempt from the real failure notice and preserves the original failed ledger',async()=>{
  const {action,row}=await failedNotice();
  await submit(action);
  const jobs=await retryJobs();
  expect(jobs).toHaveLength(1);
  expect(jobs[0]).toMatchObject({user_key:'20001',epoch:e.RECOVERY_EPOCH,state:'pending'});
  expect(await one(e,'SELECT phase,code FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:send:${row.id}`)).toEqual({phase:'failed',code:403});
  expect(requests.filter(r=>r.method==='copyMessage')).toHaveLength(1);
 });
 it.each(['ordinary-actor','different-admin','instance','group','thread','notice-message','bot','binding','expired'] as const)('rejects a real button with mismatched %s',async mismatch=>{
  const {action}=await failedNotice();
  if(mismatch==='instance')await run(e,"UPDATE callback_actions SET payload=json_set(payload,'$.instance','different-instance') WHERE token=?",action.token);
  if(mismatch==='binding')await run(e,"UPDATE users SET binding_version=2 WHERE tgid='20001'");
  if(mismatch==='expired')await run(e,'UPDATE callback_actions SET expires_at=0 WHERE token=?',action.token);
  if(mismatch==='different-admin')e={...e,ADMIN_IDS:'10001,10002'};
  const extra:Partial<TGMessage>=mismatch==='group'?{chat:{id:-100999999,type:'supergroup'}}:mismatch==='thread'?{message_thread_id:41}:mismatch==='notice-message'?{message_id:999}:mismatch==='bot'?{from:{id:654321,is_bot:true}}:{};
  await submit(action,20,extra,mismatch==='ordinary-actor'?20001:mismatch==='different-admin'?10002:10001);
  expect(await retryJobs()).toEqual([]);
  expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',action.token)).toEqual({used:0});
  expect(requests.filter(r=>r.method==='copyMessage')).toHaveLength(1);
 });
 it('does not borrow authorization from the displayed notification text or forward source',async()=>{
  const {action}=await failedNotice();
  await submit(action,20,{text:'Administrator 10001',forward_origin:{type:'user',sender_user:{id:10001},date:1}},20001);
  expect(await retryJobs()).toEqual([]);
 });
 it.each([
  ['moved topic',"UPDATE users SET thread_id=42 WHERE tgid='20001'"],
  ['deleted message',"UPDATE messages SET tombstone=1 WHERE source_id=10"],
  ['pending deletion',"UPDATE messages SET deletion='unknown' WHERE source_id=10"],
  ['already delivered',"UPDATE messages SET delivery='success',private_id=900 WHERE source_id=10"],
  ['unconfirmed delivery',"UPDATE messages SET delivery='unknown' WHERE source_id=10"],
  ['album membership',"UPDATE messages SET media_group='album' WHERE source_id=10"],
  ['different user',"UPDATE messages SET user_id='20002' WHERE source_id=10"],
  ['different original',"UPDATE messages SET source_id=11 WHERE source_id=10"],
  ['different source chat',"UPDATE messages SET source_chat='-100999999' WHERE source_id=10"],
  ['different original actor',"UPDATE messages SET payload=json_set(payload,'$.from.id',10002) WHERE source_id=10"],
  ['old message epoch',"UPDATE messages SET epoch='previous' WHERE source_id=10"],
  ['old action epoch',"UPDATE callback_actions SET payload=json_set(payload,'$.epoch','previous') WHERE kind='delivery_retry'"],
  ['wrong prior operation',"UPDATE callback_actions SET payload=json_set(payload,'$.priorOpKey','initial:send:999') WHERE kind='delivery_retry'"],
  ['wildcard actor',"UPDATE callback_actions SET actor_id='*' WHERE kind='delivery_retry'"],
  ['unconfirmed operation',"UPDATE operations SET phase='unknown' WHERE method='copyMessage'"],
  ['still issued operation',"UPDATE operations SET phase='issued' WHERE method='copyMessage'"],
  ['synthetic cancellation',"UPDATE operations SET code=409,reason='cancelled_by_state' WHERE method='copyMessage'"],
  ['rate limit',"UPDATE operations SET code=429 WHERE method='copyMessage'"],
  ['server error',"UPDATE operations SET code=500 WHERE method='copyMessage'"],
  ['different operation method',"UPDATE operations SET method='deleteMessage' WHERE method='copyMessage'"],
 ])('rechecks %s after the failure button was issued',async(_name,sql)=>{
  const {action}=await failedNotice();
  await run(e,sql);
  await submit(action);
  expect(await retryJobs()).toEqual([]);
  expect(await all(e,'SELECT * FROM delivery_attempts')).toEqual([]);
  expect(requests.filter(r=>r.method==='copyMessage')).toHaveLength(1);
 });
 it.each(['epoch','administrator'] as const)('revalidates the callback job after %s configuration changes',async changed=>{
  const {action}=await failedNotice(),update=callback(action);
  const job=await updateJob(update);
  await execute({...e,...(changed==='epoch'?{RECOVERY_EPOCH:'recovered'}:{ADMIN_IDS:'10002'})},job);
  expect(await retryJobs()).toEqual([]);
  expect(await all(e,'SELECT * FROM delivery_attempts')).toEqual([]);
 });
 it('deduplicates same-update replay and competing callback updates',async()=>{
  const {action}=await failedNotice();
  const updates=[callback(action,20),callback(action,21)];
  const jobs=await Promise.all(updates.map(updateJob));
  await Promise.all(jobs.map(job=>execute(e,job)));
  await execute(e,jobs[0]);
  expect(await retryJobs()).toHaveLength(1);
  expect(requests.filter(r=>r.method==='copyMessage')).toHaveLength(1);
 });
 it.each([
  ['deletion',"UPDATE messages SET tombstone=1 WHERE source_id=10"],
  ['binding',"UPDATE users SET binding_version=2 WHERE tgid='20001'"],
  ['unknown operation',"UPDATE operations SET phase='unknown' WHERE method='copyMessage'"],
  ['cancelled action',"UPDATE callback_actions SET used=-1 WHERE kind='delivery_retry'"],
 ])('atomically rejects %s that changes after callback checks but before reservation',async(_name,sql)=>{
  const {action}=await failedNotice(),job=await updateJob(callback(action));
  const database=e.DB;let injected=false;
  e.DB=new Proxy(database,{get(target,key){
   if(key==='batch')return async(statements:D1PreparedStatement[])=>{if(!injected){injected=true;await database.prepare(sql).run();}return target.batch(statements);};
   const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});
  await execute(e,job);
  expect(injected).toBe(true);
  expect(await retryJobs()).toEqual([]);
  expect(await all(e,'SELECT * FROM delivery_attempts')).toEqual([]);
 });
 it.each([
  ['binding',"UPDATE users SET binding_version=2 WHERE tgid='20001'"],
  ['target user',"UPDATE messages SET user_id='20002' WHERE source_id=10"],
  ['original actor',"UPDATE messages SET payload=json_set(payload,'$.from.id',10002) WHERE source_id=10"],
  ['unknown prior operation',"UPDATE operations SET phase='unknown' WHERE op_key='initial:send:1'"],
  ['tombstone',"UPDATE messages SET tombstone=1 WHERE source_id=10"],
  ['notification cancellation',"UPDATE delivery_attempts SET state='suppressed' WHERE message_id=1"],
 ])('checks %s inside the Bot immediately before a reserved attempt can contact Telegram',async(_name,sql)=>{
  const {action}=await failedNotice();await submit(action);
  let checked=false;
  const values=guardedBot(async()=>{if(!checked){checked=true;await run(e,sql);}});
  await execute(e,(await retryJobs())[0]);
  expect(checked).toBe(true);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  expect(values.has('rate:20001')).toBe(false);
  expect(await one(e,"SELECT phase,reason FROM operations WHERE op_key='initial:send-retry:1:1'")).toEqual({phase:'failed',reason:'cancelled_by_state'});
 });
 it('rechecks the actual administrator configuration at the final Bot boundary',async()=>{
  const {action}=await failedNotice();await submit(action);
  guardedBot(async()=>{e.ADMIN_IDS='10002';});
  await execute(e,(await retryJobs())[0]);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  expect(await one(e,"SELECT phase,reason FROM operations WHERE op_key='initial:send-retry:1:1'")).toEqual({phase:'failed',reason:'cancelled_by_state'});
 });
 it('never offers a button for an unknown delivery outcome',async()=>{
  sendOutcome={status:'unknown',reason:'unconfirmed external result'};
  const update:TGUpdate={update_id:10,message:original()};
  await execute(e,await updateJob(update));
  await handleDeliveryNotice(e,(await one<Job>(e,"SELECT * FROM jobs WHERE kind='delivery-notice'"))!);
  expect(await one(e,"SELECT token FROM callback_actions WHERE kind='delivery_retry'")).toBeNull();
  expect(requests.filter(r=>r.method==='sendMessage').every(r=>r.params.reply_markup===undefined)).toBe(true);
 });
});
