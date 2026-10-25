import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it,vi} from 'vitest';
import type {Env,Job,MessageRow,Params} from '../src/types';
import {all,one,run} from '../src/db';
import {bodyParts,prepareBroadcast,launchBroadcast,getBroadcast,broadcastStats,handleBroadcast,recoverBroadcasts,broadcastTestRecipients} from '../src/broadcast-delivery';
import {RetryLater} from '../src/operations';
import {latestDelivery} from '../src/delivery-retry';
import {BotCoordinator} from '../src/bot';
import {failOperationCompletion} from './completion-fault';

let e:Env,requests:{key:string;method:string;params:Params;guard?:unknown}[],response:(method:string,params:Params)=>unknown;
let identifier=3000;
async function addUser(id='20001',blocked=0,thread:number|null=71) {await run(e,'INSERT INTO users(tgid,first_contact,blocked,thread_id,binding_version) VALUES(?,1,?,?,1)',id,blocked,thread);}
async function ready(id='synthetic-broadcast',content={text:'SYNTHETIC-BROADCAST'} as {text?:string;photo?:string}) {
 await prepareBroadcast(e,{id,actor:'10001',...content});
 await run(e,"UPDATE broadcasts SET state='confirmed' WHERE id=?",id);await launchBroadcast(e,id);
 return (await one<Job>(e,'SELECT * FROM jobs WHERE dedupe=?',`broadcast:${id}`))!;
}
async function tick(j:Job,config=e) {try{await handleBroadcast(config,j);}catch(error){if(!(error instanceof RetryLater))throw error;}await run(e,'UPDATE broadcasts SET next_at=0 WHERE id=?',JSON.parse(j.payload).broadcastId);}
async function drain(j:Job,limit=100) {for(let index=0;index<limit;index++){await tick(j);if((await getBroadcast(e,JSON.parse(j.payload).broadcastId))?.state!=='sending')return;}throw new Error('synthetic_drain_limit');}
const sent=()=>requests.filter(request=>String(request.params.chat_id)!==e.ADMIN_CHAT_ID);
function actualJournalBot(){
 const values=new Map<string,unknown>();
 const storage={get:async(key:string)=>values.get(key),put:async(key:string|Record<string,unknown>,value?:unknown)=>{if(typeof key==='string')values.set(key,value);else Object.entries(key).forEach(([name,item])=>values.set(name,item));},delete:async(key:string)=>values.delete(key)};
 const bot=new BotCoordinator({storage} as unknown as DurableObjectState,e);
 e={...e,BOT:{idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:(url:string,init:RequestInit)=>bot.fetch(new Request(url,init))})} as unknown as DurableObjectNamespace};
 return values;
}
beforeEach(async()=>{
 e={...env} as unknown as Env;requests=[];identifier=3000;
 e.JOBS={send:async()=>{}} as unknown as Queue;
 e.CONVERSATIONS={idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})} as unknown as DurableObjectNamespace;
 response=()=>({status:'success',result:{message_id:++identifier}});
 e.BOT={idFromName:(name:string)=>name,get:()=>({fetch:async(_url:string,init:RequestInit)=>{const request=JSON.parse(String(init.body));requests.push(request);return Response.json(response(request.method,request.params));}})} as unknown as DurableObjectNamespace;
 await addUser();
});
describe('broadcast immutable audience and launch',()=>{
 it('freezes eligible recipients once and does not enlarge the audience on replay',async()=>{
  await addUser('20002',1,72);await addUser('20003',0,null);
  const first=await prepareBroadcast(e,{id:'snapshot',actor:'10001',text:'SYNTHETIC'});
  expect(first.total).toBe(1);
  await addUser('20004',0,74);await run(e,"UPDATE users SET blocked=0 WHERE tgid='20002'");
  expect((await prepareBroadcast(e,{id:'snapshot',actor:'10001',text:'SYNTHETIC'})).total).toBe(1);
  expect(await all(e,"SELECT user_id FROM broadcast_recipients WHERE broadcast_id='snapshot'")).toEqual([{user_id:'20001'}]);
 });
 it('rejects changed content under an existing preview identity and unauthorized creation',async()=>{
  await prepareBroadcast(e,{id:'snapshot',actor:'10001',text:'SYNTHETIC'});
  await expect(prepareBroadcast(e,{id:'snapshot',actor:'10001',text:'CHANGED'})).rejects.toThrow('broadcast_snapshot_mismatch');
  await expect(prepareBroadcast(e,{id:'denied',actor:'20001',text:'SYNTHETIC'})).rejects.toThrow('invalid_broadcast');
 });
 it('cannot launch a draft, queues only a confirmed campaign once and never counts queue admission as success',async()=>{
  await prepareBroadcast(e,{id:'snapshot',actor:'10001',text:'SYNTHETIC'});await launchBroadcast(e,'snapshot');
  expect(await one(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toBeNull();
  await run(e,"UPDATE broadcasts SET state='confirmed' WHERE id='snapshot'");await launchBroadcast(e,'snapshot');await launchBroadcast(e,'snapshot');
  expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(1);
  expect(await broadcastStats(e,'snapshot')).toMatchObject({total:1,success:0,failed:0,unknown:0,unsent:1,pending:1});
  expect(sent()).toHaveLength(0);
 });
 it('seals a zero-recipient campaign with truthful zero totals',async()=>{
  await run(e,'UPDATE users SET blocked=1');const j=await ready();await drain(j);
  expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({state:'done',total:0,success:0,unsent:0});
 });
 it('binds campaigns to instance, bot identity and recovery epoch',async()=>{
  const j=await ready();await handleBroadcast({...e,INSTANCE_ID:'other-instance'},j);await handleBroadcast({...e,EXPECTED_BOT_ID:'999999'},j);await handleBroadcast({...e,RECOVERY_EPOCH:'other-epoch'},j);
  expect(sent()).toHaveLength(0);expect(await getBroadcast({...e,INSTANCE_ID:'other-instance'},'synthetic-broadcast')).toBeNull();
 });
 it('test scope is closed without private configuration and filters the frozen audience',async()=>{
  e={...e,INSTANCE_ID:'test-a'};await addUser('20002',0,72);
  await expect(prepareBroadcast(e,{id:'scope-missing',actor:'10001',text:'SYNTHETIC'})).rejects.toThrow('broadcast_test_scope_missing');
  e={...e,BROADCAST_TEST_RECIPIENT_IDS:'20001'};
  expect((await prepareBroadcast(e,{id:'scoped',actor:'10001',text:'SYNTHETIC'})).total).toBe(1);
  expect(broadcastTestRecipients({...e,INSTANCE_ID:'prod-support'})).toBeNull();
 });
});
describe('broadcast parts and actual results',()=>{
 it.each([
  [{text:'SYNTHETIC-TEXT'},['sendMessage']],
  [{photo:'SYNTHETIC-FILE'},['sendPhoto']],
  [{text:'SYNTHETIC-CAPTION',photo:'SYNTHETIC-FILE'},['sendPhoto']],
  [{text:'x'.repeat(1200),photo:'SYNTHETIC-FILE'},['sendPhoto','sendMessage']]
 ])('sends complete unadorned content and records each known private target: %j',async(content,methods)=>{
  const j=await ready('synthetic-broadcast',content);await drain(j);await drain(j);
  expect(sent().map(item=>item.method)).toEqual(methods);
  expect(sent().map(item=>Object.keys(item.params))).toEqual(bodyParts(content).map(part=>['chat_id',...Object.keys(part.params)]));
  expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({state:'done',success:1,failed:0,unsent:0,unknown:0});
  const messages=await all<MessageRow>(e,"SELECT * FROM messages WHERE direction='broadcast'");
  expect(messages).toHaveLength(methods.length);expect(messages.every(m=>m.business===0&&m.private_id!==null&&m.delivery==='success')).toBe(true);
  expect(await latestDelivery(e,messages[0])).toMatchObject({key:'broadcast:synthetic-broadcast:20001:0',method:methods[0]});
 });
 it('continues after explicit recipient rejection without resending failed recipients',async()=>{
  await addUser('20002',0,72);response=(_method,params)=>String(params.chat_id)==='20001'?{status:'failed',reason:'synthetic blocked bot',code:403}:{status:'success',result:{message_id:++identifier}};
  const j=await ready();await drain(j);await drain(j);
  expect(sent()).toHaveLength(2);expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:1,failed:1,unsent:0,unknown:0});
 });
 it('retains unknown outcomes and continues other recipients without retrying unknown parts',async()=>{
  await addUser('20002',0,72);response=(_method,params)=>String(params.chat_id)==='20001'?{status:'unknown',reason:'synthetic timeout'}:{status:'success',result:{message_id:++identifier}};
  const j=await ready();await drain(j);await drain(j);
  expect(sent()).toHaveLength(2);expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:1,failed:0,unsent:0,unknown:1});
 });
 it('separately reports partial photo+long-text failure and does not call the recipient successful',async()=>{
  response=(method,params)=>method==='sendMessage'&&String(params.chat_id)==='20001'?{status:'failed',reason:'synthetic text rejection',code:400}:{status:'success',result:{message_id:++identifier}};
  const j=await ready('synthetic-broadcast',{text:'x'.repeat(1200),photo:'SYNTHETIC-FILE'});await drain(j);
  expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:0,failed:1,partial:1,unknown:0});
  expect(await all(e,"SELECT delivery FROM messages WHERE direction='broadcast' ORDER BY id")).toEqual([{delivery:'success'},{delivery:'failed'}]);
 });
 it('waits durably between recipients instead of processing the whole audience in one wake',async()=>{
  await addUser('20002',0,72);const j=await ready();await expect(handleBroadcast(e,j)).rejects.toBeInstanceOf(RetryLater);
  expect(sent()).toHaveLength(1);expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:1,pending:1});
  await expect(handleBroadcast(e,j)).rejects.toBeInstanceOf(RetryLater);expect(sent()).toHaveLength(1);
  await run(e,'UPDATE broadcasts SET next_at=0');await drain(j);expect(sent()).toHaveLength(2);
 });
 it('retains safe 429 retry and distinguishes a queued retry from a delivered message',async()=>{
  let limited=true;e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:async(_url:string,init:RequestInit)=>{const request=JSON.parse(String(init.body));requests.push(request);if(limited){limited=false;return Response.json({due:Date.now()+5000},{status:429});}return Response.json({status:'success',result:{message_id:++identifier}});}})} as unknown as DurableObjectNamespace;
  const j=await ready();await expect(handleBroadcast(e,j)).rejects.toBeInstanceOf(RetryLater);
  expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:0,unsent:1});
  expect(await one(e,"SELECT phase FROM operations WHERE op_key LIKE '%:broadcast:synthetic-broadcast:%'")).toEqual({phase:'prepared'});
  await drain(j);expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:1,unsent:0});
 });
 it('recovers a confirmed external success after recipient mapping write failure without another send',async()=>{
  const j=await ready();let fail=true;
  const broken={...e,DB:new Proxy(e.DB,{get(target,key){if(key==='prepare')return (sql:string)=>{if(fail&&sql.startsWith('UPDATE messages SET delivery=?')){fail=false;throw new Error('synthetic mapping failure');}return target.prepare(sql);};const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;}})};
  await expect(handleBroadcast(broken,j)).rejects.toThrow('synthetic mapping failure');await drain(j);
  expect(sent()).toHaveLength(1);expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:1});
 });
 it('does not hide a missing message identifier behind a success response',async()=>{
  response=()=>({status:'success',result:true});const j=await ready();await drain(j);
  expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:0,unknown:1});
 });
 it('resumes a partially complete large audience from durable progress across deployment',async()=>{
  for(let i=2;i<=25;i++)await addUser(String(20000+i),0,70+i);
  const j=await ready();for(let i=0;i<7;i++)await tick(j);
  expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:7,pending:18});
  await recoverBroadcasts({...e,VERSION:'synthetic-new-release'});await drain(j);
  expect(sent()).toHaveLength(25);expect(new Set(sent().map(request=>String(request.params.chat_id))).size).toBe(25);
  expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:25,pending:0,state:'done'});
 });
});
describe('broadcast safe scheduling and policy rechecks',()=>{
 it('does not consume the consecutive error budget as recipients make progress',async()=>{
  for(let i=2;i<=12;i++)await addUser(String(20000+i),0,70+i);
  const j=await ready();
  for(let i=0;i<10;i++){await run(e,'UPDATE jobs SET attempts=attempts+1 WHERE id=?',j.id);await tick(j);}
  expect(await one(e,'SELECT attempts FROM jobs WHERE id=?',j.id)).toEqual({attempts:0});
  let fail=true;const broken={...e,DB:new Proxy(e.DB,{get(target,key){if(key==='prepare')return (sql:string)=>{if(fail&&sql.startsWith('INSERT OR IGNORE INTO messages')){fail=false;throw new Error('synthetic interrupt after ten users');}return target.prepare(sql);};const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;}})};
  await expect(handleBroadcast(broken,j)).rejects.toThrow('synthetic interrupt after ten users');await drain(j);
  expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:12,pending:0});expect(sent()).toHaveLength(12);
 });
 it('keeps confirmed work interrupted and unsent when the private test scope disappears',async()=>{
  e={...e,INSTANCE_ID:'test-b',BROADCAST_TEST_RECIPIENT_IDS:'20001'};
  await prepareBroadcast(e,{id:'scope-lost',actor:'10001',text:'SYNTHETIC'});await run(e,"UPDATE broadcasts SET state='confirmed' WHERE id='scope-lost'");
  await expect(recoverBroadcasts({...e,BROADCAST_TEST_RECIPIENT_IDS:undefined})).resolves.toBeUndefined();
  expect(await broadcastStats(e,'scope-lost')).toMatchObject({state:'interrupted',unsent:1,success:0});expect(sent()).toHaveLength(0);
 });
 it.each(['0','020001','9007199254740992','20001,20001',Array.from({length:21},(_,i)=>String(20001+i)).join(',')])('rejects invalid runtime scope %s',scope=>{
  expect(()=>broadcastTestRecipients({...e,INSTANCE_ID:'test-a',BROADCAST_TEST_RECIPIENT_IDS:scope})).toThrow('broadcast_test_scope_missing');
 });
 it('skips a newly blocked recipient at actual issuance and keeps other users sending',async()=>{
  const j=await ready();actualJournalBot();await run(e,'UPDATE users SET blocked=1');
  vi.mocked(fetch).mockImplementation(async()=>Response.json({ok:true,result:{message_id:++identifier}}));
  await drain(j);expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:0,failed:0,unknown:0,unsent:1,state:'done'});
  expect(vi.mocked(fetch).mock.calls.every(([,init])=>String(JSON.parse(String(init?.body)).chat_id)!=='20001')).toBe(true);
 });
 it('a clear tombstone suppresses an unsent part before Telegram is called',async()=>{
  const j=await ready();await run(e,"INSERT INTO messages(user_id,direction,source_chat,source_id,business,received_at,payload,epoch,tombstone) VALUES('20001','broadcast','broadcast:synthetic-broadcast:20001',0,0,1,'{}',?,1)",e.RECOVERY_EPOCH);
  actualJournalBot();vi.mocked(fetch).mockImplementation(async()=>Response.json({ok:true,result:{message_id:++identifier}}));
  await drain(j);expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:0,unsent:1});
  expect(vi.mocked(fetch).mock.calls.every(([,init])=>String(JSON.parse(String(init?.body)).chat_id)!=='20001')).toBe(true);
 });
});
describe('broadcast journal and recovery boundaries',()=>{
 it('reconciles a Bot journal success when the D1 terminal operation write failed',async()=>{
  const j=await ready();actualJournalBot();let privateCalls=0;
  vi.mocked(fetch).mockImplementation(async(_url,init)=>{const params=JSON.parse(String(init?.body));if(String(params.chat_id)==='20001')privateCalls++;return Response.json({ok:true,result:{message_id:++identifier}});});
  await expect(handleBroadcast({...e,DB:failOperationCompletion(e.DB,'synthetic result failure')},j)).rejects.toThrow('synthetic result failure');
  await drain(j);expect(privateCalls).toBe(1);expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:1});
 });
 it('seals an issued journal as unknown instead of issuing another external request',async()=>{
  const j=await ready();const values=actualJournalBot();values.set(`op:${e.RECOVERY_EPOCH}:broadcast:synthetic-broadcast:20001:0`,{phase:'issued'});
  vi.mocked(fetch).mockImplementation(async()=>Response.json({ok:true,result:{message_id:++identifier}}));
  await drain(j);expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({success:0,unknown:1});
  expect(vi.mocked(fetch).mock.calls.every(([,init])=>String(JSON.parse(String(init?.body)).chat_id)!=='20001')).toBe(true);
 });
 it('recovers a committed second confirmation whose launch was interrupted',async()=>{
  await prepareBroadcast(e,{id:'synthetic-confirmed',actor:'10001',text:'SYNTHETIC'});await run(e,"UPDATE broadcasts SET state='confirmed' WHERE id='synthetic-confirmed'");
  await recoverBroadcasts(e);expect(await getBroadcast(e,'synthetic-confirmed')).toMatchObject({state:'sending'});
  expect(await one(e,"SELECT kind FROM jobs WHERE dedupe='broadcast:synthetic-confirmed'")).toEqual({kind:'broadcast'});expect(sent()).toHaveLength(0);
 });
 it('seals uncompleted old recovery-epoch deliveries unknown and never replays them',async()=>{
  const j=await ready();await recoverBroadcasts({...e,RECOVERY_EPOCH:'restored-epoch'});await handleBroadcast({...e,RECOVERY_EPOCH:'restored-epoch'},j);
  expect(await broadcastStats(e,'synthetic-broadcast')).toMatchObject({state:'interrupted',success:0,unknown:1,pending:0});expect(sent()).toHaveLength(0);
 });
});
