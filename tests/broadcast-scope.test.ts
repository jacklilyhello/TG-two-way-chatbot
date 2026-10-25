import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it} from 'vitest';
import type {Env,Job,Params} from '../src/types';
import {all,one,run} from '../src/db';
import {normalizeBroadcastAudience,parseBroadcastIds,prepareBroadcast,getBroadcast,launchBroadcast,handleBroadcast,broadcastStats,recoverBroadcasts,type BroadcastAudience} from '../src/broadcast-delivery';
import {RetryLater} from '../src/operations';

let e:Env,requests:{key:string;method:string;params:Params}[];
let outcome:(params:Params)=>unknown;
async function addUser(id:string,thread:number|null,blocked=0){await run(e,'INSERT INTO users(tgid,first_contact,thread_id,blocked) VALUES(?,1,?,?)',id,thread,blocked);}
async function prepare(id:string,audience?:BroadcastAudience){return prepareBroadcast(e,{id,actor:'10001',text:'SYNTHETIC-SCOPED-BROADCAST',audience});}
async function ready(id:string,audience?:BroadcastAudience){await prepare(id,audience);await run(e,"UPDATE broadcasts SET state='confirmed' WHERE id=?",id);await launchBroadcast(e,id);return (await one<Job>(e,'SELECT * FROM jobs WHERE dedupe=?',`broadcast:${id}`))!;}
async function drain(j:Job){for(let index=0;index<20;index++){try{await handleBroadcast(e,j);}catch(error){if(!(error instanceof RetryLater))throw error;}await run(e,'UPDATE broadcasts SET next_at=0 WHERE id=?',JSON.parse(j.payload).broadcastId);if((await getBroadcast(e,JSON.parse(j.payload).broadcastId))?.state!=='sending')return;}throw new Error('synthetic_drain_limit');}
const privateRequests=()=>requests.filter(request=>String(request.params.chat_id)!==e.ADMIN_CHAT_ID);
beforeEach(async()=>{
 e={...env} as unknown as Env;requests=[];let message=5000;
 e.JOBS={send:async()=>{}} as unknown as Queue;
 e.CONVERSATIONS={idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})} as unknown as DurableObjectNamespace;
 outcome=()=>({status:'success',result:{message_id:++message}});
 e.BOT={idFromName:(name:string)=>name,get:()=>({fetch:async(_url:string,init:RequestInit)=>{const request=JSON.parse(String(init.body));requests.push(request);return Response.json(outcome(request.params));}})} as unknown as DurableObjectNamespace;
 await addUser('20001',71);await addUser('20002',72);await addUser('20003',73);
});

describe('broadcast audience canonical input',()=>{
 it('defaults to all and normalizes directed IDs in numeric order without changing its input',()=>{
  expect(normalizeBroadcastAudience()).toEqual({mode:'all'});expect(normalizeBroadcastAudience({mode:'all'})).toEqual({mode:'all'});
  const audience:BroadcastAudience={mode:'directed',ids:['20002','9','20001','20002']};
  expect(normalizeBroadcastAudience(audience)).toEqual({mode:'directed',ids:['9','20001','20002']});
  expect(audience.ids).toEqual(['20002','9','20001','20002']);
 });
 it('accepts commas, Chinese commas, spaces and line breaks and removes duplicate IDs',()=>{
  expect(parseBroadcastIds(' 20002,20001，20002\n\t9 4503599627370495 ')).toEqual(['9','20001','20002','4503599627370495']);
 });
 it.each(['',' ,， \n','0','-1','+1','001','1.0','1e3','@synthetic','https://t.me/synthetic','4503599627370496','9007199254740991','20001;20002','20001/20002','20001\u200b'])('rejects malformed ID input %j',text=>{
  expect(()=>parseBroadcastIds(text)).toThrow('invalid_broadcast_audience');
 });
 it('allows at most 200 distinct IDs while duplicate entries do not widen the range',()=>{
  const ids=Array.from({length:200},(_,index)=>String(index+1));
  expect(parseBroadcastIds([...ids,...ids].join(','))).toEqual(ids);
  expect(()=>parseBroadcastIds([...ids,'201'].join(','))).toThrow('invalid_broadcast_audience');
 });
 it.each([null,{},[],{mode:'directed'},{mode:'directed',ids:[]},{mode:'directed',ids:[20001]},{mode:'directed',ids:Array(1)},{mode:'all',ids:['20001']},{mode:'other',ids:['20001']},{mode:'directed',ids:['20001'],extra:'ignored'}])('rejects invalid audience structures %j',input=>{
  expect(()=>normalizeBroadcastAudience(input as unknown as BroadcastAudience)).toThrow('invalid_broadcast_audience');
 });
});

describe('broadcast directed immutable snapshot',()=>{
 it('selects only requested eligible users and never registers unknown IDs',async()=>{
  await addUser('20004',74,1);await addUser('20005',null);
  const b=await prepare('directed',{mode:'directed',ids:['20006','20005','20004','20002','20001','20002']});
  expect(b).toMatchObject({total:2,scope_json:'{"mode":"directed","ids":["20001","20002","20004","20005","20006"]}'});
  expect(await all(e,'SELECT user_id FROM broadcast_recipients WHERE broadcast_id=? ORDER BY user_id',b.id)).toEqual([{user_id:'20001'},{user_id:'20002'}]);
  expect(await one(e,"SELECT tgid FROM users WHERE tgid='20006'")).toBeNull();
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20004'")).toEqual({blocked:1});
 });
 it('intersects requested IDs with the private test cap even when other users are eligible',async()=>{
  e={...e,INSTANCE_ID:'test-a',BROADCAST_TEST_RECIPIENT_IDS:'20001,20002'};
  const b=await prepare('test-directed',{mode:'directed',ids:['20002','20003']});
  expect(b.total).toBe(1);expect(await all(e,'SELECT user_id FROM broadcast_recipients WHERE broadcast_id=?',b.id)).toEqual([{user_id:'20002'}]);
  const allUsers=await prepare('test-all',{mode:'all'});expect(allUsers.total).toBe(2);
 });
 it('replays canonical equivalent input without adding newly eligible users',async()=>{
  await run(e,"UPDATE users SET blocked=1 WHERE tgid='20002'");
  expect((await prepare('frozen',{mode:'directed',ids:['20002','20001']})).total).toBe(1);
  await run(e,"UPDATE users SET blocked=0 WHERE tgid='20002'");
  expect((await prepare('frozen',{mode:'directed',ids:['20001','20002','20001']})).total).toBe(1);
  expect(await all(e,"SELECT user_id FROM broadcast_recipients WHERE broadcast_id='frozen'")).toEqual([{user_id:'20001'}]);
 });
 it.each([{mode:'all'} as BroadcastAudience,{mode:'directed',ids:['20001','20002']} as BroadcastAudience])('rejects replacing a preview scope with %j under the same campaign ID',async audience=>{
  await prepare('immutable',{mode:'directed',ids:['20001']});
  await expect(prepare('immutable',audience)).rejects.toThrow('broadcast_snapshot_mismatch');
  expect(await getBroadcast(e,'immutable')).toMatchObject({total:1,scope_json:'{"mode":"directed","ids":["20001"]}'});
  expect(await all(e,"SELECT user_id FROM broadcast_recipients WHERE broadcast_id='immutable'")).toEqual([{user_id:'20001'}]);
 });
 it('handles a valid directed list containing no eligible users with truthful zero counts',async()=>{
  const j=await ready('empty-directed',{mode:'directed',ids:['29999']});await drain(j);
  expect(await broadcastStats(e,'empty-directed')).toMatchObject({state:'done',total:0,success:0,failed:0,unknown:0,unsent:0});expect(privateRequests()).toHaveLength(0);
 });
 it('preserves legacy all-scope campaigns and already-confirmed progress when the new column is omitted',async()=>{
  await run(e,"INSERT INTO broadcasts(id,instance_id,bot_id,actor_id,epoch,text,state,total,snapshot_ready,created_at) VALUES('legacy-all',?,?,?,?,'SYNTHETIC-LEGACY','sending',2,1,1)",e.INSTANCE_ID,e.EXPECTED_BOT_ID,'10001',e.RECOVERY_EPOCH);
  await run(e,"INSERT INTO broadcast_recipients(broadcast_id,user_id,state,part_index,success_parts,updated_at) VALUES('legacy-all','20001','success',1,1,1),('legacy-all','20002','pending',0,0,1)");
  expect(await getBroadcast(e,'legacy-all')).toMatchObject({scope_json:'{"mode":"all"}',total:2,state:'sending'});
  await launchBroadcast(e,'legacy-all');const j=(await one<Job>(e,"SELECT * FROM jobs WHERE dedupe='broadcast:legacy-all'"))!;await drain(j);
  expect(privateRequests().map(request=>request.params.chat_id)).toEqual(['20002']);
  expect(await broadcastStats(e,'legacy-all')).toMatchObject({success:2,total:2,unsent:0});
 });
});

describe('broadcast directed durable delivery',()=>{
 it('identifies the completed directed campaign and its deduplicated requested count in one stable result report',async()=>{
  const id='abcd1234report',j=await ready(id,{mode:'directed',ids:['20002','20001','20001']});await drain(j);await drain(j);
  const reports=requests.filter(request=>request.key===`${e.RECOVERY_EPOCH}:broadcast-report:${id}`);
  expect(reports).toHaveLength(1);expect(reports[0].params.text).toContain('广播 abcd1234 已结束');
  expect(reports[0].params.text).toContain('范围：定向（指定 2 个 ID）');expect(reports[0].params.text).toContain('已确认成功：2');
 });
 it.each(['not-json','null','{"mode":"other"}'])('reports a corrupt saved scope as abnormal without widening it or losing the result: %s',async scope=>{
  await run(e,'UPDATE users SET blocked=1');const id='corrupt-scope',j=await ready(id);
  await run(e,'UPDATE broadcasts SET scope_json=? WHERE id=?',scope,id);await drain(j);
  const report=requests.find(request=>request.key===`${e.RECOVERY_EPOCH}:broadcast-report:${id}`)!;
  expect(report.params.text).toContain('范围：范围异常');expect(report.params.text).not.toContain('全体用户');expect(privateRequests()).toHaveLength(0);
  expect(await broadcastStats(e,id)).toMatchObject({state:'done',total:0,success:0});
 });

 it('preserves both image and long text for a directed recipient while excluding all other eligible users',async()=>{
  const text='SYNTHETIC-DIRECTED-LONG-'.repeat(60),id='directed-long-photo';
  await prepareBroadcast(e,{id,actor:'10001',text,photo:'SYNTHETIC-DIRECTED-PHOTO',audience:{mode:'directed',ids:['20002']}});
  await run(e,"UPDATE broadcasts SET state='confirmed' WHERE id=?",id);await launchBroadcast(e,id);
  const j=(await one<Job>(e,'SELECT * FROM jobs WHERE dedupe=?',`broadcast:${id}`))!;await drain(j);
  expect(privateRequests().map(request=>({method:request.method,params:request.params}))).toEqual([
   {method:'sendPhoto',params:{chat_id:'20002',photo:'SYNTHETIC-DIRECTED-PHOTO'}},
   {method:'sendMessage',params:{chat_id:'20002',text}}
  ]);
  expect(await broadcastStats(e,id)).toMatchObject({total:1,success:1,partial:0});
 });

 it('sends selected users once and resumes after deployment without sending outsiders or unknown results again',async()=>{
  outcome=params=>String(params.chat_id)==='20001'?{status:'unknown',reason:'synthetic timeout'}:{status:'success',result:{message_id:5200}};
  const j=await ready('directed-recovery',{mode:'directed',ids:['20002','20001']});
  await expect(handleBroadcast(e,j)).rejects.toBeInstanceOf(RetryLater);
  expect(await broadcastStats(e,'directed-recovery')).toMatchObject({unknown:1,pending:1});
  await recoverBroadcasts({...e,VERSION:'synthetic-upgrade'});await run(e,"UPDATE broadcasts SET next_at=0 WHERE id='directed-recovery'");await drain(j);await drain(j);
  expect(privateRequests().map(request=>request.params.chat_id)).toEqual(['20001','20002']);
  expect(await broadcastStats(e,'directed-recovery')).toMatchObject({total:2,success:1,unknown:1,failed:0,unsent:0,state:'done'});
 });
 it('keeps the directed cohort fixed through a safe Telegram rate-limit retry',async()=>{
  let limited=true;
  e.BOT={idFromName:(name:string)=>name,get:()=>({fetch:async(_url:string,init:RequestInit)=>{const request=JSON.parse(String(init.body));requests.push(request);if(limited){limited=false;return Response.json({due:Date.now()+1000},{status:429});}return Response.json({status:'success',result:{message_id:5300}});}})} as unknown as DurableObjectNamespace;
  const j=await ready('directed-limited',{mode:'directed',ids:['20002']});await expect(handleBroadcast(e,j)).rejects.toBeInstanceOf(RetryLater);
  expect(await broadcastStats(e,'directed-limited')).toMatchObject({success:0,unsent:1});await drain(j);
  expect(privateRequests().map(request=>request.params.chat_id)).toEqual(['20002','20002']);
  expect(await broadcastStats(e,'directed-limited')).toMatchObject({success:1,total:1,unsent:0});
 });
 it('recovers a directed send whose recipient-result transaction was interrupted without sending again',async()=>{
  const j=await ready('directed-mapping',{mode:'directed',ids:['20002']});let fail=true;
  const broken={...e,DB:new Proxy(e.DB,{get(target,key){if(key==='prepare')return (sql:string)=>{if(fail&&sql.startsWith('UPDATE messages SET delivery=?')){fail=false;throw new Error('synthetic directed interruption');}return target.prepare(sql);};const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;}})};
  await expect(handleBroadcast(broken,j)).rejects.toThrow('synthetic directed interruption');await drain(j);
  expect(privateRequests()).toHaveLength(1);expect(await broadcastStats(e,'directed-mapping')).toMatchObject({success:1,total:1});
 });
});
