import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it,vi} from 'vitest';
import type {Env,Job,MessageRow,Params,TGMessage,TGUpdate} from '../src/types';
import {accept} from '../src/ingress';
import {execute} from '../src/coordinator';
import {all,one,run} from '../src/db';
import {addBlockRule} from '../src/block-rules';
import {setBlocked} from '../src/moderation';
import {BotCoordinator} from '../src/bot';
import {failOperationCompletion} from './completion-fault';

let e:Env,requests:{key:string;method:string;params:Params}[],nextRuleJob:number;
const incoming=(id:number,extra:Partial<TGMessage>={}):TGMessage=>({message_id:id,date:1,chat:{id:20001,type:'private'},from:{id:20001},text:'SYNTHETIC-RECOVERY-AD',...extra});
async function admit(update:TGUpdate):Promise<Job>{await accept(e,update);return (await one<Job>(e,'SELECT * FROM jobs WHERE dedupe=?',`u:${update.update_id}`))!;}
async function addRule(){
 const j=await admit({update_id:nextRuleJob++,message:{message_id:nextRuleJob,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup'},from:{id:10001},text:'/mb rules'}});
 await addBlockRule(e,j,'10001','keyword','SYNTHETIC-RECOVERY-AD');
}
async function ledger(key:string,method:string,phase:string,result:unknown=null) {
 await run(e,'INSERT INTO operations(op_key,method,phase,result,reason,created_at,updated_at) VALUES(?,?,?,?,?,1,1)',`${e.RECOVERY_EPOCH}:${key}`,method,phase,result===null?null:JSON.stringify(result),phase==='unknown'?'synthetic outcome unknown':null);
}
function journalBot(values=new Map<string,unknown>()) {
 const storage={get:async(key:string)=>values.get(key),put:async(key:string|Record<string,unknown>,value?:unknown)=>{if(typeof key==='string')values.set(key,value);else for(const [k,v]of Object.entries(key))values.set(k,v);},delete:async(key:string)=>values.delete(key)};
 const bot=new BotCoordinator({storage} as unknown as DurableObjectState,e);
 e={...e,BOT:{idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:(url:string,init:RequestInit)=>bot.fetch(new Request(url,init))})} as unknown as DurableObjectNamespace};
 return values;
}
beforeEach(async()=>{
 e={...env} as unknown as Env;requests=[];nextRuleJob=10000;let messageId=9000;
 e.JOBS={send:async()=>{}} as unknown as Queue;
 e.CONVERSATIONS={idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})} as unknown as DurableObjectNamespace;
 e.BOT={idFromName:(name:string)=>name,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  const body=JSON.parse(String(init.body));requests.push(body);
  const result=['copyMessage','sendMessage'].includes(body.method)?{message_id:++messageId}:body.method==='sendMediaGroup'?(body.params.media as Params[]).map(()=>({message_id:++messageId})):true;
  return Response.json({status:'success',result});
 }})} as unknown as DurableObjectNamespace;
 await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES('20001',1,40,1,500)");
});
describe('moderation decisions across irreversible delivery recovery',()=>{
 it('keeps a no-hit decision when a rule is added after a copy succeeded but before its mapping committed',async()=>{
  const j=await admit({update_id:1,message:incoming(1)});let fail=true;
  const broken={...e,DB:new Proxy(e.DB,{get(db,key){
   if(key==='prepare')return (sql:string)=>{
    if(fail&&sql.startsWith('UPDATE messages SET delivery=?,admin_id=')){fail=false;throw new Error('synthetic mapping write failure');}
    return db.prepare(sql);
   };
   const value=Reflect.get(db,key);return typeof value==='function'?value.bind(db):value;
  }})};
  await expect(execute(broken,j)).rejects.toThrow('synthetic mapping write failure');
  expect(await one(e,"SELECT json_extract(payload,'$.moderationChecked') checked FROM jobs WHERE id=?",j.id)).toEqual({checked:1});
  await addRule();await execute(e,j);
  expect(requests.filter(request=>request.method==='copyMessage')).toHaveLength(1);
  expect(await one(e,'SELECT delivery,admin_id FROM messages WHERE source_id=1')).toEqual({delivery:'success',admin_id:9001});
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:0});
 });
 it.each(['success','issued','unknown'] as const)('reconciles a pre-upgrade %s copy before applying newly added rules',async phase=>{
  const j=await admit({update_id:2,message:incoming(2)}),row=(await one<MessageRow>(e,'SELECT * FROM messages WHERE source_id=2'))!;
  await ledger(`send:${row.id}`,'copyMessage',phase,phase==='success'?{message_id:8001}:null);
  await addRule();await execute(e,j);
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:0});
  expect(await one(e,'SELECT delivery,admin_id FROM messages WHERE id=?',row.id)).toEqual({delivery:phase==='unknown'?'unknown':'success',admin_id:phase==='success'?8001:phase==='issued'?9001:null});
  expect(requests.filter(request=>request.method==='copyMessage')).toHaveLength(phase==='issued'?1:0);
 });
 it('recovers a saved album mapping after a rule is added, without resending or discarding membership',async()=>{
  const first=await admit({update_id:10,message:incoming(10,{text:undefined,photo:[{file_id:'synthetic-1'}],media_group_id:'synthetic-album'})});
  await admit({update_id:11,message:incoming(11,{text:undefined,caption:'SYNTHETIC-RECOVERY-AD',photo:[{file_id:'synthetic-2'}],media_group_id:'synthetic-album'})});
  const members=await all<MessageRow>(e,"SELECT * FROM messages WHERE media_group='synthetic-album' ORDER BY source_id");
  await run(e,'UPDATE media_groups SET due_at=0');
  await run(e,"INSERT INTO media_batches(batch_key,group_key,members,thread_id) VALUES('synthetic-saved-album','20001:synthetic-album',?,40)",JSON.stringify(members.map(row=>row.id)));
  await ledger('synthetic-saved-album','sendMediaGroup','success',[{message_id:8101},{message_id:8102}]);
  await addRule();await execute(e,first);
  expect(requests.some(request=>['copyMessage','sendMediaGroup'].includes(request.method))).toBe(false);
  expect(await all(e,"SELECT delivery,admin_id FROM messages WHERE media_group='synthetic-album' ORDER BY source_id")).toEqual([{delivery:'success',admin_id:8101},{delivery:'success',admin_id:8102}]);
  expect(await one(e,"SELECT state FROM media_batches WHERE batch_key='synthetic-saved-album'")).toEqual({state:'done'});
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:0});
 });
 it('seals singleton album fallbacks before copying, so a mapping failure cannot cause a later retroactive ban',async()=>{
  const j=await admit({update_id:20,message:incoming(20,{text:undefined,caption:'SYNTHETIC-RECOVERY-AD',photo:[{file_id:'synthetic-file'}],media_group_id:'synthetic-singleton'})});
  await run(e,'UPDATE media_groups SET due_at=0');let fail=true;
  const broken={...e,DB:new Proxy(e.DB,{get(db,key){
   if(key==='prepare')return (sql:string)=>{
    if(fail&&sql.startsWith('UPDATE messages SET delivery=?,admin_id=')){fail=false;throw new Error('synthetic singleton mapping failure');}
    return db.prepare(sql);
   };
   const value=Reflect.get(db,key);return typeof value==='function'?value.bind(db):value;
  }})};
  await expect(execute(broken,j)).rejects.toThrow('synthetic singleton mapping failure');
  await addRule();await execute(e,j);
  expect(requests.filter(request=>request.method==='copyMessage')).toHaveLength(1);
  expect(await one(e,'SELECT delivery,admin_id FROM messages WHERE source_id=20')).toEqual({delivery:'success',admin_id:9001});
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:0});
 });
 it('matches each new edit separately even though the original already passed moderation',async()=>{
  const original=await admit({update_id:30,message:incoming(30,{text:'synthetic ordinary original'})});await execute(e,original);
  await addRule();await execute(e,await admit({update_id:31,edited_message:incoming(30,{edit_date:2})}));
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:1});
  expect(requests.some(request=>request.method==='editMessageText')).toBe(false);
 });
 it.each([false,true])('recovers a copy journal interrupted by D1 failure after ban, even if unblocked=%s',async unblocked=>{
  const j=await admit({update_id:40,message:incoming(40,{text:'synthetic ordinary content'})});journalBot();
  vi.mocked(fetch).mockResolvedValueOnce(Response.json({ok:true,result:{message_id:8201}}));
  await expect(execute({...e,DB:failOperationCompletion(e.DB,'synthetic D1 completion failure')},j)).rejects.toThrow('synthetic D1 completion failure');
  const row=(await one<MessageRow>(e,'SELECT * FROM messages WHERE source_id=40'))!;
  expect(await one(e,'SELECT phase FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:send:${row.id}`)).toEqual({phase:'issued'});
  await setBlocked(e,{...j,id:nextRuleJob++},'20001',true,{source:'manual'});
  expect(await one(e,'SELECT delivery FROM messages WHERE id=?',row.id)).toEqual({delivery:'blocked'});
  if(unblocked)await setBlocked(e,{...j,id:nextRuleJob++},'20001',false,{source:'manual'});
  await execute(e,j);
  expect(await one(e,'SELECT delivery,admin_id FROM messages WHERE id=?',row.id)).toEqual({delivery:'success',admin_id:8201});
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
 });
 it('uses an impossible guard when an issued operation has no journal, so quick unblock cannot issue new content',async()=>{
  const j=await admit({update_id:50,message:incoming(50)}),row=(await one<MessageRow>(e,'SELECT * FROM messages WHERE source_id=50'))!;
  await ledger(`send:${row.id}`,'copyMessage','issued');journalBot();
  await setBlocked(e,{...j,id:nextRuleJob++},'20001',true,{source:'manual'});
  await setBlocked(e,{...j,id:nextRuleJob++},'20001',false,{source:'manual'});
  await execute(e,j);
  expect(await one(e,'SELECT delivery,admin_id FROM messages WHERE id=?',row.id)).toEqual({delivery:'blocked',admin_id:null});
  expect(await one(e,'SELECT phase,reason FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:send:${row.id}`)).toEqual({phase:'failed',reason:'cancelled_by_state'});
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
 it('retains an unknown issued copy outcome after a ban without retrying Telegram',async()=>{
  const j=await admit({update_id:60,message:incoming(60)}),row=(await one<MessageRow>(e,'SELECT * FROM messages WHERE source_id=60'))!;
  await ledger(`send:${row.id}`,'copyMessage','issued');
  journalBot(new Map([[`op:${e.RECOVERY_EPOCH}:send:${row.id}`,{phase:'issued'}]]));
  await setBlocked(e,{...j,id:nextRuleJob++},'20001',true,{source:'manual'});
  await execute(e,j);await execute(e,j);
  expect(await one(e,'SELECT delivery,admin_id FROM messages WHERE id=?',row.id)).toEqual({delivery:'unknown',admin_id:null});
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });
 it('recovers every frozen album mapping after the ban suppresses its interrupted result write',async()=>{
  const j=await admit({update_id:70,message:incoming(70,{text:undefined,photo:[{file_id:'synthetic-a'}],media_group_id:'synthetic-banned-album'})});
  await admit({update_id:71,message:incoming(71,{text:undefined,photo:[{file_id:'synthetic-b'}],media_group_id:'synthetic-banned-album'})});
  await run(e,'UPDATE media_groups SET due_at=0');let fail=true;
  const broken={...e,DB:new Proxy(e.DB,{get(db,key){
   if(key==='prepare')return (sql:string)=>{
    if(fail&&sql.startsWith('UPDATE messages SET delivery=?,admin_id=')){fail=false;throw new Error('synthetic album result failure');}
    return db.prepare(sql);
   };
   const value=Reflect.get(db,key);return typeof value==='function'?value.bind(db):value;
  }})};
  await expect(execute(broken,j)).rejects.toThrow('synthetic album result failure');
  await setBlocked(e,{...j,id:nextRuleJob++},'20001',true,{source:'manual'});
  await execute(e,j);
  expect(await all(e,"SELECT delivery,admin_id FROM messages WHERE media_group='synthetic-banned-album' ORDER BY source_id")).toEqual([{delivery:'success',admin_id:9001},{delivery:'success',admin_id:9002}]);
  expect(await one(e,"SELECT state FROM media_batches WHERE group_key='20001:synthetic-banned-album'")).toEqual({state:'done'});
  expect(requests.filter(request=>request.method==='sendMediaGroup')).toHaveLength(1);
 });
});
