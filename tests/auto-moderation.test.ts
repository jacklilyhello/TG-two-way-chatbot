import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it} from 'vitest';
import type {Env,Job,Params,TGMessage,TGUpdate} from '../src/types';
import {accept} from '../src/ingress';
import {execute} from '../src/coordinator';
import {all,one,run} from '../src/db';
import {addBlockRule,changeBlockRule} from '../src/block-rules';
import {handleBanNotice} from '../src/moderation';
let e:Env;
let requests:{method:string;params:Params}[];
let sequence:number;
const incoming=(id:number,extra:Partial<TGMessage>={}):TGMessage=>({message_id:id,date:1,chat:{id:20001,type:'private'},from:{id:20001,first_name:'Synthetic'},text:'synthetic ordinary message',...extra});
async function admit(update:TGUpdate):Promise<Job>{await accept(e,update);return (await one<Job>(e,'SELECT * FROM jobs WHERE dedupe=?',`u:${update.update_id}`))!;}
async function rule(kind:'keyword'|'regex',pattern:string){
 const j=await admit({update_id:++sequence,message:{message_id:sequence,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup'},from:{id:10001},text:'/mb rules'}});
 return (await addBlockRule(e,j,'10001',kind,pattern)).rule_id!;
}
beforeEach(()=>{
 e={...env} as unknown as Env;requests=[];sequence=1000;let messageId=9000;
 e.JOBS={send:async()=>{}} as unknown as Queue;
 e.CONVERSATIONS={idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})} as unknown as DurableObjectNamespace;
 e.BOT={idFromName:(name:string)=>name,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  const b=JSON.parse(String(init.body));requests.push(b);
  const result=b.method==='getChat'?{id:20001,first_name:'Synthetic'}:b.method==='createForumTopic'?{message_thread_id:40}:b.method==='getUserProfilePhotos'?{photos:[]}:['copyMessage','sendMessage','sendPhoto'].includes(b.method)?{message_id:++messageId}:b.method==='sendMediaGroup'?(b.params.media as Params[]).map(()=>({message_id:++messageId})):true;
  return Response.json({status:'success',result});
 }})} as unknown as DurableObjectNamespace;
});
describe('automatic bans through persisted ingress and actual dispatcher',()=>{
 it('initial first-message keyword hit creates a pinned panel and exactly one admin notice, without forwarding or DM',async()=>{
  await rule('keyword','SYNTHETIC-AD');
  const j=await admit({update_id:10,message:incoming(10,{text:'synthetic-ad offer'})});
  await execute(e,j);await execute(e,j);
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:1});
  expect(await one(e,"SELECT delivery FROM messages WHERE source_id=10")).toEqual({delivery:'blocked'});
  const notice=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='ban-notice'"))!;
  await handleBanNotice(e,notice);await handleBanNotice(e,notice);
  const methods=requests.map(r=>r.method);
  expect(methods.indexOf('createForumTopic')).toBeLessThan(methods.indexOf('pinChatMessage'));
  const notes=requests.filter(r=>r.method==='sendMessage'&&String(r.params.text).startsWith('🚫'));
  expect(notes).toHaveLength(1);
  expect(notes[0].params).toMatchObject({chat_id:e.ADMIN_CHAT_ID,message_thread_id:40});
  expect(String(notes[0].params.text)).toContain('SYNTHETIC-AD');
  expect(requests.findIndex(r=>r===notes[0])).toBeGreaterThan(methods.indexOf('pinChatMessage'));
  expect(requests.some(r=>r.method==='copyMessage'||String(r.params.chat_id)==='20001'&&r.method==='sendMessage')).toBe(false);
  await execute(e,await admit({update_id:11,message:incoming(11,{text:'synthetic-ad again'})}));
  expect(await one(e,'SELECT COUNT(*) n FROM ban_events WHERE changed=1')).toEqual({n:1});
 });
 it.each(['text','caption','edit'] as const)('matches regex on %s and suppresses the triggering content',async kind=>{
  await rule('regex','SYNTHETIC-[0-9]{2}');
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES('20001',1,40,1,500)");
  let j:Job;
  if(kind==='edit'){
   await admit({update_id:20,message:incoming(20)});
   await run(e,"UPDATE messages SET delivery='success',admin_id=600,thread_id=40 WHERE source_id=20");
   j=await admit({update_id:21,edited_message:incoming(20,{text:'SYNTHETIC-42',edit_date:2})});
  }else j=await admit({update_id:20,message:incoming(20,kind==='caption'?{text:undefined,photo:[{file_id:'synthetic-file'}],caption:'SYNTHETIC-42'}:{text:'SYNTHETIC-42'})});
  await execute(e,j);
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:1});
  expect(requests.some(r=>['copyMessage','editMessageText','editMessageCaption','sendMediaGroup'].includes(r.method))).toBe(false);
 });
 it('checks a later album caption before delivering any uncaptioned member',async()=>{
  await rule('keyword','SYNTHETIC-ALBUM-AD');
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES('20001',1,40,1,500)");
  const first=await admit({update_id:30,message:incoming(30,{text:undefined,media_group_id:'synthetic-album',photo:[{file_id:'synthetic-1'}]})});
  await admit({update_id:31,message:incoming(31,{text:undefined,media_group_id:'synthetic-album',photo:[{file_id:'synthetic-2'}],caption:'SYNTHETIC-ALBUM-AD'})});
  await run(e,'UPDATE media_groups SET due_at=0');
  await execute(e,first);
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:1});
  expect(await all(e,"SELECT delivery FROM messages WHERE direction='in' ORDER BY source_id")).toEqual([{delivery:'blocked'},{delivery:'blocked'}]);
  expect(requests.some(r=>['copyMessage','sendMediaGroup'].includes(r.method))).toBe(false);
 });
 it('keeps a matched original suppressed after unblock and rule deletion on retry',async()=>{
  const id=await rule('keyword','SYNTHETIC-AD');
  const j=await admit({update_id:40,message:incoming(40,{text:'SYNTHETIC-AD'})});await execute(e,j);
  await run(e,"UPDATE users SET blocked=0 WHERE tgid='20001'");
  const mutation={...j,id:++sequence};
  await changeBlockRule(e,mutation,'10001',id,'delete');
  await execute(e,j);
  expect(await one(e,"SELECT blocked FROM users WHERE tgid='20001'")).toEqual({blocked:0});
  expect(await one(e,'SELECT delivery FROM messages WHERE source_id=40')).toEqual({delivery:'blocked'});
  expect(requests.some(r=>r.method==='copyMessage')).toBe(false);
 });
 it('delivers a nonmatch normally and rejects ordinary /mb rules management silently',async()=>{
  await rule('keyword','SYNTHETIC-AD');
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES('20001',1,40,1,500)");
  await execute(e,await admit({update_id:50,message:incoming(50)}));
  expect(requests.filter(r=>r.method==='copyMessage')).toHaveLength(1);
  await execute(e,await admit({update_id:51,message:incoming(51,{text:'/mb rules add keyword forbidden'})}));
  expect(await one(e,'SELECT COUNT(*) n FROM block_rules')).toEqual({n:1});
  expect(requests.some(r=>r.method==='sendMessage'&&String(r.params.chat_id)==='20001')).toBe(false);
 });
});
