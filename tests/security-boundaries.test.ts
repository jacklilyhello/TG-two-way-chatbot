import {env,SELF} from 'cloudflare:test';
import {beforeEach,describe,expect,it} from 'vitest';
import type {Env,Job,MessageRow,Params,TGMessage,TGMessageOrigin,TGUpdate} from '../src/types';
import {all,one,run,user} from '../src/db';
import {accept} from '../src/ingress';
import {execute} from '../src/coordinator';
import {deleteSingle,handleMessage} from '../src/messaging';
import {handleAdmin} from '../src/admin';
import {albumMemberBindingMatches} from '../src/security';

interface Request {key:string;method:string;params:Params}
let e:Env,requests:Request[],sequence:number;
const incoming=(id:number,text:string,extra:Partial<TGMessage>={}):TGMessage=>({message_id:id,date:1,chat:{id:20001,type:'private'},from:{id:20001,first_name:'Ordinary'},text,...extra});
const outgoing=(id:number,text:string,extra:Partial<TGMessage>={}):TGMessage=>({message_id:id,date:1,chat:{id:-1001234567890,type:'supergroup'},from:{id:10001},message_thread_id:40,is_topic_message:true,text,...extra});
async function admitted(id:number,m:TGMessage,edited=false) {
 const update:TGUpdate={update_id:id,[edited?'edited_message':'message']:m};
 await accept(e,update);
 return {update,job:(await one<Job>(e,'SELECT * FROM jobs WHERE dedupe=?',`u:${id}`))!};
}
async function dispatch(id:number,m:TGMessage,edited=false) {const {job}=await admitted(id,m,edited);await execute(e,job);return job;}
async function action(token:string,kind:string,payload:Params={},actor='10001',thread:number|null=40) {
 await run(e,'INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) VALUES(?,?,?,?,?,1,?,?)',token,kind,'20001',actor,thread,JSON.stringify({instance:e.INSTANCE_ID,messageId:501,...payload}),Date.now()+60000);
}
function callback(id:number,token:string,overrides:Partial<TGMessage>={},actor=10001):TGUpdate {
 return {update_id:id,callback_query:{id:`cb:${id}`,from:{id:actor},data:`a:${token}`,message:outgoing(501,'',{from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true},...overrides})}};
}
beforeEach(async()=>{
 requests=[];sequence=1000;
 e={...env} as unknown as Env;
 const journal=new Map<string,unknown>();
 e.BOT={idFromName:(name:string)=>name,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  const r=JSON.parse(String(init.body)) as Request;
  if(!journal.has(r.key)) {
   requests.push(r);
   const result=r.method==='getUserProfilePhotos'?{photos:[]}:r.method==='getChat'?{id:Number(r.params.chat_id),first_name:'Test'}:r.method==='createForumTopic'?{message_thread_id:++sequence}:['copyMessage','sendMessage','sendPhoto'].includes(r.method)?{message_id:++sequence}:true;
   journal.set(r.key,{status:'success',result});
  }
  return Response.json(journal.get(r.key));
 }})} as unknown as DurableObjectNamespace;
 await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES('20001',1,40,1,500),('20002',1,41,1,600)");
});

describe('isolated management authorization and privacy boundaries',()=>{
 it.each(['/tj 20002','/sc','/mb','/mb users','/mb search 20002','/mb timezone Europe/London','/tj@TestBot 20002','/sc@TestBot','/mb@TestBot'])('keeps ordinary-user %s silent and cannot mutate or expose management state',async text=>{
  await dispatch(1,incoming(1,text));
  expect(requests).toEqual([]);
  expect(await user(e,'20002')).toMatchObject({thread_id:41,binding_version:1,blocked:0});
  expect(await one(e,'SELECT * FROM settings')).toBeNull();
  expect(await one(e,'SELECT * FROM deletion_batches')).toBeNull();
  expect(await one(e,'SELECT business,delivery FROM messages')).toEqual({business:0,delivery:'ignored'});
 });
 it.each([
  {name:'forwarded administrator identity',extra:{forward_origin:{type:'user',sender_user:{id:10001},date:1}}},
  {name:'administrator name and username',extra:{from:{id:20001,first_name:'Administrator 10001',username:'admin'}}},
 ])('does not trust $name when the actual actor is ordinary',async({extra})=>{
  await dispatch(2,incoming(2,'/mb search 20002',extra as Partial<TGMessage>));
  expect(requests).toEqual([]);
 });
 it('does not treat forwarded slash text as a fresh administrator command',async()=>{
  await dispatch(3,outgoing(3,'/tj 29999',{forward_origin:{type:'user',sender_user:{id:20001},date:1}}));
  expect(await user(e,'29999')).toBeNull();expect(requests).toEqual([]);
 });
 it('routes supported uppercase registration through the same target-user queue',async()=>{
  expect(await accept(e,{update_id:7,message:outgoing(7,'/TJ 20002',{message_thread_id:80})})).toBe('20002');
  expect(await one(e,"SELECT user_key FROM jobs WHERE dedupe='u:7'")).toEqual({user_key:'20002'});
 });
 it.each([
  {type:'user',sender_user:{id:123456,is_bot:true},date:1},
  {type:'user',sender_user:{id:654321,is_bot:true},date:1},
  {type:'user',sender_user:{id:10001},date:1},
  {type:'chat',sender_chat:{id:-1001234567890},date:1},
  {type:'channel',chat:{id:-1001234567890},message_id:5,date:1},
 ] as TGMessageOrigin[])('does not forward Bot or administrator-group internal content ($type)',async origin=>{
  await dispatch(4,outgoing(4,'internal administrative text',{forward_origin:origin}));
  expect(await one(e,'SELECT * FROM messages')).toBeNull();expect(requests).toEqual([]);
 });
 it('preserves ordinary private forwards and administrator forwards from real users',async()=>{
  await dispatch(5,incoming(5,'forwarded public content',{forward_origin:{type:'user',sender_user:{id:654321,is_bot:true},date:1}}));
  await dispatch(6,outgoing(6,'user content',{forward_origin:{type:'user',sender_user:{id:20002},date:1}}));
  expect(requests.filter(r=>r.method==='copyMessage').map(r=>r.params.chat_id)).toEqual([e.ADMIN_CHAT_ID,'20001']);
 });
 it('prevents General, log, service and unauthorized administrator-group messages from becoming business',async()=>{
  await run(e,"INSERT INTO settings(key,value) VALUES('log_thread_id','90')");
  const messages=[outgoing(10,'General',{message_thread_id:undefined,is_topic_message:false}),outgoing(11,'log text',{message_thread_id:90}),outgoing(12,'',{text:undefined}),outgoing(13,'pretend admin',{from:{id:20001}}),outgoing(14,'anonymous',{sender_chat:{id:-1001234567890}})];
  for(const m of messages)await dispatch(m.message_id,m);
  expect(await all(e,'SELECT * FROM messages')).toEqual([]);
  expect(requests.filter(r=>r.method==='copyMessage')).toEqual([]);
 });
 it('keeps internal replies out of user payloads and ignores internal-message edits',async()=>{
  await dispatch(15,outgoing(15,'ordinary response',{reply_to_message:outgoing(500,'internal profile card',{from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true}})}));
  await dispatch(16,outgoing(500,'edited internal profile card',{from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true},edit_date:2}),true);
  const copy=requests.find(r=>r.method==='copyMessage')!;
  expect(copy.params).toMatchObject({message_id:15,chat_id:'20001'});
  expect(copy.params.reply_parameters).toBeUndefined();
  expect(JSON.stringify(copy.params)).not.toContain('internal profile');
  expect(requests.filter(r=>r.method==='editMessageText'&&r.params.chat_id==='20001')).toEqual([]);
 });
 it.each(['actor','instance','chat','thread','message','bot','binding','expired'] as const)('rejects a clear confirmation with mismatched %s before capturing any deletion range',async mismatch=>{
  await accept(e,{update_id:20,message:incoming(20,'old')});
  const token='a'.repeat(32);
  await action(token,'clear_confirm',mismatch==='instance'?{instance:'other-instance'}:{});
  if(mismatch==='binding')await run(e,"UPDATE users SET binding_version=2 WHERE tgid='20001'");
  if(mismatch==='expired')await run(e,'UPDATE callback_actions SET expires_at=0 WHERE token=?',token);
  const cb=callback(21,token,mismatch==='chat'?{chat:{id:-100999999,type:'supergroup'}}:mismatch==='thread'?{message_thread_id:41}:mismatch==='message'?{message_id:999}:mismatch==='bot'?{from:{id:654321,is_bot:true}}:{},mismatch==='actor'?20001:10001);
  await accept(e,cb);await execute(e,(await one<Job>(e,"SELECT * FROM jobs WHERE dedupe='u:21'"))!);
  expect(await one(e,'SELECT * FROM deletion_batches')).toBeNull();
  expect(await one(e,'SELECT tombstone FROM messages WHERE source_id=20')).toEqual({tombstone:0});
  expect(requests.some(r=>r.method==='deleteMessage')).toBe(false);
 });
 it('requires panel callbacks to come from the exact issued General message',async()=>{
  await dispatch(30,outgoing(30,'/mb',{message_thread_id:undefined,is_topic_message:false}));
  const a=(await one<{token:string;payload:string}>(e,"SELECT token,payload FROM callback_actions WHERE kind='panel' LIMIT 1"))!;
  requests=[];
  const cb=callback(31,a.token,{message_id:JSON.parse(a.payload).messageId+1,message_thread_id:undefined,is_topic_message:false});
  await accept(e,cb);await execute(e,(await one<Job>(e,"SELECT * FROM jobs WHERE dedupe='u:31'"))!);
  expect(requests.map(r=>r.method)).toEqual(['answerCallbackQuery']);
  expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',a.token)).toEqual({used:0});
 });
 it('allows a valid panel callback and replays the same update without duplicate panels or tokens',async()=>{
  await dispatch(32,outgoing(32,'/mb',{message_thread_id:undefined,is_topic_message:false}));
  const a=(await one<{token:string;payload:string}>(e,"SELECT token,payload FROM callback_actions WHERE kind='panel' LIMIT 1"))!;
  const cb=callback(33,a.token,{message_id:JSON.parse(a.payload).messageId,message_thread_id:undefined,is_topic_message:false});
  await accept(e,cb);const job=(await one<Job>(e,"SELECT * FROM jobs WHERE dedupe='u:33'"))!;
  await execute(e,job);
  const count=(await all(e,'SELECT token FROM callback_actions')).length,sendCount=requests.filter(r=>r.method==='sendMessage').length;
  await execute(e,job);
  expect((await all(e,'SELECT token FROM callback_actions')).length).toBe(count);
  expect(requests.filter(r=>r.method==='sendMessage')).toHaveLength(sendCount);
  expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',a.token)).toEqual({used:34});
 });
});

describe('queued binding and delete target isolation',()=>{
 it.each(['actor','instance'] as const)('revalidates queued outgoing %s authorization when executing',async changed=>{
  const {job}=await admitted(39,outgoing(39,'queued administrator reply'));
  await execute({...e,...(changed==='actor'?{ADMIN_IDS:'10002'}:{INSTANCE_ID:'other-instance'})},job);
  expect(requests.filter(r=>r.method==='copyMessage')).toEqual([]);
 });
 it('rejects an old outgoing message after a binding leaves and returns to the same topic',async()=>{
  const {job,update}=await admitted(40,outgoing(40,'old queued reply'));
  await run(e,"UPDATE users SET binding_version=3 WHERE tgid='20001'");
  await execute(e,job);
  expect(requests.filter(r=>r.method==='copyMessage')).toEqual([]);
  expect(await one(e,'SELECT delivery FROM messages WHERE source_id=40')).toEqual({delivery:'suppressed'});
  // The immutable admission applies to direct message dispatch as well.
  await handleMessage(e,job,update.message!,false);
  expect(requests.filter(r=>r.method==='copyMessage')).toEqual([]);
 });
 it('rejects a queued /sc after the binding version changes even when topic and user are unchanged',async()=>{
  await run(e,"INSERT INTO messages(user_id,direction,source_chat,source_id,admin_id,private_id,thread_id,received_at,delivery,payload) VALUES('20001','out',?,50,50,150,40,1,'success','{}')",e.ADMIN_CHAT_ID);
  const {job,update}=await admitted(51,outgoing(51,'/sc',{reply_to_message:outgoing(50,'target')}));
  await run(e,"UPDATE users SET binding_version=3 WHERE tgid='20001'");
  await handleAdmin(e,job,update);
  expect(requests.some(r=>r.method==='deleteMessage')).toBe(false);
  expect(await one(e,'SELECT tombstone,deletion FROM messages WHERE source_id=50')).toEqual({tombstone:0,deletion:'none'});
 });
 it('checks every album member admission across a binding generation change',async()=>{
  await admitted(52,outgoing(52,'album old',{media_group_id:'same-album'}));
  await run(e,"UPDATE users SET binding_version=3 WHERE tgid='20001'");
  await admitted(53,outgoing(53,'album current',{media_group_id:'same-album'}));
  const u=(await user(e,'20001'))!,rows=await all<MessageRow>(e,'SELECT * FROM messages ORDER BY source_id');
  expect(await albumMemberBindingMatches(e,rows[0],u)).toBe(false);
  expect(await albumMemberBindingMatches(e,rows[1],u)).toBe(true);
 });
 it.each(['chat','topic','mapped-topic'] as const)('refuses /sc reference from a different %s',async mismatch=>{
  await run(e,"INSERT INTO messages(user_id,direction,source_chat,source_id,admin_id,private_id,thread_id,received_at,delivery,payload) VALUES('20001','out',?,60,60,160,?,1,'success','{}')",e.ADMIN_CHAT_ID,mismatch==='mapped-topic'?39:40);
  const reply=outgoing(60,'target',mismatch==='chat'?{chat:{id:-100999999,type:'supergroup'}}:mismatch==='topic'?{message_thread_id:41}:{});
  const {job,update}=await admitted(61,outgoing(61,'/sc',{reply_to_message:reply}));
  await deleteSingle(e,job,update.message!,(await user(e,'20001'))!);
  expect(requests.some(r=>r.method==='deleteMessage')).toBe(false);
  expect(await one(e,'SELECT tombstone FROM messages WHERE source_id=60')).toEqual({tombstone:0});
 });
 it('suppresses pre-confirmation backlog while a valid clear snapshot excludes later arrivals',async()=>{
  await accept(e,{update_id:70,message:incoming(70,'old backlog')});
  const token='b'.repeat(32);await action(token,'clear_confirm');
  await accept(e,callback(71,token));await accept(e,{update_id:72,message:incoming(72,'new arrival')});
  await execute(e,(await one<Job>(e,"SELECT * FROM jobs WHERE dedupe='u:70'"))!);
  expect(requests.filter(r=>r.method==='copyMessage')).toEqual([]);
  expect((await all<MessageRow>(e,'SELECT * FROM messages ORDER BY id')).map(r=>[r.source_id,r.tombstone])).toEqual([[70,1],[72,0]]);
 });
});

it('exposes only liveness and public version while all maintenance actions require authentication',async()=>{
 const response=await SELF.fetch('https://test/health');expect(response.status).toBe(200);
 expect(Object.keys(await response.json()).sort()).toEqual(['ok','version']);
 for(const action of ['status','pause','resume','recover']) {
  const denied=await SELF.fetch('https://test/internal/maintenance',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action})});
  expect(denied.status).toBe(403);
  expect(await denied.text()).toBe('Forbidden');
 }
 expect(await one(e,"SELECT value FROM settings WHERE key='paused'")).toBeNull();
});
