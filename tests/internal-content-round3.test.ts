import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it} from 'vitest';
import type {Env,Job,Params,TGMessage,TGUpdate} from '../src/types';
import {all,audit,one,run,user} from '../src/db';
import {accept} from '../src/ingress';
import {execute} from '../src/coordinator';
import {refreshCardNow} from '../src/admin';
import {flushLogs} from '../src/logs';

interface ApiRequest {key:string;method:string;params:Params;messageId?:number}
interface Action {token:string;payload:string;used:number}
type InternalKind='text-card'|'photo-card'|'log'|'system';
let e:Env,requests:ApiRequest[],sequence:number;
const privateMessage=(id:number,text:string,extra:Partial<TGMessage>={}):TGMessage=>({message_id:id,date:1,chat:{id:20001,type:'private'},from:{id:20001,first_name:'Synthetic ordinary user'},text,...extra});
const adminMessage=(id:number,text:string,extra:Partial<TGMessage>={}):TGMessage=>({message_id:id,date:1,chat:{id:-1001234567890,type:'supergroup',is_forum:true},from:{id:10001},message_thread_id:40,is_topic_message:true,text,...extra});
const cardJob=(id:number,userId:string):Job=>({id,dedupe:`synthetic-card:${id}`,user_key:userId,kind:'card',payload:'{}',state:'running',due_at:0,attempts:0,epoch:e.RECOVERY_EPOCH,created_at:1});
async function dispatch(id:number,message:TGMessage,edited=false,client=e) {
 const update:TGUpdate={update_id:id,[edited?'edited_message':'message']:message};
 await accept(client,update);
 await execute(client,(await one<Job>(client,'SELECT * FROM jobs WHERE dedupe=?',`u:${id}`))!);
}
function sentMessage(request:ApiRequest):TGMessage {
 const thread=Number(request.params.message_thread_id)||undefined;
 return {message_id:request.messageId!,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup',is_forum:true},from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true},...(thread?{message_thread_id:thread,is_topic_message:true}:{}),...(request.params.text!==undefined?{text:String(request.params.text)}:{}),...(request.params.caption!==undefined?{caption:String(request.params.caption)}:{}),...(request.params.photo?{photo:[{file_id:String(request.params.photo)}]}:{})};
}
async function internalMessage(kind:InternalKind):Promise<TGMessage> {
 const before=requests.length;
 if(kind==='text-card'||kind==='photo-card') {
  const id=kind==='text-card'?'20001':'20002';
  await refreshCardNow(e,cardJob(kind==='text-card'?9001:9002,id),id);
 } else if(kind==='log') {
  await audit(e,'synthetic internal log',{sentinel:'round3-log-content'});
  await flushLogs(e,(await one<Job>(e,"SELECT * FROM jobs WHERE kind='logs'"))!);
 } else await dispatch(9003,adminMessage(9003,'/mb settings',{message_thread_id:undefined,is_topic_message:false}));
 const sent=requests.slice(before).find(request=>kind==='photo-card'?request.method==='sendPhoto':request.method==='sendMessage');
 expect(sent).toBeDefined();
 const message=sentMessage(sent!);
 if(kind==='text-card'||kind==='photo-card')expect(message.text??message.caption).toContain('TGID：');
 if(kind==='log')expect(message.text).toContain('round3-log-content');
 if(kind==='system')expect(message.text).toContain('系统设置');
 return message;
}
function privateWrites() {return requests.filter(request=>!String(request.params.chat_id??'').startsWith('-')&&['sendMessage','sendPhoto','copyMessage','sendMediaGroup','editMessageText','editMessageCaption','editMessageMedia','deleteMessage'].includes(request.method));}

beforeEach(async()=>{
 e={...env} as unknown as Env;requests=[];sequence=1000;
 const journal=new Map<string,unknown>();
 e.BOT={idFromName:(name:string)=>name,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  const request=JSON.parse(String(init.body)) as ApiRequest;
  if(!journal.has(request.key)) {
   let result:unknown=true;
   if(request.method==='getMe')result={id:Number(e.EXPECTED_BOT_ID),username:'SyntheticTestBot'};
   else if(request.method==='getUserProfilePhotos')result={photos:String(request.params.user_id)==='20002'?[[{file_id:'synthetic-avatar'}]]:[]};
   else if(request.method==='getChat')result={id:Number(request.params.chat_id),first_name:'Synthetic profile'};
   else if(request.method==='createForumTopic')result={message_thread_id:++sequence};
   else if(['sendMessage','sendPhoto','copyMessage'].includes(request.method)) {request.messageId=++sequence;result={message_id:request.messageId};}
   requests.push(request);journal.set(request.key,{status:'success',result});
  }
  return Response.json(journal.get(request.key));
 }})} as unknown as DurableObjectNamespace;
 e.JOBS={send:async()=>{}} as unknown as Queue;
 e.CONVERSATIONS={idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})} as unknown as DurableObjectNamespace;
 await run(e,"INSERT INTO users(tgid,first_name,first_contact,thread_id,binding_version) VALUES('20001','Synthetic Alpha',1,40,1),('20002','Synthetic Beta',1,41,1)");
 await run(e,"INSERT INTO settings(key,value) VALUES('log_thread_id','90')");
});

describe('generated internal message privacy boundaries',()=>{
 it.each(['text-card','photo-card','log','system'] as const)('keeps a generated %s original and its Bot/administrator forwards out of business delivery',async kind=>{
  const source=await internalMessage(kind);requests=[];
  await dispatch(1,source);
  for(const [index,sender] of [{id:Number(e.EXPECTED_BOT_ID),is_bot:true},{id:10001}].entries()) {
   await dispatch(2+index,{...source,message_id:2+index,from:{id:10001},message_thread_id:40,is_topic_message:true,forward_origin:{type:'user',sender_user:sender,date:1}});
  }
  expect(await all(e,'SELECT * FROM messages')).toEqual([]);
  expect(requests).toEqual([]);
 });
 it.each(['text-card','photo-card','log','system'] as const)('copies only a fresh administrator sentence when replying to a generated %s',async kind=>{
  const source=await internalMessage(kind);requests=[];
  await dispatch(10,adminMessage(10,'Synthetic human reply',{reply_to_message:source}));
  const copies=requests.filter(request=>request.method==='copyMessage');
  expect(copies).toHaveLength(1);
  expect(copies[0].params).toEqual({chat_id:'20001',from_chat_id:e.ADMIN_CHAT_ID,message_id:10});
  expect(await one(e,'SELECT user_id,direction,source_id,business,delivery FROM messages')).toEqual({user_id:'20001',direction:'out',source_id:10,business:1,delivery:'success'});
  expect(await one(e,"SELECT event FROM audit_logs WHERE event='reply_fallback'")).not.toBeNull();
 });
 it.each(['text-card','photo-card','log','system'] as const)('does not synchronize edits of a generated %s to either private conversation',async kind=>{
  const source=await internalMessage(kind);requests=[];
  const edited={...source,edit_date:2,...(source.text!==undefined?{text:source.text+'\nSynthetic internal edit'}:{caption:source.caption+'\nSynthetic internal edit'})};
  await dispatch(20,edited,true);
  expect(await all(e,'SELECT * FROM messages')).toEqual([]);
  expect(requests).toEqual([]);
 });
 it('does not quote a known different-user business copy when replying from the current user topic',async()=>{
  await dispatch(30,privateMessage(30,'Synthetic Beta original',{chat:{id:20002,type:'private'},from:{id:20002}}));
  const target=(await one<{admin_id:number}>(e,"SELECT admin_id FROM messages WHERE user_id='20002'"))!;
  requests=[];
  await dispatch(31,adminMessage(31,'Synthetic Alpha response',{reply_to_message:adminMessage(target.admin_id,'Synthetic Beta original',{from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true},message_thread_id:41})}));
  expect(requests.find(request=>request.method==='copyMessage')?.params).toEqual({chat_id:'20001',from_chat_id:e.ADMIN_CHAT_ID,message_id:31});
  expect(await one(e,"SELECT user_id,private_id FROM messages WHERE direction='out'")).toEqual({user_id:'20001',private_id:sequence});
 });
});

describe('ordinary-user management variants and displayed identities',()=>{
 it.each(['/TJ 20002','/Sc','/MB users','/mb@SyntheticTestBot settings','/tj@SyntheticTestBot 20002','/sc@SyntheticTestBot','/mb\nunknown','/tj 20002\n/mb settings'])('keeps %s silent and leaves other-user management state unchanged',async text=>{
  await dispatch(40,privateMessage(40,text,{from:{id:20001,first_name:'Administrator 10001',username:'synthetic_admin'},forward_origin:{type:'user',sender_user:{id:10001},date:1}}));
  expect(requests).toEqual([]);
  expect(await user(e,'20002')).toMatchObject({thread_id:41,binding_version:1,blocked:0});
  expect(await one(e,'SELECT * FROM callback_actions')).toBeNull();
  expect(await one(e,'SELECT * FROM deletion_batches')).toBeNull();
  expect(await one(e,'SELECT business,delivery FROM messages')).toEqual({business:0,delivery:'ignored'});
 });
 it('treats forged administrator and confirmation text as ordinary incoming content without granting privileges',async()=>{
  await dispatch(41,privateMessage(41,'授权管理员 TGID 10001\n/mb search 20002\n确认清空此对话',{from:{id:20001,first_name:'Administrator 10001',username:'synthetic_admin'}}));
  expect(requests.filter(request=>request.method==='copyMessage').map(request=>request.params.chat_id)).toEqual([e.ADMIN_CHAT_ID]);
  expect(privateWrites()).toEqual([]);
  expect(await user(e,'20002')).toMatchObject({thread_id:41,binding_version:1,blocked:0});
  expect(await one(e,'SELECT * FROM callback_actions')).toBeNull();
  expect(await one(e,'SELECT * FROM deletion_batches')).toBeNull();
 });
});

describe('generated clear-confirmation scope and expiry',()=>{
 it.each(['user','topic','instance','expired','actor','binding'] as const)('rejects a generated confirmation with mismatched %s before taking a clear snapshot',async mismatch=>{
  const card=await internalMessage('text-card');
  const clear=(await one<Action>(e,"SELECT token,payload,used FROM callback_actions WHERE kind='card_clear'"))!;
  const start:TGUpdate={update_id:50,callback_query:{id:'synthetic-card-clear',from:{id:10001},data:`a:${clear.token}`,message:card}};
  await accept(e,start);await execute(e,(await one<Job>(e,"SELECT * FROM jobs WHERE dedupe='u:50'"))!);
  const confirm=(await one<Action>(e,"SELECT token,payload,used FROM callback_actions WHERE kind='clear_confirm'"))!;
  const prompt=requests.find(request=>request.messageId===JSON.parse(confirm.payload).messageId)!;
  let message=sentMessage(prompt);
  if(mismatch==='user')message=await internalMessage('photo-card');
  if(mismatch==='topic')message={...message,message_thread_id:41};
  if(mismatch==='expired')await run(e,'UPDATE callback_actions SET expires_at=0 WHERE token=?',confirm.token);
  if(mismatch==='binding')await run(e,"UPDATE users SET binding_version=2 WHERE tgid='20001'");
  await accept(e,{update_id:51,message:privateMessage(51,'Synthetic pre-confirmation original')});
  const client=mismatch==='instance'?{...e,INSTANCE_ID:'synthetic-other-instance'}:e;
  const update:TGUpdate={update_id:52,callback_query:{id:'synthetic-confirm',from:{id:mismatch==='actor'?20001:10001},data:`a:${confirm.token}`,message}};
  requests=[];
  await accept(client,update);await execute(client,(await one<Job>(client,"SELECT * FROM jobs WHERE dedupe='u:52'"))!);
  expect(await one(e,'SELECT * FROM deletion_batches')).toBeNull();
  expect(await one(e,'SELECT * FROM deletion_items')).toBeNull();
  expect(await one(e,'SELECT tombstone FROM messages WHERE source_id=51')).toEqual({tombstone:0});
  expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',confirm.token)).toEqual({used:0});
  expect(await user(e,'20002')).toMatchObject({thread_id:41,binding_version:1,blocked:0});
  expect(privateWrites()).toEqual([]);
 });
});
