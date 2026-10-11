import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it,vi} from 'vitest';
import type {Env,Job,MessageRow,Params,TGMessage,TGUpdate} from '../src/types';
import {all,one,run,user} from '../src/db';
import {accept} from '../src/ingress';
import {execute} from '../src/coordinator';
import {clearConversation,deleteSingle,handleDeliveryNotice,handleDeliveryRetry,handleReaction} from '../src/messaging';
import {deliveryRetryGuardValid,type RetryAction} from '../src/delivery-retry';
import {receiptGuardValid} from '../src/receipts';
import type {IssueGuard,Outcome} from '../src/operations';

interface Request {key:string;method:string;params:Params;guard?:IssueGuard}
let e:Env,requests:Request[],clock:number,sequence:number;
let respond:(request:Request)=>Outcome|Promise<Outcome>;
const original=(extra:Partial<TGMessage>={}):TGMessage=>({message_id:100,date:1,chat:{id:-1001234567890,type:'supergroup'},from:{id:10001},message_thread_id:77,is_topic_message:true,text:'original failed business message',...extra});
async function admitted(update:TGUpdate) {await accept(e,update);return (await one<Job>(e,'SELECT * FROM jobs WHERE dedupe=?',`u:${update.update_id}`))!;}
async function notice() {
 const j=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='delivery-notice' ORDER BY id DESC LIMIT 1"))!;
 await handleDeliveryNotice(e,j);
 return one<RetryAction>(e,"SELECT * FROM callback_actions WHERE kind='delivery_retry' AND used=0 ORDER BY rowid DESC LIMIT 1");
}
async function failure(extra:Partial<TGMessage>={}) {
 const m=original(extra),job=await admitted({update_id:1,message:m});
 if(m.media_group_id)clock+=1500;
 await execute(e,job);
 return {m,row:(await one<MessageRow>(e,'SELECT * FROM messages WHERE source_chat=? AND source_id=100',e.ADMIN_CHAT_ID))!,action:await notice()};
}
async function reserve(action:RetryAction,updateId=10) {
 const p=JSON.parse(action.payload) as {messageId:number};
 const update:TGUpdate={update_id:updateId,callback_query:{id:`callback-${updateId}`,from:{id:10001},data:`a:${action.token}`,message:{...original(),message_id:p.messageId,from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true}}}};
 await execute(e,await admitted(update));
 return (await one<Job>(e,"SELECT * FROM jobs WHERE kind='delivery-retry' ORDER BY id DESC LIMIT 1"))!;
}
async function receipt(action='apply') {
 const j=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='reaction' AND json_extract(payload,'$.action')=? ORDER BY id DESC LIMIT 1",action))!;
 await handleReaction(e,j);
}

beforeEach(async()=>{
 e={...env} as unknown as Env;requests=[];clock=100000;sequence=800;
 vi.spyOn(Date,'now').mockImplementation(()=>clock);
 respond=request=>request.method==='copyMessage'?{status:'failed',code:403,reason:'Forbidden: bot was blocked by the user'}:{status:'success',result:request.method==='sendMessage'?{message_id:++sequence}:true,confirmedAt:clock};
 const journal=new Map<string,Outcome>();
 e.BOT={idFromName:(name:string)=>name,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  const request=JSON.parse(String(init.body)) as Request;
  if(!journal.has(request.key)) {
   const allowed=(!request.guard?.deliveryRetry||await deliveryRetryGuardValid(e,request.guard.deliveryRetry))&&(!request.guard?.receipt||await receiptGuardValid(e,request.guard.receipt));
   if(!allowed)journal.set(request.key,{status:'failed',code:409,reason:'cancelled_by_state'});
   else {requests.push(request);journal.set(request.key,await respond(request));}
  }
  return Response.json(journal.get(request.key));
 }})} as unknown as DurableObjectNamespace;
 e.JOBS={send:async()=>{}} as unknown as Queue;
 e.CONVERSATIONS={idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})} as unknown as DurableObjectNamespace;
 await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES('42',1,77,1,99)");
});

describe('explicitly rejected delivery retries',()=>{
 it.each([{text:'original failed business message'},{text:undefined,photo:[{file_id:'synthetic-photo'}]}])('moves the same original from failure to confirmed success without increasing business count: %j',async extra=>{
  const {row,action}=await failure(extra);
  await receipt();clock+=20000;
  expect(await one(e,'SELECT emoji,clear_after FROM receipts WHERE row_id=?',row.id)).toEqual({emoji:'😢',clear_after:null});
  const retry=await reserve(action!);
  const normal=respond;
  respond=request=>request.method==='copyMessage'?{status:'success',result:{message_id:900}}:normal(request);
  await handleDeliveryRetry(e,retry);await handleDeliveryRetry(e,retry);
  await receipt();
  expect(await one(e,'SELECT source_id,admin_id,private_id,delivery FROM messages WHERE id=?',row.id)).toEqual({source_id:100,admin_id:100,private_id:900,delivery:'success'});
  expect(await one(e,'SELECT COUNT(*) n FROM messages WHERE business=1')).toEqual({n:1});
  expect(await one(e,'SELECT phase,code FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:send:${row.id}`)).toEqual({phase:'failed',code:403});
  expect(requests.filter(r=>r.method==='copyMessage')).toHaveLength(2);
  clock+=10000;await receipt('clear');
  expect(requests.filter(r=>r.method==='setMessageReaction').map(r=>r.params.reaction)).toEqual([[{type:'emoji',emoji:'😢'}],[{type:'emoji',emoji:'👍'}],[]]);
 });

 it('allows a new explicit rejection to be retried once and never lets its old job overwrite the later success',async()=>{
  const {action,row}=await failure(),first=await reserve(action!);
  await handleDeliveryRetry(e,first);
  const secondAction=(await notice())!;
  expect(secondAction.token).not.toBe(action!.token);
  const second=await reserve(secondAction,11);
  const normal=respond;respond=r=>r.method==='copyMessage'?{status:'success',result:{message_id:901}}:normal(r);
  await handleDeliveryRetry(e,second);
  await handleDeliveryRetry(e,first);await handleDeliveryRetry(e,second);
  expect(await one(e,'SELECT delivery,private_id FROM messages WHERE id=?',row.id)).toEqual({delivery:'success',private_id:901});
  expect((await all<{state:string}>(e,'SELECT state FROM delivery_attempts ORDER BY attempt')).map(r=>r.state)).toEqual(['failed','success']);
  expect(requests.filter(r=>r.method==='copyMessage')).toHaveLength(3);
 });

 it('seals an unknown retry result and refuses all further retry buttons or replayed sends',async()=>{
  const {action,row}=await failure(),retry=await reserve(action!);
  const normal=respond;respond=r=>r.method==='copyMessage'?{status:'unknown',reason:'connection lost'}:normal(r);
  await handleDeliveryRetry(e,retry);await handleDeliveryRetry(e,retry);
  expect(await notice()).toBeNull();
  await reserve(action!,11);
  expect(await all(e,'SELECT attempt FROM delivery_attempts')).toEqual([{attempt:1}]);
  expect(await one(e,'SELECT delivery,private_id FROM messages WHERE id=?',row.id)).toEqual({delivery:'unknown',private_id:null});
  expect(requests.filter(r=>r.method==='copyMessage')).toHaveLength(2);
 });

 it('never offers single-message retry for an album member',async()=>{
  const {action}=await failure({media_group_id:'album-failure',text:undefined,photo:[{file_id:'synthetic-album-photo'}]});
  expect(action).toBeNull();
  expect(await all(e,'SELECT * FROM delivery_attempts')).toEqual([]);
 });

 it.each(['retry','delete'] as const)('recovers a successful retry fallback target after a mapping-write interruption during %s recovery',async recovery=>{
  await run(e,"INSERT INTO messages(user_id,direction,source_chat,source_id,admin_id,private_id,thread_id,received_at,delivery,payload) VALUES('42','in','42',50,500,50,77,0,'success','{}')");
  const {action,row,m}=await failure({reply_to_message:{...original(),message_id:500}}),retry=await reserve(action!);
  const normal=respond;
  respond=r=>r.method==='copyMessage'?r.key.endsWith(':without-reply')?{status:'success',result:{message_id:902}}:{status:'failed',code:400,reason:'message to be replied not found'}:normal(r);
  const db=e.DB;let fail=true;
  function wrap(statement:D1PreparedStatement,sql:string):D1PreparedStatement {
   return new Proxy(statement,{get(target,property){
    if(property==='bind')return (...args:unknown[])=>wrap(target.bind(...args),sql);
    if(property==='run')return async()=>{if(fail&&sql.startsWith('UPDATE messages SET delivery=?,admin_id=')){fail=false;throw new Error('mapping write interrupted');}return target.run();};
    const value=Reflect.get(target,property);return typeof value==='function'?value.bind(target):value;
   }});
  }
  e.DB=new Proxy(db,{get(target,property){if(property==='prepare')return (sql:string)=>wrap(target.prepare(sql),sql);const value=Reflect.get(target,property);return typeof value==='function'?value.bind(target):value;}});
  await expect(handleDeliveryRetry(e,retry)).rejects.toThrow('mapping write interrupted');
  await run(e,"UPDATE messages SET tombstone=1,deletion='deleted' WHERE direction='in' AND source_id=50");
  if(recovery==='delete') {
   const command={...original(),message_id:101,text:'/sc',reply_to_message:m};
   const deleteJob=await admitted({update_id:12,message:command});
   await deleteSingle(e,deleteJob,command,(await user(e,'42'))!);
  }
  await handleDeliveryRetry(e,retry);
  expect(await one(e,'SELECT private_id,delivery,deletion,tombstone FROM messages WHERE id=?',row.id)).toEqual({private_id:902,delivery:'success',deletion:recovery==='delete'?'deleted':'none',tombstone:recovery==='delete'?1:0});
  expect(requests.filter(r=>r.method==='copyMessage')).toHaveLength(3);
  expect(requests.filter(r=>r.method==='deleteMessage').map(r=>r.params.message_id)).toEqual(recovery==='delete'?[902]:[]);
 });

 it('keeps a clear snapshot authoritative when it arrives after reservation and before retry execution',async()=>{
  const {action,row}=await failure(),retry=await reserve(action!);
  await run(e,"INSERT INTO deletion_batches(id,user_id,boundary,created_at) VALUES('clear-after-retry','42',?,?)",row.id,clock);
  await run(e,"INSERT INTO deletion_items(batch_id,message_id) VALUES('clear-after-retry',?)",row.id);
  await run(e,'UPDATE messages SET tombstone=1 WHERE id=?',row.id);
  await handleDeliveryRetry(e,retry);
  await clearConversation(e,retry,(await user(e,'42'))!,'clear-after-retry');
  expect(requests.filter(r=>r.method==='copyMessage')).toHaveLength(1);
  expect(await one(e,'SELECT deletion FROM messages WHERE id=?',row.id)).toEqual({deletion:'unavailable'});
  expect(await one(e,'SELECT state FROM delivery_attempts WHERE message_id=?',row.id)).toEqual({state:'suppressed'});
 });
});
