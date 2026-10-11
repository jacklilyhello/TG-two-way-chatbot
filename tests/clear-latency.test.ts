import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it,vi} from 'vitest';
import type {Env,Job,Params,TGUpdate} from '../src/types';
import {BotCoordinator} from '../src/bot';
import {ConversationCoordinator} from '../src/coordinator';
import {accept} from '../src/ingress';
import {all,enqueue,one,run,stmt,user} from '../src/db';
import {clearConversation} from '../src/messaging';

let e:Env,clock:number;
let requests:{method:string;params:Params;at:number}[];
let respond:(method:string,params:Params)=>Promise<Response>;
const batchId='bounded-latency';
beforeEach(async()=>{
 e={...env} as unknown as Env;clock=100000;requests=[];
 vi.spyOn(Date,'now').mockImplementation(()=>clock);
 e.JOBS={send:async()=>{}} as unknown as Queue;
 e.CONVERSATIONS={idFromName:(key:string)=>key,get:()=>({fetch:async()=>new Response('ok')})} as unknown as DurableObjectNamespace;
 await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES('20001',1,40,1,500)");
 respond=async(method,params)=>{
  if(method==='deleteMessage')clock+=2500;
  return Response.json({ok:true,result:['copyMessage','sendMessage'].includes(method)?{message_id:Number(params.message_id??10000)+1000}:true});
 };
 vi.mocked(fetch).mockImplementation(async(input,init)=>{
  const method=new URL(String(input)).pathname.split('/').at(-1)!,params=JSON.parse(String(init?.body));
  requests.push({method,params,at:clock});return respond(method,params);
 });
});

async function snapshot(count=3) {
 for(let id=1;id<=count;id++)await run(e,"INSERT INTO messages(user_id,direction,source_chat,source_id,private_id,received_at,payload) VALUES('20001','in','20001',?,?,1,'{}')",id,id);
 await e.DB.batch([
  stmt(e,"INSERT INTO deletion_batches(id,user_id,boundary,created_at) SELECT ?,'20001',MAX(id),? FROM messages WHERE user_id='20001'",batchId,clock),
  stmt(e,"INSERT INTO deletion_items(batch_id,message_id) SELECT ?,id FROM messages WHERE user_id='20001'",batchId),
  stmt(e,"UPDATE messages SET tombstone=1 WHERE user_id='20001'")
 ]);
 await enqueue(e,'20001','clear',{batchId},`clear:${batchId}`);
 return (await one<Job>(e,"SELECT * FROM jobs WHERE kind='clear' ORDER BY id LIMIT 1"))!;
}
function bot() {
 const values=new Map<string,unknown>();
 const storage={get:async(key:string)=>values.get(key),put:async(key:string|Record<string,unknown>,value?:unknown)=>{if(typeof key==='string')values.set(key,value);else for(const [k,v]of Object.entries(key))values.set(k,v);},delete:async(key:string)=>values.delete(key)};
 const coordinator=new BotCoordinator({storage} as unknown as DurableObjectState,e);
 e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:(url:string,init:RequestInit)=>coordinator.fetch(new Request(url,init))})} as unknown as DurableObjectNamespace;
}
function conversation() {
 const values=new Map<string,unknown>(),pending:Promise<unknown>[]=[];
 const state={storage:{get:async(key:string)=>values.get(key),put:async(key:string,value:unknown)=>{values.set(key,value);},setAlarm:async()=>{}},waitUntil:(promise:Promise<unknown>)=>{pending.push(promise);}} as unknown as DurableObjectState;
 const coordinator=new ConversationCoordinator(state,e);
 return {coordinator,pending,wake:()=>coordinator.fetch(new Request('https://conversation/wake',{method:'POST',body:JSON.stringify({key:'20001'})}))};
}

describe('bounded clear turns preserve the snapshot while yielding the user queue',()=>{
 it('answers a queued valid callback and delivers a later message after one slow completed deletion',async()=>{
  await snapshot();bot();
  const target='a'.repeat(32),token='b'.repeat(32);
  await run(e,"INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) VALUES(?,'bind_confirm','20001','10001',40,1,?,?)",target,JSON.stringify({instance:e.INSTANCE_ID,messageId:501,to:41}),clock+60000);
  await run(e,"INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) VALUES(?,'cancel','20001','10001',40,1,?,?)",token,JSON.stringify({instance:e.INSTANCE_ID,messageId:501,target}),clock+60000);
  const callback:TGUpdate={update_id:100,callback_query:{id:'valid-during-clear',from:{id:10001},data:`a:${token}`,message:{message_id:501,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup'},message_thread_id:40,is_topic_message:true,from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true}}}};
  const normal=respond;let admittedAt:number|null=null;
  respond=async(method,params)=>{
   if(method==='deleteMessage'&&admittedAt===null){
    admittedAt=clock;
    await accept(e,callback);
    await accept(e,{update_id:101,message:{message_id:1000,date:1,chat:{id:20001,type:'private'},from:{id:20001},text:'after snapshot'}});
   }
   if(method==='answerCallbackQuery'){
    expect(clock-admittedAt!).toBe(2500);
    expect(await one(e,"SELECT dedupe FROM jobs WHERE kind='clear' AND state='pending' ORDER BY id LIMIT 1")).toEqual({dedupe:`clear:${batchId}:after:1`});
   }
   return normal(method,params);
  };
  const {wake,pending}=conversation();await wake();await Promise.all(pending);
  // The newly delivered message may reserve the real Bot's global rate slot.
  // Resume its durable alarm only when that slot is eligible again.
  for(let turn=0;turn<5&&(await one<{state:string}>(e,'SELECT state FROM deletion_batches WHERE id=?',batchId))?.state!=='done';turn++){clock+=1050;await wake();}
  expect(requests.filter(r=>['deleteMessage','answerCallbackQuery','copyMessage'].includes(r.method)).map(r=>r.method==='deleteMessage'?`delete:${r.params.message_id}`:r.method)).toEqual(['delete:1','answerCallbackQuery','copyMessage','delete:2','delete:3']);
  expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',target)).toEqual({used:-1});
  expect(await one(e,"SELECT delivery,tombstone,deletion FROM messages WHERE source_id=1000")).toEqual({delivery:'success',tombstone:0,deletion:'none'});
  expect(await one(e,'SELECT state FROM deletion_batches WHERE id=?',batchId)).toEqual({state:'done'});
  expect(await all(e,'SELECT result FROM deletion_items ORDER BY message_id')).toEqual([{result:'deleted'},{result:'deleted'},{result:'deleted'}]);
  await wake();
  expect(requests.filter(r=>r.method==='deleteMessage')).toHaveLength(3);
 });

 it.each(['item-result','continuation'] as const)('recovers interruption at %s without repeating an unknown delete or dropping the final selected item',async fault=>{
  const first=await snapshot();
  const original=e.DB;let interrupted=false;
  const wrap=(statement:D1PreparedStatement,sql:string,values:unknown[]=[]):D1PreparedStatement=>new Proxy(statement,{get(target,key){
   if(key==='bind')return(...args:unknown[])=>wrap(target.bind(...args),sql,args);
   if(key==='run')return async()=>{
    const selected=fault==='item-result'?sql.startsWith('UPDATE deletion_items SET result='):sql.startsWith('INSERT OR IGNORE INTO jobs')&&String(values[0]).startsWith(`clear:${batchId}:after:`);
    if(selected&&!interrupted){interrupted=true;throw Error('Synthetic clear persistence interruption');}
    return target.run();
   };
   const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});
  e.DB={prepare:(sql:string)=>wrap(original.prepare(sql),sql),batch:original.batch.bind(original)} as D1Database;
  const normal=respond;
  respond=async(method,params)=>{
   if(method==='deleteMessage'&&params.message_id===1){clock+=2500;return Response.json({ok:false,error_code:500});}
   return normal(method,params);
  };
  bot();
  await expect(clearConversation(e,first,(await user(e,'20001'))!,batchId)).rejects.toThrow('Synthetic clear persistence interruption');
  expect(await one(e,'SELECT deletion FROM messages WHERE source_id=1')).toEqual({deletion:'unknown'});
  expect((await all(e,'SELECT tombstone FROM messages')).every(row=>row.tombstone===1)).toBe(true);
  await clearConversation(e,first,(await user(e,'20001'))!,batchId);
  for(let round=0;round<5;round++){
   const next=await one<Job>(e,"SELECT * FROM jobs WHERE kind='clear' AND dedupe!=? AND state='pending' ORDER BY id LIMIT 1",first.dedupe);
   if(!next)break;
   await clearConversation(e,next,(await user(e,'20001'))!,batchId);
   await run(e,"UPDATE jobs SET state='done' WHERE id=?",next.id);
  }
  expect(requests.filter(r=>r.method==='deleteMessage').map(r=>r.params.message_id)).toEqual([1,2,3]);
  expect(await all(e,'SELECT result FROM deletion_items ORDER BY message_id')).toEqual([{result:'unknown'},{result:'deleted'},{result:'deleted'}]);
  expect(await one(e,'SELECT state FROM deletion_batches WHERE id=?',batchId)).toEqual({state:'done'});
  await clearConversation(e,first,(await user(e,'20001'))!,batchId);
  expect(requests.filter(r=>r.method==='deleteMessage')).toHaveLength(3);
 });
});
