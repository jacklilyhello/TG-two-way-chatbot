import {env} from 'cloudflare:test';
import {describe,it,expect,vi} from 'vitest';
import {ensureUser} from '../src/admin';
import {accept} from '../src/ingress';
import {ConversationCoordinator} from '../src/coordinator';
import {all,nextWork,one,run,setSetting} from '../src/db';
import type {Env,Params,TGMessage} from '../src/types';

const e=env as unknown as Env;
function countedDatabase() {
 const calls:{kind:string;sql:string[]}[]=[];
 const statements=new WeakMap<object,{original:D1PreparedStatement;sql:string}>();
 const wrap=(original:D1PreparedStatement,sql:string):D1PreparedStatement=>{
  const statement=new Proxy(original,{get(target,property){
   if(property==='bind')return (...args:unknown[])=>wrap(target.bind(...args),sql);
   const value=Reflect.get(target,property);
   if(typeof value==='function')return (...args:unknown[])=>{
    calls.push({kind:String(property),sql:[sql]});return value.apply(target,args);
   };
   return value;
  }});
  statements.set(statement,{original,sql});return statement;
 };
 const DB={prepare:(sql:string)=>wrap(e.DB.prepare(sql),sql),batch:async(items:D1PreparedStatement[])=>{
  calls.push({kind:'batch',sql:items.map(item=>statements.get(item)!.sql)});
  return e.DB.batch(items.map(item=>statements.get(item)!.original));
 }} as D1Database;
 return {client:{...e,DB},calls};
}

describe('fewer database round trips without changing durable ordering',()=>{
 it('returns refreshed profile and existing binding from one durable transaction',async()=>{
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,40,2)");
  const {client,calls}=countedDatabase();
  expect(await ensureUser(client,'20001',{id:20001,first_name:'Updated',username:'updated_name'},999)).toMatchObject({first_name:'Updated',username:'updated_name',first_contact:1,thread_id:40,binding_version:2});
  expect(calls).toHaveLength(1);expect(calls[0].kind).toBe('batch');
  calls.length=0;
  expect(await ensureUser(client,'20001',{id:20002,first_name:'Wrong identity'})).toMatchObject({first_name:'Updated',username:'updated_name',thread_id:40});
  expect(calls).toHaveLength(1);
 });
 it('reads pause and the earliest durable job together without jumping a delayed predecessor',async()=>{
  await run(e,"INSERT INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES('earlier','20001','update','{}','retry',9999999999999,'initial',1),('later','20001','update','{}','pending',0,'initial',2)");
  await setSetting(e,'paused','1');
  const {client,calls}=countedDatabase();
  const work=await nextWork(client,'20001');
  expect(work.paused).toBe(true);expect(work.job?.dedupe).toBe('earlier');
  expect(calls).toHaveLength(1);expect(calls[0].kind).toBe('batch');
 });
 it('finishes FIFO content while committed card wake hints are slow, then recovers a waiting edit',async()=>{
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,40,1)");
  const message=(id:number,text:string):TGMessage=>({message_id:id,date:1,chat:{id:20001,type:'private'},from:{id:20001,first_name:'Test'},text});
  await accept(e,{update_id:100,edited_message:{...message(11,'edited'),edit_date:2}});
  await accept(e,{update_id:101,message:message(11,'original')});
  await accept(e,{update_id:102,message:message(12,'next')});
  let finish!:()=>void;
  const gate=new Promise<void>(resolve=>{finish=resolve;});
  const pending:Promise<unknown>[]=[];
  const requests:{method:string;params:Params}[]=[];
  const values=new Map<string,unknown>();
  const {client,calls}=countedDatabase();
  const runtime={...client,
   JOBS:{send:async()=>gate},
   CONVERSATIONS:{idFromName:(key:string)=>key,get:()=>({fetch:async()=>{await gate;return new Response('ok');}})},
   BOT:{idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
    const {method,params}=JSON.parse(String(init.body));requests.push({method,params});
    return Response.json({status:'success',result:method==='copyMessage'?{message_id:Number(params.message_id)+1000}:true});
   }})}
  } as unknown as Env;
  const state={storage:{get:async(key:string)=>values.get(key),put:async(key:string,value:unknown)=>{values.set(key,value);},setAlarm:async()=>{}},waitUntil:(promise:Promise<unknown>)=>{pending.push(promise);}} as unknown as DurableObjectState;
  const coordinator=new ConversationCoordinator(state,runtime);
  try {
   const response=await coordinator.fetch(new Request('https://conversation/wake',{method:'POST',body:JSON.stringify({key:'20001'})}));
   expect(response.ok).toBe(true);
   expect(requests.filter(r=>r.method==='copyMessage').map(r=>r.params.message_id)).toEqual([11,12]);
   expect(requests.map(r=>r.method)).toEqual(['copyMessage','editMessageText','copyMessage']);
   expect(await all(e,"SELECT state FROM jobs WHERE user_key='20001' ORDER BY id")).toEqual([{state:'done'},{state:'done'},{state:'done'}]);
   expect(await one(e,"SELECT state FROM jobs WHERE kind='card'")).toEqual({state:'pending'});
   expect(pending).toHaveLength(2);
   // Each processed private update refreshes its user once, inside a batch.
   // Existing bindings never take the separate ensureThread lookup path.
   const profileReads=calls.filter(c=>c.sql.some(sql=>sql==='SELECT * FROM users WHERE tgid=?'));
   expect(profileReads).toHaveLength(4);expect(profileReads.every(c=>c.kind==='batch')).toBe(true);
   expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  } finally {finish();await Promise.all(pending);}
 });
});
