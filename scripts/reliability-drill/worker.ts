/** Isolated drill entrypoint. Never deploy this bundle with business bindings or
 * Telegram credentials. It has no network fallback, even on injection failure. */
import {AsyncLocalStorage} from 'node:async_hooks';
import production, {recover} from '../../src/index';
import {BotCoordinator as ProductionBot} from '../../src/bot';
import {call,RetryLater} from '../../src/operations';
import {one,run,setSetting} from '../../src/db';
import type {Env,Params} from '../../src/types';
export {ConversationCoordinator} from '../../src/coordinator';

interface DrillEnv extends Env {
 STUB:DurableObjectNamespace; DRILL_KEY:string; DRILL_RUN_ID:string;
 DRILL_HOSTNAME:string; DRILL_REVISION:string;
}
interface Context {env:DrillEnv;state:DurableObjectState;key:string}
const scope=new AsyncLocalStorage<Context>();
const token='0:reliability-drill-only';
const source=-1004242424242,target=42424242;
const params:Params={chat_id:target,from_chat_id:source,message_id:1};
const scenarios=new Set(['rate429','completion','interrupted','unknown']);
const path='/__reliability-drill';
function check(e:DrillEnv) {
 if(!['test-a','test-b'].includes(e.INSTANCE_ID)||e.TELEGRAM_BOT_TOKEN!==token||e.ADMIN_CHAT_ID!==String(source)||e.ADMIN_IDS!=='42424240'||e.EXPECTED_BOT_ID!=='42424240'||e.RECOVERY_EPOCH!==`drill-${e.DRILL_RUN_ID}`||!/^\d{1,20}-\d{1,3}$/.test(e.DRILL_RUN_ID))throw new Error('drill_binding_rejected');
}
function stub(e:DrillEnv) {return e.STUB.get(e.STUB.idFromName(e.DRILL_RUN_ID));}
async function count(e:DrillEnv,key:string) {
 const r=await stub(e).fetch('https://stub/count',{method:'POST',body:JSON.stringify({key:`${e.RECOVERY_EPOCH}:${key}`})});
 return (await r.json<{count:number}>()).count;
}
// This mutation is deliberately confined to this separate entrypoint. The
// production bundle never imports it, and no native fetch is retained.
globalThis.fetch=(async(input:RequestInfo|URL,init?:RequestInit)=>{
 const c=scope.getStore();if(!c)throw new Error('drill_external_network_disabled');
 check(c.env);
 if(typeof input!=='string'||typeof init?.body!=='string')throw new Error('drill_external_target_rejected');
 const url=new URL(input);
 if(url.origin!=='https://api.telegram.org'||url.pathname!==`/bot${token}/copyMessage`||init.method!=='POST')throw new Error('drill_external_target_rejected');
 const p=JSON.parse(init.body) as Params;
 if(p.chat_id!==target||p.from_chat_id!==source||p.message_id!==1||Object.keys(p).length!==3)throw new Error('drill_payload_rejected');
 const response=await stub(c.env).fetch('https://stub/api',{method:'POST',body:JSON.stringify({key:c.key,method:'copyMessage'})});
 if(c.key===`${c.env.RECOVERY_EPOCH}:interrupted`&&response.ok) {
  // The production issued marker and the separate stub effect are both already
  // durable. abort() discards this object's memory before it can journal done.
  c.state.abort('controlled reliability drill interruption');
 }
 return response;
}) as typeof fetch;

export class DrillBotCoordinator {
 private production:ProductionBot;
 private generation=crypto.randomUUID();
 constructor(private state:DurableObjectState,private env:DrillEnv) {check(env);this.production=new ProductionBot(state,env);}
 async fetch(request:Request) {
  const route=new URL(request.url).pathname;
  if(route==='/drill/generation')return Response.json({generation:this.generation});
  if(route==='/drill/reset'){this.state.abort('controlled reliability drill reconstruction');return new Response('unreachable');}
  const body=await request.text(),{key}=JSON.parse(body) as {key:string};
  if(typeof key!=='string'||!scenarios.has(key.slice(`${this.env.RECOVERY_EPOCH}:`.length)))return new Response('invalid',{status:400});
  return scope.run({env:this.env,state:this.state,key},()=>this.production.fetch(new Request(request.url,{method:request.method,body})));
 }
}

/** A persistent stand-in for Telegram, never a real Telegram transport. */
export class DrillApiStub {
 constructor(private state:DurableObjectState,private env:DrillEnv) {check(env);}
 async fetch(request:Request) {
  const {key,method}=await request.json<{key:string;method?:string}>();
  const scenario=key?.slice(`${this.env.RECOVERY_EPOCH}:`.length);
  if(!key?.startsWith(`${this.env.RECOVERY_EPOCH}:`)||!scenarios.has(scenario))return new Response('invalid',{status:400});
  const previous=await this.state.storage.get<number>(key)??0;
  if(new URL(request.url).pathname==='/count')return Response.json({count:previous});
  if(method!=='copyMessage')return new Response('invalid',{status:400});
  await this.state.storage.put(key,previous+1);
  if(scenario==='rate429'&&previous===0)return Response.json({ok:false,error_code:429,parameters:{retry_after:1}});
  if(scenario==='unknown')return new Response('synthetic response lost after effect',{status:502});
  return Response.json({ok:true,result:{message_id:700001}});
 }
}

function webhook(e:DrillEnv,id:number,user=target) {
 return new Request(`https://${e.DRILL_HOSTNAME}/webhook`,{method:'POST',headers:{'Content-Type':'application/json','X-Telegram-Bot-Api-Secret-Token':e.TELEGRAM_WEBHOOK_SECRET},body:JSON.stringify({update_id:id,message:{message_id:id,date:1,chat:{id:user,type:'private'},from:{id:user},text:'synthetic reliability fixture'}})});
}
function faultDatabase(db:D1Database,stage:'prepare'|'batch'):D1Database {
 return new Proxy(db,{get(object,key){if(key===stage)return ()=>{throw new Error(`controlled_${stage}_failure`);};const value=Reflect.get(object,key);return typeof value==='function'?value.bind(object):value;}});
}
async function operation(e:DrillEnv,key:string,completionFault=false) {
 try {
  const result=await call(completionFault?{...e,DB:faultDatabase(e.DB,'batch')}:e,key,'copyMessage',params);
  return {result,count:await count(e,key),phase:(await one<{phase:string}>(e,'SELECT phase FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:${key}`))?.phase};
 } catch(error) {
  if(error instanceof RetryLater)return {deferred:true,due:error.due,count:await count(e,key),phase:(await one<{phase:string}>(e,'SELECT phase FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:${key}`))?.phase};
  if(completionFault&&error instanceof Error&&error.message==='controlled_batch_failure')return {completionFailed:true,count:await count(e,key),phase:(await one<{phase:string}>(e,'SELECT phase FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:${key}`))?.phase};
  throw error;
 }
}
function bot(e:DrillEnv) {return e.BOT.get(e.BOT.idFromName(e.INSTANCE_ID));}
async function action(e:DrillEnv,command:string) {
 if(command==='setup') {
  await run(e,'INSERT OR IGNORE INTO users(tgid,first_contact,blocked) VALUES(?,1,1)',String(target));
  await run(e,'INSERT OR IGNORE INTO users(tgid,first_contact,blocked) VALUES(?,1,1)',String(target+1));
  await setSetting(e,'last_version',e.VERSION);await setSetting(e,'log_topic_unknown','1');
  return {ready:true};
 }
 if(command==='ingress-d1') {
  const first=await production.fetch(webhook(e,880001),{...e,DB:faultDatabase(e.DB,'prepare')});await first.text();
  const before=await one<{n:number}>(e,'SELECT COUNT(*) n FROM inbox_updates WHERE update_id=880001');
  const retry=await production.fetch(webhook(e,880001),e);await retry.text();
  const duplicate=await production.fetch(webhook(e,880001),e);await duplicate.text();
  return {failedStatus:first.status,persistedBeforeRetry:before?.n,retryStatus:retry.status,duplicateStatus:duplicate.status,inbox:(await one<{n:number}>(e,'SELECT COUNT(*) n FROM inbox_updates WHERE update_id=880001'))?.n,messages:(await one<{n:number}>(e,'SELECT COUNT(*) n FROM messages WHERE source_id=880001'))?.n};
 }
 if(command==='queue-failure') {
  let notifications=0;
  const broken={...e,JOBS:new Proxy(e.JOBS,{get(queue,key){if(key==='send')return async()=>{notifications++;throw new Error('controlled_queue_notification_failure');};const value=Reflect.get(queue,key);return typeof value==='function'?value.bind(queue):value;}})};
  const response=await production.fetch(webhook(e,880002,target+1),broken);await response.text();
  return {accepted:response.status,failedNotifications:notifications,job:(await one(e,"SELECT state FROM jobs WHERE dedupe='u:880002'")),message:(await one(e,'SELECT delivery FROM messages WHERE source_id=880002'))};
 }
 if(command==='recover') {await recover(e);return {notified:true};}
 if(command==='queue-status')return {job:await one(e,"SELECT state FROM jobs WHERE dedupe='u:880002'"),message:await one(e,'SELECT delivery FROM messages WHERE source_id=880002')};
 if(command==='generation')return bot(e).fetch('https://bot/drill/generation').then(r=>r.json());
 if(command==='reset') {try{const r=await bot(e).fetch('https://bot/drill/reset');await r.text();}catch{/* reset intentionally aborts the response */}return {resetRequested:true};}
 if(command==='rate429')return operation(e,'rate429');
 if(command==='completion-fail')return operation(e,'completion',true);
 if(command==='completion-recover')return operation(e,'completion');
 if(command==='interrupted')return operation(e,'interrupted');
 if(command==='unknown')return operation(e,'unknown');
 if(command==='verify') {
  const phases:Record<string,unknown>={};
  for(const key of scenarios)phases[key]={...(await one<{phase:string}>(e,'SELECT phase FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:${key}`)),count:await count(e,key)};
  return {run:e.DRILL_RUN_ID,version:e.VERSION,revision:e.DRILL_REVISION,phases,queue:await one(e,"SELECT state FROM jobs WHERE dedupe='u:880002'"),inbox:(await one<{n:number}>(e,'SELECT COUNT(*) n FROM inbox_updates'))?.n};
 }
 throw new Error('invalid_drill_action');
}
export default {
 async fetch(request:Request,e:DrillEnv):Promise<Response> {
  try{check(e);}catch{return new Response('Unavailable',{status:503});}
  const url=new URL(request.url);
  if(url.hostname!==e.DRILL_HOSTNAME||url.pathname!==path||request.method!=='POST')return new Response('Not found',{status:404});
  if(request.headers.get('Authorization')!==`Bearer ${e.DRILL_KEY}`||!e.DRILL_KEY||e.DRILL_KEY.length<40)return new Response('Forbidden',{status:403});
  if(Number(request.headers.get('Content-Length'))>256)return new Response('Too large',{status:413});
  const body=await request.text();if(body.length>256)return new Response('Too large',{status:413});
  try{const input=JSON.parse(body);if(typeof input.action!=='string')return new Response('Invalid',{status:400});return Response.json(await action(e,input.action));}catch{return new Response('Controlled drill failed',{status:500});}
 },
 queue:production.queue,
} satisfies ExportedHandler<DrillEnv,{key:string}>;
