import {env} from 'cloudflare:test';
import {describe,expect,it,vi} from 'vitest';
import type {Env,Params} from '../src/types';
import {BotCoordinator} from '../src/bot';
import {broadcastGuardValid} from '../src/broadcast-guard';
import {prepareBroadcast,type BroadcastGuard} from '../src/broadcast-delivery';
import {run} from '../src/db';

const base=env as unknown as Env;
const guard:BroadcastGuard={id:'synthetic-broadcast',userId:'20001',part:0};
async function setup(e=base,text='Synthetic broadcast',photo:string|null=null) {
 await run(e,"INSERT INTO users(tgid,first_contact,thread_id) VALUES('20001',1,20)");
 await prepareBroadcast(e,{id:guard.id,actor:'10001',text,photo});
 await run(e,"UPDATE broadcasts SET state='sending' WHERE id=?",guard.id);
 return e;
}
function bot(e=base) {
 const values=new Map<string,unknown>();
 const storage={get:async(key:string)=>values.get(key),put:async(key:string|Record<string,unknown>,value?:unknown)=>{
  for(const [k,v]of Object.entries(typeof key==='string'?{[key]:value}:key))values.set(k,v);
 },delete:async(key:string)=>values.delete(key)};
 const instance=new BotCoordinator({storage} as unknown as DurableObjectState,e);
 const invoke=(key:string,method:string,params:Params,g?:BroadcastGuard)=>instance.fetch(new Request('https://bot/execute',{method:'POST',body:JSON.stringify({key:`${e.RECOVERY_EPOCH}:${key}`,method,params,...(g?{guard:{broadcast:g}}:{})})}));
 return {instance,invoke,values};
}

describe('broadcast issue guards and scheduling',()=>{
 it('checks frozen content, target and absence of internal controls at the final API boundary',async()=>{
  await setup(base,'Complete caption','synthetic-photo');
  const params={chat_id:'20001',photo:'synthetic-photo',caption:'Complete caption'};
  expect(await broadcastGuardValid(base,guard,'sendPhoto',params)).toBe(true);
  for(const wrong of [{...params,chat_id:'20002'},{...params,caption:'changed'},{...params,reply_markup:{inline_keyboard:[]}},{...params,message_thread_id:1}])expect(await broadcastGuardValid(base,guard,'sendPhoto',wrong)).toBe(false);
  expect(await broadcastGuardValid(base,guard,'sendMessage',params)).toBe(false);
  expect(await broadcastGuardValid(base,{...guard,part:1},'sendPhoto',params)).toBe(false);
  expect(await broadcastGuardValid({...base,INSTANCE_ID:'other'},guard,'sendPhoto',params)).toBe(false);
  expect(await broadcastGuardValid({...base,RECOVERY_EPOCH:'other'},guard,'sendPhoto',params)).toBe(false);
  expect(await broadcastGuardValid({...base,ADMIN_IDS:'10002'},guard,'sendPhoto',params)).toBe(false);
 });
 it.each(['blocked','unbound','canceled','already-sent'])('prevents an API call after the target becomes %s',async condition=>{
  await setup();
  if(condition==='blocked')await run(base,"UPDATE users SET blocked=1 WHERE tgid='20001'");
  if(condition==='unbound')await run(base,"UPDATE users SET thread_id=NULL WHERE tgid='20001'");
  if(condition==='canceled')await run(base,"UPDATE broadcasts SET state='canceled'");
  if(condition==='already-sent')await run(base,"UPDATE broadcast_recipients SET state='success'");
  const {invoke}=bot();
  expect(await (await invoke('guarded','sendMessage',{chat_id:'20001',text:'Synthetic broadcast'},guard)).json()).toMatchObject({status:'failed',code:409});
  expect(fetch).not.toHaveBeenCalled();
 });
 it('fails closed if the private test scope is absent, malformed or no longer includes the recipient',async()=>{
  const e={...base,INSTANCE_ID:'test-a',BROADCAST_TEST_RECIPIENT_IDS:'20001'};
  await setup(e);
  const params={chat_id:'20001',text:'Synthetic broadcast'};
  expect(await broadcastGuardValid(e,guard,'sendMessage',params)).toBe(true);
  for(const scope of [undefined,'disabled','20002','20001,invalid'])expect(await broadcastGuardValid({...e,BROADCAST_TEST_RECIPIENT_IDS:scope},guard,'sendMessage',params)).toBe(false);
 });
 it('does not issue to an injected recipient outside the frozen directed audience',async()=>{
  await setup();
  await run(base,"UPDATE broadcasts SET scope_json=?",JSON.stringify({mode:'directed',ids:['20001']}));
  await run(base,"INSERT INTO users(tgid,first_contact,thread_id) VALUES('20002',1,21)");
  await run(base,'INSERT INTO broadcast_recipients(broadcast_id,user_id,updated_at) VALUES(?,?,1)',guard.id,'20002');
  expect(await broadcastGuardValid(base,guard,'sendMessage',{chat_id:'20001',text:'Synthetic broadcast'})).toBe(true);
  const {invoke}=bot();
  expect(await (await invoke('outside-directed','sendMessage',{chat_id:'20002',text:'Synthetic broadcast'},{...guard,userId:'20002'})).json()).toMatchObject({status:'failed',code:409});
  expect(fetch).not.toHaveBeenCalled();
 });
 it.each(['not-json','null','{}','[]','{"mode":"invalid"}','{"mode":"directed","ids":[]}','{"mode":"directed","ids":["abc"]}','{"mode":"directed","ids":["20002"]}'])('fails closed rather than treating damaged or mismatched scope %s as all users',async scope=>{
  await setup();await run(base,'UPDATE broadcasts SET scope_json=?',scope);
  const {invoke}=bot();
  expect(await (await invoke('damaged-scope','sendMessage',{chat_id:'20001',text:'Synthetic broadcast'},guard)).json()).toMatchObject({status:'failed',code:409});
  expect(fetch).not.toHaveBeenCalled();
 });
 it('does not let a slow broadcast network request block ordinary chat or callback acknowledgements',async()=>{
  await setup();let clock=100000;
  vi.spyOn(Date,'now').mockImplementation(()=>clock);
  let release!:()=>void,entered!:()=>void;
  const started=new Promise<void>(resolve=>{entered=resolve;});
  const gate=new Promise<void>(resolve=>{release=resolve;});
  const methods:string[]=[];
  vi.mocked(fetch).mockImplementation(async(url)=>{
   const method=String(url).split('/').at(-1)!;methods.push(method);
   if(methods.length===1){entered();await gate;}
   return Response.json({ok:true,result:{message_id:70}});
  });
  const {invoke}=bot();
  const pending=invoke('slow','sendMessage',{chat_id:'20001',text:'Synthetic broadcast'},guard);
  await started;clock+=2000;
  expect((await invoke('chat','copyMessage',{chat_id:'20002',from_chat_id:base.ADMIN_CHAT_ID,message_id:8})).status).toBe(200);
  expect((await invoke('ack','answerCallbackQuery',{callback_query_id:'synthetic'})).status).toBe(200);
  expect(methods).toEqual(['sendMessage','copyMessage','answerCallbackQuery']);
  release();expect((await pending).status).toBe(200);
 });
 it('shares a conservative broadcast allowance between campaigns without consuming the ordinary chat allowance',async()=>{
  await setup();let clock=100000;
  vi.spyOn(Date,'now').mockImplementation(()=>clock);
  vi.mocked(fetch).mockResolvedValue(Response.json({ok:true,result:{message_id:70}}));
  const {invoke,values}=bot();
  await invoke('first','sendMessage',{chat_id:'20001',text:'Synthetic broadcast'},guard);
  expect(values.get('rate:broadcast')).toBe(101050);
  const blocked=await invoke('second','sendMessage',{chat_id:'20001',text:'Synthetic broadcast'},guard);
  expect(blocked.status).toBe(429);
  clock+=50;
  expect((await invoke('business','copyMessage',{chat_id:'20002',from_chat_id:base.ADMIN_CHAT_ID,message_id:8})).status).toBe(200);
  expect(fetch).toHaveBeenCalledTimes(2);
 });
 it('waits for an active broadcast before maintenance reconciles its journal',async()=>{
  await setup();let release!:()=>void,entered!:()=>void;
  const started=new Promise<void>(resolve=>{entered=resolve;});
  const gate=new Promise<void>(resolve=>{release=resolve;});
  vi.mocked(fetch).mockImplementation(async()=>{entered();await gate;return Response.json({ok:true,result:{message_id:70}});});
  const {invoke,instance}=bot();
  const send=invoke('active','sendMessage',{chat_id:'20001',text:'Synthetic broadcast'},guard);
  await started;
  const reconciliation=instance.fetch(new Request('https://bot/journal',{method:'POST',body:JSON.stringify({key:`${base.RECOVERY_EPOCH}:active`})}));
  release();await send;
  expect(await (await reconciliation).json()).toMatchObject({status:'success',result:{message_id:70}});
 });
 it('lets a clear recover an active broadcast through the ordinary lane without sealing it unknown',async()=>{
  await setup();let release!:()=>void,entered!:()=>void;
  const started=new Promise<void>(resolve=>{entered=resolve;});
  const gate=new Promise<void>(resolve=>{release=resolve;});
  vi.mocked(fetch).mockImplementation(async()=>{entered();await gate;return Response.json({ok:true,result:{message_id:70}});});
  const {invoke}=bot();
  const send=invoke('clearing','sendMessage',{chat_id:'20001',text:'Synthetic broadcast'},guard);
  await started;
  const recovery=invoke('clearing','sendMessage',{});
  release();await send;
  expect(await (await recovery).json()).toMatchObject({status:'success',result:{message_id:70}});
  expect(fetch).toHaveBeenCalledTimes(1);
 });
});
