import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it,vi} from 'vitest';
import type {Env,Params} from '../src/types';
import {BotCoordinator} from '../src/bot';
import {run} from '../src/db';

const base=env as unknown as Env;
const mutations=['editMessageText','editMessageCaption','editMessageMedia','editMessageReplyMarkup','deleteMessage','unpinChatMessage'];
let clock:number,apiStarts:number[];
function directBot(options:{afterChecks?:()=>void;beforePut?:(entries:Record<string,unknown>)=>void}={}) {
 const values=new Map<string,unknown>();
 const storage={
  get:async(key:string)=>values.get(key),
  put:async(key:string|Record<string,unknown>,value?:unknown)=>{
   const entries=typeof key==='string'?{[key]:value}:key;
   options.beforePut?.(entries);
   for(const [name,item]of Object.entries(entries))values.set(name,item);
  },
  delete:async(key:string)=>values.delete(key),
 };
 const client={...base,DB:new Proxy(base.DB,{get(target,key){
  if(key==='batch')return async(statements:D1PreparedStatement[])=>{const result=await target.batch(statements);options.afterChecks?.();return result;};
  const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
 }})};
 const bot=new BotCoordinator({storage} as unknown as DurableObjectState,client);
 const invoke=(key:string,method:string,params:Params={chat_id:base.ADMIN_CHAT_ID,message_id:1},guard?:unknown)=>bot.fetch(new Request('https://bot/execute',{method:'POST',body:JSON.stringify({key:`${base.RECOVERY_EPOCH}:${key}`,method,params,guard})}));
 return {values,invoke};
}
beforeEach(()=>{
 clock=100000;apiStarts=[];vi.spyOn(Date,'now').mockImplementation(()=>clock);
 vi.mocked(fetch).mockImplementation(async()=>{apiStarts.push(clock);return Response.json({ok:true,result:true});});
});

describe('non-sending Telegram mutation pacing',()=>{
 it.each(mutations)('keeps %s separate from the group send allowance while retaining the global limit',async method=>{
  const {values,invoke}=directBot();
  expect((await invoke('mutation',method)).status).toBe(200);
  expect(values.get(`rate:mutation:${base.ADMIN_CHAT_ID}`)).toBe(103100);
  expect(values.has(`rate:${base.ADMIN_CHAT_ID}`)).toBe(false);
  clock=100039;
  expect((await invoke('global-blocked','sendMessage',{chat_id:20001,text:'Synthetic reply'})).status).toBe(429);
  clock=100050;
  expect((await invoke('send-after-mutation','sendMessage')).status).toBe(200);
  expect(apiStarts).toEqual([100000,100050]);
 });
 it('shares one conservative mutation lane across edits, deletion and unpinning',async()=>{
  const {invoke}=directBot();
  for(const [index,method]of mutations.entries()) {
   if(index>0) {
    clock=100000+(index-1)*3100+50;
    const blocked=await invoke(`mutation-${index}`,method);
    expect(blocked.status).toBe(429);
    expect(await blocked.json()).toEqual({due:100000+index*3100});
   }
   clock=100000+index*3100;
   expect((await invoke(`mutation-${index}`,method)).status).toBe(200);
  }
  expect(apiStarts).toEqual(mutations.map((_,index)=>100000+index*3100));
 });
 it('allows a mutation while the shared group send lane is waiting and keeps private mutations paced',async()=>{
  const {values,invoke}=directBot();
  await invoke('send','sendMessage');clock+=50;
  expect((await invoke('edit','editMessageText')).status).toBe(200);
  expect(values.get(`rate:${base.ADMIN_CHAT_ID}`)).toBe(103100);
  clock+=50;
  expect((await invoke('private-edit','editMessageText',{chat_id:20001,message_id:2})).status).toBe(200);
  expect(values.get('rate:mutation:20001')).toBe(101150);
  clock+=50;
  expect((await invoke('private-delete','deleteMessage',{chat_id:20001,message_id:2})).status).toBe(429);
 });
 it.each(['pinChatMessage','createForumTopic'])('retains %s in the group send lane',async method=>{
  const {values,invoke}=directBot();await invoke('service',method);clock+=50;
  expect((await invoke('send','sendMessage')).status).toBe(429);
  expect(values.get(`rate:${base.ADMIN_CHAT_ID}`)).toBe(103100);
  expect(values.has(`rate:mutation:${base.ADMIN_CHAT_ID}`)).toBe(false);
 });
 it('shares a Telegram retry_after from a mutation across every lane and exempt methods',async()=>{
  const {values,invoke}=directBot();
  vi.mocked(fetch).mockImplementationOnce(async()=>{apiStarts.push(clock);clock+=2500;return Response.json({ok:false,error_code:429,parameters:{retry_after:2}});});
  const limited=await invoke('limited','editMessageText');
  expect(limited.status).toBe(429);expect(await limited.json()).toEqual({due:104500});
  expect(values.get('rate:backoff')).toBe(104500);
  expect(values.get(`rate:mutation:${base.ADMIN_CHAT_ID}`)).toBe(103100);
  clock=104499;
  for(const method of ['sendMessage','deleteMessage','setMessageReaction','getMe','answerCallbackQuery']) {
   const response=await invoke(`blocked-${method}`,method);
   expect(response.status).toBe(429);expect(await response.json()).toEqual({due:104500});
  }
  expect(apiStarts).toHaveLength(1);
  clock=104500;expect((await invoke('read','getMe',{})).status).toBe(200);
 });
 it('does not reserve either rate lane when the final binding guard cancels a mutation',async()=>{
  await run(base,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,40,2)");
  const {values,invoke}=directBot();
  const response=await invoke('stale','editMessageText',undefined,{threadVersion:{userId:'20001',version:1,threadId:40}});
  expect(await response.json()).toMatchObject({status:'failed',reason:'cancelled_by_state'});
  expect([...values.keys()].some(key=>key.startsWith('rate:'))).toBe(false);
  expect(apiStarts).toEqual([]);
 });
});

describe('rate deadlines use the real API start after slow guards and storage',()=>{
 it.each([
  {method:'sendMessage',lane:`rate:${base.ADMIN_CHAT_ID}`,gap:3100},
  {method:'editMessageText',lane:`rate:mutation:${base.ADMIN_CHAT_ID}`,gap:3100},
  {method:'setMessageReaction',lane:`rate:reaction:${base.ADMIN_CHAT_ID}`,gap:1050},
 ])('preserves the $method interval after a slow final database check',async({method,lane,gap})=>{
  let first=true;
  const {values,invoke}=directBot({afterChecks:()=>{if(first){clock+=5000;first=false;}}});
  await invoke('first',method);
  expect(apiStarts).toEqual([105000]);expect(values.get(lane)).toBe(105000+gap);
  clock+=50;
  expect((await invoke('next',method)).status).toBe(429);
  clock=105000+gap;expect((await invoke('next',method)).status).toBe(200);
  expect(apiStarts[1]-apiStarts[0]).toBe(gap);
 });
 it.each(['reservation','issued-journal'] as const)('preserves the interval after a slow %s write using bounded combined journal writes',async delayed=>{
  let first=true,writes=0;
  const {values,invoke}=directBot({beforePut:entries=>{
   writes++;
   const selected=delayed==='reservation'?Object.hasOwn(entries,'rate:global'):Object.values(entries).some(value=>(value as {phase?:string})?.phase==='issued');
   if(first&&selected){clock+=5000;first=false;}
  }});
  await invoke('first','sendMessage');
  expect(apiStarts).toEqual([105000]);
  expect(values.get(`rate:${base.ADMIN_CHAT_ID}`)).toBe(108100);
  expect(writes).toBe(2);
  clock+=50;expect((await invoke('next','sendMessage')).status).toBe(429);
  clock=108100;expect((await invoke('next','sendMessage')).status).toBe(200);
  expect(apiStarts[1]-apiStarts[0]).toBe(3100);
 });
});
