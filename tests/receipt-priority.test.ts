import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it,vi} from 'vitest';
import type {Env,Job} from '../src/types';
import {one,run} from '../src/db';
import {BotCoordinator} from '../src/bot';
import {handleReaction,syncReceipt} from '../src/receipts';
import {RetryLater} from '../src/operations';

let e:Env,clock:number,values:Map<string,unknown>;
async function receipt(adminId:number,delivery='success',deletion='none') {
 await run(e,"INSERT INTO messages(user_id,direction,source_chat,source_id,admin_id,private_id,thread_id,received_at,delivery,deletion,tombstone,payload) VALUES('20001','out',?,?,?,?,40,1,?,?,?,'{}')",e.ADMIN_CHAT_ID,adminId,adminId,delivery==='success'?adminId+100:null,delivery,deletion,deletion==='deleted'?1:0);
 const row=(await one<{id:number}>(e,'SELECT id FROM messages WHERE admin_id=?',adminId))!;
 await syncReceipt(e,row.id);
 return (await one<Job>(e,"SELECT * FROM jobs WHERE user_key=? AND json_extract(payload,'$.action')='apply'",`receipt:${adminId}`))!;
}
async function visibleSuccess() {
 await handleReaction(e,await receipt(10));
 const clear=(await one<Job>(e,"SELECT * FROM jobs WHERE user_key='receipt:10' AND json_extract(payload,'$.action')='clear'"))!;
 clock+=10000;
 return clear;
}
beforeEach(()=>{
 e={...env} as unknown as Env;clock=100000;values=new Map();
 vi.spyOn(Date,'now').mockImplementation(()=>clock);
 vi.mocked(fetch).mockImplementation(async()=>Response.json({ok:true,result:true}));
 e.JOBS={send:async()=>{}} as unknown as Queue;
 e.CONVERSATIONS={idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})} as unknown as DurableObjectNamespace;
 const storage={get:async(key:string)=>values.get(key),put:async(key:string|Record<string,unknown>,value?:unknown)=>{if(typeof key==='string')values.set(key,value);else for(const [k,v]of Object.entries(key))values.set(k,v);},delete:async(key:string)=>values.delete(key)};
 const bot=new BotCoordinator({storage} as unknown as DurableObjectState,e);
 e.BOT={idFromName:()=>e.INSTANCE_ID,get:()=>({fetch:(url:string,init:RequestInit)=>bot.fetch(new Request(url,init))})} as unknown as DurableObjectNamespace;
});

describe('bounded priority for due success-receipt cleanup',()=>{
 it('yields a new success marker to ready cleanup without issuing Telegram or reserving rate slots',async()=>{
  const clear=await visibleSuccess(),apply=await receipt(11);
  const rates=[values.get('rate:global'),values.get(`rate:reaction:${e.ADMIN_CHAT_ID}`)];
  await expect(handleReaction(e,apply)).rejects.toBeInstanceOf(RetryLater);
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
  expect([values.get('rate:global'),values.get(`rate:reaction:${e.ADMIN_CHAT_ID}`)]).toEqual(rates);
  expect(values.has('op:initial:receipt:11:1:apply:1')).toBe(false);
  expect(await one(e,"SELECT phase FROM operations WHERE op_key='initial:receipt:11:1:apply:1'")).toEqual({phase:'prepared'});
  await handleReaction(e,clear);
  clock+=1050;await handleReaction(e,apply);
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(3);
  expect(await one(e,'SELECT phase FROM receipts WHERE admin_id=10')).toEqual({phase:'cleared'});
  expect(await one(e,'SELECT phase FROM receipts WHERE admin_id=11')).toEqual({phase:'visible'});
 });
 it.each([
  ['failure','failed','none','😢'],
  ['deletion','success','deleted','👎'],
 ])('does not delay a %s marker behind the cleanup priority lease',async(_name,delivery,deletion,emoji)=>{
  await visibleSuccess();await handleReaction(e,await receipt(11,delivery,deletion));
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(2);
  const params=JSON.parse(String(vi.mocked(fetch).mock.calls[1][1]?.body));
  expect(params.reaction).toEqual([{type:'emoji',emoji}]);
 });
 it.each([
  ['future retry',"UPDATE jobs SET state='retry',due_at=110001 WHERE user_key='receipt:10' AND json_extract(payload,'$.action')='clear'"],
  ['attention job',"UPDATE jobs SET state='attention' WHERE user_key='receipt:10' AND json_extract(payload,'$.action')='clear'"],
  ['obsolete job version',"UPDATE jobs SET payload=json_set(payload,'$.version',0) WHERE user_key='receipt:10' AND json_extract(payload,'$.action')='clear'"],
  ['changed source state',"UPDATE messages SET tombstone=1 WHERE admin_id=10"],
  ['old recovery epoch',"UPDATE receipts SET epoch='previous' WHERE admin_id=10"],
  ['expired readiness lease',"UPDATE jobs SET due_at=104999 WHERE user_key='receipt:10' AND json_extract(payload,'$.action')='clear'"],
 ])('does not starve new success markers on %s',async(_name,sql)=>{
  await visibleSuccess();await run(e,sql);
  await handleReaction(e,await receipt(11));
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(2);
  expect(await one(e,'SELECT phase FROM receipts WHERE admin_id=11')).toEqual({phase:'visible'});
 });
});
