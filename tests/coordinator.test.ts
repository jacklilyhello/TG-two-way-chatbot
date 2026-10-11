import {env,evictDurableObject} from 'cloudflare:test';
import {describe,it,expect,vi} from 'vitest';
import type {Env} from '../src/types';
import {accept} from '../src/ingress';
import {all,run,one,setSetting} from '../src/db';
import {recover} from '../src/index';
const e=env as unknown as Env;
describe('durable scheduling',()=>{
 it('drains persisted user tasks in receive order across duplicate wakes and reconstruction',async()=>{
  await run(e,"INSERT INTO users(tgid,first_contact,blocked) VALUES('42',1,1)");
  for(const id of [100,99,103])await accept(e,{update_id:id,message:{message_id:id,date:1,chat:{id:42,type:'private'},from:{id:42},text:'blocked-message'}});
  const stub=e.CONVERSATIONS.get(e.CONVERSATIONS.idFromName('42'));
  const wake=async()=>{const r=await stub.fetch('https://conversation/wake',{method:'POST',body:JSON.stringify({key:'42'})});return r.text();};
  await Promise.all([wake(),wake()]);await evictDurableObject(stub);await wake();
  const jobs=await all<{dedupe:string;state:string}>(e,"SELECT dedupe,state FROM jobs WHERE user_key='42' ORDER BY id");
  expect(jobs).toEqual([{dedupe:'u:100',state:'done'},{dedupe:'u:99',state:'done'},{dedupe:'u:103',state:'done'}]);
  expect((await all<{delivery:string}>(e,'SELECT delivery FROM messages')).map(m=>m.delivery)).toEqual(['blocked','blocked','blocked']);
 });
 it('quarantines old restore generations while re-notifying only current durable work',async()=>{
  await run(e,"INSERT INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES('old','42','update','{}','pending',0,'old',1),('new','43','update','{}','pending',0,'initial',1)");
  const notifications:unknown[]=[];
  await recover({...e,JOBS:{send:async(body:unknown)=>{notifications.push(body);}}} as Env);
  expect(await all(e,"SELECT state FROM jobs WHERE dedupe='old'")).toEqual([{state:'quarantined'}]);
  expect(notifications).toEqual([{key:'43'},{key:'system'},{key:'system'}]);
 });
 it('recovers a current-generation original after a registered binding becomes available',async()=>{
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES('42',1,77,1,99)");
  await accept(e,{update_id:501,message:{message_id:501,date:1,chat:{id:42,type:'private'},from:{id:42},text:'recover current original'}});
  // Earlier topic setup returned without sending; its original job has ended.
  await run(e,"UPDATE jobs SET state='done' WHERE dedupe='u:501'");
  const source=await one<{id:number;epoch:string}>(e,'SELECT id,epoch FROM messages WHERE source_id=501');
  expect(source?.epoch).toBe(e.RECOVERY_EPOCH);
  await setSetting(e,'card_state:42:1',JSON.stringify([99,'','',null,1,null,0,'Asia/Singapore']));
  const notifications:unknown[]=[];
  await recover({...e,JOBS:{send:async(body:unknown)=>{notifications.push(body);}}} as Env);
  expect(notifications).toEqual([{key:'42'},{key:'system'},{key:'system'}]);
  vi.mocked(fetch).mockImplementation(async request=>{
   const path=new URL(String(request)).pathname;
   if(path.endsWith('/getUserProfilePhotos'))return Response.json({ok:true,result:{photos:[]}});
   if(path.endsWith('/copyMessage'))return Response.json({ok:true,result:{message_id:701}});
   throw new Error('Unexpected outbound method');
  });
  const stub=e.CONVERSATIONS.get(e.CONVERSATIONS.idFromName('42'));
  const response=await stub.fetch('https://conversation/wake',{method:'POST',body:JSON.stringify({key:'42'})});await response.text();
  expect(await one(e,'SELECT delivery,admin_id FROM messages WHERE source_id=501')).toEqual({delivery:'success',admin_id:701});
  expect(await one(e,'SELECT state,epoch FROM jobs WHERE dedupe=?',`route:${source!.id}:1`)).toEqual({state:'done',epoch:e.RECOVERY_EPOCH});
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
  expect(await one(e,"SELECT state FROM jobs WHERE kind='card' AND user_key='card:42'")).toEqual({state:'pending'});
 });
 it('never promotes an old-generation pending mapping gap into a new send',async()=>{
  await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('42',1,77,1)");
  await accept({...e,RECOVERY_EPOCH:'before-restore'},{update_id:502,message:{message_id:502,date:1,chat:{id:42,type:'private'},from:{id:42},text:'possibly already sent'}});
  const source=await one<{id:number;epoch:string}>(e,'SELECT id,epoch FROM messages WHERE source_id=502');
  expect(source?.epoch).toBe('before-restore');
  await run(e,"INSERT INTO operations(op_key,method,phase,created_at,updated_at) VALUES(?,'copyMessage','issued',1,1)",`before-restore:send:${source!.id}`);
  const notifications:unknown[]=[];
  await recover({...e,JOBS:{send:async(body:unknown)=>{notifications.push(body);}}} as Env);
  expect(notifications).toEqual([{key:'system'},{key:'system'}]);
  expect(await one(e,"SELECT state FROM jobs WHERE dedupe='u:502'")).toEqual({state:'quarantined'});
  expect(await all(e,"SELECT id FROM jobs WHERE dedupe LIKE 'route:%'")).toEqual([]);
  // A duplicate queue wake after DO reconstruction must still find no eligible
  // task. No Telegram interceptor is registered: any external call fails QA.
  const stub=e.CONVERSATIONS.get(e.CONVERSATIONS.idFromName('42'));
  const response=await stub.fetch('https://conversation/wake',{method:'POST',body:JSON.stringify({key:'42'})});await response.text();
  expect(await one(e,'SELECT delivery,epoch FROM messages WHERE source_id=502')).toEqual({delivery:'pending',epoch:'before-restore'});
 });
});
