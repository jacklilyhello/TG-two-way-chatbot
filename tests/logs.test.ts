import {env} from 'cloudflare:test';
import {describe,expect,it} from 'vitest';
import type {Env,Job,Params} from '../src/types';
import {audit,one,run,setSetting,setting} from '../src/db';
import {flushLogs} from '../src/logs';

const base=env as unknown as Env;
const job=(e:Env)=>one<Job>(e,"SELECT * FROM jobs WHERE kind='logs'").then(j=>j!);
interface Request {key:string;method:string;params:Params}
function client(onCreate:()=>Promise<void>) {
 const requests:Request[]=[];
 const e={...base,BOT:{idFromName:()=>base.INSTANCE_ID,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  const request=JSON.parse(init.body as string) as Request;requests.push(request);
  if(request.method==='getMe')return Response.json({status:'success',result:{id:Number(base.EXPECTED_BOT_ID)}});
  if(request.method==='createForumTopic'){await onCreate();return Response.json({status:'success',result:{message_thread_id:400}});}
  return Response.json({status:'success',result:{message_id:900}});
 }})} as unknown as DurableObjectNamespace};
 return {e,requests};
}

describe('automatic log topic reservation',()=>{
 it('preserves a concurrent explicit log topic choice and flushes into that topic',async()=>{
  const {e,requests}=client(()=>setSetting(base,'log_thread_id','300'));
  await audit(e,'测试事件',{});
  await flushLogs(e,await job(e));
  expect(await setting(e,'log_thread_id')).toBe('300');
  expect(await setting(e,'log_topic_attempt')).toBe('');
  expect(requests.filter(r=>r.method==='createForumTopic')).toHaveLength(1);
  expect(requests.find(r=>r.method==='sendMessage')?.params.message_thread_id).toBe(300);
  expect(await one(e,"SELECT reported FROM audit_logs WHERE event='测试事件'")).toEqual({reported:1});
 });

 it('preserves user binding and audits an occupied created topic without reserving it',async()=>{
  const {e,requests}=client(async()=>{
   await run(base,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('42',1,400,1)");
   await run(base,"INSERT INTO thread_history(user_id,thread_id,version,created_at) VALUES('42',400,1,1)");
  });
  await audit(e,'测试事件',{});
  await flushLogs(e,await job(e));
  expect(await setting(e,'log_thread_id')).toBe('');
  expect(await setting(e,'log_topic_attempt')).toBe('1');
  expect(await one(e,"SELECT thread_id FROM users WHERE tgid='42'")).toEqual({thread_id:400});
  expect(await one(e,"SELECT event FROM audit_logs WHERE event='日志话题已被绑定；保留管理员历史'")).not.toBeNull();
  expect(requests.some(r=>r.method==='sendMessage')).toBe(false);
 });

 it('uses a concurrent administrator choice while still auditing a creation/binding collision',async()=>{
  const {e,requests}=client(async()=>{
   await run(base,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('42',1,400,1)");
   await setSetting(base,'log_thread_id','300');
  });
  await audit(e,'测试事件',{});
  await flushLogs(e,await job(e));
  expect(await setting(e,'log_thread_id')).toBe('300');
  expect(await setting(e,'log_topic_attempt')).toBe('');
  expect(await one(e,"SELECT event FROM audit_logs WHERE event='日志话题已被绑定；保留管理员历史'")).not.toBeNull();
  expect(requests.find(r=>r.method==='sendMessage')?.params.message_thread_id).toBe(300);
 });
});
