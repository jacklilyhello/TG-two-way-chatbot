import {env} from 'cloudflare:test';
import {describe,expect,it} from 'vitest';
import type {Env,Job,Params,TGUpdate} from '../src/types';
import {handleAdmin} from '../src/admin';
import {accept} from '../src/ingress';
import {all,one,run,setSetting,setting} from '../src/db';
import {RetryLater} from '../src/operations';

const base=env as unknown as Env;
const cases=[
 {view:'logtopic',key:'log_thread_id',initial:'300',value:'400',later:'401',event:'log_topic_changed',suffix:'logtopic:ok'},
 {view:'timezone',key:'timezone',initial:'Asia/Singapore',value:'Asia/Tokyo',later:'Europe/London',event:'setting_changed',suffix:'timezone:ok'},
 {view:'start',key:'start_text',initial:'Old welcome',value:'New welcome {name}',later:'Latest welcome {name}',event:'setting_changed',suffix:'template:ok'},
 {view:'help',key:'help_text',initial:'Old help',value:'New help {tgid}',later:'Latest help {tgid}',event:'setting_changed',suffix:'template:ok'}
];
interface Request {key:string;method:string;params:Params}
function client(onRequest:(request:Request)=>Response|Promise<Response>) {
 const requests:Request[]=[];
 const e={...base,JOBS:{send:async()=>{}},CONVERSATIONS:{idFromName:()=>'',get:()=>({fetch:async()=>new Response('ok')})},BOT:{idFromName:()=>'',get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  const body=JSON.parse(String(init.body)) as Request;requests.push(body);return onRequest(body);
 }})}} as unknown as Env;
 return {e,requests};
}
const success=()=>Response.json({status:'success',result:{message_id:900}});
async function command(e:Env,id:number,view:string,value:string) {
 const update:TGUpdate={update_id:id,message:{message_id:id,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup',is_forum:true},from:{id:10001},text:`/mb ${view} ${value}`}};
 await accept(e,update);
 return {update,job:(await one<Job>(e,'SELECT * FROM jobs WHERE dedupe=?',`u:${id}`))!};
}
async function applied(e:Env,job:Job) {
 return JSON.parse((await one<{payload:string}>(e,'SELECT payload FROM jobs WHERE id=?',job.id))!.payload);
}

describe('administrator settings mutation retry boundaries',()=>{
 it.each(cases)('applies $view once across 429 repairs without overwriting a newer setting',async item=>{
  let confirmations=0,oldJobId=0;
  const {e,requests}=client(request=>{
   if(request.key===`${e.RECOVERY_EPOCH}:job:${oldJobId}:${item.suffix}`&&confirmations++<2)return Response.json({due:Date.now()+1000},{status:429});
   return success();
  });
  await setSetting(e,item.key,item.initial);
  const old=await command(e,1001,item.view,item.value);oldJobId=old.job.id;
  await expect(handleAdmin(e,old.job,old.update)).rejects.toBeInstanceOf(RetryLater);
  expect(await setting(e,item.key)).toBe(item.value);
  expect(await applied(e,old.job)).toEqual({update:old.update,ruleInputAdmission:null,adminSettingApplied:item.key});
  expect(await all(e,'SELECT event FROM audit_logs')).toEqual([{event:item.event}]);
  expect(await one(e,"SELECT COUNT(*) n FROM jobs WHERE kind='logs'")).toEqual({n:1});
  const later=await command(e,1002,item.view,item.later);
  await handleAdmin(e,later.job,later.update);
  expect(await setting(e,item.key)).toBe(item.later);
  await expect(handleAdmin(e,old.job,old.update)).rejects.toBeInstanceOf(RetryLater);
  await handleAdmin(e,old.job,old.update);
  expect(await setting(e,item.key)).toBe(item.later);
  expect(await all(e,'SELECT event FROM audit_logs')).toEqual([{event:item.event},{event:item.event}]);
  expect(requests.filter(request=>request.key===`${e.RECOVERY_EPOCH}:job:${old.job.id}:${item.suffix}`)).toHaveLength(3);
  if(item.view==='logtopic')expect(requests.filter(request=>request.key===`${e.RECOVERY_EPOCH}:job:${old.job.id}:logtopic:test`)).toHaveLength(1);
  expect(await one(e,'SELECT phase FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:job:${old.job.id}:${item.suffix}`)).toEqual({phase:'success'});
 });

 it.each(cases)('rolls back $view, audit, completion flag and log outbox together on D1 failure',async item=>{
  const {e}=client(success);
  await setSetting(e,item.key,item.initial);
  const task=await command(e,1003,item.view,item.value);
  await run(e,"CREATE TRIGGER reject_setting_audit BEFORE INSERT ON audit_logs BEGIN SELECT RAISE(ABORT,'synthetic D1 mutation failure'); END");
  await expect(handleAdmin(e,task.job,task.update)).rejects.toThrow();
  expect(await setting(e,item.key)).toBe(item.initial);
  expect(await applied(e,task.job)).toEqual({update:task.update,ruleInputAdmission:null});
  expect(await all(e,'SELECT id FROM audit_logs')).toEqual([]);
  expect(await all(e,"SELECT id FROM jobs WHERE kind IN ('logs','bootstrap')")).toEqual([]);
  await run(e,'DROP TRIGGER reject_setting_audit');
  await handleAdmin(e,task.job,task.update);
  expect(await setting(e,item.key)).toBe(item.value);
  expect(await applied(e,task.job)).toEqual({update:task.update,ruleInputAdmission:null,adminSettingApplied:item.key});
  expect(await all(e,'SELECT event FROM audit_logs')).toEqual([{event:item.event}]);
  expect(await one(e,"SELECT COUNT(*) n FROM jobs WHERE kind='logs'")).toEqual({n:1});
 });

 it('rejects a log topic occupied between API validation and the mutation transaction',async()=>{
  const {e}=client(async request=>{
   if(request.key.endsWith(':logtopic:test'))await run(base,"INSERT INTO users(tgid,first_contact,thread_id,binding_version) VALUES('20001',1,400,1)");
   return success();
  });
  await setSetting(e,'log_thread_id','300');
  const task=await command(e,1004,'logtopic','400');
  await handleAdmin(e,task.job,task.update);
  expect(await setting(e,'log_thread_id')).toBe('300');
  expect(await applied(e,task.job)).toEqual({update:task.update,ruleInputAdmission:null});
  expect(await all(e,'SELECT id FROM audit_logs')).toEqual([]);
  expect(await one(e,"SELECT thread_id FROM users WHERE tgid='20001'")).toEqual({thread_id:400});
 });

 it('keeps the per-job gate atomic when duplicate executions race',async()=>{
  const {e}=client(success);
  const task=await command(e,1005,'timezone','Asia/Tokyo');
  await Promise.all([handleAdmin(e,task.job,task.update),handleAdmin(e,task.job,task.update)]);
  expect(await setting(e,'timezone')).toBe('Asia/Tokyo');
  expect(await applied(e,task.job)).toEqual({update:task.update,ruleInputAdmission:null,adminSettingApplied:'timezone'});
  expect(await all(e,'SELECT event FROM audit_logs')).toEqual([{event:'setting_changed'}]);
 });
});
