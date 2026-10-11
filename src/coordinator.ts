import type {Env,Job,TGUpdate} from './types';
import {nextJob,one,run,setting,now,audit,scheduleLogs} from './db';
import {RetryLater} from './operations';
import {handleAdmin,handleCard} from './admin';
import {handleMessage,clearConversation,handleReaction,handleDeliveryNotice} from './messaging';
import {user} from './db';
import {flushLogs,bootstrap} from './logs';
export class ConversationCoordinator {
 private tail:Promise<unknown>=Promise.resolve();
 constructor(private state:DurableObjectState,private env:Env){}
 fetch(r:Request):Promise<Response> {
  // Arm-only self-calls must not join tail: audit() can await this request while
  // the same system coordinator is executing a job on that tail.
  if(new URL(r.url).pathname==='/logs/arm')return this.armLogs(r);
  if(new URL(r.url).pathname==='/arm')return this.arm(r);
  const promise=this.tail.then(async()=>{const {key}=await r.json<{key:string}>();await this.state.storage.put('key',key);await this.drain(key);return new Response('ok');});
  this.tail=promise.catch(()=>{});return promise;
 }
 alarm():Promise<void> {
  const promise=this.tail.then(async()=>{const key=await this.state.storage.get<string>('key');if(key)await this.drain(key);});
  this.tail=promise.catch(()=>{});return promise;
 }
 private async armLogs(r:Request) {
  const {epoch}=await r.json<{epoch:string}>();
  if(epoch!==this.env.RECOVERY_EPOCH||!this.state.id.equals(this.env.CONVERSATIONS.idFromName('system')))return new Response('Invalid wake',{status:409});
  await this.state.storage.put('key','system');
  const job=await nextJob(this.env,'system');
  if(job) {
   const due=this.env.PAUSED==='true'||await setting(this.env,'paused')==='1'?now()+60000:Math.max(now()+100,job.due_at);
   const alarm=await this.state.storage.getAlarm();
   if(alarm===null||due<alarm)await this.state.storage.setAlarm(due);
  }
  return new Response('ok');
 }
 private async arm(r:Request) {
  const {key,epoch}=await r.json<{key?:unknown;epoch?:unknown}>();
  if(typeof key!=='string'||!key||key.length>100||epoch!==this.env.RECOVERY_EPOCH||!this.state.id.equals(this.env.CONVERSATIONS.idFromName(key)))return new Response('Invalid wake',{status:409});
  await this.state.storage.put('key',key);
  const job=await nextJob(this.env,key);
  if(job) {
   const due=this.env.PAUSED==='true'||await setting(this.env,'paused')==='1'?now()+60000:Math.max(now()+100,job.due_at);
   const alarm=await this.state.storage.getAlarm();
   if(alarm===null||due<alarm)await this.state.storage.setAlarm(due);
  }
  return new Response('ok');
 }
 private async drain(key:string) {
  for(let i=0;i<16;i++) {
   if(this.env.PAUSED==='true'||await setting(this.env,'paused')==='1') {await this.state.storage.setAlarm(now()+60000);return;}
   // A completed/failed batch leaves a durable continuation for remaining
   // events. Its future minute slot is driven by this DO's Alarm, without Cron.
   if(key==='system')await scheduleLogs(this.env);
   const job=await nextJob(this.env,key);if(!job)return;
   if(job.due_at>now()) {await this.state.storage.setAlarm(job.due_at);return;}
   const started=now();
   await run(this.env,"UPDATE jobs SET state='running',attempts=attempts+1,first_started_at=COALESCE(first_started_at,?) WHERE id=?",started,job.id);
   try {
    await execute(this.env,job);
    await run(this.env,"UPDATE jobs SET state='done',error=NULL,completed_at=?,execution_ms=execution_ms+? WHERE id=? AND state='running'",now(),Math.max(0,now()-started),job.id);
    // Edits waiting for original mapping do not block new-message dependencies.
    await run(this.env,"UPDATE jobs SET state='pending',due_at=? WHERE user_key=? AND state='waiting' AND EXISTS(SELECT 1 FROM messages m WHERE m.source_chat=CAST(json_extract(jobs.payload,'$.update.edited_message.chat.id') AS TEXT) AND m.source_id=json_extract(jobs.payload,'$.update.edited_message.message_id') AND m.delivery!='pending')",now(),key);
   } catch(error) {
    const delay=error instanceof RetryLater?error.due:now()+Math.min(60000,1000*2**Math.min(job.attempts,6));
    if(!(error instanceof RetryLater)&&job.attempts>=8) {
     await run(this.env,"UPDATE jobs SET state='attention',error='执行中断；需要检查持久操作记录',execution_ms=execution_ms+? WHERE id=?",Math.max(0,now()-started),job.id);
     await audit(this.env,'任务需要处理',{job:job.id});
    } else {
     await run(this.env,"UPDATE jobs SET state='retry',due_at=?,error=?,execution_ms=execution_ms+? WHERE id=?",delay,error instanceof RetryLater?'限流或依赖延迟':'执行中断；已保存恢复状态',Math.max(0,now()-started),job.id);
     await this.state.storage.setAlarm(delay);return;
    }
   }
  }
  await this.state.storage.setAlarm(now()+100);
 }
}
export async function execute(e:Env,j:Job) {
 const p=JSON.parse(j.payload);
 if(j.kind==='card'){await handleCard(e,j);return;}
 if(j.kind==='delivery-notice'){await handleDeliveryNotice(e,j);return;}
 if(j.kind==='reaction'){await handleReaction(e,j);return;}
 if(j.kind==='bootstrap'){await bootstrap(e,j);return;}
 if(j.kind==='logs') {await flushLogs(e,j);return;}
 if(j.kind==='clear') {const u=await user(e,j.user_key);if(u)await clearConversation(e,j,u,p.batchId);return;}
 const update=p.update as TGUpdate;
 if(!update)return;
 if(await handleAdmin(e,j,update))return;
 const message=update.message??update.edited_message;
 if(message)await handleMessage(e,j,message,!!update.edited_message);
}
