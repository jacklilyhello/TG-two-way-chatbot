import type {Env,Job,TGUpdate} from './types';
import {nextJob,one,run,setting,now,audit} from './db';
import {RetryLater} from './operations';
import {handleAdmin} from './admin';
import {handleMessage,clearConversation,handleReaction} from './messaging';
import {user} from './db';
import {flushLogs,bootstrap} from './logs';
export class ConversationCoordinator {
 private tail:Promise<unknown>=Promise.resolve();
 constructor(private state:DurableObjectState,private env:Env){}
 fetch(r:Request):Promise<Response> {
  const promise=this.tail.then(async()=>{const {key}=await r.json<{key:string}>();await this.state.storage.put('key',key);await this.drain(key);return new Response('ok');});
  this.tail=promise.catch(()=>{});return promise;
 }
 alarm():Promise<void> {
  const promise=this.tail.then(async()=>{const key=await this.state.storage.get<string>('key');if(key)await this.drain(key);});
  this.tail=promise.catch(()=>{});return promise;
 }
 private async drain(key:string) {
  if(this.env.PAUSED==='true'||await setting(this.env,'paused')==='1') {await this.state.storage.setAlarm(now()+60000);return;}
  for(let i=0;i<16;i++) {
   const job=await nextJob(this.env,key);if(!job)return;
   if(job.due_at>now()) {await this.state.storage.setAlarm(job.due_at);return;}
   await run(this.env,"UPDATE jobs SET state='running',attempts=attempts+1 WHERE id=?",job.id);
   try {
    await execute(this.env,job);
    await run(this.env,"UPDATE jobs SET state='done',error=NULL WHERE id=? AND state='running'",job.id);
    // Edits waiting for original mapping do not block new-message dependencies.
    await run(this.env,"UPDATE jobs SET state='pending',due_at=? WHERE user_key=? AND state='waiting' AND EXISTS(SELECT 1 FROM messages m WHERE m.source_chat=CAST(json_extract(jobs.payload,'$.update.edited_message.chat.id') AS TEXT) AND m.source_id=json_extract(jobs.payload,'$.update.edited_message.message_id') AND m.delivery!='pending')",now(),key);
   } catch(error) {
    const delay=error instanceof RetryLater?error.due:now()+Math.min(60000,1000*2**Math.min(job.attempts,6));
    if(!(error instanceof RetryLater)&&job.attempts>=8) {
     await run(this.env,"UPDATE jobs SET state='attention',error='执行中断；需要检查持久操作记录' WHERE id=?",job.id);
     await audit(this.env,'任务需要处理',{job:job.id});
    } else {
     await run(this.env,"UPDATE jobs SET state='retry',due_at=?,error=? WHERE id=?",delay,error instanceof RetryLater?'限流或依赖延迟':'执行中断；已保存恢复状态',job.id);
     await this.state.storage.setAlarm(delay);return;
    }
   }
  }
  await this.state.storage.setAlarm(now()+100);
 }
}
export async function execute(e:Env,j:Job) {
 const p=JSON.parse(j.payload);
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
