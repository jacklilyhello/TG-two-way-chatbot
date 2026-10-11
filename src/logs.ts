import {all,setting,setSetting,audit,enqueue,run,one,bootstrapKey,scheduleLogs,wakeLogs,logEpochStatements} from './db';
import {call,RetryLater} from './operations';
import type {Env,Job} from './types';
/** Finite, durable log batches; group rate limits are shared with business delivery. */
export async function flushLogs(e:Env,j:Job) {
 if(j.epoch!==e.RECOVERY_EPOCH)return;
 await e.DB.batch(logEpochStatements(e));
 const saved=await one<{payload:string}>(e,"SELECT payload FROM jobs WHERE id=? AND epoch=? AND kind='logs' AND user_key='system'",j.id,e.RECOVERY_EPOCH);
 if(!saved)return;
 // A batch whose D1 completion repeatedly failed may already exist externally.
 // Neither a newly reserved nor an older queued batch may bypass its journal.
 if(await one(e,"SELECT id FROM jobs WHERE kind='logs' AND user_key='system' AND epoch=? AND state='attention' AND id!=? LIMIT 1",e.RECOVERY_EPOCH,j.id))throw new RetryLater(Date.now()+60000);
 // Audits can precede the deployment event. Never issue logging requests before
 // this version/epoch has validated the bot identity and attempted its menus.
 if(await setting(e,'bootstrap_ready')!==bootstrapKey(e)&&!await bootstrap(e,j))return;
 let ids=(JSON.parse(saved.payload) as {auditIds?:number[]}).auditIds;
 if(!ids) {
  // A pre-upgrade interrupted job may already have issued the former key. Its
  // original membership wins over audits that arrived after that request.
  const prefix=`${e.RECOVERY_EPOCH}:logs:${j.id}:`;
  const previous=await all<{op_key:string}>(e,'SELECT op_key FROM operations WHERE substr(op_key,1,?)=?',prefix.length,prefix);
  if(previous.length>1)throw new Error('log_batch_requires_reconciliation');
  if(previous.length) {
   const suffix=previous[0].op_key.slice(prefix.length);
   if(!/^\d+(,\d+){0,11}$/.test(suffix))throw new Error('log_batch_requires_reconciliation');
   ids=suffix.split(',').map(Number);
  } else ids=(await all<{id:number}>(e,'SELECT id FROM audit_logs WHERE reported=0 ORDER BY id LIMIT 12')).map(x=>x.id);
  if(!ids.length)return;
  // Freeze membership before any external request. New audits cannot change a
  // resumed job's operation key after an already-issued send.
  await run(e,'UPDATE jobs SET payload=? WHERE id=? AND epoch=?',JSON.stringify({auditIds:ids}),j.id,e.RECOVERY_EPOCH);
 }
 const events=await all<{id:number;event:string;detail:string;created_at:number;reported:number}>(e,'SELECT id,event,detail,created_at,reported FROM audit_logs WHERE id IN (SELECT value FROM json_each(?)) ORDER BY id',JSON.stringify(ids));
 if(!events.some(event=>event.reported===0))return;
 // Also bound batches that were already queued by an older release. A 429 or
 // journal reconciliation of the same job keeps its original reservation.
 const cursor=JSON.parse(await setting(e,'log_batch_started','null')) as {epoch:string;job:number;time:number}|null;
 if(cursor?.epoch!==e.RECOVERY_EPOCH||cursor.job!==j.id) {
  if(cursor?.epoch===e.RECOVERY_EPOCH&&cursor.time+60000>Date.now())throw new RetryLater(cursor.time+60000);
  await setSetting(e,'log_batch_started',JSON.stringify({epoch:e.RECOVERY_EPOCH,job:j.id,time:Date.now()}));
 }
 let thread=Number(await setting(e,'log_thread_id','0'));
 if(!thread) {
  if(await setting(e,'log_topic_unknown')==='1')return;
  const attempt=Number(await setting(e,'log_topic_attempt','0'));
  const result=await call<{message_thread_id:number}>(e,`log-topic:${attempt}`,'createForumTopic',{chat_id:e.ADMIN_CHAT_ID,name:'系统日志'});
  if(result.status!=='success') {
   if(result.status==='failed')await setSetting(e,'log_topic_attempt',String(attempt+1));
   else await setSetting(e,'log_topic_unknown','1');
   return;
  }
  thread=result.result.message_thread_id;
  if(!Number.isSafeInteger(thread)||thread<=1){await setSetting(e,'log_topic_unknown','1');await audit(e,'日志话题返回值无效',{});return;}
  const createdThread=thread;
  await run(e,"INSERT INTO settings(key,value) SELECT 'log_thread_id',? WHERE NOT EXISTS(SELECT 1 FROM users WHERE thread_id=?) AND NOT EXISTS(SELECT 1 FROM thread_history WHERE thread_id=?) ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE settings.value IN ('','0')",String(createdThread),createdThread,createdThread);
  thread=Number(await setting(e,'log_thread_id','0'));
  if(thread!==createdThread) {
   const occupied=await one(e,'SELECT tgid FROM users WHERE thread_id=? UNION ALL SELECT user_id FROM thread_history WHERE thread_id=? LIMIT 1',createdThread,createdThread);
   if(occupied)await audit(e,'日志话题已被绑定；保留管理员历史',{thread:createdThread});
   // A concurrent explicit administrator choice wins. Reuse it without creating
   // another topic or changing the known creation attempt's identity.
   if(!Number.isSafeInteger(thread)||thread<=1){await setSetting(e,'log_topic_attempt',String(attempt+1));return;}
  }

 }
 const response=await call(e,`logs:${j.id}:${ids.join(',')}`,'sendMessage',{chat_id:e.ADMIN_CHAT_ID,message_thread_id:thread,text:events.map(x=>`${new Date(x.created_at).toISOString()} ${x.event} #${x.id} ${x.detail.slice(0,240)}`).join('\n')});
 if(response.status==='success') await e.DB.batch(events.map(x=>e.DB.prepare('UPDATE audit_logs SET reported=1 WHERE id=? AND reported=0').bind(x.id)));
 else if(response.status==='unknown') await e.DB.batch(events.map(x=>e.DB.prepare('UPDATE audit_logs SET reported=2 WHERE id=? AND reported=0').bind(x.id)));
 // Explicit failures remain unreported; a later job uses a fresh, safe attempt key.

}
export async function versionEvent(e:Env) {
 const version=await setting(e,'last_version');
 await enqueue(e,'system','bootstrap',{},bootstrapKey(e));
 if(version!==e.VERSION) {await audit(e,'部署版本',{version:e.VERSION,epoch:e.RECOVERY_EPOCH});await setSetting(e,'last_version',e.VERSION);}
 await scheduleLogs(e);
 await wakeLogs(e);
}
export async function bootstrap(e:Env,j:Job) {
 const result=await call<{id:number;username?:string}>(e,`bootstrap:${e.VERSION}:identity`,'getMe',{});
 if(result.status!=='success'||String(result.result.id)!==e.EXPECTED_BOT_ID) {
  await setSetting(e,'paused','1');await audit(e,'实例身份验证失败；已暂停出站',{});return false;
 }
 await setSetting(e,'bot_username',result.result.username??'');
 const menus=[
  {scope:{type:'all_private_chats'},commands:[{command:'start',description:'欢迎信息'},{command:'help',description:'使用帮助'}]},
  {scope:{type:'chat_administrators',chat_id:e.ADMIN_CHAT_ID},commands:[{command:'tj',description:'注册或绑定 TGID'},{command:'sc',description:'删除被回复消息的用户端目标'},{command:'mb',description:'统计与系统设置'}]}
 ];
 for(let i=0;i<menus.length;i++) {
  const outcome=await call(e,`bootstrap:${e.VERSION}:menu:${i}`,'setMyCommands',menus[i]);
  if(outcome.status!=='success')await audit(e,'命令菜单设置异常',{scope:i,status:outcome.status});
 }
 await setSetting(e,'bootstrap_ready',bootstrapKey(e));
 return true;
}
