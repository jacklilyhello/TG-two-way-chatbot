import {env,evictDurableObject,runDurableObjectAlarm,runInDurableObject} from 'cloudflare:test';
import {describe,expect,it,vi} from 'vitest';
import type {Env,Job} from '../src/types';
import {all,audit,bootstrapKey,enqueue,one,run,setSetting,setting} from '../src/db';
import {flushLogs} from '../src/logs';

const base=env as unknown as Env;
const system=()=>base.CONVERSATIONS.get(base.CONVERSATIONS.idFromName('system'));
function quiet():Env {
 return {...base,JOBS:{send:vi.fn(async()=>{})},CONVERSATIONS:{idFromName:()=>'',get:()=>({fetch:vi.fn(async()=>new Response('ok'))})}} as unknown as Env;
}
async function ready() {
 await enqueue(base,'system','bootstrap',{},bootstrapKey(base));
 await run(base,"UPDATE jobs SET state='done' WHERE kind='bootstrap'");
 await setSetting(base,'bootstrap_ready',bootstrapKey(base));
 await setSetting(base,'log_thread_id','300');
}
async function wake() {
 const response=await system().fetch('https://conversation/wake',{method:'POST',body:JSON.stringify({key:'system'})});
 expect(await response.text()).toBe('ok');
}
const logJobs=()=>all<Job>(base,"SELECT * FROM jobs WHERE kind='logs' ORDER BY id");
async function clearRate() {
 const bot=base.BOT.get(base.BOT.idFromName(base.INSTANCE_ID));
 await runInDurableObject(bot,async(_instance,state)=>{
  await state.storage.delete(['rate:global',`rate:${base.ADMIN_CHAT_ID}`]);
 });
}
function telegram(send:(params:Record<string,unknown>)=>Response|Promise<Response>) {
 const methods:string[]=[];
 vi.mocked(fetch).mockImplementation(async(request,init)=>{
  const method=new URL(String(request)).pathname.split('/').at(-1)!;
  methods.push(method);
  if(method==='getMe')return Response.json({ok:true,result:{id:Number(base.EXPECTED_BOT_ID)}});
  if(method==='setMyCommands')return Response.json({ok:true,result:true});
  if(method==='sendMessage')return send(JSON.parse(String(init?.body)));
  throw new Error('Unexpected outbound method');
 });
 return methods;
}

describe('durable event-driven log outbox',()=>{
 it('rolls back audit and its wakeable work together when D1 commit fails',async()=>{
  const e=quiet();
  const broken={...e,DB:new Proxy(e.DB,{get(db,key){
   if(key==='batch')return (statements:D1PreparedStatement[])=>db.batch([...statements,db.prepare('INSERT INTO absent_outbox_table VALUES(1)')]);
   const value=Reflect.get(db,key);return typeof value==='function'?value.bind(db):value;
  }})};
  await expect(audit(broken,'synthetic event')).rejects.toThrow();
  expect(await all(base,'SELECT id FROM audit_logs')).toEqual([]);
  expect(await all(base,'SELECT id FROM jobs')).toEqual([]);
  expect(e.JOBS.send).not.toHaveBeenCalled();
 });

 it('coalesces concurrent audits into one batch and reserves the next minute after completion',async()=>{
  const e=quiet();
  await Promise.all(Array.from({length:50},()=>audit(e,'synthetic burst')));
  expect(await one(base,'SELECT COUNT(*) n FROM audit_logs')).toEqual({n:50});
  const [first]=await logJobs();
  expect(await logJobs()).toHaveLength(1);
  expect(e.JOBS.send).toHaveBeenCalledTimes(1);
  await run(base,"UPDATE jobs SET state='done' WHERE id=?",first.id);
  await audit(e,'synthetic later event');
  const jobs=await logJobs();
  expect(jobs).toHaveLength(2);
  expect(jobs[1].due_at).toBeGreaterThanOrEqual((Math.floor(first.due_at/60000)+1)*60000);
  await audit(e,'synthetic same outstanding batch');
  expect(await logJobs()).toHaveLength(2);
 });

 it('arms a persistent system Alarm when Queue notification fails, surviving DO reconstruction',async()=>{
  await ready();
  const methods=telegram(()=>Response.json({ok:true,result:{message_id:900}}));
  const e={...base,JOBS:{send:vi.fn(async()=>{throw new Error('synthetic queue failure');})}} as unknown as Env;
  await audit(e,'synthetic queue fallback');
  expect(await runInDurableObject(system(),(_instance,state)=>state.storage.getAlarm())).not.toBeNull();
  expect(methods).toEqual([]); // Arming never performs business or logging work.
  await evictDurableObject(system());
  expect(await runDurableObjectAlarm(system())).toBe(true);
  expect(methods).toEqual(['sendMessage']);
  expect(await one(base,'SELECT reported FROM audit_logs')).toEqual({reported:1});
  expect((await logJobs())[0].state).toBe('done');
 });

 it('retains recoverable work if both notification paths fail without recursive audits',async()=>{
  await ready();
  const e={...quiet(),JOBS:{send:async()=>{throw new Error('synthetic queue failure');}},CONVERSATIONS:{idFromName:()=>'',get:()=>({fetch:async()=>{throw new Error('synthetic alarm failure');}})}} as unknown as Env;
  await audit(e,'synthetic unavailable hints');
  expect(await one(base,'SELECT COUNT(*) n FROM audit_logs')).toEqual({n:1});
  expect((await logJobs())[0].state).toBe('pending');
  const methods=telegram(()=>Response.json({ok:true,result:{message_id:900}}));
  await wake(); // The durable row remains sufficient for a later recovery wake.
  expect(methods).toEqual(['sendMessage']);
  expect(await one(base,'SELECT reported FROM audit_logs')).toEqual({reported:1});
 });

 it('validates identity before an older queued log job and stops immediately on mismatch',async()=>{
  await enqueue(base,'system','logs',{},'legacy-log');
  await audit(quiet(),'synthetic first audit');
  vi.mocked(fetch).mockImplementation(async()=>Response.json({ok:true,result:{id:999999}}));
  await wake();
  expect(vi.mocked(fetch)).toHaveBeenCalledTimes(1);
  expect(String(vi.mocked(fetch).mock.calls[0][0])).toContain('/getMe');
  expect(await setting(base,'paused')).toBe('1');
  expect((await logJobs())[0].state).toBe('pending');
  expect(await one(base,"SELECT reported FROM audit_logs WHERE event='synthetic first audit'")).toEqual({reported:0});
 });

 it('allows a system job to await its own arm-only request without joining its drain tail',async()=>{
  await enqueue(base,'system','bootstrap',{},bootstrapKey(base));
  await runInDurableObject(system(),(instance:any,state)=>{
   const arm=instance.armLogs.bind(instance);
   instance.armLogs=async(request:Request)=>{
    await state.storage.put('self-arm-seen',true);
    const response=await arm(request);
    // Read native persisted state before bootstrap resumes: its limiter retry
    // can advance this short Alarm, which may fire before the final assertions.
    await state.storage.put('self-arm-persisted-alarm',await state.storage.getAlarm());
    return response;
   };
  });
  vi.mocked(fetch).mockImplementation(async request=>String(request).endsWith('/getMe')
   ?Response.json({ok:true,result:{id:Number(base.EXPECTED_BOT_ID)}})
   :Response.json({ok:false,error_code:400,description:'synthetic menu refusal'}));
  await wake(); // A menu failure creates the first audit/outbox inside the DO.
  expect(await runInDurableObject(system(),(_instance,state)=>state.storage.get('self-arm-seen'))).toBe(true);
  expect(await one(base,"SELECT COUNT(*) n FROM audit_logs WHERE event='命令菜单设置异常'")).toEqual({n:1});
  expect(await logJobs()).toHaveLength(1);
  const armedAt=await runInDurableObject(system(),(_instance,state)=>state.storage.get<number>('self-arm-persisted-alarm'));
  expect(armedAt).toBeGreaterThan(0);
 },5000);

 it('continues a backlog in bounded persisted batches using Alarm without another audit or Cron',async()=>{
  await ready();
  for(let i=0;i<15;i++)await audit(quiet(),'synthetic backlog');
  const bodies:Record<string,unknown>[]=[];
  telegram(params=>{bodies.push(params);return Response.json({ok:true,result:{message_id:900+bodies.length}});});
  await wake();
  expect(bodies).toHaveLength(1);
  expect(String(bodies[0].text).split('\n')).toHaveLength(12);
  const jobs=await logJobs();
  expect(jobs.map(j=>j.state)).toEqual(['done','pending']);
  expect(jobs[1].due_at).toBeGreaterThan(Date.now());
  // Advance the persisted due boundary and limiter to simulate the next slot.
  await run(base,'UPDATE jobs SET due_at=0 WHERE id=?',jobs[1].id);
  await setSetting(base,'log_batch_started','null');
  await clearRate();
  await evictDurableObject(system());
  expect(await runDurableObjectAlarm(system())).toBe(true);
  expect(bodies).toHaveLength(2);
  expect(String(bodies[1].text).split('\n')).toHaveLength(3);
  expect(await one(base,'SELECT COUNT(*) n FROM audit_logs WHERE reported=1')).toEqual({n:15});
  expect((await logJobs()).map(j=>j.state)).toEqual(['done','done']);
 });

 it('uses a fresh bounded attempt after an explicit rejection while preserving the audit',async()=>{
  await ready();await audit(quiet(),'synthetic rejected log');
  const methods=telegram(()=>Response.json({ok:false,error_code:400,description:'synthetic unwritable topic'}));
  await wake();
  expect(methods).toEqual(['sendMessage']);
  expect(await one(base,'SELECT reported FROM audit_logs')).toEqual({reported:0});
  const jobs=await logJobs();
  expect(jobs.map(j=>j.state)).toEqual(['done','pending']);
  expect(jobs[1].due_at).toBeGreaterThan(Date.now());
  await run(base,'UPDATE jobs SET due_at=0 WHERE id=?',jobs[1].id);
  await setSetting(base,'log_batch_started','null');await clearRate();
  telegram(()=>Response.json({ok:true,result:{message_id:901}}));
  await wake();
  expect(await all(base,"SELECT phase FROM operations WHERE method='sendMessage' ORDER BY created_at,op_key")).toEqual([{phase:'failed'},{phase:'success'}]);
  expect(await one(base,'SELECT reported FROM audit_logs')).toEqual({reported:1});
 });

 it('never includes an unknown batch in a later audit batch',async()=>{
  await ready();await audit(quiet(),'synthetic unknown log');
  const bodies:Record<string,unknown>[]=[];
  telegram(params=>{bodies.push(params);return Response.json({ok:false,error_code:500});});
  await wake();
  expect(await one(base,"SELECT reported FROM audit_logs WHERE event='synthetic unknown log'")).toEqual({reported:2});
  await audit(quiet(),'synthetic new log');
  const jobs=await logJobs();
  await run(base,'UPDATE jobs SET due_at=0 WHERE id=?',jobs.at(-1)!.id);
  await setSetting(base,'log_batch_started','null');await clearRate();
  telegram(params=>{bodies.push(params);return Response.json({ok:true,result:{message_id:902}});});
  await wake();
  expect(bodies).toHaveLength(2);
  expect(String(bodies[1].text)).toContain('synthetic new log');
  expect(String(bodies[1].text)).not.toContain('synthetic unknown log');
  expect(await all(base,'SELECT reported FROM audit_logs ORDER BY id')).toEqual([{reported:2},{reported:1}]);
 });

 it('reuses a frozen 429 batch and leaves later audits for a separate job',async()=>{
  await ready();await audit(quiet(),'synthetic rate limited log');
  const bodies:Record<string,unknown>[]=[];
  telegram(params=>{bodies.push(params);return bodies.length===1?Response.json({ok:false,error_code:429,parameters:{retry_after:1}}):Response.json({ok:true,result:{message_id:900}});});
  await wake();
  const [job]=await logJobs();
  expect(job.state).toBe('retry');
  expect(JSON.parse(job.payload).auditIds).toHaveLength(1);
  await audit(quiet(),'synthetic log after 429');
  expect(await logJobs()).toHaveLength(1);
  await run(base,'UPDATE jobs SET due_at=0 WHERE id=?',job.id);await clearRate();
  await evictDurableObject(system());await wake();
  expect(bodies).toHaveLength(2);
  expect(bodies[1]).toEqual(bodies[0]);
  expect(await one(base,"SELECT COUNT(*) n FROM operations WHERE method='sendMessage'")).toEqual({n:1});
  expect(await all(base,'SELECT reported FROM audit_logs ORDER BY id')).toEqual([{reported:1},{reported:0}]);
  expect((await logJobs()).map(j=>j.state)).toEqual(['done','pending']);
 });

 it.each(['success','unknown'])('holds all further log batches when %s completion repeatedly fails in D1',async outcome=>{
  await ready();await audit(quiet(),'synthetic interrupted batch');
  await run(base,"CREATE TRIGGER reject_log_completion BEFORE UPDATE OF reported ON audit_logs BEGIN SELECT RAISE(ABORT,'synthetic D1 failure'); END");
  const methods=telegram(()=>outcome==='success'?Response.json({ok:true,result:{message_id:900}}):Response.json({ok:false,error_code:500}));
  await wake();
  const [first]=await logJobs();
  expect(first.state).toBe('retry');
  await audit(quiet(),'synthetic event during recovery');
  await run(base,'UPDATE jobs SET attempts=8,due_at=0 WHERE id=?',first.id);
  await evictDurableObject(system());await wake();
  expect((await logJobs())[0].state).toBe('attention');
  await audit(quiet(),'synthetic event after attention');
  expect(await logJobs()).toHaveLength(1);
  // An already queued legacy job is also unable to bypass the unresolved batch.
  await enqueue(base,'system','logs',{},'legacy-after-attention');
  await wake();
  expect((await logJobs())[1].state).toBe('retry');
  expect(methods).toEqual(['sendMessage']);
  expect(await one(base,"SELECT phase FROM operations WHERE method='sendMessage'")).toEqual({phase:outcome});
  expect(await one(base,'SELECT COUNT(*) n FROM audit_logs WHERE reported!=0')).toEqual({n:0});
 });

 it('recovers a pre-upgrade unknown operation using its original member IDs',async()=>{
  await ready();
  await enqueue(base,'system','logs',{},'legacy-interrupted');
  const [legacy]=await logJobs();
  await audit(quiet(),'synthetic old member');
  const original=(await one<{id:number}>(base,'SELECT id FROM audit_logs'))!;
  await run(base,"INSERT INTO operations(op_key,method,phase,reason,created_at,updated_at) VALUES(?,'sendMessage','unknown','synthetic unknown',1,1)",`${base.RECOVERY_EPOCH}:logs:${legacy.id}:${original.id}`);
  await audit(quiet(),'synthetic newer member');
  await wake();
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  expect(await all(base,'SELECT reported FROM audit_logs ORDER BY id')).toEqual([{reported:2},{reported:0}]);
  expect(JSON.parse((await logJobs())[0].payload).auditIds).toEqual([original.id]);
 });

 it('bounds pre-existing pending log jobs across reconstruction instead of draining them in a burst',async()=>{
  await ready();
  for(let i=0;i<3;i++)await enqueue(base,'system','logs',{},`legacy:${i}`);
  for(let i=0;i<25;i++)await audit(quiet(),'synthetic legacy backlog');
  const methods=telegram(()=>Response.json({ok:true,result:{message_id:900}}));
  await wake();
  expect(methods).toEqual(['sendMessage']);
  expect((await logJobs()).map(j=>j.state)).toEqual(['done','retry','pending']);
  expect((await logJobs())[1].due_at).toBeGreaterThan(Date.now());
  await evictDurableObject(system());await wake();
  expect(methods).toEqual(['sendMessage']);
  expect(await one(base,'SELECT COUNT(*) n FROM audit_logs WHERE reported=1')).toEqual({n:12});
 });

 it('keeps paused and old-epoch work silent, rejecting an old-epoch arm request',async()=>{
  await ready();await setSetting(base,'paused','1');
  await audit({...quiet(),RECOVERY_EPOCH:'retired'},'synthetic old epoch');
  await audit(quiet(),'synthetic paused current epoch');
  await wake();
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  const response=await system().fetch('https://conversation/logs/arm',{method:'POST',body:JSON.stringify({epoch:'retired'})});
  expect(response.status).toBe(409);await response.text();
  const old=(await logJobs()).find(j=>j.epoch==='retired')!;
  await setSetting(base,'paused','0');await flushLogs(base,old);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
  expect((await logJobs()).every(j=>j.state==='pending')).toBe(true);
  // An unresolved old-generation attention row must not block new work.
  await run(base,"UPDATE jobs SET state='attention' WHERE kind='logs' AND epoch='retired'");
  telegram(()=>Response.json({ok:true,result:{message_id:900}}));
  await wake();
  expect(await all(base,'SELECT reported FROM audit_logs ORDER BY id')).toEqual([{reported:2},{reported:1}]);
 });

 it.each(['issued','unknown','success','absent'])('seals restored audits before new events even with an old %s operation and no frozen members',async phase=>{
  await ready();
  await setSetting(base,'log_outbox_epoch','retired');
  await run(base,"INSERT INTO audit_logs(event,detail,created_at) VALUES('synthetic restored audit','{}',1)");
  await enqueue({...base,RECOVERY_EPOCH:'retired'},'system','logs',{},'retired-log');
  const old=(await logJobs())[0];
  await run(base,"UPDATE jobs SET state='quarantined' WHERE id=?",old.id);
  if(phase!=='absent')await run(base,'INSERT INTO operations(op_key,method,phase,result,created_at,updated_at) VALUES(?,?,?,?,1,1)',`retired:logs:${old.id}:1`,'sendMessage',phase,phase==='success'?JSON.stringify({message_id:800}):null);
  await audit(quiet(),'synthetic current audit');
  expect(await setting(base,'log_outbox_epoch')).toBe(base.RECOVERY_EPOCH);
  expect(await all(base,'SELECT reported FROM audit_logs ORDER BY id')).toEqual([{reported:2},{reported:0}]);
  const bodies:Record<string,unknown>[]=[];
  telegram(params=>{bodies.push(params);return Response.json({ok:true,result:{message_id:900}});});
  await wake();
  expect(bodies).toHaveLength(1);
  expect(String(bodies[0].text)).toContain('synthetic current audit');
  expect(String(bodies[0].text)).not.toContain('synthetic restored audit');
  expect(await all(base,'SELECT reported FROM audit_logs ORDER BY id')).toEqual([{reported:2},{reported:1}]);
  if(phase!=='absent')expect(await one(base,'SELECT phase FROM operations WHERE op_key=?',`retired:logs:${old.id}:1`)).toEqual({phase});
 });

 it('initializes a legacy recovery conservatively while a same-epoch upgrade preserves catch-up',async()=>{
  await ready();
  await run(base,"INSERT INTO audit_logs(event,detail,created_at) VALUES('synthetic upgrade backlog','{}',1)");
  await audit(quiet(),'synthetic normal upgrade event');
  expect(await all(base,'SELECT reported FROM audit_logs ORDER BY id')).toEqual([{reported:0},{reported:0}]);
  // Simulate the first enhanced runtime encountering a legacy restored database
  // without the marker. Existing backlog is ambiguous, including any audits
  // created by an older runtime after restore; keep it for manual reconciliation.
  await run(base,"DELETE FROM settings WHERE key='log_outbox_epoch'");
  await enqueue({...base,RECOVERY_EPOCH:'retired'},'system','logs',{},'ambiguous-legacy-restore');
  await audit(quiet(),'synthetic post-boundary event');
  expect(await all(base,'SELECT reported FROM audit_logs ORDER BY id')).toEqual([{reported:2},{reported:2},{reported:0}]);
  await audit(quiet(),'synthetic next current event');
  expect(await all(base,'SELECT reported FROM audit_logs ORDER BY id')).toEqual([{reported:2},{reported:2},{reported:0},{reported:0}]);
 });

 it('rolls back epoch sealing and new audit together if the cutover transaction fails',async()=>{
  await ready();await setSetting(base,'log_outbox_epoch','retired');
  await run(base,"INSERT INTO audit_logs(event,detail,created_at) VALUES('synthetic old backlog','{}',1)");
  const e=quiet();
  const broken={...e,DB:new Proxy(e.DB,{get(db,key){
   if(key==='batch')return (statements:D1PreparedStatement[])=>db.batch([...statements,db.prepare('INSERT INTO absent_epoch_table VALUES(1)')]);
   const value=Reflect.get(db,key);return typeof value==='function'?value.bind(db):value;
  }})};
  await expect(audit(broken,'synthetic new boundary')).rejects.toThrow();
  expect(await setting(base,'log_outbox_epoch')).toBe('retired');
  expect(await all(base,'SELECT event,reported FROM audit_logs')).toEqual([{event:'synthetic old backlog',reported:0}]);
  await audit(e,'synthetic new boundary');
  expect(await all(base,'SELECT reported FROM audit_logs ORDER BY id')).toEqual([{reported:2},{reported:0}]);
 });

 it('recognizes a legacy bookmark taken before any log job or operation was saved',async()=>{
  await run(base,'INSERT INTO audit_logs(event,detail,created_at) VALUES(?,?,1)','部署版本',JSON.stringify({version:'legacy',epoch:'retired'}));
  expect(await all(base,'SELECT id FROM jobs')).toEqual([]);
  expect(await all(base,'SELECT op_key FROM operations')).toEqual([]);
  await audit(quiet(),'synthetic first post-restore event');
  expect(await all(base,'SELECT reported FROM audit_logs ORDER BY id')).toEqual([{reported:2},{reported:0}]);
  expect(await setting(base,'log_outbox_epoch')).toBe(base.RECOVERY_EPOCH);
 });
});
