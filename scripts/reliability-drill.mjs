import {mkdir,readFile,writeFile} from 'node:fs/promises';
import {randomBytes} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {resolve} from 'node:path';
import assert from 'node:assert/strict';
import {apiClient,wrangler} from './deployment-lib.mjs';
import {drillPlan,assertDrillOrigin,assertDrillBindings,renderDrill} from './reliability-drill-lib.mjs';

async function main() {
 const e=process.env,plan=drillPlan(e,{cleanupRun:process.argv.includes('--remove-route')?e.DRILL_CLEANUP_RUN:undefined});
 if(!/^[a-f0-9]{32}$/.test(e.CLOUDFLARE_ACCOUNT_ID||'')||!e.CLOUDFLARE_API_TOKEN)throw new Error('Actions Cloudflare credential is missing');
 if(e.TELEGRAM_BOT_TOKEN||e.TELEGRAM_WEBHOOK_SECRET)throw new Error('Real Telegram credentials are forbidden in the reliability drill job');
 const cf=apiClient(e.CLOUDFLARE_API_TOKEN),account=`/accounts/${e.CLOUDFLARE_ACCOUNT_ID}`;
 async function list(path) {
  const records=[];
  for(let page=1;page<=100;page++) {
   const data=await cf(`${path}${path.includes('?')?'&':'?'}page=${page}&per_page=100`);
   if(!Array.isArray(data.result))throw new Error('Cloudflare inventory unavailable');records.push(...data.result);
   if(!data.result_info||page>=data.result_info.total_pages||data.result.length<100)return records;
  }
  throw new Error('Cloudflare inventory exceeded bound');
 }
 const primary=(await cf(`${account}/workers/scripts/${plan.config.worker_name}/settings`)).result;
 const domains=await list(`${account}/workers/domains`);
 const zones=await list(`/zones?name=lilyya.top&account.id=${e.CLOUDFLARE_ACCOUNT_ID}`);
 if(zones.length!==1)throw new Error('Authorized zone unavailable');
 const zone=zones[0].id,routes=await list(`/zones/${zone}/workers/routes`);
 assertDrillOrigin(plan,domains,primary,routes);
 if(process.argv.includes('--remove-route')) {
  const matches=routes.filter(r=>r.pattern===plan.route&&r.script===plan.name);
  if(matches.length>1)throw new Error('Ambiguous drill route; no cleanup performed');
  if(matches.length) {
   const settings=(await cf(`${account}/workers/scripts/${plan.name}/settings`)).result;
   const binding=settings.bindings?.find(b=>b.name==='DB');assertDrillBindings(plan,settings,binding?.id??binding?.database_id);
   await cf(`/zones/${zone}/workers/routes/${matches[0].id}`,{method:'DELETE'});
  }
  let report;try{report=JSON.parse(await readFile('.local/reliability-result.json','utf8'));}catch{report={kind:'controlled-cloud-reliability-drill',instance:plan.config.instance_id,run:plan.run,outcome:'not_completed',checks:[]};}
  report.routeRemoved=true;await mkdir('.local',{recursive:true,mode:0o700});await writeFile('.local/reliability-result.json',JSON.stringify(report,null,2),{mode:0o600});
  console.log('Exact temporary drill route is absent; durable synthetic evidence retained');return;
 }
 const scripts=await list(`${account}/workers/scripts`),databases=await list(`${account}/d1/database`),queues=await list(`${account}/queues`);
 if(scripts.some(s=>s.id===plan.name)||databases.some(d=>d.name===plan.database)||queues.some(q=>q.queue_name===plan.queue))throw new Error('Drill run resources already exist; inspect them instead of adopting or replaying');
 if(routes.some(r=>r.pattern===plan.route))throw new Error('An existing drill route requires separate review; no takeover');
 await mkdir('.local',{recursive:true,mode:0o700});
 const key=randomBytes(32).toString('hex');
 const configPath=resolve('.local/wrangler.reliability.json');
 const report={kind:'controlled-cloud-reliability-drill',instance:plan.config.instance_id,run:plan.run,release:plan.version,outcome:'not_completed',telegram:'internal persistent stub only; not real Telegram acceptance',faults:'D1 and Queue failures are injected binding exceptions, not an induced Cloudflare outage',checks:[],observations:{},routeRemoved:false};
 let routeId,drillConfig,createdWorker=false;
 const proof=(name)=>{report.checks.push(name);console.log(`Confirmed: ${name}`);};
 async function deploy(revision) {
  drillConfig=renderDrill(plan,drillConfig.d1_databases[0].database_id,revision);
  // Wrangler synchronizes a Worker's routes on redeploy. Preserve the single
  // already-authorized drill path when installing the persistence revision.
  if(routeId)drillConfig.routes=[{pattern:plan.route,zone_id:zone}];
  await writeFile(configPath,JSON.stringify(drillConfig),{mode:0o600});
  await wrangler(['deploy','--config',configPath],{label:`isolated reliability revision ${revision}`});createdWorker=true;
  if(routeId) {
   const matches=(await list(`/zones/${zone}/workers/routes`)).filter(r=>r.pattern===plan.route&&r.script===plan.name);
   if(matches.length!==1)throw new Error('Drill route readback failed after revision deployment');routeId=matches[0].id;
  }
 }
 async function post(action) {
  let response;
  try{response=await fetch(`https://${plan.route}`,{method:'POST',headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},body:JSON.stringify({action}),redirect:'error',signal:AbortSignal.timeout(45000)});}
  catch{throw new Error(`Drill ${action} response unknown; no automatic request replay`);}
  if(!response.ok)throw new Error(`Drill ${action} rejected (HTTP ${response.status})`);
  return response.json();
 }
 async function eventually(action,done) {
  for(let i=0;i<12;i++) {
   const value=await post(action);if(done(value))return value;
   if(!value.deferred||!Number.isSafeInteger(value.due))throw new Error(`Unexpected drill ${action} state`);
   await delay(Math.min(10000,Math.max(100,Math.min(value.due-Date.now()+100,10000))));
  }
  throw new Error(`Drill ${action} exceeded bounded retry budget`);
 }
 try {
  const db=(await cf(`${account}/d1/database`,{method:'POST',body:{name:plan.database,primary_location_hint:'apac',read_replication:{mode:'disabled'}}})).result;
  if(typeof db?.uuid!=='string')throw new Error('Drill database creation response unknown; inspect named resource');
  await cf(`${account}/queues`,{method:'POST',body:{queue_name:plan.queue}});
  drillConfig=renderDrill(plan,db.uuid);
  await writeFile(configPath,JSON.stringify(drillConfig),{mode:0o600});
  await wrangler(['d1','migrations','apply','DB','--remote','--config',configPath],{label:'new synthetic drill database schema'});
  await deploy('1');
  await wrangler(['secret','bulk','--config',configPath],{input:JSON.stringify({DRILL_KEY:key}),label:'ephemeral drill authorization'});
  assertDrillBindings(plan,(await cf(`${account}/workers/scripts/${plan.name}/settings`)).result,db.uuid);
  // Close the preflight/upload race without replacing any existing route.
  const fresh=await list(`/zones/${zone}/workers/routes`);assertDrillOrigin(plan,domains,primary,fresh);
  if(fresh.some(r=>r.pattern===plan.route))throw new Error('Drill path became occupied during deployment');
  const route=(await cf(`/zones/${zone}/workers/routes`,{method:'POST',body:{pattern:plan.route,script:plan.name}})).result;
  if(typeof route?.id!=='string')throw new Error('Drill route creation response unknown; inspect exact path before retry');routeId=route.id;
  const denied=await fetch(`https://${plan.route}`,{method:'POST',headers:{'Content-Type':'application/json'},body:'{"action":"setup"}',redirect:'error',signal:AbortSignal.timeout(15000)});await denied.text();
  assert.equal(denied.status,403);proof('Public drill path rejects missing ephemeral authorization');
  assert.deepEqual(await post('setup'),{ready:true});
  const ingress=await post('ingress-d1');
  assert.deepEqual(ingress,{failedStatus:503,persistedBeforeRetry:0,retryStatus:200,duplicateStatus:200,inbox:1,messages:1});proof('D1 binding failure rejects ingress, retry persists once');
  report.observations.ingress=ingress;
  assert.deepEqual(await post('queue-failure'),{accepted:200,failedNotifications:1,job:{state:'pending'},message:{delivery:'pending'}});proof('Queue notification failure retains an accepted durable task');
  await post('recover');let queueDone=false;
  for(let i=0;i<25;i++){const q=await post('queue-status');if(q.job?.state==='done'&&q.message?.delivery==='blocked'){queueDone=true;break;}await delay(2000);}
  assert.equal(queueDone,true);proof('Real Cloudflare Queue consumption recovers the unnotified task');
  report.observations.queue={failedNotificationAccepted:200,before:'pending',after:'done',delivery:'blocked'};
  const limited=await post('rate429');assert.equal(limited.deferred,true);assert.equal(limited.count,1);assert.equal(limited.phase,'prepared');
  const retried=await eventually('rate429',v=>!v.deferred);assert.equal(retried.result?.status,'success');assert.equal(retried.count,2);
  assert.equal((await post('rate429')).count,2);proof('429 retries only after retry_after and cached success never sends again');
  report.observations.rate429={beforePhase:limited.phase,beforeRequests:limited.count,afterPhase:retried.phase,afterRequests:retried.count};
  const failed=await eventually('completion-fail',v=>v.completionFailed);assert.equal(failed.count,1);assert.equal(failed.phase,'issued');
  const before=await post('generation');await post('reset');const after=await post('generation');assert.notEqual(after.generation,before.generation);
  const completed=await post('completion-recover');assert.equal(completed.result?.status,'success');assert.equal(completed.count,1);proof('D1 completion failure recovers the Bot journal after actual DO reconstruction');
  report.observations.completion={afterFailure:failed.phase,afterRebuild:completed.phase,requests:completed.count,generationChanged:after.generation!==before.generation};
  const interruptedBefore=await post('generation');
  const interrupted=await eventually('interrupted',v=>v.deferred&&v.count===1&&v.phase==='issued');assert.equal(interrupted.count,1);
  const interruptedAfter=await post('generation');assert.notEqual(interruptedAfter.generation,interruptedBefore.generation);
  const sealed=await post('interrupted');assert.equal(sealed.result?.status,'unknown');assert.equal(sealed.count,1);
  assert.equal((await post('interrupted')).count,1);proof('Abort after the stub effect reconstructs DO and seals issued request unknown without resend');
  report.observations.interruption={beforePhase:interrupted.phase,afterPhase:sealed.phase,requests:sealed.count,generationChanged:interruptedAfter.generation!==interruptedBefore.generation};
  const unknown=await eventually('unknown',v=>!v.deferred);assert.equal(unknown.result?.status,'unknown');assert.equal(unknown.count,1);
  assert.equal((await post('unknown')).count,1);proof('Malformed upstream response stays unknown and is never automatically resent');
  report.observations.unknown={phase:unknown.phase,requests:unknown.count};
  const persisted=await post('verify');await deploy('2');
  assertDrillBindings(plan,(await cf(`${account}/workers/scripts/${plan.name}/settings`)).result,db.uuid);
  const rebuilt=await post('verify');assert.equal(rebuilt.revision,'2');assert.deepEqual(rebuilt.phases,persisted.phases);assert.equal(rebuilt.queue?.state,'done');assert.equal(rebuilt.inbox,2);
  for(const action of ['rate429','completion-recover','interrupted','unknown'])await post(action);
  assert.deepEqual((await post('verify')).phases,rebuilt.phases);proof('Second real Worker deployment preserves D1, DO journals and stub effect counts');
  report.observations.afterRedeploy={revision:rebuilt.revision,phases:rebuilt.phases,inbox:rebuilt.inbox,queue:rebuilt.queue};
  report.outcome='passed';
 } finally {
  // Remove only the exact temporary route this run created. Keep all synthetic
  // durable evidence resources; never delete a database, Queue or DO namespace.
  let cleanupError;
  try {
   if(createdWorker&&!routeId) {
    const matches=(await list(`/zones/${zone}/workers/routes`)).filter(r=>r.pattern===plan.route&&r.script===plan.name);
    if(matches.length>1)throw new Error('Ambiguous temporary drill routes');routeId=matches[0]?.id;
   }
   if(routeId) {
    const route=(await cf(`/zones/${zone}/workers/routes/${routeId}`)).result;
    if(route.pattern!==plan.route||route.script!==plan.name)throw new Error('Drill route changed; refusing cleanup of another route');
    await cf(`/zones/${zone}/workers/routes/${routeId}`,{method:'DELETE'});report.routeRemoved=true;
   } else report.routeRemoved=true;
  }catch(error){cleanupError=error;report.routeRemoved=false;}
  report.evidenceResourcesRetained=createdWorker;
  await writeFile('.local/reliability-result.json',JSON.stringify(report,null,2),{mode:0o600});
  if(cleanupError)throw cleanupError;
 }
}
main().catch(error=>{console.error(error instanceof SyntaxError?'Invalid private drill configuration':error.message);process.exitCode=1;});
