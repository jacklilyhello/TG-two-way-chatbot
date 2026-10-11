import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {drillPlan,assertDrillOrigin,assertDrillBindings,renderDrill,DRILL_PATH} from './reliability-drill-lib.mjs';
const config={instance_id:'test-a',expected_bot_id:'123456',admin_chat_id:'-1001234567890',admin_ids:['10001'],hostname:'synthetic-a.lilyya.top',worker_name:'tg-two-way-test-a',d1_name:'tg-two-way-test-a-db',queue_name:'tg-two-way-test-a-jobs',dlq_name:'tg-two-way-test-a-dlq',recovery_epoch:'initial'};
const env={GITHUB_ACTIONS:'true',GITHUB_REPOSITORY:'jacklilyhello/TG-two-way-chatbot',GITHUB_REF:'refs/heads/main',INSTANCE_ID:'test-a',INSTANCE_CONFIG_JSON:JSON.stringify(config),RELEASE_SHA:'a'.repeat(40),GITHUB_RUN_ID:'1234',GITHUB_RUN_ATTEMPT:'1'};
const plan=drillPlan(env),db='00000000-0000-0000-0000-000000000001';
const origin={bindings:[{name:'INSTANCE_ID',text:'test-a'},{name:'EXPECTED_BOT_ID',text:'123456'}]};
const domains=[{hostname:config.hostname,service:config.worker_name}];
function settings() {
 return {bindings:[
  {type:'plain_text',name:'DRILL_RUN_ID',text:plan.run},{type:'plain_text',name:'INSTANCE_ID',text:'test-a'},{type:'plain_text',name:'TELEGRAM_BOT_TOKEN',text:'0:reliability-drill-only'},
  {type:'d1',name:'DB',id:db},{type:'queue',name:'JOBS',queue_name:plan.queue},{type:'secret_text',name:'DRILL_KEY'},
  ...Object.entries({BOT:'DrillBotCoordinator',CONVERSATIONS:'ConversationCoordinator',STUB:'DrillApiStub'}).map(([name,class_name])=>({type:'durable_object_namespace',name,class_name,namespace_id:name,script_name:plan.name}))
 ]};
}
test('drill requires main Actions and exactly one of the two test aliases',()=>{
 for(const change of [{GITHUB_ACTIONS:'false'},{GITHUB_REPOSITORY:'other/repo'},{GITHUB_REF:'refs/heads/topic'},{INSTANCE_ID:'production'},{GITHUB_RUN_ID:'../../evil'},{GITHUB_RUN_ATTEMPT:'1;echo'},{RELEASE_SHA:'main'}])assert.throws(()=>drillPlan({...env,...change}));
 const b={...config,instance_id:'test-b',worker_name:'tg-two-way-test-b',d1_name:'tg-two-way-test-b-db',queue_name:'tg-two-way-test-b-jobs',dlq_name:'tg-two-way-test-b-dlq',hostname:'synthetic-b.lilyya.top'};
 assert.equal(drillPlan({...env,INSTANCE_ID:'test-b',INSTANCE_CONFIG_JSON:JSON.stringify(b)}).config.instance_id,'test-b');
 assert.equal(drillPlan(env,{cleanupRun:'9999-1'}).run,'9999-1');
 assert.throws(()=>drillPlan(env,{cleanupRun:'../../production'}));
});
test('drill path is accepted only on the selected test Worker domain with no occupying route',()=>{
 assert.doesNotThrow(()=>assertDrillOrigin(plan,domains,origin,[]));
 assert.throws(()=>assertDrillOrigin(plan,[{...domains[0],service:'other'}],origin,[]));
 assert.throws(()=>assertDrillOrigin(plan,domains,{bindings:[]},[]));
 for(const pattern of [`${config.hostname}/*`,`https://${config.hostname}/*`,`*.lilyya.top/*`,plan.route])assert.throws(()=>assertDrillOrigin(plan,domains,origin,[{pattern,script:'another-worker'}]));
 assert.doesNotThrow(()=>assertDrillOrigin(plan,domains,origin,[{pattern:`synthetic-b.lilyya.top/*`,script:'other'}]));
});
test('rendered deployment has only new synthetic resources and no public route until authorization is installed',()=>{
 const r=renderDrill(plan,db);
 assert.deepEqual(r.routes,[]);assert.equal(r.workers_dev,false);assert.equal(r.preview_urls,false);
 assert.equal(r.vars.TELEGRAM_BOT_TOKEN,'0:reliability-drill-only');assert.notEqual(r.vars.ADMIN_CHAT_ID,config.admin_chat_id);
 assert.equal(r.d1_databases[0].database_name,plan.database);assert.notEqual(r.d1_databases[0].database_name,config.d1_name);
 assert.equal(r.queues.producers[0].queue,plan.queue);assert.notEqual(plan.queue,config.queue_name);
 assert.equal(r.vars.RECOVERY_EPOCH,`drill-${plan.run}`);assert.equal(plan.route,`${config.hostname}${DRILL_PATH}`);
 assert.equal(r.triggers,undefined);assert.equal(r.services,undefined);
});
test('readback rejects cross-worker DOs, shared namespaces, real secrets and extra bindings',()=>{
 assert.doesNotThrow(()=>assertDrillBindings(plan,settings(),db));
 for(const modify of [s=>s.bindings.find(x=>x.name==='DB').id='another',s=>s.bindings.find(x=>x.name==='BOT').script_name=config.worker_name,s=>s.bindings.find(x=>x.name==='STUB').namespace_id='BOT',s=>s.bindings.push({type:'secret_text',name:'TELEGRAM_BOT_TOKEN'}),s=>s.bindings.push({type:'service',name:'REAL_BOT'}),s=>s.bindings.push({type:'d1',name:'REAL_DB',id:'another'})]) {
  const s=settings();modify(s);assert.throws(()=>assertDrillBindings(plan,s,db));
 }
});
test('production entrypoint and deployment configuration never import or enable drill entrypoint',async()=>{
 for(const file of ['src/index.ts','wrangler.jsonc','scripts/deploy-instance.mjs'])assert.doesNotMatch(await readFile(file,'utf8'),/reliability-drill\/worker|DrillBotCoordinator|__reliability-drill/);
});
