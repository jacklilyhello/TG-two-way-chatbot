import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile, unlink, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
import { validateDrillInput, assertDrillWorker, sanitizeAggregate, assertRecoveryEvidence, runDrill } from './test-recovery-drill.mjs';

const example = JSON.parse(await readFile(new URL('../instances/test-a.example.json',import.meta.url)));
const config = {...example,d1_id:'11111111-1111-1111-1111-111111111111',queue_id:'2'.repeat(32),dlq_id:'3'.repeat(32),recovery_epoch:'fresh_epoch'};
const bookmark = '00000001-00000002-00000003-'+'a'.repeat(32);
const currentBookmark = '00000002-00000002-00000003-'+'b'.repeat(32);
const afterBookmark = '00000003-00000002-00000003-'+'c'.repeat(32);
const hash=value=>createHash('sha256').update(value).digest('hex');
const env = {GITHUB_ACTIONS:'true',GITHUB_REPOSITORY:'jacklilyhello/TG-two-way-chatbot',GITHUB_REF:'refs/heads/main',GITHUB_RUN_ID:'12345',INSTANCE_ID:'test-a',TEST_RECOVERY_DRILL_ALLOWED:'true',CONFIRM_TEST_RESTORE:'true',INSTANCE_CONFIG_JSON:JSON.stringify(config),RELEASE_SHA:'a'.repeat(40),RESTORE_BOOKMARK:bookmark,RESTORE_BOOKMARK_SHA256:hash(bookmark),NEW_RECOVERY_EPOCH_SHA256:hash(config.recovery_epoch),BACKUP_AGE_RECIPIENT:'age1'+'q'.repeat(58),CLOUDFLARE_ACCOUNT_ID:'4'.repeat(32),CLOUDFLARE_API_TOKEN:'FAKE_CF_TOKEN',TELEGRAM_BOT_TOKEN:'123456:'+'x'.repeat(20),TELEGRAM_WEBHOOK_SECRET:'FAKE_'+'s'.repeat(32)};
const counts = {users:1,messages:2,old_pending_messages:1,jobs:4,old_unfinished_jobs:3,quarantined_jobs:0,operations:2,issued_operations:0,unknown_operations:1,successful_operations:1,failed_operations:0,unfinished_deletions:1};
function workerSettings(vars = {}) {
  return {bindings:[...Object.entries({INSTANCE_ID:config.instance_id,EXPECTED_BOT_ID:config.expected_bot_id,ADMIN_CHAT_ID:config.admin_chat_id,ADMIN_IDS:config.admin_ids.join(','),VERSION:env.RELEASE_SHA,RECOVERY_EPOCH:'initial',...vars}).map(([name,text])=>({name,text,type:'plain_text'})),{name:'DB',type:'d1',id:config.d1_id},{name:'JOBS',type:'queue',queue_name:config.queue_name},{name:'CONVERSATIONS',type:'durable_object_namespace',class_name:'ConversationCoordinator',namespace_id:'ns-conversations'},{name:'BOT',type:'durable_object_namespace',class_name:'BotCoordinator',namespace_id:'ns-bot'}]};
}

test('writes require exact test Environment opt-in, fresh private epoch, bookmark and Actions main scope',()=>{
  assert.equal(validateDrillInput(env).instance_id,'test-a');
  for(const changed of [{GITHUB_ACTIONS:'false'},{GITHUB_REF:'refs/heads/codex/test'},{GITHUB_REPOSITORY:'foreign/repo'},{INSTANCE_ID:'production'},{TEST_RECOVERY_DRILL_ALLOWED:'false'},{CONFIRM_TEST_RESTORE:'false'},{NEW_RECOVERY_EPOCH_SHA256:hash('different')},{RESTORE_BOOKMARK_SHA256:hash('different')},{RESTORE_BOOKMARK:''},{RESTORE_BOOKMARK:'2026-10-11'},{RELEASE_SHA:'main'},{BACKUP_AGE_RECIPIENT:''},{INSTANCE_CONFIG_JSON:'{"PRIVATE_MARKER":'}])assert.throws(()=>validateDrillInput({...env,...changed}),error=>!error.message.includes('PRIVATE_MARKER'));
  assert.throws(()=>validateDrillInput({...env,INSTANCE_CONFIG_JSON:JSON.stringify({...config,d1_id:undefined})}),/explicit existing/);
});

test('static pause, run, epoch, version, administrator and namespace identities are mandatory',()=>{
  const settings=workerSettings({PAUSED:'true',RECOVERY_DRILL_STAGE:'prepared',RECOVERY_DRILL_RUN:env.GITHUB_RUN_ID});
  const expected={version:env.RELEASE_SHA,epoch:'initial',stage:'prepared',runId:env.GITHUB_RUN_ID,namespaces:['ns-bot','ns-conversations']};
  assertDrillWorker(config,settings,expected);
  for(const [name,text] of [['PAUSED','false'],['RECOVERY_DRILL_RUN','54321'],['RECOVERY_EPOCH','other'],['VERSION','b'.repeat(40)],['ADMIN_IDS','999']])assert.throws(()=>assertDrillWorker(config,{bindings:settings.bindings.map(item=>item.name===name?{...item,text}:item)},expected));
  assert.throws(()=>assertDrillWorker(config,settings,{...expected,namespaces:['foreign','ns-conversations']}));
  assert.throws(()=>assertDrillWorker(config,settings,{version:env.RELEASE_SHA}),/earlier recovery/);
});

test('aggregate evidence discards private columns and catches lost records, unknown outcomes or unquarantined tasks',()=>{
  assert.deepEqual(sanitizeAggregate({...counts,tgid:'PRIVATE_ID',payload:'PRIVATE_CONTENT'}),counts);
  assert.throws(()=>sanitizeAggregate({...counts,jobs:'4'}));
  const final={...counts,old_unfinished_jobs:0,quarantined_jobs:3};
  assertRecoveryEvidence(counts,final);
  for(const changed of [{messages:1},{unknown_operations:0},{old_unfinished_jobs:1},{quarantined_jobs:2},{issued_operations:1}])assert.throws(()=>assertRecoveryEvidence(counts,{...final,...changed}));
});

async function fixture(t,options={}) {
  const cwd=await mkdtemp(join(tmpdir(),'test-recovery-drill-'));
  t.after(()=>rm(cwd,{recursive:true,force:true}));
  let selected=workerSettings();
  let restored=false;
  let recovered=false;
  let paused=false;
  let auditsSealed=false;
  const events=[];
  const telegramMethods=[];
  const directory=join(cwd,'.local/recovery-drill');
  const dependencies={env:{...env},cwd,base:{migrations:[{tag:'v1',new_sqlite_classes:['ConversationCoordinator','BotCoordinator']}]},manifests:[],wait:async()=>{},
    telegram:async(method)=>{
      telegramMethods.push(method);
      assert(method.startsWith('get'));
      if(method==='getMe')return{id:Number(config.expected_bot_id),is_bot:true};
      if(method==='getChat')return{id:Number(config.admin_chat_id),type:'supergroup',is_forum:true};
      if(method==='getChatMember')return{status:'administrator',can_manage_topics:true,can_pin_messages:true};
      if(method==='getWebhookInfo')return{url:`https://${config.hostname}/webhook`};
      throw new Error('Unexpected Telegram method');
    },
    cf:async(path,request={})=>{
      const account=`/accounts/${env.CLOUDFLARE_ACCOUNT_ID}`;
      const db=`${account}/d1/database/${config.d1_id}`;
      if(path===`${account}/tokens/verify`)return{result:{status:'active'}};
      if(path.endsWith(`/workers/scripts/${config.worker_name}/settings`))return{result:selected};
      if(path===db)return{result:{uuid:config.d1_id,name:config.d1_name,version:'production'}};
      if(path.includes('/workers/scripts?'))return{result:[{id:config.worker_name}]};
      if(path.includes('/workers/domains?'))return{result:[{hostname:config.hostname,service:config.worker_name}]};
      if(path.startsWith('/zones?'))return{result:[{id:'zone',name:'lilyya.top'}]};
      if(path.startsWith('/zones/zone/workers/routes?'))return{result:[]};
      for(const [id,name]of[[config.queue_id,config.queue_name],[config.dlq_id,config.dlq_name]])if(path===`${account}/queues/${id}`)return{result:{queue_id:id,queue_name:name,consumers:id===config.queue_id?[{type:'worker',script:options.foreignConsumer?'foreign':config.worker_name}]:[]}};
      if(path===`${db}/time_travel/bookmark`)return{result:{bookmark:currentBookmark}};
      if(path===`${db}/query`){
        if(request.body.batch){
          assert.deepEqual(Object.keys(request.body),['batch']);
          assert(restored);assert(paused);
          assert.equal(selected.bindings.find(item=>item.name==='RECOVERY_DRILL_STAGE').text,'restoring');
          const [seal,marker]=request.body.batch;
          assert.equal(seal.sql,'UPDATE audit_logs SET reported=2 WHERE reported=0');
          assert.deepEqual(seal.params,[]);
          assert(marker.sql.includes("'log_outbox_epoch',?"));
          assert(!marker.sql.includes(config.recovery_epoch));
          assert.deepEqual(marker.params,[config.recovery_epoch]);
          events.push('seal-restored-audits');
          if(options.sealFailure)return{result:[{success:true,results:[]},{success:false,results:[]}]};
          auditsSealed=true;
          return{result:[{success:true,results:[]},{success:true,results:[]}]};
        }
        const sql=request.body.sql;
        assert.deepEqual(Object.keys(request.body).sort(),['params','sql']);
        if(sql.startsWith('INSERT')){assert.equal(selected.bindings.find(item=>item.name==='PAUSED').text,'true');events.push('persistent-pause');paused=true;return{result:[{success:true,results:[]}]};}
        if(sql.includes('unreported_audits'))return{result:[{success:true,results:[{unreported_audits:auditsSealed?0:1,epoch_marked:auditsSealed&&!options.sealReadbackFailure?1:0}]}]};
        if(sql.includes("substr(op_key"))return{result:[{success:true,results:[{n:options.usedEpoch?1:0}]}]};
        if(sql.includes('old_pending_messages'))return{result:[{success:true,results:[{...counts,...(recovered?{old_unfinished_jobs:0,quarantined_jobs:3}:{})}]}]};
      }
      if(path===`${db}/time_travel/restore?bookmark=${bookmark}`){
        events.push('restore');
        assert.equal(request.method,'POST');
        assert.equal(selected.bindings.find(item=>item.name==='PAUSED').text,'true');
        assert.equal(selected.bindings.find(item=>item.name==='RECOVERY_DRILL_STAGE').text,'restoring');
        assert(events.includes('encrypted-database-before.sql'));
        assert.equal(dependencies.env.RECOVERY_BACKUP_SAVED,'true');
        paused=false;
        if(options.unknownRestore)throw new Error('Cloudflare POST request result unknown; inspect resource state before retrying');
        restored=true;
        return{result:{bookmark:afterBookmark,previous_bookmark:currentBookmark}};
      }
      throw new Error(`Unexpected test endpoint: ${path}`);
    },
    fetcher:async(url,request)=>{
      if(url.endsWith('/webhook'))return new Response('',{status:selected.bindings.some(item=>item.name==='RECOVERY_DRILL_STAGE')?503:403});
      assert(url.endsWith('/internal/maintenance'));
      const {action}=JSON.parse(request.body);
      events.push(`maintenance-${action}`);
      assert.equal(request.headers['X-Telegram-Bot-Api-Secret-Token'],env.TELEGRAM_WEBHOOK_SECRET);
      if(action==='pause'&&options.unknownPause)throw new Error('controlled unknown pause response');
      if(action==='pause')paused=true;
      if(action==='recover'){
        assert(restored);assert(paused);assert(auditsSealed);assert.equal(selected.bindings.find(item=>item.name==='PAUSED').text,'true');
        assert.equal(selected.bindings.find(item=>item.name==='RECOVERY_EPOCH').text,config.recovery_epoch);
        recovered=true;
      }
      return Response.json({ok:true,version:env.RELEASE_SHA,epoch:selected.bindings.find(item=>item.name==='RECOVERY_EPOCH').text,paused:paused||selected.bindings.some(item=>item.name==='PAUSED'&&item.text==='true'),issued:0});
    },
    wrangler:async(args)=>{
      if(args[0]==='deploy'){
        const rendered=JSON.parse(await readFile(args[args.indexOf('--config')+1],'utf8'));
        assert.equal(rendered.vars.PAUSED,'true');assert.equal(rendered.main,'./maintenance.ts');
        assert.equal(rendered.d1_databases[0].database_id,config.d1_id);
        assert.equal(rendered.queues.producers[0].queue,config.queue_name);
        const source=await readFile(join(directory,'maintenance.ts'),'utf8');
        assert(source.includes("pathname==='/webhook'"));assert(source.includes('status:503'));
        selected=workerSettings(rendered.vars);events.push(`install-${rendered.vars.RECOVERY_DRILL_STAGE}`);
        return;
      }
      if(args[1]==='export'){
        assert.equal(restored,false);events.push('export');
        await writeFile(args[args.indexOf('--output')+1],'PRIVATE_SQL_TEST_MARKER');return;
      }
      if(args[1]==='migrations'){
        assert(restored);assert.equal(selected.bindings.find(item=>item.name==='PAUSED').text,'true');events.push('migrations');return;
      }
      throw new Error('Unexpected Wrangler operation');
    },
    encrypt:async(file)=>{
      if(options.encryptionFailure)throw new Error('Backup encryption failed');
      await writeFile(`${file}.age`,'test encrypted placeholder');await unlink(file);
      events.push(`encrypted-${file.split('/').at(-1)}`);
    }
  };
  return{dependencies,events,telegramMethods,directory,settings:()=>selected};
}

test('full drill saves backup first and retains static pause through rollback, fresh epoch and quarantine',async t=>{
  const f=await fixture(t);
  await runDrill('prepare',f.dependencies);
  assert(!f.events.includes('restore'));
  f.dependencies.env.RECOVERY_BACKUP_SAVED='true';
  const result=await runDrill('restore',f.dependencies);
  assert(f.events.indexOf('encrypted-database-before.sql')<f.events.indexOf('restore'));
  assert(f.events.indexOf('install-restoring')<f.events.indexOf('restore'));
  assert(f.events.indexOf('persistent-pause')<f.events.indexOf('install-restored'));
  assert(f.events.indexOf('migrations')<f.events.indexOf('seal-restored-audits'));
  assert(f.events.indexOf('persistent-pause')<f.events.indexOf('seal-restored-audits'));
  assert(f.events.indexOf('seal-restored-audits')<f.events.indexOf('install-restored'));
  assert(f.events.indexOf('install-restored')<f.events.indexOf('maintenance-recover'));
  assert.equal(f.events.filter(item=>item==='restore').length,1);
  assert.equal(result.stage,'restored_quarantined_still_paused');
  assert.equal(result.final.quarantined_jobs,3);
  assert.equal(result.final.unknown_operations,1);
  assert.equal(result.outbound_paused,true);
  assert.equal(result.webhook_retry_during_restore,true);
  assert.equal(result.restored_audits_sealed,true);
  assert.equal(f.settings().bindings.find(item=>item.name==='PAUSED').text,'true');
  const evidence=await readFile(join(f.directory,'evidence.json'),'utf8');
  for(const privateValue of ['PRIVATE_SQL_TEST_MARKER',env.TELEGRAM_WEBHOOK_SECRET,config.admin_chat_id,config.admin_ids[0],config.d1_id,bookmark,config.recovery_epoch])assert(!evidence.includes(privateValue));
});

test('failed audit seal or readback keeps the restoring gate and never installs or wakes a fresh epoch',async t=>{
  for(const options of [{sealFailure:true},{sealReadbackFailure:true}]){
    const f=await fixture(t,options);
    await runDrill('prepare',f.dependencies);
    f.dependencies.env.RECOVERY_BACKUP_SAVED='true';
    await assert.rejects(runDrill('restore',f.dependencies),/audit seal/);
    assert.equal(f.events.filter(item=>item==='restore').length,1);
    assert(!f.events.includes('install-restored'));
    assert(!f.events.includes('maintenance-recover'));
    assert.equal(f.settings().bindings.find(item=>item.name==='PAUSED').text,'true');
    assert.equal(f.settings().bindings.find(item=>item.name==='RECOVERY_DRILL_STAGE').text,'restoring');
    const evidence=JSON.parse(await readFile(join(f.directory,'evidence.json'),'utf8'));
    assert.equal(evidence.stage,'failed_keep_paused');
    assert.notEqual(evidence.restored_audits_sealed,true);
    await assert.rejects(runDrill('restore',f.dependencies),/Static recovery pause/);
    assert.equal(f.events.filter(item=>item==='restore').length,1);
  }
});

test('failed encryption never restores or removes the static maintenance gate',async t=>{
  const f=await fixture(t,{encryptionFailure:true});
  await assert.rejects(runDrill('prepare',f.dependencies),/encryption failed/);
  assert(!f.events.includes('restore'));
  assert.equal(f.settings().bindings.find(item=>item.name==='PAUSED').text,'true');
});

test('unknown restore remains sealed at restoring stage and rerun cannot issue another restore',async t=>{
  const f=await fixture(t,{unknownRestore:true});
  await runDrill('prepare',f.dependencies);
  f.dependencies.env.RECOVERY_BACKUP_SAVED='true';
  await assert.rejects(runDrill('restore',f.dependencies),/result unknown/);
  await assert.rejects(runDrill('restore',f.dependencies),/Static recovery pause/);
  await assert.rejects(runDrill('prepare',f.dependencies),/earlier recovery/);
  assert.equal(f.events.filter(item=>item==='restore').length,1);
  assert.equal(f.settings().bindings.find(item=>item.name==='PAUSED').text,'true');
});

test('foreign consumers and previously used epochs fail before any Worker or database mutation',async t=>{
  for(const options of [{foreignConsumer:true},{usedEpoch:true}]){
    const f=await fixture(t,options);
    await assert.rejects(runDrill('prepare',f.dependencies));
    assert.deepEqual(f.events,[]);
    const evidence=JSON.parse(await readFile(join(f.directory,'evidence.json'),'utf8'));
    assert.equal(evidence.stage,'failed_before_pause');
    assert.equal(evidence.outbound_paused,null);
    assert.equal(evidence.webhook_retry_during_restore,null);
  }
});

test('unknown pause response does not claim a confirmed pause or webhook retry gate',async t=>{
  const f=await fixture(t,{unknownPause:true});
  await assert.rejects(runDrill('prepare',f.dependencies),/Maintenance result unknown/);
  assert.deepEqual(f.events,['maintenance-pause']);
  const evidence=JSON.parse(await readFile(join(f.directory,'evidence.json'),'utf8'));
  assert.equal(evidence.stage,'failed_pause_unknown');
  assert.equal(evidence.outbound_paused,null);
  assert.equal(evidence.webhook_retry_during_restore,null);
});

test('restore requires saved backup and identical private configuration from the prepare phase',async t=>{
  const f=await fixture(t);
  await runDrill('prepare',f.dependencies);
  await assert.rejects(runDrill('restore',f.dependencies),/artifact must be saved/);
  f.dependencies.env.RECOVERY_BACKUP_SAVED='true';
  f.dependencies.env.INSTANCE_CONFIG_JSON=JSON.stringify({...config,admin_ids:[...config.admin_ids,'98765']});
  await assert.rejects(runDrill('restore',f.dependencies),/ownership does not match/);
  assert(!f.events.includes('restore'));
});

test('workflow uploads only encrypted backup and aggregate evidence under the normal instance lock',async()=>{
  const text=await readFile(new URL('../.github/workflows/test-recovery-drill.yml',import.meta.url),'utf8');
  assert(text.includes('group: instance-${{ inputs.instance }}'));
  assert(text.includes("if: github.ref == 'refs/heads/main'"));
  assert(text.includes('git merge-base --is-ancestor'));
  assert(text.includes('environment: tg-two-way-${{ inputs.instance }}'));
  assert(!text.includes('contents: write'));assert(!text.includes('adminToken'));assert(!text.includes('secrets: write'));
  const jobEnvironment=text.slice(text.indexOf('    env:\n',text.indexOf('  drill:')),text.indexOf('    steps:',text.indexOf('  drill:')));
  assert(!jobEnvironment.includes('secrets.'));
  assert(!text.includes('${{ inputs.bookmark }}'));
  assert(!text.includes('${{ inputs.new_epoch }}'));
  assert.equal([...text.matchAll(/RESTORE_BOOKMARK: \$\{\{ secrets.TEST_RECOVERY_BOOKMARK \}\}/g)].length,2);
  assert.equal([...text.matchAll(/INSTANCE_CONFIG_JSON: \$\{\{ secrets.INSTANCE_CONFIG_JSON \}\}/g)].length,2);
  assert(text.indexOf('Save encrypted pre-restore backup')<text.indexOf('node scripts/test-recovery-drill.mjs restore'));
  const artifactPaths=[...text.matchAll(/path: ([^\n]+)/g)].map(match=>match[1]);
  assert.deepEqual(artifactPaths,['|','.local/recovery-drill/evidence.json']);
  assert(text.includes('.local/recovery-drill/database-before.sql.age'));
  assert(text.includes('.local/recovery-drill/restore-points.json.age'));
  assert(!text.includes('state.json'));assert(!text.includes('wrangler.json'));
});
