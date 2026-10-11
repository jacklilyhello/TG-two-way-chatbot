import {validateConfig,requireActions} from './instance-config.mjs';

export const DRILL_PATH='/__reliability-drill';
export function drillPlan(env,{cleanupRun}={}) {
 requireActions(env);
 if(!['test-a','test-b'].includes(env.INSTANCE_ID))throw new Error('Reliability drills are restricted to the two authorized test aliases');
 const config=validateConfig(JSON.parse(env.INSTANCE_CONFIG_JSON||'null'),env.INSTANCE_ID);
 if(!/^[a-f0-9]{40}$/.test(env.RELEASE_SHA||''))throw new Error('An immutable main release is required');
 if(!/^\d{1,20}$/.test(env.GITHUB_RUN_ID||'')||!/^\d{1,3}$/.test(env.GITHUB_RUN_ATTEMPT||''))throw new Error('An Actions run identity is required');
 if(cleanupRun&&!/^\d{1,20}-\d{1,3}$/.test(cleanupRun))throw new Error('Cleanup requires the exact previous drill run identity');
 const run=cleanupRun||`${env.GITHUB_RUN_ID}-${env.GITHUB_RUN_ATTEMPT}`;
 const name=`tg-two-way-${config.instance_id}-reliability-${run}`;
 return {config,run,name,database:`${name}-db`,queue:`${name}-jobs`,route:`${config.hostname}${DRILL_PATH}`,version:env.RELEASE_SHA};
}
export function assertDrillOrigin(plan,domains,settings,routes) {
 const domain=domains.filter(d=>d.hostname===plan.config.hostname);
 if(domain.length!==1||domain[0].service!==plan.config.worker_name)throw new Error('Drill host is not owned by the selected test Worker');
 const value=name=>settings.bindings?.find(b=>b.name===name)?.text;
 if(value('INSTANCE_ID')!==plan.config.instance_id||value('EXPECTED_BOT_ID')!==plan.config.expected_bot_id)throw new Error('Selected test Worker identity mismatch');
 for(const r of routes) {
  if(r.pattern===plan.route&&r.script===plan.name)continue;
  const regex=new RegExp(`^${String(r.pattern).replace(/^https?:\/\//i,'').replace(/[.+?^${}()|[\]\\]/g,'\\$&').replaceAll('*','.*')}$`,'i');
  if(regex.test(plan.route))throw new Error('Another zone route already covers the drill path');
 }
}
export function assertDrillBindings(plan,settings,databaseId) {
 if(typeof databaseId!=='string'||!/^(?:[a-f0-9]{32}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})$/.test(databaseId))throw new Error('Drill database identity unavailable');
 const b=settings.bindings||[];
 const text=name=>b.find(x=>x.name===name)?.text;
 if(text('DRILL_RUN_ID')!==plan.run||text('INSTANCE_ID')!==plan.config.instance_id||text('TELEGRAM_BOT_TOKEN')!=='0:reliability-drill-only')throw new Error('Existing drill Worker cannot be adopted');
 if((b.find(x=>x.name==='DB')?.id??b.find(x=>x.name==='DB')?.database_id)!==databaseId)throw new Error('Drill D1 binding mismatch');
 if(b.find(x=>x.name==='JOBS')?.queue_name!==plan.queue)throw new Error('Drill queue binding mismatch');
 const objects=b.filter(x=>x.type==='durable_object_namespace');
 const classes={BOT:'DrillBotCoordinator',CONVERSATIONS:'ConversationCoordinator',STUB:'DrillApiStub'};
 if(objects.length!==3||objects.some(x=>classes[x.name]!==x.class_name||x.script_name&&x.script_name!==plan.name)||new Set(objects.map(x=>x.namespace_id)).size!==3)throw new Error('Drill DO namespace is not independent');
 if(b.some(x=>!['plain_text','secret_text','d1','queue','durable_object_namespace'].includes(x.type)||x.type==='secret_text'&&x.name!=='DRILL_KEY'||x.type==='d1'&&x.name!=='DB'||x.type==='queue'&&x.name!=='JOBS'))throw new Error('Drill must have no external services or Telegram secrets');
}
export function renderDrill(plan,databaseId,revision='1') {
 if(!/^[a-f0-9-]{32,36}$/.test(databaseId))throw new Error('Explicit drill database ID required');
 return {
  name:plan.name,main:'../scripts/reliability-drill/worker.ts',compatibility_date:'2026-10-11',compatibility_flags:['nodejs_compat'],workers_dev:false,preview_urls:false,
  routes:[],
  vars:{INSTANCE_ID:plan.config.instance_id,EXPECTED_BOT_ID:'42424240',ADMIN_CHAT_ID:'-1004242424242',ADMIN_IDS:'42424240',VERSION:plan.version,RECOVERY_EPOCH:`drill-${plan.run}`,TELEGRAM_BOT_TOKEN:'0:reliability-drill-only',TELEGRAM_WEBHOOK_SECRET:'synthetic-drill-webhook',DRILL_RUN_ID:plan.run,DRILL_HOSTNAME:plan.config.hostname,DRILL_REVISION:revision},
  d1_databases:[{binding:'DB',database_name:plan.database,database_id:databaseId,migrations_dir:'../migrations'}],
  queues:{producers:[{binding:'JOBS',queue:plan.queue}],consumers:[{queue:plan.queue,max_batch_size:1,max_batch_timeout:1,max_retries:3}]},
  durable_objects:{bindings:[{name:'BOT',class_name:'DrillBotCoordinator'},{name:'CONVERSATIONS',class_name:'ConversationCoordinator'},{name:'STUB',class_name:'DrillApiStub'}]},
  migrations:[{tag:'reliability-v1',new_sqlite_classes:['DrillBotCoordinator','ConversationCoordinator','DrillApiStub']}],
  observability:{enabled:false},
 };
}
