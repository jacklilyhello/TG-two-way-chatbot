import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile, unlink, stat } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { validateConfig, validateIsolation, readManifests, renderWrangler, requireActions } from './instance-config.mjs';
import { apiClient, telegramClient, wrangler, assertRemoteIdentity, assertBotPermissions, assertWebhookOwnership, assertRouteIsolation, assertQueueConsumers } from './deployment-lib.mjs';

const bookmarkPattern = /^[a-f0-9]{8}-[a-f0-9]{8}-[a-f0-9]{8}-[a-f0-9]{32}$/;
const epochPattern = /^[a-zA-Z0-9_-]{1,64}$/;
const digest = value => createHash('sha256').update(value).digest('hex');
const binding = (settings, name) => settings.bindings?.find(item => item.name === name)?.text;
const namespaces = settings => settings.bindings.filter(item => item.type === 'durable_object_namespace').map(item => item.namespace_id).sort();
const privateDirectory = '.local/recovery-drill';
export const maintenanceEntrySource = "import runtime from '../../src/index';\nexport { BotCoordinator, ConversationCoordinator } from '../../src/index';\nexport default { ...runtime, async fetch(request:Request, env:any) { if(new URL(request.url).pathname==='/webhook') return new Response('Recovery maintenance',{status:503}); return runtime.fetch(request,env); }, async scheduled() {} };\n";

// Before dispatch, the operator saves the fresh epoch in INSTANCE_CONFIG_JSON
// with `gh secret set ...` via stdin. No secret-management token is needed here.
export function validateDrillInput(env) {
  requireActions(env);
  if (!['test-a', 'test-b'].includes(env.INSTANCE_ID) || env.TEST_RECOVERY_DRILL_ALLOWED !== 'true' || env.CONFIRM_TEST_RESTORE !== 'true') throw new Error('Recovery drills require an explicitly enabled test-a/test-b Environment');
  let raw;
  try { raw = JSON.parse(env.INSTANCE_CONFIG_JSON); } catch { throw new Error('Invalid private instance configuration JSON'); }
  const config = validateConfig(raw, env.INSTANCE_ID);
  if (!config.d1_id || !config.queue_id || !config.dlq_id) throw new Error('Recovery requires explicit existing resource IDs');
  if (!/^[a-f0-9]{40}$/.test(env.RELEASE_SHA ?? '') || !/^\d+$/.test(env.GITHUB_RUN_ID ?? '')) throw new Error('Recovery requires an immutable release and Actions run identity');
  if (!bookmarkPattern.test(env.RESTORE_BOOKMARK ?? '') || !/^[a-f0-9]{64}$/.test(env.RESTORE_BOOKMARK_SHA256 ?? '') || digest(env.RESTORE_BOOKMARK) !== env.RESTORE_BOOKMARK_SHA256) throw new Error('Explicit bookmark selection must match the private test recovery bookmark');
  if (!/^[a-f0-9]{64}$/.test(env.NEW_RECOVERY_EPOCH_SHA256 ?? '') || digest(config.recovery_epoch) !== env.NEW_RECOVERY_EPOCH_SHA256) throw new Error('Fresh recovery epoch selection must match the private Environment configuration');
  if (!/^age1[0-9a-z]{58}$/.test(env.BACKUP_AGE_RECIPIENT ?? '')) throw new Error('Recovery requires an offline age backup recipient');
  if (!/^[a-f0-9]{32}$/.test(env.CLOUDFLARE_ACCOUNT_ID ?? '') || !env.CLOUDFLARE_API_TOKEN || !/^\d+:[A-Za-z0-9_-]{20,}$/.test(env.TELEGRAM_BOT_TOKEN ?? '') || !/^[A-Za-z0-9_-]{32,256}$/.test(env.TELEGRAM_WEBHOOK_SECRET ?? '')) throw new Error('Recovery credentials are missing or malformed');
  return config;
}

export function assertDrillWorker(config, settings, expected) {
  assertRemoteIdentity(config, settings);
  if (binding(settings, 'VERSION') !== expected.version || binding(settings, 'ADMIN_IDS') !== config.admin_ids.join(',')) throw new Error('Deployed version or administrators differ from selected private configuration');
  const epoch = binding(settings, 'RECOVERY_EPOCH');
  if (!epochPattern.test(epoch ?? '') || (expected.epoch && epoch !== expected.epoch)) throw new Error('Deployed recovery epoch does not match this drill stage');
  const ids = namespaces(settings);
  if (ids.some(id => typeof id !== 'string' || !id) || new Set(ids).size !== 2) throw new Error('Existing independent coordinator namespaces are required');
  if (expected.namespaces && JSON.stringify(ids) !== JSON.stringify(expected.namespaces)) throw new Error('Recovery must preserve both existing coordinator namespaces');
  if (expected.stage) {
    if (binding(settings, 'PAUSED') !== 'true' || binding(settings, 'RECOVERY_DRILL_STAGE') !== expected.stage || binding(settings, 'RECOVERY_DRILL_RUN') !== expected.runId) throw new Error('Static recovery pause or run ownership is not confirmed');
  } else if (binding(settings, 'RECOVERY_DRILL_STAGE')) throw new Error('An earlier recovery maintenance release remains installed; inspect its outcome before another drill');
  return epoch;
}

const aggregateSql = `SELECT
 (SELECT COUNT(*) FROM users) users,
 (SELECT COUNT(*) FROM messages) messages,
 (SELECT COUNT(*) FROM messages WHERE epoch!=? AND delivery='pending') old_pending_messages,
 (SELECT COUNT(*) FROM jobs) jobs,
 (SELECT COUNT(*) FROM jobs WHERE epoch!=? AND state NOT IN ('done','quarantined')) old_unfinished_jobs,
 (SELECT COUNT(*) FROM jobs WHERE state='quarantined') quarantined_jobs,
 (SELECT COUNT(*) FROM operations) operations,
 (SELECT COUNT(*) FROM operations WHERE phase='issued') issued_operations,
 (SELECT COUNT(*) FROM operations WHERE phase='unknown') unknown_operations,
 (SELECT COUNT(*) FROM operations WHERE phase='success') successful_operations,
 (SELECT COUNT(*) FROM operations WHERE phase='failed') failed_operations,
 (SELECT COUNT(*) FROM deletion_batches WHERE state!='done') unfinished_deletions`;
const aggregateFields = ['users','messages','old_pending_messages','jobs','old_unfinished_jobs','quarantined_jobs','operations','issued_operations','unknown_operations','successful_operations','failed_operations','unfinished_deletions'];

export function sanitizeAggregate(row) {
  const safe = {};
  for (const name of aggregateFields) {
    if (!Number.isSafeInteger(row?.[name]) || row[name] < 0) throw new Error('Invalid aggregate recovery evidence');
    safe[name] = row[name];
  }
  return safe;
}

export function assertRecoveryEvidence(restored, final) {
  if (final.old_unfinished_jobs !== 0 || final.issued_operations !== 0) throw new Error('Restored work has not been fully quarantined or reconciled');
  if (final.users !== restored.users || final.messages !== restored.messages || final.jobs < restored.jobs || final.operations < restored.operations || final.quarantined_jobs < restored.quarantined_jobs + restored.old_unfinished_jobs || final.unknown_operations < restored.unknown_operations || final.successful_operations < restored.successful_operations || final.failed_operations < restored.failed_operations) throw new Error('Restored business records or terminal outcomes were not preserved');
}

async function encryptFile(file, recipient) {
  try {
    await new Promise((done, reject) => {
      const child = spawn('age', ['-r', recipient, '-o', `${file}.age`, file], { stdio: ['ignore', 'ignore', 'ignore'] });
      child.on('error', () => reject(new Error('Backup encryption could not start')));
      child.on('close', code => code === 0 ? done() : reject(new Error('Backup encryption failed; restore was not permitted')));
    });
  } finally { await unlink(file).catch(() => {}); }
}

/** Both phases fail closed. A failed/unknown restore is never retried here. */
export async function runDrill(phase, dependencies = {}) {
  if (!['prepare','restore'].includes(phase)) throw new Error('Choose prepare or restore');
  const env = dependencies.env ?? process.env;
  const config = validateDrillInput(env);
  const cwd = dependencies.cwd ?? process.cwd();
  const directory = resolve(cwd, privateDirectory);
  const base = dependencies.base ?? JSON.parse(await readFile(resolve(cwd, 'wrangler.jsonc'), 'utf8'));
  if (base.migrations?.some(item => item.deleted_classes?.length || item.transferred_classes?.length)) throw new Error('Destructive coordinator migrations are forbidden in a recovery drill');
  const manifests = dependencies.manifests ?? await readManifests(resolve(cwd, 'instances'));
  validateIsolation([...manifests.filter(item => item.instance_id !== config.instance_id), config]);
  const cf = dependencies.cf ?? apiClient(env.CLOUDFLARE_API_TOKEN);
  const telegram = dependencies.telegram ?? telegramClient(env.TELEGRAM_BOT_TOKEN);
  const invoke = dependencies.wrangler ?? wrangler;
  const fetcher = dependencies.fetcher ?? fetch;
  const encrypt = dependencies.encrypt ?? encryptFile;
  const wait = dependencies.wait ?? delay;
  const account = `/accounts/${env.CLOUDFLARE_ACCOUNT_ID}`;
  const database = `${account}/d1/database/${config.d1_id}`;
  const worker = `${account}/workers/scripts/${config.worker_name}`;
  const configPath = resolve(directory, 'wrangler.json');
  const statePath = resolve(directory, 'state.json');
  const evidencePath = resolve(directory, 'evidence.json');
  let evidence = { release_sha: env.RELEASE_SHA, phase, outbound_paused: true, webhook_retry_during_restore: true };
  const saveEvidence = async stage => { evidence.stage = stage; await writeFile(evidencePath, JSON.stringify(evidence, null, 2), { mode: 0o600 }); };
  const settings = async () => (await cf(`${worker}/settings`)).result;
  async function query(sql, params = []) {
    const data = await cf(`${database}/query`, { method: 'POST', body: { sql, params } });
    if (!Array.isArray(data.result) || data.result.length !== 1 || data.result[0].success !== true || !Array.isArray(data.result[0].results)) throw new Error('Scoped D1 query did not confirm success');
    return data.result[0].results;
  }
  const aggregate = async () => sanitizeAggregate((await query(aggregateSql, [config.recovery_epoch,config.recovery_epoch]))[0]);
  async function unusedEpoch() {
    const rows = await query("SELECT (SELECT COUNT(*) FROM jobs WHERE epoch=?) + (SELECT COUNT(*) FROM messages WHERE epoch=?) + (SELECT COUNT(*) FROM operations WHERE substr(op_key,1,length(?)+1)=?||':') n", Array(4).fill(config.recovery_epoch));
    if (rows[0]?.n !== 0) throw new Error('Recovery epoch has already been used; choose and privately save a fresh epoch');
  }
  async function maintenance(action, epoch) {
    let response;
    try { response = await fetcher(`https://${config.hostname}/internal/maintenance`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Telegram-Bot-Api-Secret-Token': env.TELEGRAM_WEBHOOK_SECRET }, body: JSON.stringify({ action }), redirect: 'error', signal: AbortSignal.timeout(30000) }); }
    catch { throw new Error('Maintenance result unknown; inspect the paused instance before continuing'); }
    let data;
    try { data = await response.json(); } catch { throw new Error('Maintenance returned an invalid response'); }
    if (!response.ok || data.ok !== true || data.version !== env.RELEASE_SHA || data.epoch !== epoch || data.paused !== true) throw new Error('Maintenance did not confirm selected version, epoch and outbound pause');
    return data;
  }
  async function drain(epoch) {
    for (let attempt = 0; attempt < 20; attempt++) {
      const data = await maintenance('status', epoch);
      if (data.issued === 0) return;
      if (!Number.isSafeInteger(data.issued) || data.issued < 0) throw new Error('Invalid issued operation count');
      if (attempt < 19) await wait(5000);
    }
    throw new Error('Issued operations did not drain; static pause must remain installed');
  }
  async function install(epoch, stage, ids) {
    const rendered = renderWrangler({ ...config, recovery_epoch: epoch }, config.d1_id, env.RELEASE_SHA, base);
    rendered.main = './maintenance.ts';
    rendered.d1_databases[0].migrations_dir = '../../migrations';
    rendered.vars.PAUSED = 'true';
    rendered.vars.RECOVERY_DRILL_STAGE = stage;
    rendered.vars.RECOVERY_DRILL_RUN = env.GITHUB_RUN_ID;
    // The maintenance source exists only in this runner, never in the repo.
    await writeFile(resolve(directory, 'maintenance.ts'), maintenanceEntrySource, { mode: 0o600 });
    await writeFile(configPath, JSON.stringify(rendered), { mode: 0o600 });
    await invoke(['deploy','--config',configPath], { label: 'test recovery static maintenance gate' });
    assertDrillWorker(config, await settings(), { version: env.RELEASE_SHA, epoch, stage, runId: env.GITHUB_RUN_ID, namespaces: ids });
    const webhook = await fetcher(`https://${config.hostname}/webhook`, { method: 'POST', redirect: 'error', signal: AbortSignal.timeout(30000) });
    await webhook.body?.cancel();
    if (webhook.status !== 503) throw new Error('Maintenance webhook retry gate was not confirmed');
    await maintenance('status', epoch);
  }
  async function list(path) {
    const result = [];
    for (let page = 1; page <= 100; page++) {
      const data = await cf(`${path}${path.includes('?')?'&':'?'}page=${page}&per_page=100`);
      if (!Array.isArray(data.result)) throw new Error('Cloud resource inventory is invalid');
      result.push(...data.result);
      if (data.result.length < 100 || (Number.isSafeInteger(data.result_info?.total_pages) && page >= data.result_info.total_pages)) return result;
    }
    throw new Error('Cloud resource inventory exceeded the safe bound');
  }
  async function preflight() {
    if ((await cf(`${account}/tokens/verify`)).result?.status !== 'active') throw new Error('Cloudflare Account API Token is not active');
    const selected = await settings();
    const info = (await cf(database)).result;
    if (info?.uuid !== config.d1_id || info.name !== config.d1_name || info.version !== 'production') throw new Error('Selected test D1 identity or Time Travel storage is not confirmed');
    const scripts = await list(`${account}/workers/scripts`);
    for (const item of scripts.filter(item => item.id !== config.worker_name)) {
      const other = (await cf(`${account}/workers/scripts/${encodeURIComponent(item.id)}/settings`)).result;
      if (other.bindings?.some(b => (b.name === 'EXPECTED_BOT_ID' && b.text === config.expected_bot_id) || (b.type === 'd1' && (b.database_id ?? b.id) === config.d1_id) || (b.type === 'queue' && [config.queue_name,config.dlq_name].includes(b.queue_name)) || (b.type === 'durable_object_namespace' && namespaces(selected).includes(b.namespace_id)))) throw new Error('Another Worker shares a protected selected test resource');
    }
    const domains = await list(`${account}/workers/domains`);
    if (!domains.some(item => item.hostname === config.hostname && item.service === config.worker_name)) throw new Error('Selected test hostname ownership is not confirmed');
    const zones = await list(`/zones?name=lilyya.top&account.id=${env.CLOUDFLARE_ACCOUNT_ID}`);
    if (zones.length !== 1 || zones[0].name !== 'lilyya.top') throw new Error('Authorized zone was not identified');
    assertRouteIsolation(await list(`/zones/${zones[0].id}/workers/routes`), config.hostname, config.worker_name);
    for (const [id, name] of [[config.queue_id,config.queue_name],[config.dlq_id,config.dlq_name]]) {
      const queue = (await cf(`${account}/queues/${id}`)).result;
      if (queue?.queue_id !== id || queue.queue_name !== name) throw new Error('Selected test queue identity is not confirmed');
      if (!Array.isArray(queue.consumers)) throw new Error('Selected test queue consumers were not enumerated');
      if ((queue.consumers_total_count ?? 0) > (queue.consumers?.length ?? 0)) throw new Error('Selected test queue consumer inventory is incomplete');
      if (id === config.dlq_id && queue.consumers?.length) throw new Error('Selected test failure queue has an automatic consumer');
      assertQueueConsumers(queue.consumers,config.worker_name);
      if (queue.producers?.some(item => item.type !== 'worker' || item.script !== config.worker_name)) throw new Error('Selected test queue has a foreign producer');
    }
    const me = await telegram('getMe');
    assertBotPermissions(me, await telegram('getChat',{chat_id:config.admin_chat_id}), await telegram('getChatMember',{chat_id:config.admin_chat_id,user_id:me.id}),config);
    assertWebhookOwnership(await telegram('getWebhookInfo'), `https://${config.hostname}/webhook`);
    return selected;
  }

  await mkdir(directory, { recursive: true, mode: 0o700 });
  try {
    const selected = await preflight();
    if (phase === 'prepare') {
      const oldEpoch = assertDrillWorker(config, selected, { version: env.RELEASE_SHA });
      if (oldEpoch === config.recovery_epoch) throw new Error('New recovery epoch must differ from the currently deployed epoch');
      await unusedEpoch();
      await maintenance('pause', oldEpoch);
      await drain(oldEpoch);
      const ids = namespaces(selected);
      await install(oldEpoch,'prepared',ids);
      evidence.before = await aggregate();
      const currentBookmark = (await cf(`${database}/time_travel/bookmark`)).result?.bookmark;
      // D1 documents bookmarks as lexicographically sortable, oldest to newest:
      // https://developers.cloudflare.com/d1/reference/time-travel/#bookmarks
      if (!bookmarkPattern.test(currentBookmark ?? '') || env.RESTORE_BOOKMARK >= currentBookmark) throw new Error('Explicit restore bookmark must precede the selected database current bookmark');
      await invoke(['d1','export','DB','--remote','--output',resolve(directory,'database-before.sql'),'--config',configPath], { label: 'private pre-restore test backup' });
      await encrypt(resolve(directory,'database-before.sql'),env.BACKUP_AGE_RECIPIENT);
      if (!(await stat(resolve(directory,'database-before.sql.age'))).size) throw new Error('Encrypted database backup is empty');
      const state = { config_digest:digest(JSON.stringify(config)), release:env.RELEASE_SHA, run:env.GITHUB_RUN_ID, bookmark:env.RESTORE_BOOKMARK, current_bookmark:currentBookmark, old_epoch:oldEpoch, namespaces:ids, before:evidence.before };
      await writeFile(statePath,JSON.stringify(state),{mode:0o600});
      await writeFile(resolve(directory,'restore-points.json'),JSON.stringify({database_id:config.d1_id,restore_bookmark:env.RESTORE_BOOKMARK,before_bookmark:currentBookmark,release:env.RELEASE_SHA}),{mode:0o600});
      await encrypt(resolve(directory,'restore-points.json'),env.BACKUP_AGE_RECIPIENT);
      await saveEvidence('encrypted_backup_ready');
      console.log('Test recovery prepared: static outbound pause, webhook retry gate and encrypted backup confirmed. Restore has not run.');
    } else {
      if (env.RECOVERY_BACKUP_SAVED !== 'true') throw new Error('Encrypted backup artifact must be saved before restore');
      let state;
      try { state = JSON.parse(await readFile(statePath,'utf8')); } catch { throw new Error('Private prepared recovery state is unavailable'); }
      if (state.config_digest !== digest(JSON.stringify(config)) || state.release !== env.RELEASE_SHA || state.run !== env.GITHUB_RUN_ID || state.bookmark !== env.RESTORE_BOOKMARK) throw new Error('Prepared recovery ownership does not match selected Environment or run');
      assertDrillWorker(config,selected,{version:env.RELEASE_SHA,epoch:state.old_epoch,stage:'prepared',runId:env.GITHUB_RUN_ID,namespaces:state.namespaces});
      if (!(await stat(resolve(directory,'database-before.sql.age'))).size) throw new Error('Encrypted pre-restore backup is unavailable');
      evidence.before = sanitizeAggregate(state.before);
      await install(state.old_epoch,'restoring',state.namespaces);
      // This POST is deliberately single-shot. The persistent Worker stage also
      // refuses a blind rerun after a timeout or an interrupted runner.
      const restored = (await cf(`${database}/time_travel/restore?bookmark=${encodeURIComponent(env.RESTORE_BOOKMARK)}`,{method:'POST'})).result;
      if (!bookmarkPattern.test(restored?.bookmark ?? '') || !bookmarkPattern.test(restored?.previous_bookmark ?? '')) throw new Error('Restore outcome lacks confirmed recovery bookmarks; inspect while statically paused');
      await invoke(['d1','migrations','apply','DB','--remote','--config',configPath],{label:'restored test schema compatibility'});
      await query("INSERT INTO settings(key,value) VALUES('paused','1') ON CONFLICT(key) DO UPDATE SET value='1'");
      await unusedEpoch();
      evidence.restored = await aggregate();
      await install(config.recovery_epoch,'restored',state.namespaces);
      await maintenance('recover',config.recovery_epoch);
      await drain(config.recovery_epoch);
      evidence.final = await aggregate();
      assertRecoveryEvidence(evidence.restored,evidence.final);
      evidence.epoch_changed = true;
      evidence.unknown_not_replayed = true;
      await saveEvidence('restored_quarantined_still_paused');
      console.log(JSON.stringify(evidence));
      console.log('Test restore complete and still statically paused. Keep the fresh private epoch, review evidence, then use normal deploy/status/explicit resume.');
    }
    return evidence;
  } catch (error) {
    await saveEvidence('failed_keep_paused').catch(() => {});
    throw error;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  runDrill(process.argv[2]).catch(error => { console.error(error instanceof SyntaxError ? 'Invalid private recovery JSON' : error.message); process.exitCode = 1; });
}
