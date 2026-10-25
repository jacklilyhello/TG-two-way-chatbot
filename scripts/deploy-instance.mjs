import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { readManifests, validateConfig, validateIsolation, renderWrangler, requireActions } from './instance-config.mjs';
import { apiClient, telegramClient, wrangler, assertBotPermissions, assertRemoteIdentity, assertProjectWorkerIsolation, planInstanceResources, assertPlannedQueueOwnership, provisionPlannedResources, assertWebhookChange, assertWebhookOwnership, assertRouteIsolation, installPausedRelease, awaitReleaseHealth } from './deployment-lib.mjs';

/** Keep recipient identities in the selected Environment and Worker Secrets.
 * Missing configuration intentionally disables test broadcasts, including when
 * a previous release installed a now-removed allowlist. */
export function validateBroadcastTestRecipients(instance, value = '') {
  if (value === '') return '';
  if (!['test-a', 'test-b'].includes(instance)) throw new Error('Broadcast test recipient restriction is allowed only on test instances');
  if (typeof value !== 'string') throw new Error('Invalid broadcast test recipient restriction');
  const ids = value.split(',').map(id => id.trim());
  if (!ids.length || ids.length > 20 || ids.some(id => !/^[1-9]\d{0,15}$/.test(id) || !Number.isSafeInteger(Number(id))) || new Set(ids).size !== ids.length) throw new Error('Invalid broadcast test recipient restriction');
  return ids.join(',');
}

export async function runInstance(env = process.env, dependencies = {}) {
  requireActions(env);
  const config = validateConfig(JSON.parse(env.INSTANCE_CONFIG_JSON || 'null'), env.INSTANCE_ID);
  const broadcastTestRecipients = validateBroadcastTestRecipients(config.instance_id, env.BROADCAST_TEST_RECIPIENT_IDS);
  const base = dependencies.base ?? JSON.parse(await readFile('wrangler.jsonc', 'utf8'));
  if (base.migrations?.some(m => m.deleted_classes?.length || m.transferred_classes?.length)) throw new Error('Destructive/transferred Durable Object migrations require a separate reviewed recovery procedure');
  const manifests = dependencies.manifests ?? await readManifests(fileURLToPath(new URL('../instances', import.meta.url)));
  validateIsolation([...manifests.filter(c => c.instance_id !== config.instance_id), config]);
  if (!/^[a-f0-9]{40}$/.test(env.RELEASE_SHA ?? '')) throw new Error('An exact release commit SHA is required');
  if (!/^[a-f0-9]{32}$/.test(env.CLOUDFLARE_ACCOUNT_ID ?? '') || !env.CLOUDFLARE_API_TOKEN) throw new Error('Cloudflare Actions credential/variable is missing');
  if (!/^\d+:[A-Za-z0-9_-]{20,}$/.test(env.TELEGRAM_BOT_TOKEN ?? '') || !/^[A-Za-z0-9_-]{32,256}$/.test(env.TELEGRAM_WEBHOOK_SECRET ?? '')) throw new Error('Instance Telegram secrets are missing or malformed');
  const operation = env.OPERATION || 'status';
  if (!['deploy', 'pause', 'resume', 'recover', 'status', 'export'].includes(operation)) throw new Error('Unsupported operation');
  if (env.PROVISION === 'true' && (operation !== 'deploy' || env.INITIALIZATION_LOCK_HELD !== 'true')) throw new Error('Provisioning requires deploy through the current main Actions workflow with the account initialization lock');
  const cf = dependencies.cf ?? apiClient(env.CLOUDFLARE_API_TOKEN);
  const telegram = dependencies.telegram ?? telegramClient(env.TELEGRAM_BOT_TOKEN);
  const invoke = dependencies.wrangler ?? wrangler;
  const fetcher = dependencies.fetcher ?? fetch;
  const wait = dependencies.wait ?? delay;
  const write = dependencies.writeFile ?? writeFile;
  const makeDirectory = dependencies.mkdir ?? mkdir;
  const log = dependencies.log ?? console.log;
  const account = `/accounts/${env.CLOUDFLARE_ACCOUNT_ID}`;
  const verification = (await cf(`${account}/tokens/verify`)).result;
  if (verification?.status !== 'active') throw new Error('Cloudflare Account API Token is not active');
  const me = await telegram('getMe');
  if (String(me.id) !== config.expected_bot_id || me.is_bot !== true) throw new Error('Token does not match expected Bot ID');

  async function list(path) {
    const records = [];
    for (let page = 1; page <= 100; page++) {
      const data = await cf(`${path}${path.includes('?') ? '&' : '?'}page=${page}&per_page=100`);
      if (!Array.isArray(data.result)) throw new Error('Cloudflare list response has unexpected shape');
      records.push(...data.result);
      const totalPages = data.result_info?.total_pages;
      if (Number.isSafeInteger(totalPages) && totalPages >= page) {
        if (page === totalPages) return records;
      } else if (data.result.length < 100) return records;
    }
    throw new Error('Cloudflare inventory exceeded safe listing budget');
  }
  const scripts = await list(`${account}/workers/scripts`);
  let selectedSettings;
  const projectSettings = [];
  // Binding inventory is read-only, including unrelated Workers, so an existing
  // selected resource cannot silently be shared with another application.
  for (const script of scripts) {
    const settings = (await cf(`${account}/workers/scripts/${encodeURIComponent(script.id)}/settings`)).result;
    projectSettings.push({ name: script.id, settings });
    if (script.id === config.worker_name) selectedSettings = settings;
  }
  assertProjectWorkerIsolation(config, projectSettings);
  const selectedNamespaces = selectedSettings?.bindings?.filter(b => b.type === 'durable_object_namespace').map(b => b.namespace_id).filter(Boolean) ?? [];
  for (const entry of projectSettings.filter(entry => entry.name !== config.worker_name)) {
    if (entry.settings.bindings?.some(b => b.type === 'd1' && (b.database_id ?? b.id) === config.d1_id && config.d1_id)) throw new Error('Another Worker shares this D1 database');
    if (entry.settings.bindings?.some(b => b.type === 'durable_object_namespace' && selectedNamespaces.includes(b.namespace_id))) throw new Error('Another instance shares a Durable Object namespace');
    if (entry.settings.bindings?.some(b => b.type === 'queue' && [config.queue_name, config.dlq_name].includes(b.queue_name))) throw new Error('Another instance shares a queue binding');
  }
  if (scripts.some(s => s.id === config.worker_name) && !selectedSettings) throw new Error('Existing Worker identity cannot be established');
  const databases = await list(`${account}/d1/database`);
  const queues = await list(`${account}/queues`);
  const resources = planInstanceResources(config, { databases, queues }, { operation, provision: env.PROVISION === 'true', workerExists: Boolean(selectedSettings), initializationLockHeld: env.INITIALIZATION_LOCK_HELD === 'true' });
  if (selectedSettings) assertRemoteIdentity(config, selectedSettings);
  const domains = await list(`${account}/workers/domains`);
  const zones = await list(`/zones?name=lilyya.top&account.id=${env.CLOUDFLARE_ACCOUNT_ID}`);
  if (zones.length !== 1 || zones[0].name !== 'lilyya.top') throw new Error('The authorized lilyya.top zone could not be identified');
  const zone = zones[0].id;
  const domain = domains.find(d => d.hostname === config.hostname);
  if (domain && domain.service !== config.worker_name) throw new Error('Selected hostname belongs to another Worker');
  const dns = await list(`/zones/${zone}/dns_records?name=${encodeURIComponent(config.hostname)}`);
  if (dns.length && !domain) throw new Error('Selected hostname has existing DNS records; refusing takeover');
  const routes = await list(`/zones/${zone}/workers/routes`);
  assertRouteIsolation(routes, config.hostname, config.worker_name);

  await assertPlannedQueueOwnership(resources, cf, account, config.worker_name);

  // All Telegram/platform read checks precede the first Cloudflare write.
  if (operation === 'deploy') {
    const chat = await telegram('getChat', { chat_id: config.admin_chat_id });
    const member = await telegram('getChatMember', { chat_id: config.admin_chat_id, user_id: me.id });
    assertBotPermissions(me, chat, member, config);
    for (const userId of config.admin_ids) {
      const admin = await telegram('getChatMember', { chat_id: config.admin_chat_id, user_id: Number(userId) });
      if (!['administrator', 'creator'].includes(admin.status)) throw new Error('Configured operator is not a Forum administrator');
    }
    const webhook = await telegram('getWebhookInfo');
    if (env.CONFIGURE_WEBHOOK === 'true') assertWebhookChange(webhook, `https://${config.hostname}/webhook`, env.OLD_CONSUMER_STOPPED === 'true');
  }
  const { d1_id: databaseId } = await provisionPlannedResources(resources, cf, account, log);
  await makeDirectory('.local', { recursive: true, mode: 0o700 });
  const configPath = resolve('.local/wrangler.instance.json');
  const workerConfig = renderWrangler(config, databaseId, env.RELEASE_SHA, base);
  // The initial upload must not run bootstrap/cron against absent or old secrets.
  if (operation === 'deploy') workerConfig.vars.PAUSED = 'true';
  await write(configPath, JSON.stringify(workerConfig, null, 2), { mode: 0o600 });

  async function maintenance(action) {
    let response;
    try { response = await fetcher(`https://${config.hostname}/internal/maintenance`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Telegram-Bot-Api-Secret-Token': env.TELEGRAM_WEBHOOK_SECRET }, body: JSON.stringify({ action }), signal: AbortSignal.timeout(30000), redirect: 'error' }); }
    catch { throw new Error(`Maintenance ${action} result unknown; inspect status before retry`); }
    if (!response.ok) throw new Error(`Maintenance ${action} rejected (HTTP ${response.status})`);
    const result = await response.json();
    if (result.ok !== true) throw new Error(`Maintenance ${action} did not confirm success`);
    log(`Maintenance ${action} confirmed`);
    if (action === 'status') {
      log(`Installed release: ${/^[a-f0-9]{40}$/.test(result.version ?? '') ? result.version : 'unavailable'}`);
      log(`Outbound paused: ${result.paused === true}; unresolved issued operations: ${Number.isSafeInteger(result.issued) ? result.issued : 'unavailable'}`);
      for (const item of result.jobs ?? []) if (['pending','retry','running','done','attention','waiting','quarantined'].includes(item.state) && Number.isSafeInteger(item.count)) log(`Jobs ${item.state}: ${item.count}`);
    }
    return result;
  }
  if (operation === 'deploy') {
    if (selectedSettings) {
      await maintenance('pause');
      let drained = false;
      for (let attempt = 0; attempt < 5; attempt++) {
        const state = await maintenance('status');
        if (state.paused === true && state.issued === 0) { drained = true; break; }
        if (attempt < 4) await wait(8000);
      }
      if (!drained) throw new Error('Instance paused, but issued operations have not drained; no migration or code change performed');
    }
    await installPausedRelease({
      migrate: () => invoke(['d1', 'migrations', 'apply', 'DB', '--remote', '--config', configPath], { label: 'compatible migrations' }),
      pause: () => invoke(['d1', 'execute', 'DB', '--remote', '--command', "INSERT INTO settings(key,value) VALUES('paused','1') ON CONFLICT(key) DO UPDATE SET value='1'", '--config', configPath], { label: 'persistent outbound pause' }),
      uploadPaused: () => invoke(['deploy', '--config', configPath], { label: 'instance deployment' }),
      installSecrets: () => invoke(['secret', 'bulk', '--config', configPath], { input: JSON.stringify({ TELEGRAM_BOT_TOKEN: env.TELEGRAM_BOT_TOKEN, TELEGRAM_WEBHOOK_SECRET: env.TELEGRAM_WEBHOOK_SECRET, ...(['test-a', 'test-b'].includes(config.instance_id) ? { BROADCAST_TEST_RECIPIENT_IDS: broadcastTestRecipients || 'disabled' } : {}) }), label: 'instance secrets' }),
      activate: async () => {
        workerConfig.vars.PAUSED = 'false';
        await write(configPath, JSON.stringify(workerConfig, null, 2), { mode: 0o600 });
        await invoke(['deploy', '--config', configPath], { label: 'activation with installed secrets' });
      },
    });
    await awaitReleaseHealth(`https://${config.hostname}/health`, env.RELEASE_SHA, { fetcher, wait });
    if (env.CONFIGURE_WEBHOOK === 'true') {
      await telegram('setWebhook', { url: `https://${config.hostname}/webhook`, secret_token: env.TELEGRAM_WEBHOOK_SECRET, allowed_updates: ['message', 'edited_message', 'callback_query'], drop_pending_updates: false });
      const info = await telegram('getWebhookInfo');
      if (info.url !== `https://${config.hostname}/webhook`) throw new Error('Webhook readback does not match this instance');
      log('Webhook configured and read back; pending updates preserved');
    }
    // Every release remains paused until an explicit resume run; schema migrations
    // and deployment success do not authorize replay of unknown external effects.
    log('Release installed with outbound processing paused. Verify webhook ownership and status, then explicitly resume after reviewing uncertain operations.');
  } else if (operation === 'export') {
    if (env.BACKUP_AGE_RECIPIENT?.match(/^age1[0-9a-z]{58}$/) === null || !env.BACKUP_AGE_RECIPIENT) throw new Error('Encrypted export requires an age public recipient');
    const state = await maintenance('status');
    if (state.paused !== true || state.issued !== 0) throw new Error('Pause and drain outbound processing before export');
    await invoke(['d1', 'export', 'DB', '--remote', '--output', '.local/database.sql', '--config', configPath], { label: 'private database export' });
    // Encryption runs in a dedicated step; plaintext is never uploaded.
  } else {
    if (operation === 'resume') assertWebhookOwnership(await telegram('getWebhookInfo'), `https://${config.hostname}/webhook`);
    await maintenance(operation);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) runInstance().catch(error => { console.error(error instanceof SyntaxError ? 'Invalid instance configuration JSON' : error.message); process.exitCode = 1; });
