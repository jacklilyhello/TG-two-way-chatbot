import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { readManifests, validateConfig, validateIsolation, renderWrangler, requireActions } from './instance-config.mjs';
import { apiClient, telegramClient, wrangler, assertBotPermissions, assertRemoteIdentity, assertWebhookChange, assertWebhookOwnership, assertRouteIsolation, installPausedRelease } from './deployment-lib.mjs';

async function main() {
  requireActions();
  const env = process.env;
  const config = validateConfig(JSON.parse(env.INSTANCE_CONFIG_JSON || 'null'), env.INSTANCE_ID);
  const base = JSON.parse(await readFile('wrangler.jsonc', 'utf8'));
  if (base.migrations?.some(m => m.deleted_classes?.length || m.transferred_classes?.length)) throw new Error('Destructive/transferred Durable Object migrations require a separate reviewed recovery procedure');
  validateIsolation([...(await readManifests()).filter(c => c.instance_id !== config.instance_id), config]);
  if (!/^[a-f0-9]{40}$/.test(env.RELEASE_SHA ?? '')) throw new Error('An exact release commit SHA is required');
  if (!/^[a-f0-9]{32}$/.test(env.CLOUDFLARE_ACCOUNT_ID ?? '') || !env.CLOUDFLARE_API_TOKEN) throw new Error('Cloudflare Actions credential/variable is missing');
  if (!/^\d+:[A-Za-z0-9_-]{20,}$/.test(env.TELEGRAM_BOT_TOKEN ?? '') || !/^[A-Za-z0-9_-]{32,256}$/.test(env.TELEGRAM_WEBHOOK_SECRET ?? '')) throw new Error('Instance Telegram secrets are missing or malformed');
  const operation = env.OPERATION || 'status';
  if (!['deploy', 'pause', 'resume', 'recover', 'status', 'export'].includes(operation)) throw new Error('Unsupported operation');
  const cf = apiClient(env.CLOUDFLARE_API_TOKEN);
  const account = `/accounts/${env.CLOUDFLARE_ACCOUNT_ID}`;
  const verification = (await cf(`${account}/tokens/verify`)).result;
  if (verification?.status !== 'active') throw new Error('Cloudflare Account API Token is not active');
  const telegram = telegramClient(env.TELEGRAM_BOT_TOKEN);
  const me = await telegram('getMe');
  if (String(me.id) !== config.expected_bot_id || me.is_bot !== true) throw new Error('Token does not match expected Bot ID');

  async function list(path) {
    const records = [];
    for (let page = 1; page <= 100; page++) {
      const data = await cf(`${path}${path.includes('?') ? '&' : '?'}page=${page}&per_page=100`);
      if (!Array.isArray(data.result)) throw new Error('Cloudflare list response has unexpected shape');
      records.push(...data.result);
      if (!data.result_info || page >= data.result_info.total_pages || data.result.length < 100) return records;
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
    if (script.id === config.worker_name) { assertRemoteIdentity(config, settings); selectedSettings = settings; }
    else if (settings.bindings?.some(b => b.name === 'EXPECTED_BOT_ID' && b.text === config.expected_bot_id)) throw new Error('Expected Bot is already bound to another project Worker');
    if (script.id !== config.worker_name && settings.bindings?.some(b => b.type === 'd1' && (b.database_id ?? b.id) === config.d1_id && config.d1_id)) throw new Error('Another Worker shares this D1 database');
  }
  const selectedNamespaces = selectedSettings?.bindings?.filter(b => b.type === 'durable_object_namespace').map(b => b.namespace_id).filter(Boolean) ?? [];
  for (const entry of projectSettings.filter(entry => entry.name !== config.worker_name)) {
    if (entry.settings.bindings?.some(b => b.type === 'durable_object_namespace' && selectedNamespaces.includes(b.namespace_id))) throw new Error('Another instance shares a Durable Object namespace');
    if (entry.settings.bindings?.some(b => b.type === 'queue' && [config.queue_name, config.dlq_name].includes(b.queue_name))) throw new Error('Another instance shares a queue binding');
  }
  if (scripts.some(s => s.id === config.worker_name) && !selectedSettings) throw new Error('Existing Worker identity cannot be established');
  const databases = await list(`${account}/d1/database`);
  const queues = await list(`${account}/queues`);
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

  async function resource(collection, nameKey, name, idKey, configuredId, createPath, body) {
    const existing = collection.filter(item => item[nameKey] === name);
    if (existing.length > 1) throw new Error('Ambiguous instance resource');
    if (existing.length) {
      if (!configuredId || existing[0][idKey] !== configuredId) throw new Error('Existing resource requires its exact ID in private configuration; no automatic adoption');
      return existing[0][idKey];
    }
    if (configuredId) throw new Error('Configured resource ID was not found under the expected name');
    if (operation !== 'deploy' || env.PROVISION !== 'true') throw new Error('Instance resource is absent; initial deployment requires explicit provisioning');
    const created = (await cf(createPath, { method: 'POST', body })).result;
    if (!created?.[idKey]) throw new Error('Provisioning response lacks resource identifier; inspect state before retry');
    console.log('One scoped instance resource created; record its ID in the private environment before the next run');
    return created[idKey];
  }

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
  const databaseId = await resource(databases, 'name', config.d1_name, 'uuid', config.d1_id, `${account}/d1/database`, { name: config.d1_name, primary_location_hint: 'apac', read_replication: { mode: 'disabled' } });
  const queueId = await resource(queues, 'queue_name', config.queue_name, 'queue_id', config.queue_id, `${account}/queues`, { queue_name: config.queue_name });
  const dlqId = await resource(queues, 'queue_name', config.dlq_name, 'queue_id', config.dlq_id, `${account}/queues`, { queue_name: config.dlq_name });
  for (const queue of queues.filter(q => [queueId, dlqId].includes(q.queue_id))) {
    const q = (await cf(`${account}/queues/${queue.queue_id}`)).result;
    if (q.queue_id !== queue.queue_id || q.queue_name !== queue.queue_name) throw new Error('Queue identity changed during preflight');
    if ((q.consumers_total_count ?? 0) > (q.consumers?.length ?? 0)) throw new Error('Queue consumer inventory is incomplete');
    if (q.queue_id === dlqId && q.consumers?.length) throw new Error('Dead letter queue must not have an automatic consumer');
    if (q.consumers?.some(c => c.script_name !== config.worker_name)) throw new Error('Instance queue has another consumer');
    if (q.producers?.some(p => p.type !== 'worker' || p.script !== config.worker_name)) throw new Error('Instance queue has another producer');
  }
  await mkdir('.local', { recursive: true, mode: 0o700 });
  const configPath = resolve('.local/wrangler.instance.json');
  const workerConfig = renderWrangler(config, databaseId, env.RELEASE_SHA, base);
  // The initial upload must not run bootstrap/cron against absent or old secrets.
  if (operation === 'deploy') workerConfig.vars.PAUSED = 'true';
  await writeFile(configPath, JSON.stringify(workerConfig, null, 2), { mode: 0o600 });

  async function maintenance(action) {
    let response;
    try { response = await fetch(`https://${config.hostname}/internal/maintenance`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Telegram-Bot-Api-Secret-Token': env.TELEGRAM_WEBHOOK_SECRET }, body: JSON.stringify({ action }), signal: AbortSignal.timeout(30000), redirect: 'error' }); }
    catch { throw new Error(`Maintenance ${action} result unknown; inspect status before retry`); }
    if (!response.ok) throw new Error(`Maintenance ${action} rejected (HTTP ${response.status})`);
    const result = await response.json();
    if (result.ok !== true) throw new Error(`Maintenance ${action} did not confirm success`);
    console.log(`Maintenance ${action} confirmed`);
    if (action === 'status') {
      console.log(`Outbound paused: ${result.paused === true}; unresolved issued operations: ${Number.isSafeInteger(result.issued) ? result.issued : 'unavailable'}`);
      for (const item of result.jobs ?? []) if (['pending','retry','running','done','attention','waiting','quarantined'].includes(item.state) && Number.isSafeInteger(item.count)) console.log(`Jobs ${item.state}: ${item.count}`);
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
        if (attempt < 4) await delay(8000);
      }
      if (!drained) throw new Error('Instance paused, but issued operations have not drained; no migration or code change performed');
    }
    await installPausedRelease({
      migrate: () => wrangler(['d1', 'migrations', 'apply', 'DB', '--remote', '--config', configPath], { label: 'compatible migrations' }),
      pause: () => wrangler(['d1', 'execute', 'DB', '--remote', '--command', "INSERT INTO settings(key,value) VALUES('paused','1') ON CONFLICT(key) DO UPDATE SET value='1'", '--config', configPath], { label: 'persistent outbound pause' }),
      uploadPaused: () => wrangler(['deploy', '--config', configPath], { label: 'instance deployment' }),
      installSecrets: () => wrangler(['secret', 'bulk', '--config', configPath], { input: JSON.stringify({ TELEGRAM_BOT_TOKEN: env.TELEGRAM_BOT_TOKEN, TELEGRAM_WEBHOOK_SECRET: env.TELEGRAM_WEBHOOK_SECRET }), label: 'instance secrets' }),
      activate: async () => {
        workerConfig.vars.PAUSED = 'false';
        await writeFile(configPath, JSON.stringify(workerConfig, null, 2), { mode: 0o600 });
        await wrangler(['deploy', '--config', configPath], { label: 'activation with installed secrets' });
      },
    });
    const health = await fetch(`https://${config.hostname}/health`, { redirect: 'error', signal: AbortSignal.timeout(30000) });
    if (!health.ok) throw new Error(`Deployed health endpoint failed (HTTP ${health.status})`);
    const state = await health.json();
    if (state.version !== env.RELEASE_SHA) throw new Error('Health version does not match requested release');
    if (env.CONFIGURE_WEBHOOK === 'true') {
      await telegram('setWebhook', { url: `https://${config.hostname}/webhook`, secret_token: env.TELEGRAM_WEBHOOK_SECRET, allowed_updates: ['message', 'edited_message', 'callback_query'], drop_pending_updates: false });
      const info = await telegram('getWebhookInfo');
      if (info.url !== `https://${config.hostname}/webhook`) throw new Error('Webhook readback does not match this instance');
      console.log('Webhook configured and read back; pending updates preserved');
    }
    // Every release remains paused until an explicit resume run; schema migrations
    // and deployment success do not authorize replay of unknown external effects.
    console.log('Release installed with outbound processing paused. Verify webhook ownership and status, then explicitly resume after reviewing uncertain operations.');
  } else if (operation === 'export') {
    if (env.BACKUP_AGE_RECIPIENT?.match(/^age1[0-9a-z]{58}$/) === null || !env.BACKUP_AGE_RECIPIENT) throw new Error('Encrypted export requires an age public recipient');
    const state = await maintenance('status');
    if (state.paused !== true || state.issued !== 0) throw new Error('Pause and drain outbound processing before export');
    await wrangler(['d1', 'export', 'DB', '--remote', '--output', '.local/database.sql', '--config', configPath], { label: 'private database export' });
    // Encryption runs in a dedicated step; plaintext is never uploaded.
  } else {
    if (operation === 'resume') assertWebhookOwnership(await telegram('getWebhookInfo'), `https://${config.hostname}/webhook`);
    await maintenance(operation);
  }
}

main().catch(error => { console.error(error instanceof SyntaxError ? 'Invalid instance configuration JSON' : error.message); process.exitCode = 1; });
