import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { assertDrillBindings } from './reliability-drill-lib.mjs';

export async function awaitReleaseHealth(url, version, { fetcher = fetch, wait = delay, attempts = 6 } = {}) {
  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const response = await fetcher(url, { method: 'GET', redirect: 'error', signal: AbortSignal.timeout(10000) });
      if (response.ok && (await response.json()).version === version) return;
    } catch { /* A new custom domain may need DNS/TLS propagation; only repeat reads. */ }
    if (attempt + 1 < attempts) await wait(5000);
  }
  throw new Error('Deployed health endpoint did not confirm requested release after bounded read-only readiness checks');
}

export function assertWebhookChange(info, url, stopped) {
  if (info.url !== url && stopped !== true) throw new Error('Webhook takeover requires confirmed stopped old consumer and drained outbound requests');
}

export function assertWebhookOwnership(info, url) {
  if (info?.url !== url) throw new Error('Resume refused: Telegram webhook does not belong to the selected instance');
}

/** Installation failures always leave the persistent outbound gate closed. */
export async function installPausedRelease(steps) {
  await steps.migrate();
  await steps.pause();
  await steps.uploadPaused();
  await steps.installSecrets();
  await steps.activate();
}

export function assertRouteIsolation(routes, hostname, workerName) {
  for (const route of routes) {
    if (typeof route.pattern !== 'string') throw new Error('Worker route inventory has an invalid pattern');
    // A path-specific route still occupies this host. Testing only '/' would
    // overlook an existing '/webhook*' route and install a conflicting origin.
    const host = route.pattern.replace(/^https?:\/\//i, '').split('/')[0].toLowerCase();
    const pattern = host.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replaceAll('*', '.*');
    if (new RegExp(`^${pattern}$`).test(hostname) && route.script !== workerName) throw new Error('A zone route already controls the selected hostname');
  }
}

export function assertRemoteIdentity(config, settings) {
  const bindings = settings.bindings ?? [];
  const value = name => bindings.find(binding => binding.name === name)?.text;
  if (value('INSTANCE_ID') !== config.instance_id || value('EXPECTED_BOT_ID') !== config.expected_bot_id || value('ADMIN_CHAT_ID') !== config.admin_chat_id) throw new Error('Existing Worker identity does not match selected instance');
  const db = bindings.find(binding => binding.name === 'DB');
  if (!config.d1_id || (db?.database_id ?? db?.id) !== config.d1_id) throw new Error('Existing Worker D1 binding does not match explicit database ID');
  const jobs = bindings.find(binding => binding.name === 'JOBS');
  if (jobs?.queue_name !== config.queue_name) throw new Error('Existing Worker queue binding does not match selected instance');
  const coordinators = bindings.filter(b => b.type === 'durable_object_namespace');
  if (coordinators.length !== 2 || !coordinators.some(b => b.name === 'CONVERSATIONS' && b.class_name === 'ConversationCoordinator') || !coordinators.some(b => b.name === 'BOT' && b.class_name === 'BotCoordinator')) throw new Error('Existing Worker coordinator bindings are incomplete');
  for (const binding of bindings.filter(b => b.type === 'durable_object_namespace')) {
    if (binding.script_name && binding.script_name !== config.worker_name) throw new Error('Cross-Worker Durable Object namespace is forbidden');
  }
}

/** Read actual Worker settings, rather than trusting committed example manifests. */
export function assertProjectWorkerIsolation(config, workers) {
  for (const { name, settings } of workers) {
    const bindings = settings?.bindings;
    if (!Array.isArray(bindings)) throw new Error('Worker binding inventory is incomplete; inspect account Worker settings before retrying');
    const markers = ['INSTANCE_ID', 'EXPECTED_BOT_ID', 'ADMIN_CHAT_ID'];
    const isProject = name.startsWith('tg-two-way-') || bindings.some(binding => markers.includes(binding.name));
    if (!isProject) continue;
    const text = key => {
      const matches = bindings.filter(binding => binding.name === key);
      return matches.length === 1 ? matches[0].text : undefined;
    };
    const instance = text('INSTANCE_ID');
    const bot = text('EXPECTED_BOT_ID');
    const chat = text('ADMIN_CHAT_ID');
    const ownershipError = () => new Error('Project Worker ownership cannot be confirmed; inspect INSTANCE_ID, EXPECTED_BOT_ID and ADMIN_CHAT_ID bindings and any retained reliability drill before retrying');
    if (!/^[a-z][a-z0-9-]{0,30}$/.test(instance ?? '') || !/^[1-9]\d{0,15}$/.test(bot ?? '') || !Number.isSafeInteger(Number(bot)) || !/^-100\d{5,15}$/.test(chat ?? '') || !Number.isSafeInteger(Number(chat))) throw ownershipError();
    const drillRun = text('DRILL_RUN_ID');
    const isDrill = ['test-a', 'test-b'].includes(instance) && /^\d{1,20}-\d{1,3}$/.test(drillRun ?? '') && name === `tg-two-way-${instance}-reliability-${drillRun}`;
    if (name !== `tg-two-way-${instance}`) {
      if (!isDrill || bot !== '42424240' || chat !== '-1004242424242') throw ownershipError();
      try {
        const db = bindings.find(binding => binding.name === 'DB');
        assertDrillBindings({ config: { instance_id: instance }, run: drillRun, name, queue: `${name}-jobs` }, settings, db?.database_id ?? db?.id);
      } catch { throw ownershipError(); }
    }
    if (name === config.worker_name) continue;
    if (!isDrill && instance === config.instance_id) throw new Error('Selected instance identity is claimed by another Worker; inspect project Worker settings');
    if (bot === config.expected_bot_id) throw new Error('Expected Bot is already bound to another project Worker');
    if (chat === config.admin_chat_id) throw new Error('Admin Forum is already used by another project instance; select a separate Forum group in this Environment INSTANCE_CONFIG_JSON');
  }
}

const resourceIdentifier = /^(?:[a-f0-9]{32}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})$/;

/** Validate the complete inventory before the first create, including partial initialization. */
export function planInstanceResources(config, { databases, queues }, { operation, provision, workerExists = false, initializationLockHeld = false }) {
  const resources = [
    { field: 'd1_id', collection: databases, nameKey: 'name', name: config.d1_name, idKey: 'uuid', path: '/d1/database', body: { name: config.d1_name, primary_location_hint: 'apac', read_replication: { mode: 'disabled' } } },
    { field: 'queue_id', collection: queues, nameKey: 'queue_name', name: config.queue_name, idKey: 'queue_id', path: '/queues', body: { queue_name: config.queue_name } },
    { field: 'dlq_id', collection: queues, nameKey: 'queue_name', name: config.dlq_name, idKey: 'queue_id', path: '/queues', body: { queue_name: config.dlq_name } },
  ];
  const missingIds = [];
  for (const resource of resources) {
    const existing = resource.collection.filter(item => item[resource.nameKey] === resource.name);
    if (existing.length > 1) throw new Error(`Ambiguous instance resource: ${resource.field}; inspect account inventory`);
    if (existing.length) {
      resource.id = existing[0][resource.idKey];
      if (!config[resource.field]) missingIds.push(resource.field);
      else if (resource.id !== config[resource.field]) throw new Error(`Existing resource does not match configured ${resource.field}; no automatic adoption`);
      if (!resourceIdentifier.test(resource.id ?? '')) throw new Error(`Resource inventory has an invalid identifier: ${resource.field}`);
    } else if (config[resource.field]) throw new Error(`Configured ${resource.field} was not found under the expected resource name; inspect actual state before retrying`);
    delete resource.collection;
  }
  if (missingIds.length) throw new Error(`Exact IDs must be filled back for existing resources: ${missingIds.join(', ')}. Read IDs from Cloudflare, update only this Environment INSTANCE_CONFIG_JSON, then rerun; same-name resources are never adopted automatically`);
  const absent = resources.filter(resource => !resource.id);
  if (absent.length && workerExists) throw new Error(`Existing Worker has missing resources: ${absent.map(resource => resource.field).join(', ')}; inspect resource state, do not recreate an established instance`);
  if (absent.length && (operation !== 'deploy' || provision !== true)) throw new Error(`Instance resources are absent: ${absent.map(resource => resource.field).join(', ')}; first or partial initialization requires deploy with provision enabled`);
  if (absent.length && initializationLockHeld !== true) throw new Error('Resource initialization requires the Actions account initialization lock; rerun deploy with provision enabled through the current main workflow');
  if (!workerExists && operation === 'deploy' && (provision !== true || initializationLockHeld !== true)) throw new Error('First or partial Worker initialization requires deploy with provision enabled and the Actions account initialization lock, including when resource IDs are already filled back');
  if (!workerExists && operation !== 'deploy') throw new Error('Selected Worker is absent; inspect first or partial initialization state before a maintenance operation');
  return resources;
}

export async function assertPlannedQueueOwnership(resources, cf, account, workerName) {
  for (const resource of resources.filter(item => item.id && item.idKey === 'queue_id')) {
    const queue = (await cf(`${account}/queues/${resource.id}`)).result;
    if (queue?.queue_id !== resource.id || queue.queue_name !== resource.name) throw new Error('Queue identity changed during preflight');
    if ((queue.consumers_total_count ?? 0) > (queue.consumers?.length ?? 0)) throw new Error('Queue consumer inventory is incomplete');
    if ((queue.producers_total_count ?? 0) > (queue.producers?.length ?? 0)) throw new Error('Queue producer inventory is incomplete');
    if (resource.field === 'dlq_id' && queue.consumers?.length) throw new Error('Dead letter queue must not have an automatic consumer');
    assertQueueConsumers(queue.consumers, workerName);
    if (queue.producers?.some(producer => producer.type !== 'worker' || producer.script !== workerName)) throw new Error('Instance queue has another producer');
  }
}

export async function provisionPlannedResources(resources, cf, account, log = console.log) {
  const ids = {};
  for (const resource of resources) {
    let id = resource.id;
    if (!id) {
      // A single attempt only. Any unknown response stops before another create.
      const created = (await cf(`${account}${resource.path}`, { method: 'POST', body: resource.body })).result;
      id = created?.[resource.idKey];
      if (!resourceIdentifier.test(id ?? '') || created[resource.nameKey] !== resource.name) throw new Error(`Provisioning result for ${resource.field} did not confirm resource identity; inspect actual state and fill back its exact ID before retrying`);
      log(`Scoped resource created for ${resource.field}; read its exact Cloudflare ID and fill back this Environment INSTANCE_CONFIG_JSON before another run`);
    }
    ids[resource.field] = id;
  }
  return ids;
}

export function assertQueueConsumers(consumers, workerName) {
  for (const consumer of consumers ?? []) {
    // Queue detail responses use `script`; the consumer API also documents
    // `script_name`. Require every supplied identity to match the same Worker.
    const identities = [consumer.script, consumer.script_name].filter(value => value !== undefined);
    if (consumer.type !== 'worker' || identities.length === 0 || identities.some(value => value !== workerName)) throw new Error('Instance queue has another or unidentified consumer');
  }
}

export function assertBotPermissions(me, chat, member, config) {
  if (me.is_bot !== true || String(me.id) !== config.expected_bot_id) throw new Error('Token does not match expected Bot identity');
  if (String(chat.id) !== config.admin_chat_id || chat.type !== 'supergroup' || chat.is_forum !== true) throw new Error('Admin destination is not the expected Forum supergroup');
  if (member.status !== 'administrator' || member.can_manage_topics !== true || member.can_pin_messages !== true) throw new Error('Bot requires administrator, topic management and pin permissions');
  if (chat.available_reactions && ['👍', '👎'].some(emoji => !chat.available_reactions.some(r => r.type === 'emoji' && r.emoji === emoji))) throw new Error('Forum must allow required delivery/deletion reactions');
}

export function apiClient(token, fetcher = fetch) {
  return async function call(path, { method = 'GET', body } = {}) {
    let response;
    try {
      response = await fetcher(`https://api.cloudflare.com/client/v4${path}`, { method, headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(30000), redirect: 'error' });
    } catch { throw new Error(`Cloudflare ${method} request result unknown; inspect resource state before retrying`); }
    let data;
    try { data = await response.json(); } catch { throw new Error(`Cloudflare returned non-JSON response (HTTP ${response.status}); inspect actual resource state before retrying`); }
    if (!response.ok || data.success !== true) throw new Error(`Cloudflare request rejected (HTTP ${response.status}; codes ${(data.errors ?? []).map(e => e.code).filter(Number.isInteger).join(',')})`);
    return data;
  };
}

export function telegramClient(token, fetcher = fetch) {
  return async (method, params = {}) => {
    let response;
    try { response = await fetcher(`https://api.telegram.org/bot${token}/${method}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(params), signal: AbortSignal.timeout(30000), redirect: 'error' }); }
    catch { throw new Error(`Telegram ${method} result unknown; no automatic replay`); }
    let data;
    try { data = await response.json(); } catch { throw new Error(`Telegram ${method} returned non-JSON (HTTP ${response.status})`); }
    if (!response.ok || !data.ok) throw new Error(`Telegram ${method} rejected (HTTP ${response.status}; code ${Number(data.error_code) || 0})`);
    return data.result;
  };
}

// Wrangler prints all configured vars by default. Do not pipe its output into
// public workflow logs, including on failure; private IDs and URLs stay private.
export async function wrangler(args, { input, label = args[0] } = {}) {
  await new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['node_modules/wrangler/bin/wrangler.js', ...args], { env: { ...process.env, CI: 'true', WRANGLER_SEND_METRICS: 'false', WRANGLER_WRITE_LOGS: 'false', WRANGLER_LOG_SANITIZE: 'true' }, stdio: ['pipe', 'pipe', 'pipe'] });
    // Keep only numeric Cloudflare error codes for diagnostics.
    const codes = new Set();
    const inspect = chunk => { for (const match of chunk.toString().matchAll(/\[code:\s*(\d+)\]/g)) codes.add(match[1]); };
    child.stdout.on('data', inspect); child.stderr.on('data', inspect);
    child.on('error', () => reject(new Error(`Wrangler ${label} could not start`)));
    child.on('close', code => code === 0 ? resolve() : reject(new Error(`Wrangler ${label} failed (exit ${code}; Cloudflare codes ${[...codes].join(',') || 'unavailable'}); inspect the scoped resource in Cloudflare before retrying`)));
    child.stdin.end(input);
  });
  console.log(`Wrangler ${label} completed`);
}
