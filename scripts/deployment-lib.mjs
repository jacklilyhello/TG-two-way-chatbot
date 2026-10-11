import { spawn } from 'node:child_process';

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
    try { data = await response.json(); } catch { throw new Error(`Cloudflare returned non-JSON response (HTTP ${response.status})`); }
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
