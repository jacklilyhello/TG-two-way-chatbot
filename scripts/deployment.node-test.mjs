import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { validateConfig, validateIsolation, renderWrangler, requireActions } from './instance-config.mjs';
import { assertBotPermissions, assertRemoteIdentity, assertWebhookChange, assertWebhookOwnership, assertRouteIsolation, installPausedRelease, apiClient, telegramClient } from './deployment-lib.mjs';

const a = JSON.parse(await readFile(new URL('../instances/test-a.example.json', import.meta.url)));
const b = JSON.parse(await readFile(new URL('../instances/test-b.example.json', import.meta.url)));
const id = '11111111-1111-1111-1111-111111111111';

test('instances cannot inherit another instance resource or select another private config', () => {
  assert.throws(() => validateConfig(a, 'test-b'));
  assert.throws(() => validateConfig({ ...a, d1_name: b.d1_name }, a.instance_id));
  assert.throws(() => validateConfig({ ...a, hostname: 'unrelated.example.com' }, a.instance_id));
  assert.throws(() => validateConfig({ ...a, hostname: 'lilyya.top' }, a.instance_id));
  assert.throws(() => validateConfig({ ...a, TELEGRAM_BOT_TOKEN: 'secret' }, a.instance_id));
  assert.throws(() => validateConfig({ ...a, admin_ids: [] }, a.instance_id));
  assert.throws(() => validateConfig({ ...a, expected_bot_id: '9007199254740992' }, a.instance_id));
});

test('duplicate Bot, DB, hostname and cross-role queues fail closed', () => {
  validateIsolation([a, b]);
  for (const field of ['expected_bot_id', 'hostname', 'worker_name', 'd1_name', 'queue_name', 'dlq_name']) assert.throws(() => validateIsolation([a, { ...b, [field]: a[field] }]));
  assert.throws(() => validateIsolation([{ ...a, d1_id: id }, { ...b, d1_id: id }]));
  assert.throws(() => validateIsolation([a, { ...b, queue_name: a.dlq_name }]));
  assert.throws(() => validateIsolation([{ ...a, queue_id: id }, { ...b, dlq_id: id }]));
});

test('rendered bindings never share DO, queue, D1 or secret references', () => {
  const first = renderWrangler(a, id, 'a'.repeat(40));
  const second = renderWrangler(b, '22222222-2222-2222-2222-222222222222', 'b'.repeat(40));
  assert.notEqual(first.name, second.name);
  assert.notDeepEqual(first.d1_databases, second.d1_databases);
  assert.notDeepEqual(first.queues, second.queues);
  assert(first.durable_objects.bindings.every(binding => !binding.script_name && !binding.namespace_id));
  assert.equal(first.vars.ADMIN_IDS, a.admin_ids.join(','));
  assert.equal(first.workers_dev, false);
  assert(!JSON.stringify(first).includes('TELEGRAM_BOT_TOKEN'));
  assert.throws(() => renderWrangler(a, 'guessed', 'a'.repeat(40)));
});

test('local runs and non-main/foreign repository Actions cannot mutate remotely', () => {
  assert.throws(() => requireActions({}));
  assert.throws(() => requireActions({ GITHUB_ACTIONS: 'true', GITHUB_REPOSITORY: 'another/repo', GITHUB_REF: 'refs/heads/main' }));
  assert.throws(() => requireActions({ GITHUB_ACTIONS: 'true', GITHUB_REPOSITORY: 'jacklilyhello/TG-two-way-chatbot', GITHUB_REF: 'refs/pull/1/merge' }));
  requireActions({ GITHUB_ACTIONS: 'true', GITHUB_REPOSITORY: 'jacklilyhello/TG-two-way-chatbot', GITHUB_REF: 'refs/heads/main' });
});

test('takeover and existing resource adoption require explicit evidence', () => {
  assert.throws(() => assertWebhookChange({ url: 'https://old.invalid/webhook' }, 'https://new.invalid/webhook', false));
  assert.throws(() => assertWebhookChange({ url: '' }, 'https://new.invalid/webhook', false));
  assertWebhookChange({ url: '' }, 'https://new.invalid/webhook', true);
  assertWebhookChange({ url: 'https://same.invalid/webhook' }, 'https://same.invalid/webhook', false);
  assert.throws(() => assertWebhookOwnership({ url: '' }, 'https://selected.invalid/webhook'));
  assert.throws(() => assertWebhookOwnership({ url: 'https://old.invalid/webhook' }, 'https://selected.invalid/webhook'));
  assertWebhookOwnership({ url: 'https://selected.invalid/webhook' }, 'https://selected.invalid/webhook');
  const bindings = [{ name: 'INSTANCE_ID', text: a.instance_id }, { name: 'EXPECTED_BOT_ID', text: a.expected_bot_id }, { name: 'ADMIN_CHAT_ID', text: a.admin_chat_id }, { name: 'DB', type: 'd1', id }, { name: 'JOBS', type: 'queue', queue_name: a.queue_name }, { name: 'CONVERSATIONS', type: 'durable_object_namespace', class_name: 'ConversationCoordinator' }, { name: 'BOT', type: 'durable_object_namespace', class_name: 'BotCoordinator' }];
  assertRemoteIdentity({ ...a, d1_id: id }, { bindings });
  assertRemoteIdentity({ ...a, d1_id: id }, { bindings: bindings.map(b => b.name === 'DB' ? { name: 'DB', type: 'd1', database_id: id } : b) });
  assert.throws(() => assertRemoteIdentity(a, { bindings }));
  assert.throws(() => assertRemoteIdentity({ ...a, d1_id: id }, { bindings: [...bindings, { type: 'durable_object_namespace', script_name: b.worker_name }] }));
});

test('failed secret installation never activates code or reopens outbound processing', async () => {
  let paused = false;
  let uploaded = false;
  let activated = false;
  await assert.rejects(installPausedRelease({
    migrate: async () => {},
    pause: async () => { paused = true; },
    uploadPaused: async () => { assert.equal(paused, true); uploaded = true; },
    installSecrets: async () => { throw new Error('Secret installation result unknown'); },
    activate: async () => { activated = true; },
  }), /unknown/);
  assert.equal(uploaded, true);
  assert.equal(paused, true);
  assert.equal(activated, false);
  let uploads = 0;
  await assert.rejects(installPausedRelease({ migrate: async () => {}, pause: async () => { throw new Error('Cannot persist pause'); }, uploadPaused: async () => { uploads++; }, installSecrets: async () => {}, activate: async () => { uploads++; } }), /Cannot persist pause/);
  assert.equal(uploads, 0);
});

test('identity, Forum and privilege checks precede provisioning', () => {
  const me = { id: Number(a.expected_bot_id), is_bot: true };
  const chat = { id: Number(a.admin_chat_id), type: 'supergroup', is_forum: true };
  const member = { status: 'administrator', can_manage_topics: true, can_pin_messages: true };
  assertBotPermissions(me, chat, member, a);
  assert.throws(() => assertBotPermissions({ ...me, id: 2 }, chat, member, a));
  assert.throws(() => assertBotPermissions(me, { ...chat, is_forum: false }, member, a));
  assert.throws(() => assertBotPermissions(me, chat, { ...member, can_pin_messages: false }, a));
  assert.throws(() => assertBotPermissions(me, { ...chat, available_reactions: [{ type: 'emoji', emoji: '👍' }] }, member, a));
});

test('path-specific and wildcard routes prevent unrelated hostname takeover', () => {
  for (const pattern of [`${a.hostname}/webhook*`, `https://${a.hostname}/api/*`, 'https://*.lilyya.top/internal/*']) assert.throws(() => assertRouteIsolation([{ pattern, script: 'another-project' }], a.hostname, a.worker_name));
  assertRouteIsolation([{ pattern: `${b.hostname}/*`, script: b.worker_name }], a.hostname, a.worker_name);
  assertRouteIsolation([{ pattern: `${a.hostname}/webhook*`, script: a.worker_name }], a.hostname, a.worker_name);
});

test('malformed private configuration never appears in checker diagnostics', () => {
  const missing = spawnSync(process.execPath, ['scripts/check-instances.mjs'], { env: { ...process.env, INSTANCE_ID: a.instance_id, INSTANCE_CONFIG_JSON: '' }, encoding: 'utf8' });
  assert.equal(missing.status, 1);
  assert.match(missing.stderr, /Private INSTANCE_CONFIG_JSON is missing/);
  const child = spawnSync(process.execPath, ['scripts/check-instances.mjs'], { env: { ...process.env, INSTANCE_ID: a.instance_id, INSTANCE_CONFIG_JSON: '{"private":"PRIVATE_CONFIG_MARKER",' }, encoding: 'utf8' });
  assert.equal(child.status, 1);
  assert.match(child.stderr, /Invalid instance configuration JSON/);
  assert(!`${child.stdout}${child.stderr}`.includes('PRIVATE_CONFIG_MARKER'));
  const unknownField = spawnSync(process.execPath, ['scripts/check-instances.mjs'], { env: { ...process.env, INSTANCE_ID: a.instance_id, INSTANCE_CONFIG_JSON: JSON.stringify({ ...a, PRIVATE_CONFIG_MARKER: 'mistakenly pasted field' }) }, encoding: 'utf8' });
  assert.equal(unknownField.status, 1);
  assert.match(unknownField.stderr, /Unsupported instance configuration field/);
  assert(!`${unknownField.stdout}${unknownField.stderr}`.includes('PRIVATE_CONFIG_MARKER'));
});

test('unknown API effects are not retried and secret URLs never enter errors', async () => {
  let requests = 0;
  const failing = async () => { requests++; throw new Error('https://api.telegram.org/botSECRET/sendMessage'); };
  await assert.rejects(telegramClient('SECRET', failing)('setWebhook'), error => !error.message.includes('SECRET') && error.message.includes('unknown'));
  assert.equal(requests, 1);
  await assert.rejects(apiClient('TOKEN', async () => new Response('<html>challenge</html>', { status: 403 }))('/accounts/account/queues'), /non-JSON/);
  await assert.rejects(apiClient('TOKEN', async () => Response.json({ success: false, errors: [{ code: 10000, message: 'SECRET' }] }, { status: 403 }))('/accounts/account/queues'), error => !error.message.includes('SECRET') && error.message.includes('10000'));
});
