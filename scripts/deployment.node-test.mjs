import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { validateConfig, validateIsolation, renderWrangler, requireActions } from './instance-config.mjs';
import { assertBotPermissions, assertRemoteIdentity, assertQueueConsumers, assertWebhookChange, assertWebhookOwnership, assertRouteIsolation, installPausedRelease, awaitReleaseHealth, apiClient, telegramClient } from './deployment-lib.mjs';
import { runInstance, validateBroadcastTestRecipients } from './deploy-instance.mjs';
import { drillPlan, renderDrill } from './reliability-drill-lib.mjs';

const a = JSON.parse(await readFile(new URL('../instances/test-a.example.json', import.meta.url)));
const b = JSON.parse(await readFile(new URL('../instances/test-b.example.json', import.meta.url)));
const id = '11111111-1111-1111-1111-111111111111';
const support = JSON.parse(await readFile(new URL('../instances/prod-support.example.json', import.meta.url)));
const ad = JSON.parse(await readFile(new URL('../instances/prod-ad.example.json', import.meta.url)));

test('broadcast test restrictions validate bounded explicit identities without leaking input', () => {
  for (const instance of ['test-a', 'test-b']) {
    assert.equal(validateBroadcastTestRecipients(instance), '');
    assert.equal(validateBroadcastTestRecipients(instance, ''), '');
    assert.equal(validateBroadcastTestRecipients(instance, ' 10001, 20001 '), '10001,20001');
    assert.equal(validateBroadcastTestRecipients(instance, Array.from({ length: 20 }, (_, i) => String(i + 1)).join(',')).split(',').length, 20);
    for (const value of [' ', '0', '-10001', '01', '10001,', ',10001', '10001,10001', '9007199254740992', 'PRIVATE_RECIPIENT_MARKER', Array.from({ length: 21 }, (_, i) => String(i + 1)).join(','), null, ['10001']]) {
      assert.throws(() => validateBroadcastTestRecipients(instance, value), error => error.message === 'Invalid broadcast test recipient restriction');
    }
  }
  for (const instance of ['prod-support', 'prod-ad']) {
    assert.equal(validateBroadcastTestRecipients(instance), '');
    assert.throws(() => validateBroadcastTestRecipients(instance, 'PRIVATE_RECIPIENT_MARKER'), error => error.message === 'Broadcast test recipient restriction is allowed only on test instances');
  }
});

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

test('live and documented Queue consumer schemas retain strict Worker ownership', () => {
  assertQueueConsumers([{ type: 'worker', script: a.worker_name }], a.worker_name);
  assertQueueConsumers([{ type: 'worker', script_name: a.worker_name }], a.worker_name);
  assertQueueConsumers([{ type: 'worker', script: a.worker_name, script_name: a.worker_name }], a.worker_name);
  for (const consumer of [
    { type: 'worker', script: b.worker_name },
    { type: 'worker', script_name: b.worker_name },
    { type: 'worker', script: a.worker_name, script_name: b.worker_name },
    { type: 'worker' },
    { type: 'http_pull', script: a.worker_name },
    { script: a.worker_name },
  ]) assert.throws(() => assertQueueConsumers([consumer], a.worker_name), /another or unidentified consumer/);
  assertQueueConsumers([], a.worker_name);
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

test('read-only release readiness tolerates propagation but requires exact version and bounded attempts', async () => {
  let requests = 0;
  let waits = 0;
  const version = 'a'.repeat(40);
  await awaitReleaseHealth('https://example.invalid/health', version, {
    wait: async () => { waits++; },
    fetcher: async (_url, options) => {
      assert.equal(options.method, 'GET');
      assert.equal(options.redirect, 'error');
      requests++;
      if (requests === 1) throw new Error('TLS propagation');
      if (requests === 2) return new Response('', { status: 503 });
      return Response.json({ version: requests === 3 ? 'b'.repeat(40) : version });
    },
  });
  assert.equal(requests, 4);
  assert.equal(waits, 3);
  let rejectedReads = 0;
  await assert.rejects(awaitReleaseHealth('https://example.invalid/health', version, { attempts: 3, wait: async () => {}, fetcher: async () => { rejectedReads++; return Response.json({ version: 'stale' }); } }), /bounded read-only readiness/);
  assert.equal(rejectedReads, 3);
});

test('unknown API effects are not retried and secret URLs never enter errors', async () => {
  let requests = 0;
  const failing = async () => { requests++; throw new Error('https://api.telegram.org/botSECRET/sendMessage'); };
  await assert.rejects(telegramClient('SECRET', failing)('setWebhook'), error => !error.message.includes('SECRET') && error.message.includes('unknown'));
  assert.equal(requests, 1);
  await assert.rejects(apiClient('TOKEN', async () => new Response('<html>challenge</html>', { status: 403 }))('/accounts/account/queues'), /non-JSON/);
  await assert.rejects(apiClient('TOKEN', async () => Response.json({ success: false, errors: [{ code: 10000, message: 'SECRET' }] }, { status: 403 }))('/accounts/account/queues'), error => !error.message.includes('SECRET') && error.message.includes('10000'));
});

const resourceIds = { d1_id: '11111111-1111-1111-1111-111111111111', queue_id: '2'.repeat(32), dlq_id: '3'.repeat(32) };
const withIds = config => ({ ...config, ...resourceIds });
function workerSettings(config) {
  return { bindings: [
    ...Object.entries({ INSTANCE_ID: config.instance_id, EXPECTED_BOT_ID: config.expected_bot_id, ADMIN_CHAT_ID: config.admin_chat_id, ADMIN_IDS: config.admin_ids.join(',') }).map(([name, text]) => ({ name, type: 'plain_text', text })),
    { name: 'DB', type: 'd1', database_id: config.d1_id ?? resourceIds.d1_id },
    { name: 'JOBS', type: 'queue', queue_name: config.queue_name },
    { name: 'CONVERSATIONS', type: 'durable_object_namespace', class_name: 'ConversationCoordinator', namespace_id: `${config.instance_id}-conversations` },
    { name: 'BOT', type: 'durable_object_namespace', class_name: 'BotCoordinator', namespace_id: `${config.instance_id}-bot` },
  ] };
}

/** All transports and filesystem writes are synthetic; this fixture cannot reach a Bot or account. */
function deploymentFixture(raw = support, options = {}) {
  const config = structuredClone(raw);
  const events = [];
  const logs = [];
  const files = [];
  const secretInputs = [];
  const existing = options.existing === true;
  const database = { uuid: resourceIds.d1_id, name: config.d1_name };
  const job = { queue_id: resourceIds.queue_id, queue_name: config.queue_name, consumers: [{ type: 'worker', script: config.worker_name }], producers: [{ type: 'worker', script: config.worker_name }] };
  const dlq = { queue_id: resourceIds.dlq_id, queue_name: config.dlq_name, consumers: [], producers: [] };
  const databases = options.databases ?? (existing ? [database] : []);
  const queues = options.queues ?? (existing ? [job, dlq] : []);
  const workers = [...(existing ? [{ name: config.worker_name, settings: options.settings ?? workerSettings(config) }] : []), ...(options.workers ?? [])];
  const env = {
    GITHUB_ACTIONS: 'true', GITHUB_REPOSITORY: 'jacklilyhello/TG-two-way-chatbot', GITHUB_REF: 'refs/heads/main',
    INSTANCE_ID: config.instance_id, INSTANCE_CONFIG_JSON: JSON.stringify(config), RELEASE_SHA: 'a'.repeat(40),
    CLOUDFLARE_ACCOUNT_ID: 'f'.repeat(32), CLOUDFLARE_API_TOKEN: 'SYNTHETIC_PRIVATE_CF_TOKEN',
    TELEGRAM_BOT_TOKEN: `${config.expected_bot_id}:${'SYNTHETIC_PRIVATE_TG_TOKEN'.repeat(2)}`,
    TELEGRAM_WEBHOOK_SECRET: 'SYNTHETIC_PRIVATE_WEBHOOK_SECRET_123456',
    OPERATION: 'deploy', PROVISION: existing ? 'false' : 'true', INITIALIZATION_LOCK_HELD: existing ? 'false' : 'true',
    CONFIGURE_WEBHOOK: 'true', OLD_CONSUMER_STOPPED: 'true', ...options.env,
  };
  let webhookUrl = existing ? `https://${config.hostname}/webhook` : '';
  const account = `/accounts/${env.CLOUDFLARE_ACCOUNT_ID}`;
  const dependencies = {
    base: {}, manifests: [], log: line => logs.push(line), wait: async () => {},
    mkdir: async () => {}, writeFile: async (_path, content) => { files.push(JSON.parse(content)); },
    cf: async (path, request = {}) => {
      const method = request.method ?? 'GET';
      events.push({ platform: 'cf', method, path });
      if (method !== 'GET') {
        if (options.unknownCreate) return apiClient('SYNTHETIC_PRIVATE_CF_TOKEN', async () => { throw new Error('SYNTHETIC_PRIVATE_NETWORK_PAYLOAD'); })(path, request);
        if (path === `${account}/d1/database`) return { result: { ...database, ...options.createdDatabase } };
        if (path === `${account}/queues`) return { result: request.body.queue_name === config.queue_name ? job : dlq };
        throw new Error('Unexpected synthetic Cloudflare mutation');
      }
      if (path.endsWith('/tokens/verify')) return { result: { status: 'active' } };
      const url = new URL(`https://synthetic.invalid${path}`);
      if (url.pathname === `${account}/workers/scripts`) return { result: workers.map(worker => ({ id: worker.name })) };
      if (url.pathname.endsWith('/settings')) {
        const name = decodeURIComponent(url.pathname.split('/').at(-2));
        return { result: workers.find(worker => worker.name === name)?.settings };
      }
      if (url.pathname === `${account}/d1/database`) return { result: databases };
      if (url.pathname === `${account}/queues`) return { result: queues };
      if (url.pathname.startsWith(`${account}/queues/`)) return { result: queues.find(queue => queue.queue_id === url.pathname.split('/').at(-1)) };
      if (url.pathname === `${account}/workers/domains`) return { result: options.domains ?? (existing ? [{ hostname: config.hostname, service: config.worker_name }] : []) };
      if (url.pathname === '/zones') return { result: [{ name: 'lilyya.top', id: 'synthetic-zone' }] };
      if (url.pathname.endsWith('/dns_records') || url.pathname.endsWith('/workers/routes')) return { result: [] };
      throw new Error('Unexpected synthetic Cloudflare read');
    },
    telegram: async (method, params = {}) => {
      events.push({ platform: 'telegram', method });
      if (method === 'getMe') return { id: Number(options.botId ?? config.expected_bot_id), is_bot: true };
      if (method === 'getChat') return { id: Number(config.admin_chat_id), type: 'supergroup', is_forum: true };
      if (method === 'getChatMember') return params.user_id === Number(config.expected_bot_id) ? { status: 'administrator', can_manage_topics: true, can_pin_messages: true } : { status: 'administrator' };
      if (method === 'getWebhookInfo') return { url: webhookUrl };
      if (method === 'setWebhook') { assert.equal(params.drop_pending_updates, false); webhookUrl = params.url; return true; }
      throw new Error('Unexpected synthetic Telegram call');
    },
    fetcher: async (url, request = {}) => {
      events.push({ platform: 'worker', method: request.method ?? 'GET', path: new URL(url).pathname });
      if (new URL(url).pathname === '/health') return Response.json({ version: env.RELEASE_SHA });
      return Response.json({ ok: true, paused: true, issued: 0, version: options.statusVersion ?? env.RELEASE_SHA, jobs: [] });
    },
    wrangler: async (args, request) => { events.push({ platform: 'wrangler', method: request.label }); if (args[0] === 'secret') secretInputs.push(JSON.parse(request.input)); },
  };
  const mutations = () => events.filter(event => event.platform === 'wrangler' || event.platform === 'cf' && event.method !== 'GET' || event.platform === 'telegram' && event.method === 'setWebhook' || event.platform === 'worker' && event.method !== 'GET');
  return { config, env, dependencies, events, logs, files, secretInputs, mutations, run: () => runInstance(env, dependencies) };
}

test('test recipient restrictions are installed only as selected Worker Secrets and missing values disable stale restrictions', async () => {
  for (const config of [a, b]) {
    for (const value of ['', '10001, 20001']) {
      const fixture = deploymentFixture(withIds(config), { existing: true, env: { BROADCAST_TEST_RECIPIENT_IDS: value } });
      await fixture.run();
      assert.equal(fixture.secretInputs.length, 1);
      assert.equal(fixture.secretInputs[0].BROADCAST_TEST_RECIPIENT_IDS, value ? '10001,20001' : 'disabled');
      assert(fixture.files.every(file => !Object.hasOwn(file.vars, 'BROADCAST_TEST_RECIPIENT_IDS')));
      assert(!fixture.logs.join('\n').includes('10001'));
      assert(!fixture.logs.join('\n').includes('20001'));
    }
  }
  const production = deploymentFixture(withIds(support), { existing: true });
  await production.run();
  assert(!Object.hasOwn(production.secretInputs[0], 'BROADCAST_TEST_RECIPIENT_IDS'));
});

test('production or malformed test recipient restrictions fail before any external request or write', async () => {
  for (const config of [support, ad, a, b]) {
    const fixture = deploymentFixture(withIds(config), { existing: true, env: { BROADCAST_TEST_RECIPIENT_IDS: 'PRIVATE_RECIPIENT_MARKER' } });
    await assert.rejects(fixture.run(), error => !error.message.includes('PRIVATE_RECIPIENT_MARKER') && /recipient restriction/.test(error.message));
    assert.deepEqual(fixture.events, []);
    assert.deepEqual(fixture.files, []);
    assert.deepEqual(fixture.logs, []);
  }
});

test('both production templates initialize separately with all read preflight before any create', async () => {
  validateIsolation([support, ad]);
  for (const config of [support, ad]) {
    const fixture = deploymentFixture(config);
    await fixture.run();
    const firstWrite = fixture.events.findIndex(event => event.platform === 'cf' && event.method === 'POST');
    for (const method of ['getMe', 'getChat', 'getChatMember', 'getWebhookInfo']) assert(fixture.events.slice(0, firstWrite).some(event => event.platform === 'telegram' && event.method === method));
    assert.equal(fixture.events.filter(event => event.platform === 'cf' && event.method === 'POST').length, 3);
    assert.equal(fixture.files[0].name, config.worker_name);
    assert.equal(fixture.files[0].vars.PAUSED, 'true');
    assert.equal(fixture.files[0].d1_databases[0].database_id, resourceIds.d1_id);
    const output = fixture.logs.join('\n');
    for (const value of [config.expected_bot_id, config.admin_chat_id, config.hostname, ...Object.values(resourceIds), fixture.env.CLOUDFLARE_API_TOKEN, fixture.env.TELEGRAM_BOT_TOKEN, fixture.env.TELEGRAM_WEBHOOK_SECRET]) assert(!output.includes(value));
  }
});

test('live project settings allow shared administrators and reject shared Forum before writes', async () => {
  const other = { ...ad, admin_ids: support.admin_ids };
  const fixture = deploymentFixture(support, { workers: [{ name: other.worker_name, settings: workerSettings(other) }] });
  await fixture.run();
  const conflict = deploymentFixture(support, { workers: [{ name: other.worker_name, settings: workerSettings({ ...other, admin_chat_id: support.admin_chat_id }) }] });
  await assert.rejects(conflict.run(), error => /Admin Forum.*separate Forum/.test(error.message) && !error.message.includes(support.admin_chat_id));
  assert.deepEqual(conflict.mutations(), []);
});

test('incomplete, duplicate or misnamed project identity fails closed while unrelated Workers remain usable', async () => {
  for (const worker of [
    { name: ad.worker_name, settings: { bindings: workerSettings(ad).bindings.filter(binding => binding.name !== 'ADMIN_CHAT_ID') } },
    { name: ad.worker_name, settings: { bindings: [...workerSettings(ad).bindings, { name: 'INSTANCE_ID', text: ad.instance_id }] } },
    { name: 'tg-two-way-unknown', settings: workerSettings(ad) },
    { name: 'another-application', settings: { bindings: [{ name: 'EXPECTED_BOT_ID', text: support.expected_bot_id }] } },
  ]) {
    const fixture = deploymentFixture(support, { workers: [worker] });
    await assert.rejects(fixture.run(), /ownership cannot be confirmed/);
    assert.deepEqual(fixture.mutations(), []);
  }
  const unrelated = deploymentFixture(support, { workers: [{ name: 'another-application', settings: { bindings: [] } }] });
  await unrelated.run();
  const botConflict = deploymentFixture(support, { workers: [{ name: ad.worker_name, settings: workerSettings({ ...ad, expected_bot_id: support.expected_bot_id }) }] });
  await assert.rejects(botConflict.run(), /Bot is already bound/);
  assert.deepEqual(botConflict.mutations(), []);
});

function independentWorker() {
  const settings = workerSettings({ ...b, instance_id: a.instance_id, d1_id: '4'.repeat(32), queue_name: 'independent-jobs' });
  for (const binding of settings.bindings) if (binding.type === 'durable_object_namespace') binding.namespace_id = `independent-${binding.name}`;
  return { name: 'independent-telegram-application', settings };
}

test('independent applications with complete identities do not claim the project namespace or generic instance label', async () => {
  for (const withDrillMarker of [false, true]) {
    const worker = independentWorker();
    if (withDrillMarker) worker.settings.bindings.push({ name: 'DRILL_RUN_ID', type: 'plain_text', text: '12345-1' });
    const fixture = deploymentFixture(withIds(a), { existing: true, workers: [worker] });
    await fixture.run();
    assert(fixture.files.length > 0);
    assert(fixture.files.every(file => file.name === a.worker_name));
  }
});

test('independent identity fields retain all selected Bot, Forum, database, queue and namespace collision checks', async () => {
  const collisions = [
    ['EXPECTED_BOT_ID', { text: a.expected_bot_id }, /Bot is already bound/],
    ['ADMIN_CHAT_ID', { text: a.admin_chat_id }, /Admin Forum.*separate Forum/],
    ['DB', { database_id: resourceIds.d1_id }, /shares this D1/],
    ['JOBS', { queue_name: a.queue_name }, /shares a queue/],
    ['BOT', { namespace_id: `${a.instance_id}-bot` }, /shares a Durable Object/],
  ];
  for (const [field, change, expected] of collisions) {
    const worker = independentWorker();
    Object.assign(worker.settings.bindings.find(binding => binding.name === field), change);
    const fixture = deploymentFixture(withIds(a), { existing: true, workers: [worker] });
    await assert.rejects(fixture.run(), expected);
    assert.deepEqual(fixture.mutations(), []);
  }
});

test('incomplete or duplicate independent identities remain blocked before writes', async () => {
  for (const malformed of ['missing', 'duplicate']) {
    const worker = independentWorker();
    if (malformed === 'missing') worker.settings.bindings = worker.settings.bindings.filter(binding => binding.name !== 'EXPECTED_BOT_ID');
    else worker.settings.bindings.push({ name: 'EXPECTED_BOT_ID', type: 'plain_text', text: b.expected_bot_id });
    const fixture = deploymentFixture(withIds(a), { existing: true, workers: [worker] });
    await assert.rejects(fixture.run(), /ownership cannot be confirmed/);
    assert.deepEqual(fixture.mutations(), []);
  }
});

test('a full inventory page without metadata still reads subsequent Workers before creating', async () => {
  const other = { ...ad, admin_chat_id: support.admin_chat_id };
  const fixture = deploymentFixture(support, { workers: [{ name: ad.worker_name, settings: workerSettings(other) }] });
  const original = fixture.dependencies.cf;
  let inventoryPages = 0;
  fixture.dependencies.cf = async (path, request) => {
    const url = new URL(`https://synthetic.invalid${path}`);
    if (url.pathname.endsWith('/workers/scripts')) {
      inventoryPages++;
      if (url.searchParams.get('page') === '1') return { result: Array.from({ length: 100 }, (_, index) => ({ id: `unrelated-worker-${index}` })) };
      return original(path, request);
    }
    if (/\/unrelated-worker-\d+\/settings$/.test(url.pathname)) return { result: { bindings: [] } };
    return original(path, request);
  };
  await assert.rejects(fixture.run(), /Admin Forum/);
  assert.equal(inventoryPages, 2);
  assert.deepEqual(fixture.mutations(), []);
});

test('pagination metadata takes precedence over a short inventory page', async () => {
  const fixture = deploymentFixture(support, { workers: [{ name: ad.worker_name, settings: workerSettings({ ...ad, admin_chat_id: support.admin_chat_id }) }] });
  const original = fixture.dependencies.cf;
  let pages = 0;
  fixture.dependencies.cf = async (path, request) => {
    const url = new URL(`https://synthetic.invalid${path}`);
    if (url.pathname.endsWith('/workers/scripts')) {
      pages++;
      if (url.searchParams.get('page') === '1') return { result: [], result_info: { total_pages: 2 } };
    }
    return original(path, request);
  };
  await assert.rejects(fixture.run(), /Admin Forum/);
  assert.equal(pages, 2);
  assert.deepEqual(fixture.mutations(), []);
});

test('an initializer starting after the preceding lock holder rereads changed Worker settings', async () => {
  const first = deploymentFixture(support);
  await first.run();
  // Model the second Actions job acquiring the account lock after the first finishes.
  // Its private config deliberately collides with the now-installed Worker, while
  // no manifest is available to detect that collision before the actual API reads.
  const second = deploymentFixture({ ...ad, admin_chat_id: support.admin_chat_id }, {
    workers: [{ name: support.worker_name, settings: workerSettings(withIds(support)) }],
  });
  await assert.rejects(second.run(), /Admin Forum/);
  assert(second.events.some(event => event.platform === 'cf' && event.path.includes('/workers/scripts?page=1')));
  assert(second.events.some(event => event.platform === 'cf' && event.path.endsWith(`/workers/scripts/${support.worker_name}/settings`)));
  assert.deepEqual(second.mutations(), []);
});

test('retained authorized reliability drill has independently verified synthetic ownership', async () => {
  const drillEnv = { GITHUB_ACTIONS: 'true', GITHUB_REPOSITORY: 'jacklilyhello/TG-two-way-chatbot', GITHUB_REF: 'refs/heads/main', INSTANCE_ID: a.instance_id, INSTANCE_CONFIG_JSON: JSON.stringify(a), RELEASE_SHA: 'a'.repeat(40), GITHUB_RUN_ID: '123456', GITHUB_RUN_ATTEMPT: '1' };
  const plan = drillPlan(drillEnv);
  const rendered = renderDrill(plan, '4'.repeat(32));
  const settings = { bindings: [
    ...Object.entries(rendered.vars).map(([name, text]) => ({ name, type: 'plain_text', text })),
    { name: 'DB', type: 'd1', database_id: '4'.repeat(32) }, { name: 'JOBS', type: 'queue', queue_name: plan.queue },
    ...rendered.durable_objects.bindings.map(binding => ({ ...binding, type: 'durable_object_namespace', namespace_id: `drill-${binding.name}` })),
  ] };
  await deploymentFixture(support, { workers: [{ name: plan.name, settings }] }).run();
  for (const field of ['DRILL_RUN_ID', 'TELEGRAM_BOT_TOKEN', 'STUB']) {
    const fixture = deploymentFixture(support, { workers: [{ name: plan.name, settings: { bindings: settings.bindings.filter(binding => binding.name !== field) } }] });
    await assert.rejects(fixture.run(), /ownership cannot be confirmed/);
    assert.deepEqual(fixture.mutations(), []);
  }
});

test('wrong selection, Bot, DB, queue and domain fail before any remote mutation', async () => {
  const cases = [
    deploymentFixture(support, { env: { INSTANCE_ID: ad.instance_id } }),
    deploymentFixture(support, { botId: ad.expected_bot_id }),
    deploymentFixture(withIds(support), { existing: true, databases: [{ uuid: '4'.repeat(32), name: support.d1_name }] }),
    deploymentFixture(withIds(support), { existing: true, queues: [{ queue_id: resourceIds.queue_id, queue_name: support.queue_name, consumers: [{ type: 'worker', script: ad.worker_name }] }, { queue_id: resourceIds.dlq_id, queue_name: support.dlq_name, consumers: [] }] }),
    deploymentFixture(support, { domains: [{ hostname: support.hostname, service: ad.worker_name }] }),
    deploymentFixture({ ...support, hostname: 'unrelated.example.com' }),
  ];
  for (const fixture of cases) {
    await assert.rejects(fixture.run());
    assert.deepEqual(fixture.mutations(), []);
  }
});

test('existing resources require every exact ID backfill before any missing resource can be created', async () => {
  const missing = deploymentFixture(support, { existing: true });
  await assert.rejects(missing.run(), /d1_id, queue_id, dlq_id.*Environment INSTANCE_CONFIG_JSON/);
  assert.deepEqual(missing.mutations(), []);
  // D1 is absent, but the existing Queue is not yet backfilled. Do not create D1 first.
  const partial = deploymentFixture(support, { queues: [{ queue_id: resourceIds.queue_id, queue_name: support.queue_name }] });
  await assert.rejects(partial.run(), /queue_id.*Environment INSTANCE_CONFIG_JSON/);
  assert.deepEqual(partial.mutations(), []);
});

test('partial initialization validates all existing queue ownership before creating only missing resources', async () => {
  const config = { ...support, queue_id: resourceIds.queue_id, dlq_id: resourceIds.dlq_id };
  const queues = [{ queue_id: resourceIds.queue_id, queue_name: support.queue_name, consumers: [], producers: [] }, { queue_id: resourceIds.dlq_id, queue_name: support.dlq_name, consumers: [], producers: [] }];
  const fixture = deploymentFixture(config, { queues });
  await fixture.run();
  const creates = fixture.events.filter(event => event.platform === 'cf' && event.method === 'POST');
  assert.equal(creates.length, 1);
  assert.match(creates[0].path, /\/d1\/database$/);
  for (const queue of queues) assert(fixture.events.findIndex(event => event.path?.endsWith(`/queues/${queue.queue_id}`)) < fixture.events.indexOf(creates[0]));
  for (const invalid of [
    { ...queues[0], consumers: [{ type: 'worker', script: ad.worker_name }] },
    { ...queues[0], producers: [{ type: 'worker', script: ad.worker_name }] },
    { ...queues[0], consumers_total_count: 1, consumers: [] },
    { ...queues[0], producers_total_count: 1, producers: [] },
  ]) {
    const rejected = deploymentFixture(config, { queues: [invalid, queues[1]] });
    await assert.rejects(rejected.run());
    assert.deepEqual(rejected.mutations(), []);
  }
});

test('first and partial Worker installation always require initialization lock even with all IDs filled', async () => {
  const inventory = { databases: [{ uuid: resourceIds.d1_id, name: support.d1_name }], queues: [{ queue_id: resourceIds.queue_id, queue_name: support.queue_name }, { queue_id: resourceIds.dlq_id, queue_name: support.dlq_name }] };
  for (const [config, options] of [
    [support, { env: { INITIALIZATION_LOCK_HELD: 'false' } }],
    [support, { env: { PROVISION: 'false', INITIALIZATION_LOCK_HELD: 'false' } }],
    [withIds(support), { ...inventory, env: { PROVISION: 'false', INITIALIZATION_LOCK_HELD: 'false' } }],
  ]) {
    const fixture = deploymentFixture(config, options);
    await assert.rejects(fixture.run(), /provision|initialization lock/i);
    assert.deepEqual(fixture.mutations(), []);
  }
  const continued = deploymentFixture(withIds(support), inventory);
  await continued.run();
  assert.equal(continued.events.filter(event => event.platform === 'cf' && event.method === 'POST').length, 0);
});

test('unknown or incomplete creation outcome stops without retry, later creates, or private output', async () => {
  for (const options of [{ unknownCreate: true }, { createdDatabase: { uuid: undefined } }, { createdDatabase: { name: 'PRIVATE_CONFIG_MARKER' } }]) {
    const fixture = deploymentFixture(support, options);
    await assert.rejects(fixture.run(), error => /inspect.*state/i.test(error.message) && !/PRIVATE|SYNTHETIC|lilyya\.top/.test(error.message));
    assert.equal(fixture.events.filter(event => event.platform === 'cf' && event.method === 'POST').length, 1);
    assert.equal(fixture.events.filter(event => event.platform === 'wrangler' || event.method === 'setWebhook').length, 0);
    assert.deepEqual(fixture.logs, []);
  }
});

test('test-a and test-b existing upgrades and status retain per-instance pause and exact release reporting', async () => {
  for (const config of [withIds(a), withIds(b)]) {
    const fixture = deploymentFixture(config, { existing: true, env: { CONFIGURE_WEBHOOK: 'false' } });
    await fixture.run();
    assert.equal(fixture.events.filter(event => event.platform === 'cf' && event.method === 'POST').length, 0);
    assert.equal(fixture.events.filter(event => event.method === 'setWebhook').length, 0);
    const pause = fixture.events.findIndex(event => event.platform === 'worker' && event.path === '/internal/maintenance');
    const migration = fixture.events.findIndex(event => event.platform === 'wrangler');
    assert(pause >= 0 && pause < migration);
    assert(fixture.logs.includes(`Installed release: ${fixture.env.RELEASE_SHA}`));
    const status = deploymentFixture(config, { existing: true, env: { OPERATION: 'status' }, statusVersion: 'PRIVATE_CONFIG_MARKER' });
    await status.run();
    assert(status.logs.includes('Installed release: unavailable'));
    assert(!status.logs.join('\n').includes('PRIVATE_CONFIG_MARKER'));
    assert.equal(status.events.filter(event => event.platform === 'wrangler').length, 0);
  }
});
