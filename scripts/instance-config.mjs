import { readFile, readdir } from 'node:fs/promises';
import { resolve } from 'node:path';

const fields = new Set(['instance_id', 'expected_bot_id', 'admin_chat_id', 'admin_ids', 'hostname', 'worker_name', 'd1_name', 'queue_name', 'dlq_name', 'd1_id', 'queue_id', 'dlq_id', 'recovery_epoch']);
const slug = /^[a-z][a-z0-9-]{0,30}$/;
const positive = /^[1-9]\d{0,15}$/;
const resourceId = /^(?:[a-f0-9]{32}|[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})$/;

export function validateConfig(value, expectedInstance) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Instance configuration must be an object');
  for (const field of Object.keys(value)) if (!fields.has(field)) throw new Error('Unsupported instance configuration field');
  if (!slug.test(value.instance_id) || value.instance_id !== expectedInstance) throw new Error('Instance selection does not match configuration');
  for (const key of ['expected_bot_id', 'admin_chat_id', 'hostname', 'recovery_epoch']) if (typeof value[key] !== 'string') throw new Error(`Missing string field: ${key}`);
  if (!positive.test(value.expected_bot_id) || !Number.isSafeInteger(Number(value.expected_bot_id))) throw new Error('Invalid expected Bot ID');
  if (!/^-100\d{5,15}$/.test(value.admin_chat_id) || !Number.isSafeInteger(Number(value.admin_chat_id))) throw new Error('Admin chat must be an explicit supergroup ID');
  if (!Array.isArray(value.admin_ids) || !value.admin_ids.length || value.admin_ids.some(id => typeof id !== 'string' || !positive.test(id) || !Number.isSafeInteger(Number(id))) || new Set(value.admin_ids).size !== value.admin_ids.length) throw new Error('Admin IDs must be unique explicit numeric strings');
  if (!/^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+lilyya\.top$/.test(value.hostname)) throw new Error('Hostname must be a lowercase subdomain of lilyya.top');
  if (!/^[a-zA-Z0-9_-]{1,64}$/.test(value.recovery_epoch)) throw new Error('Invalid recovery epoch');
  const base = `tg-two-way-${value.instance_id}`;
  const names = { worker_name: base, d1_name: `${base}-db`, queue_name: `${base}-jobs`, dlq_name: `${base}-dlq` };
  for (const [key, name] of Object.entries(names)) if (value[key] !== name) throw new Error(`Resource name outside selected instance: ${key}`);
  for (const key of ['d1_id', 'queue_id', 'dlq_id']) if (value[key] !== undefined && (typeof value[key] !== 'string' || !resourceId.test(value[key]))) throw new Error(`Invalid resource identifier: ${key}`);
  return structuredClone(value);
}

export function validateIsolation(configs) {
  for (const field of ['instance_id', 'expected_bot_id', 'hostname', 'worker_name', 'd1_name', 'queue_name', 'dlq_name', 'd1_id', 'queue_id', 'dlq_id']) {
    const values = configs.map(c => c[field]).filter(Boolean);
    if (values.length !== new Set(values).size) throw new Error(`Instances share a protected identity/resource: ${field}`);
  }
  // A queue cannot be another instance's DLQ either.
  const queues = configs.flatMap(c => [c.queue_name, c.dlq_name]);
  if (queues.length !== new Set(queues).size) throw new Error('Instances share a queue across binding roles');
  const queueIds = configs.flatMap(c => [c.queue_id, c.dlq_id]).filter(Boolean);
  if (queueIds.length !== new Set(queueIds).size) throw new Error('Instances share a queue identifier across binding roles');
}

export async function readManifests(directory = 'instances') {
  const files = (await readdir(directory)).filter(name => name.endsWith('.json'));
  const configs = [];
  for (const name of files) {
    const raw = JSON.parse(await readFile(resolve(directory, name), 'utf8'));
    configs.push(validateConfig(raw, raw.instance_id));
  }
  validateIsolation(configs);
  return configs;
}

export function renderWrangler(config, databaseId, version, base = {}) {
  if (!resourceId.test(databaseId)) throw new Error('A real D1 identifier is required');
  if (!/^[a-f0-9]{40}$/.test(version)) throw new Error('Release must be a full commit SHA');
  return {
    name: config.worker_name, main: '../src/index.ts', compatibility_date: base.compatibility_date ?? '2026-10-11', compatibility_flags: base.compatibility_flags ?? ['nodejs_compat'], workers_dev: false, preview_urls: false,
    routes: [{ pattern: config.hostname, custom_domain: true }],
    vars: { INSTANCE_ID: config.instance_id, EXPECTED_BOT_ID: config.expected_bot_id, ADMIN_CHAT_ID: config.admin_chat_id, ADMIN_IDS: config.admin_ids.join(','), VERSION: version, RECOVERY_EPOCH: config.recovery_epoch },
    d1_databases: [{ binding: 'DB', database_name: config.d1_name, database_id: databaseId, migrations_dir: '../migrations' }],
    queues: { producers: [{ binding: 'JOBS', queue: config.queue_name }], consumers: [{ queue: config.queue_name, max_batch_size: 10, max_batch_timeout: 1, max_retries: 3, dead_letter_queue: config.dlq_name }] },
    durable_objects: { bindings: [{ name: 'CONVERSATIONS', class_name: 'ConversationCoordinator' }, { name: 'BOT', class_name: 'BotCoordinator' }] },
    migrations: base.migrations ?? [{ tag: 'v1', new_sqlite_classes: ['ConversationCoordinator', 'BotCoordinator'] }],
    triggers: { crons: ['* * * * *'] }, observability: { enabled: true, head_sampling_rate: 1 },
  };
}

export function requireActions(env = process.env) {
  if (env.GITHUB_ACTIONS !== 'true' || env.GITHUB_REPOSITORY !== 'jacklilyhello/TG-two-way-chatbot' || env.GITHUB_REF !== 'refs/heads/main') throw new Error('Remote writes are restricted to this repository Actions on main');
}
