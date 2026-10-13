import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { readManifests, validateConfig, validateIsolation, renderWrangler } from './instance-config.mjs';

const support = JSON.parse(await readFile(new URL('../instances/prod-support.example.json', import.meta.url)));
const advertising = JSON.parse(await readFile(new URL('../instances/prod-ad.example.json', import.meta.url)));
const d1 = '11111111-1111-1111-1111-111111111111';

test('all test and production templates remain independently valid with shared human administrators', async () => {
  const configs = await readManifests();
  assert.deepEqual(configs.map(config => config.instance_id).sort(), ['prod-ad', 'prod-support', 'test-a', 'test-b']);
  assert.deepEqual(support.admin_ids, advertising.admin_ids);
  assert.notEqual(support.admin_chat_id, advertising.admin_chat_id);
  for (const config of [support, advertising]) {
    assert.deepEqual(validateConfig(config, config.instance_id), config);
    assert.equal(renderWrangler(config, d1, 'a'.repeat(40)).vars.ADMIN_CHAT_ID, config.admin_chat_id);
    for (const field of ['d1_id', 'queue_id', 'dlq_id']) assert.equal(config[field], undefined);
  }
});

test('production instance selection and protected identities cannot be crossed', () => {
  assert.throws(() => validateConfig(support, advertising.instance_id), /selection/);
  for (const field of ['expected_bot_id', 'admin_chat_id', 'hostname', 'worker_name', 'd1_name', 'queue_name', 'dlq_name']) {
    assert.throws(() => validateIsolation([support, { ...advertising, [field]: support[field] }]), new RegExp(field));
  }
  assert.throws(() => validateConfig({ ...support, queue_name: advertising.queue_name }, support.instance_id), /queue_name/);
  assert.throws(() => validateConfig({ ...support, hostname: 'example.com' }, support.instance_id), /lilyya/);
});

test('Forum collision and missing private config diagnostics disclose no private values', () => {
  const group = '-1006667778880';
  assert.throws(() => validateIsolation([{ ...support, admin_chat_id: group }, { ...advertising, admin_chat_id: group }]), error => error.message.includes('admin_chat_id') && !error.message.includes(group));
  const missing = spawnSync(process.execPath, ['scripts/check-instances.mjs'], { env: { ...process.env, INSTANCE_ID: support.instance_id, INSTANCE_CONFIG_JSON: '' }, encoding: 'utf8' });
  assert.equal(missing.status, 1);
  assert.match(missing.stderr, /Private INSTANCE_CONFIG_JSON is missing/);
  const conflict = spawnSync(process.execPath, ['scripts/check-instances.mjs'], { env: { ...process.env, INSTANCE_ID: support.instance_id, INSTANCE_CONFIG_JSON: JSON.stringify({ ...support, admin_chat_id: advertising.admin_chat_id }) }, encoding: 'utf8' });
  assert.equal(conflict.status, 1);
  assert.match(conflict.stderr, /admin_chat_id/);
  assert(!`${conflict.stdout}${conflict.stderr}`.includes(advertising.admin_chat_id));
});
