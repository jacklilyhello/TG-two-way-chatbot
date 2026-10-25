import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { runInNewContext } from 'node:vm';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';

const workflow = await readFile(new URL('../.github/workflows/instance.yml', import.meta.url), 'utf8');
const ci = await readFile(new URL('../.github/workflows/ci.yml', import.meta.url), 'utf8');
const guard = workflow.slice(workflow.indexOf('  guard:'), workflow.indexOf('  operate:'));
const operate = workflow.slice(workflow.indexOf('  operate:'));
const workflowLock = workflow.match(/^concurrency:\n((?:^  .*\n)+)/m)?.[1];
const jobLock = operate.match(/^    concurrency:\n((?:^      .*\n)+)/m)?.[1];
const aliases = ['test-a', 'test-b', 'prod-support', 'prod-ad'];

function property(block, key) {
  const value = block?.match(new RegExp(`^ +${key}: (.+)$`, 'm'))?.[1];
  assert(value, `Missing workflow property: ${key}`);
  return value;
}

// Evaluate only expressions from this repository's static workflow, with no
// process, network or filesystem objects exposed to the isolated context.
function expression(value, inputs, github = {}) {
  return value.replace(/\$\{\{\s*(.*?)\s*\}\}/g, (_match, code) => String(runInNewContext(code, {
    inputs, github,
    format: (template, ...args) => template.replace(/\{(\d+)\}/g, (_placeholder, index) => String(args[index])),
  }, { timeout: 1000 })));
}

function guardResult(overrides = {}) {
  const code = guard.match(/node --input-type=module -e '([^']+)'/)?.[1];
  assert(code, 'Immutable input guard must execute before any cloud operation');
  return spawnSync(process.execPath, ['--input-type=module', '-e', code], {
    env: {
      RELEASE_SHA: 'a'.repeat(40), INSTANCE_ID: 'prod-support',
      OPERATION: 'deploy', PROVISION: 'false', ...overrides,
    }, encoding: 'utf8',
  });
}

test('operations expose only the four selected independent Environments and reject forged aliases', () => {
  assert.match(workflow, /^on:\n  workflow_dispatch:/m);
  assert.doesNotMatch(workflow, /^  (?:push|pull_request|schedule):/m);
  const options = workflow.match(/^        options: \[([^\]]+)\]/m)?.[1].split(',').map(value => value.trim());
  assert.deepEqual(options, aliases);
  for (const instance of aliases) {
    assert.equal(guardResult({ INSTANCE_ID: instance }).status, 0);
    assert.equal(expression(property(operate, 'environment'), { instance }), `tg-two-way-${instance}`);
  }
  for (const instance of ['prod', 'prod-support\n', 'test-c', '../prod-ad', 'Prod-ad']) {
    const result = guardResult({ INSTANCE_ID: instance });
    assert.equal(result.status, 1);
    assert.equal(result.stdout + result.stderr, '');
  }
});

test('main trigger, full immutable release, successful main CI and provisioning operation remain mandatory', () => {
  assert.match(guard, /^    if: github\.ref == 'refs\/heads\/main'$/m);
  assert.match(guard, /git cat-file -e "\$RELEASE_SHA\^\{commit\}"/);
  assert.match(guard, /git merge-base --is-ancestor "\$RELEASE_SHA" origin\/main/);
  assert.match(guard, /actions\/workflows\/ci\.yml\/runs\?head_sha=\$RELEASE_SHA&status=success/);
  assert.match(guard, /\.head_sha == env\.RELEASE_SHA and \.event == "push" and \.head_branch == "main"/);
  assert.match(ci, /^  push:\n    branches: \[main, 'codex\/\*\*'\]/m);
  for (const sha of ['a'.repeat(39), 'a'.repeat(41), 'A'.repeat(40), 'main', 'PRIVATE_INPUT_MARKER']) {
    const result = guardResult({ RELEASE_SHA: sha });
    assert.equal(result.status, 1);
    assert.equal(result.stdout + result.stderr, '');
  }
  for (const operation of ['status', 'pause', 'resume', 'recover', 'export']) {
    assert.equal(guardResult({ OPERATION: operation }).status, 0);
    assert.equal(guardResult({ OPERATION: operation, PROVISION: 'true' }).status, 1);
  }
  assert.equal(guardResult({ PROVISION: 'true' }).status, 0);
  assert.equal(guardResult({ OPERATION: 'delete' }).status, 1);
});

test('initializations share the account job lock; normal releases retain independent instance locks', () => {
  const accountGroups = new Set();
  const normalGroups = new Set();
  for (const instance of aliases) {
    const inputs = { instance, operation: 'deploy', provision: true };
    const outer = expression(property(workflowLock, 'group'), inputs);
    const initial = expression(property(jobLock, 'group'), inputs);
    const normal = expression(property(jobLock, 'group'), { ...inputs, provision: false });
    assert.equal(outer, `instance-${instance}`);
    assert.notEqual(initial, outer, 'A job must not wait on its own workflow concurrency group');
    assert.notEqual(normal, outer, 'Normal operations must not wait on their own workflow group');
    assert.notEqual(initial, normal);
    accountGroups.add(initial);
    normalGroups.add(normal);
    assert.equal(expression(property(operate, 'INITIALIZATION_LOCK_HELD'), inputs), 'true');
    assert.equal(expression(property(operate, 'INITIALIZATION_LOCK_HELD'), { ...inputs, provision: false }), 'false');
  }
  assert.equal(accountGroups.size, 1, 'All initializations in the only account must serialize');
  assert.equal(normalGroups.size, aliases.length, 'Established instances must release independently');
  for (const block of [workflowLock, jobLock]) {
    assert.equal(property(block, 'cancel-in-progress'), 'false');
    assert.equal(property(block, 'queue'), 'max', 'Additional pending initializations must queue instead of replacing a waiter');
  }
});

test('inventory and creation execute inside the locked job after current main safety controls are checked out', () => {
  assert.match(operate, /^    needs: guard$/m);
  assert.doesNotMatch(guard, /deploy-instance|check-instances|CLOUDFLARE_API_TOKEN|INSTANCE_CONFIG_JSON/);
  const appCheckout = operate.indexOf('ref: ${{ inputs.release_sha }}');
  const controlCheckout = operate.indexOf('ref: ${{ github.sha }}');
  const controlPath = operate.indexOf('path: .deployment-control');
  const configCheck = operate.indexOf('run: node .deployment-control/scripts/check-instances.mjs');
  const execution = operate.indexOf('run: node .deployment-control/scripts/deploy-instance.mjs');
  assert(appCheckout >= 0 && appCheckout < controlCheckout && controlCheckout < controlPath);
  assert(controlPath < configCheck && configCheck < execution);
  assert.doesNotMatch(operate, /working-directory:/, 'Application src and migrations must remain rooted at the selected release');
  assert.doesNotMatch(operate, /run: node scripts\/(?:check-instances|deploy-instance)\.mjs/);
  assert.match(operate, /name: Remove private temporary files\n        if: always\(\)\n        run: rm -rf \.local \.deployment-control/);
  for (const secret of ['INSTANCE_CONFIG_JSON', 'TELEGRAM_BOT_TOKEN', 'TELEGRAM_WEBHOOK_SECRET', 'BROADCAST_TEST_RECIPIENT_IDS']) {
    assert.match(operate, new RegExp(`${secret}: \\$\\{\\{ secrets\\.${secret} \\}\\}`));
    assert.doesNotMatch(operate, new RegExp(`secrets\\.${secret}\\s*\\|\\|`));
  }
});

test('broadcast test recipient restriction remains a selected Environment Secret outside dispatch inputs', () => {
  const inputs = workflow.slice(workflow.indexOf('    inputs:'), workflow.indexOf('permissions:'));
  assert.doesNotMatch(inputs, /BROADCAST_TEST_RECIPIENT_IDS/);
  assert.doesNotMatch(operate, /vars\.BROADCAST_TEST_RECIPIENT_IDS/);
  assert.match(operate, /BROADCAST_TEST_RECIPIENT_IDS: \$\{\{ secrets\.BROADCAST_TEST_RECIPIENT_IDS \}\}/);
});

test('the main control checker reads its own manifests even when the application working directory differs', () => {
  const result = spawnSync(process.execPath, [fileURLToPath(new URL('./check-instances.mjs', import.meta.url))], {
    cwd: tmpdir(), env: {}, encoding: 'utf8',
  });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout.trim(), 'Instance configuration and resource isolation checks passed');
});

test('test drills keep the same per-instance exclusion and never gain production aliases', async () => {
  for (const file of ['test-recovery-drill.yml', 'reliability-drill.yml']) {
    const drill = await readFile(new URL(`../.github/workflows/${file}`, import.meta.url), 'utf8');
    assert.match(drill, /^concurrency:\n  group: instance-\$\{\{ inputs\.instance \}\}\n  cancel-in-progress: false/m);
    assert.match(drill, /^        options: \[test-a, test-b\]$/m);
    assert.doesNotMatch(drill, /prod-support|prod-ad/);
  }
});
