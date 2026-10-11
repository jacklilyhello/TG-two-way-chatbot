import {defineConfig} from 'vitest/config';
import {cloudflareTest,readD1Migrations} from '@cloudflare/vitest-plugin';
export default defineConfig({
 plugins:[cloudflareTest(async()=>({wrangler:{configPath:'./scripts/reliability-drill/wrangler.test.jsonc'},miniflare:{bindings:{TEST_MIGRATIONS:await readD1Migrations('./migrations')}}}))],
 test:{include:['tests/reliability-drill.worker-test.ts'],fileParallelism:false,testTimeout:10000}
});
