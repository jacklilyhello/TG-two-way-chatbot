import {defineConfig} from 'vitest/config';
import {cloudflareTest,readD1Migrations} from '@cloudflare/vitest-plugin';
export default defineConfig({
 plugins:[cloudflareTest(async()=>({wrangler:{configPath:'./wrangler.jsonc'},miniflare:{bindings:{TEST_MIGRATIONS:await readD1Migrations('./migrations'),TELEGRAM_BOT_TOKEN:'123456:dummy-token',TELEGRAM_WEBHOOK_SECRET:'test-webhook-secret'}}}))],
 test:{setupFiles:['./tests/setup.ts'],include:['tests/**/*.test.ts'],fileParallelism:false}
});
