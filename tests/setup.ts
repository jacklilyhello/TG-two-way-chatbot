import {env,applyD1Migrations,reset} from 'cloudflare:test';
import {beforeEach,afterEach,vi} from 'vitest';
beforeEach(async()=>{await reset();await applyD1Migrations(env.DB,env.TEST_MIGRATIONS);vi.spyOn(globalThis,'fetch').mockRejectedValue(new Error('Unmocked outbound request blocked'));});
afterEach(()=>{vi.restoreAllMocks();});
