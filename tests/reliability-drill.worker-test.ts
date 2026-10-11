import {env,applyD1Migrations,reset,evictDurableObject} from 'cloudflare:test';
import {beforeEach,describe,it,expect} from 'vitest';
import worker from '../scripts/reliability-drill/worker';
const e=env as unknown as Parameters<typeof worker.fetch>[1];
async function post(action:string,key=e.DRILL_KEY) {
 const r=await worker.fetch(new Request(`https://${e.DRILL_HOSTNAME}/__reliability-drill`,{method:'POST',headers:{Authorization:`Bearer ${key}`},body:JSON.stringify({action})}),e);
 expect(r.status).toBe(200);return r.json<any>();
}
beforeEach(async()=>{await reset();await applyD1Migrations(env.DB,env.TEST_MIGRATIONS);await post('setup');});
describe('isolated drill transport and durable production journals',()=>{
 it('rejects unauthenticated controls and all ordinary external network requests',async()=>{
  const denied=await worker.fetch(new Request(`https://${e.DRILL_HOSTNAME}/__reliability-drill`,{method:'POST',body:'{"action":"setup"}'}),e);
  expect(denied.status).toBe(403);
  await expect(fetch('https://api.telegram.org/bot-real-token/copyMessage')).rejects.toThrow('drill_external_network_disabled');
 });
 it('D1 rejection leaves no accepted ingress and normal retry deduplicates the synthetic original',async()=>{
  expect(await post('ingress-d1')).toEqual({failedStatus:503,persistedBeforeRetry:0,retryStatus:200,duplicateStatus:200,inbox:1,messages:1});
 });
 it('failed Queue notification still leaves durable pending work',async()=>{
  expect(await post('queue-failure')).toEqual({accepted:200,failedNotifications:1,job:{state:'pending'},message:{delivery:'pending'}});
 });
 it('native Bot DO journal restores a completed API effect after D1 write failure and local eviction',async()=>{
  expect(await post('completion-fail')).toMatchObject({completionFailed:true,phase:'issued',count:1});
  // Local eviction waits for the production 20s timeout signal to settle.
  // The cloud drill uses state.abort() to interrupt the object immediately.
  const bot=e.BOT.get(e.BOT.idFromName(e.INSTANCE_ID));await evictDurableObject(bot);
  expect(await post('completion-recover')).toMatchObject({result:{status:'success'},phase:'success',count:1});
  expect(await post('completion-recover')).toMatchObject({count:1});
 },30000);
 it('unknown stub response never repeats a mutating API effect',async()=>{
  expect(await post('unknown')).toMatchObject({result:{status:'unknown'},count:1,phase:'unknown'});
  await evictDurableObject(e.BOT.get(e.BOT.idFromName(e.INSTANCE_ID)));
  expect(await post('unknown')).toMatchObject({result:{status:'unknown'},count:1});
 },30000);
 it('429 leaves the production operation prepared and issues only one safe retry',async()=>{
  const first=await post('rate429');expect(first).toMatchObject({deferred:true,count:1,phase:'prepared'});
  await new Promise(resolve=>setTimeout(resolve,Math.max(0,first.due-Date.now()+150)));
  expect(await post('rate429')).toMatchObject({result:{status:'success'},count:2,phase:'success'});
  expect(await post('rate429')).toMatchObject({count:2});
 });
});
