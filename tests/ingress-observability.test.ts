import {env} from 'cloudflare:test';
import {expect,it,vi} from 'vitest';
import worker from '../src/index';
import type {Env} from '../src/types';
import {one} from '../src/db';

const e=env as unknown as Env;
const request=(secret=e.TELEGRAM_WEBHOOK_SECRET)=>new Request('https://test/webhook',{method:'POST',headers:{'Content-Type':'application/json','X-Telegram-Bot-Api-Secret-Token':secret},body:JSON.stringify({update_id:9001,message:{message_id:777,chat:{id:20001,type:'private'},from:{id:20001,first_name:'PRIVATE-NAME'},date:1,caption:'PRIVATE-CAPTION',photo:[{file_id:'PRIVATE-FILE',file_unique_id:'PRIVATE-UNIQUE',width:1,height:1}],media_group_id:'PRIVATE-ALBUM'}})});

it('correlates authenticated receipt and durable commit without logging private content or routing',async()=>{
 const log=vi.spyOn(console,'info').mockImplementation(()=>{});
 expect((await worker.fetch(request(),e)).status).toBe(200);
 expect(log.mock.calls.map(call=>call[0].phase)).toEqual(['received','committed']);
 expect(log.mock.calls[0][0]).toMatchObject({event:'webhook_commit',update_id:9001,kind:'message',media:'photo',album:true});
 expect(await one(e,'SELECT update_id FROM inbox_updates')).toEqual({update_id:9001});
 const text=JSON.stringify(log.mock.calls);
 for(const privateValue of ['PRIVATE-','20001','777',e.TELEGRAM_BOT_TOKEN,e.TELEGRAM_WEBHOOK_SECRET])expect(text).not.toContain(privateValue);
 expect(log.mock.calls[1][0].elapsed_ms).toBeGreaterThanOrEqual(0);
});

it('records commit failure and keeps 503, with no exception text or fake successful receipt',async()=>{
 const log=vi.spyOn(console,'info').mockImplementation(()=>{});
 const broken={...e,DB:{prepare:e.DB.prepare.bind(e.DB),batch:async()=>{throw Error(`PRIVATE-CAPTION ${e.TELEGRAM_BOT_TOKEN}`);}}} as Env;
 expect((await worker.fetch(request(),broken)).status).toBe(503);
 expect(log.mock.calls.map(call=>call[0].phase)).toEqual(['received','failed']);
 expect(JSON.stringify(log.mock.calls)).not.toContain('PRIVATE-');
 expect(JSON.stringify(log.mock.calls)).not.toContain(e.TELEGRAM_BOT_TOKEN);
 expect(await one(e,'SELECT update_id FROM inbox_updates')).toBeNull();
});

it('does not emit authenticated ingress evidence for rejected requests',async()=>{
 const log=vi.spyOn(console,'info').mockImplementation(()=>{});
 expect((await worker.fetch(request('wrong'),e)).status).toBe(403);
 expect(log).not.toHaveBeenCalled();
});
