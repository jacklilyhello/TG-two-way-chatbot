import {env} from 'cloudflare:test';
import {beforeEach, describe, expect, it, vi} from 'vitest';
import type {Env, Job, Params} from '../src/types';
import {all, nextJob, one, run} from '../src/db';
import {FAILURE_EMOJI, handleReaction, receiptGuardValid, recoverPreparedReceipts, syncReceipt} from '../src/receipts';
import {RetryLater, type IssueGuard, type Outcome} from '../src/operations';

interface Request {key: string; method: string; params: Params; guard: IssueGuard}
let e: Env;
let time: number;
let requests: Request[];
let respond: (request: Request) => Outcome | Promise<Outcome>;

async function message(id: number, delivery = 'success', album: string | null = null) {
  await run(e, "INSERT INTO messages(id,user_id,direction,source_chat,source_id,admin_id,private_id,thread_id,received_at,delivery,media_group,payload) VALUES(?,'42','out',?, ?,?, ?,77,0,?,?, '{}')",id,e.ADMIN_CHAT_ID,id+100,id+100,delivery === 'success' ? id+800 : null,delivery,album);
}
async function job(action = 'apply', attempt = 1, version = 1) {
  return (await one<Job>(e, "SELECT * FROM jobs WHERE kind='reaction' AND json_extract(payload,'$.action')=? AND json_extract(payload,'$.attempt')=? AND json_extract(payload,'$.version')=? ORDER BY id DESC LIMIT 1",action,attempt,version))!;
}
const marker = () => requests.map(r => r.params.reaction);

beforeEach(async () => {
  e = {...env} as unknown as Env;
  time = 100000;
  vi.spyOn(Date, 'now').mockImplementation(() => time);
  requests = [];
  respond = () => ({status:'success',result:true,confirmedAt:time});
  e.JOBS = {send: vi.fn().mockResolvedValue(undefined)} as unknown as Queue<{key:string}>;
  e.CONVERSATIONS = {idFromName:(name:string)=>name,get:()=>({fetch:async()=>new Response('ok')})} as unknown as DurableObjectNamespace;
  const journal = new Map<string, Outcome>();
  e.BOT = {idFromName: (name: string) => name,get: () => ({fetch: async (_url: string, init: RequestInit) => {
    const request = JSON.parse(init.body as string) as Request;
    let outcome = journal.get(request.key);
    if (!outcome) {
      if (request.guard?.receipt && !await receiptGuardValid(e, request.guard.receipt)) outcome = {status:'failed',code:409,reason:'cancelled_by_state'};
      else {requests.push(request);outcome = await respond(request);}
      journal.set(request.key, outcome);
    }
    return Response.json(outcome);
  }})} as unknown as DurableObjectNamespace;
});

describe('durable administrator receipts', () => {
  it('starts ten seconds at API confirmation, leaves user FIFO free, and clears only its own reaction', async () => {
    await message(1);
    await syncReceipt(e,1);
    expect(requests).toHaveLength(0);
    const apply = await job();
    expect(apply.user_key).toBe('receipt:101');
    time += 3500;
    await handleReaction(e,apply);
    expect(await one(e,'SELECT shown_at,clear_after,phase FROM receipts WHERE admin_id=101')).toEqual({shown_at:103500,clear_after:113500,phase:'visible'});
    await run(e,"INSERT INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES('next','42','update','{}','pending',?,?,?)",time,e.RECOVERY_EPOCH,time);
    expect((await nextJob(e,'42'))?.dedupe).toBe('next');
    const clear = await job('clear');
    time = 113499;
    await expect(handleReaction(e,clear)).rejects.toBeInstanceOf(RetryLater);
    expect(requests).toHaveLength(1);
    time++;
    await handleReaction(e,clear);
    await handleReaction(e,clear);
    expect(marker()).toEqual([[{type:'emoji',emoji:'👍'}],[]]);
    expect(await one(e,'SELECT shown_at,cleared_at,phase FROM receipts WHERE admin_id=101')).toEqual({shown_at:103500,cleared_at:113500,phase:'cleared'});
    expect(requests.every(r => r.method === 'setMessageReaction' && r.params.chat_id === e.ADMIN_CHAT_ID)).toBe(true);
  });

  it('keeps failure beyond ten seconds and preserves its history when confirmed delivery later succeeds', async () => {
    await message(1,'failed');
    await syncReceipt(e,1);
    await handleReaction(e,await job());
    time += 20000;
    await syncReceipt(e,1);
    await handleReaction(e,await job());
    expect(marker()).toEqual([[{type:'emoji',emoji:FAILURE_EMOJI}]]);
    expect(await job('clear')).toBeNull();
    await run(e,"UPDATE messages SET delivery='success',private_id=801 WHERE id=1");
    await syncReceipt(e,1);
    await handleReaction(e,await job('apply',1,2));
    expect(marker()).toEqual([[{type:'emoji',emoji:FAILURE_EMOJI}],[{type:'emoji',emoji:'👍'}]]);
    expect(await one(e,'SELECT version,clear_after FROM receipts WHERE admin_id=101')).toEqual({version:2,clear_after:130000});
    expect(await all(e,'SELECT version,status FROM receipt_events ORDER BY version')).toEqual([{version:1,status:'success'},{version:2,status:'success'}]);
  });

  it('retires an old cleanup immediately when deletion replaces success, including replay after the due time', async () => {
    await message(1);
    await syncReceipt(e,1);
    await handleReaction(e,await job());
    const oldClear = await job('clear');
    time += 5000;
    await run(e,"UPDATE messages SET tombstone=1,deletion='deleted' WHERE id=1");
    await syncReceipt(e,1);
    expect(await one(e,'SELECT state FROM jobs WHERE id=?',oldClear.id)).toEqual({state:'done'});
    await handleReaction(e,await job('apply',1,2));
    time += 15000;
    await handleReaction(e,oldClear);
    expect(marker()).toEqual([[{type:'emoji',emoji:'👍'}],[{type:'emoji',emoji:'👎'}]]);
  });

  it('does not label unknown or pending delivery as failure or success', async () => {
    await message(1,'unknown');
    await message(2,'pending');
    await syncReceipt(e,1);
    await syncReceipt(e,2);
    expect(await all(e,'SELECT * FROM receipts')).toEqual([]);
    expect(requests).toEqual([]);
  });

  it('requires every album member to succeed and rejects a stale snapshot when a late member arrives', async () => {
    await message(1,'success','album');
    await message(2,'failed','album');
    await syncReceipt(e,1);
    await handleReaction(e,await job());
    expect(marker()).toEqual([[{type:'emoji',emoji:FAILURE_EMOJI}]]);
    await run(e,"UPDATE messages SET delivery='success',private_id=802 WHERE id=2");
    await syncReceipt(e,2);
    const stale = await job('apply',1,2);
    await message(3,'pending','album');
    await handleReaction(e,stale);
    expect(requests).toHaveLength(1);
    expect(await one(e,'SELECT version,emoji FROM receipts WHERE admin_id=101')).toEqual({version:3,emoji:null});
    await run(e,"UPDATE messages SET delivery='success',private_id=803 WHERE id=3");
    await syncReceipt(e,3);
    await handleReaction(e,await job('apply',1,4));
    expect(marker().at(-1)).toEqual([{type:'emoji',emoji:'👍'}]);
  });

  it('recovers the original confirmation timestamp after receipt persistence fails, without a second API call', async () => {
    await message(1);
    await syncReceipt(e,1);
    const apply = await job();
    const db = e.DB;
    let interrupt = true;
    // D1 statement internals are intentionally not assumed; intercept the
    // receipt-result batch by its three statements (ledger batch has two).
    e.DB = new Proxy(db,{get(target, property) {
      if (property === 'batch') return async (statements: D1PreparedStatement[]) => {
        if (interrupt && statements.length === 3) {interrupt=false;throw new Error('receipt write interrupted');}
        return target.batch(statements);
      };
      const value = Reflect.get(target,property);return typeof value === 'function' ? value.bind(target) : value;
    }});
    await expect(handleReaction(e,apply)).rejects.toThrow('receipt write interrupted');
    time += 15000;
    await handleReaction(e,apply);
    expect(requests).toHaveLength(1);
    expect(await one(e,'SELECT shown_at,clear_after FROM receipts WHERE admin_id=101')).toEqual({shown_at:100000,clear_after:110000});
    await handleReaction(e,await job('clear'));
    expect(marker()).toEqual([[{type:'emoji',emoji:'👍'}],[]]);
  });

  it('bounds unknown marker repair, records each attempt, and never touches delivery', async () => {
    await message(1);
    await syncReceipt(e,1);
    respond = () => ({status:'unknown',reason:'connection lost'});
    for (let attempt=1;attempt<=3;attempt++) await handleReaction(e,await job('apply',attempt));
    expect(requests).toHaveLength(3);
    expect(requests.every(r=>r.method==='setMessageReaction')).toBe(true);
    expect(await one(e,'SELECT delivery,private_id FROM messages WHERE id=1')).toEqual({delivery:'success',private_id:801});
    expect(await one(e,'SELECT phase,last_error FROM receipts WHERE admin_id=101')).toEqual({phase:'attention',last_error:'unknown'});
    expect(await all(e,'SELECT status FROM receipt_events')).toEqual([{status:'unknown'},{status:'unknown'},{status:'unknown'}]);
  });

  it('retries only a failed cleanup and does not restart an expired success on duplicate delivery jobs', async () => {
    await message(1);
    await syncReceipt(e,1);
    await handleReaction(e,await job());
    time += 10000;
    respond = () => ({status:'failed',code:400,reason:'temporary marker rejection'});
    await handleReaction(e,await job('clear'));
    expect(await one(e,'SELECT phase,shown_at,cleared_at FROM receipts WHERE admin_id=101')).toEqual({phase:'visible',shown_at:100000,cleared_at:null});
    respond = () => ({status:'success',result:true,confirmedAt:time});
    time += 3000;
    await handleReaction(e,await job('clear',2));
    await syncReceipt(e,1);
    await handleReaction(e,await job());
    expect(marker()).toEqual([[{type:'emoji',emoji:'👍'}],[],[]]);
    expect(await one(e,'SELECT shown_at,cleared_at,phase FROM receipts WHERE admin_id=101')).toEqual({shown_at:100000,cleared_at:113000,phase:'cleared'});
  });

  it('keeps durable intent after wake notification fails and upgrades legacy marker repairs safely', async () => {
    await message(1,'failed');
    e.JOBS = {send:vi.fn().mockRejectedValue(new Error('queue unavailable'))} as unknown as Queue<{key:string}>;
    await syncReceipt(e,1);
    expect(await job()).not.toBeNull();
    await handleReaction(e,{...(await job()),payload:JSON.stringify({rowId:1,emoji:'👍',attempt:1,parentKey:'legacy-success'})});
    await handleReaction(e,await job());
    expect(marker()).toEqual([[{type:'emoji',emoji:FAILURE_EMOJI}]]);
  });

  it('protects a newer failure at the final API guard even if the message changes after preliminary validation', async () => {
    await message(1);
    await syncReceipt(e,1);
    await handleReaction(e,await job());
    const clear = await job('clear');
    time += 10000;
    const bot = e.BOT;
    e.BOT = {idFromName:(name:string)=>name,get:(id:DurableObjectId)=>({fetch:async (url:string,init:RequestInit)=>{
      await run(e,"UPDATE messages SET delivery='failed' WHERE id=1");
      return bot.get(id).fetch(url,init);
    }})} as unknown as DurableObjectNamespace;
    await handleReaction(e,clear);
    expect(requests).toHaveLength(1);
    expect(await one(e,'SELECT version,emoji FROM receipts WHERE admin_id=101')).toEqual({version:2,emoji:FAILURE_EMOJI});
  });

  it.each(['success','failed'])('recovers only a prepared current-epoch legacy marker with confirmed %s delivery evidence', async delivery => {
    await message(1,delivery);
    await run(e,"INSERT INTO operations(op_key,method,phase,result,reason,created_at,updated_at) VALUES(?,'copyMessage',?,?,?,0,0)",`${e.RECOVERY_EPOCH}:send:1`,delivery,delivery==='success'?JSON.stringify({message_id:801}):null,delivery==='failed'?'Telegram explicit rejection':null);
    await run(e,"INSERT INTO operations(op_key,method,phase,created_at,updated_at) VALUES(?,'setMessageReaction','prepared',0,0)",`${e.RECOVERY_EPOCH}:sent-reaction:1`);
    expect(await recoverPreparedReceipts(e)).toBe(1);
    expect(await recoverPreparedReceipts(e)).toBe(0);
    expect(requests).toEqual([]);
    expect(await one(e,'SELECT phase,reason FROM operations WHERE op_key=?',`${e.RECOVERY_EPOCH}:sent-reaction:1`)).toEqual({phase:'failed',reason:'superseded_by_receipt'});
    await handleReaction(e,await job());
    expect(marker()).toEqual([[{type:'emoji',emoji:delivery==='success'?'👍':FAILURE_EMOJI}]]);
  });

  it('leaves old epochs, deletion history, absent delivery evidence, existing receipts and unmarked successes untouched', async () => {
    for (let id=1;id<=5;id++) await message(id);
    await run(e,"UPDATE messages SET epoch='old' WHERE id=1");
    await run(e,"UPDATE messages SET tombstone=1,deletion='deleted' WHERE id=2");
    await syncReceipt(e,4);
    for (let id=1;id<=4;id++) await run(e,"INSERT INTO operations(op_key,method,phase,created_at,updated_at) VALUES(?,'setMessageReaction','prepared',0,0)",`${id===1?'old':e.RECOVERY_EPOCH}:sent-reaction:${id}`);
    expect(await recoverPreparedReceipts(e)).toBe(0);
    expect(await all(e,'SELECT row_id FROM receipts')).toEqual([{row_id:4}]);
    expect((await all<{phase:string}>(e,'SELECT phase FROM operations')).every(row=>row.phase==='prepared')).toBe(true);
    expect(requests).toEqual([]);
  });

  it('recovers a prepared album marker only from the complete exact confirmed batch mapping', async () => {
    await message(1,'success','album-recover');await message(2,'success','album-recover');
    const batchKey = 'album-send:recovery-test';
    await run(e,"INSERT INTO media_batches(batch_key,group_key,members,thread_id,state) VALUES(?,?,'[1,2]',77,'done')",batchKey,`${e.ADMIN_CHAT_ID}:album-recover`);
    await run(e,"INSERT INTO operations(op_key,method,phase,result,created_at,updated_at) VALUES(?,'sendMediaGroup','success',?,0,0)",`${e.RECOVERY_EPOCH}:${batchKey}`,JSON.stringify([{message_id:801},{message_id:802}]));
    await run(e,"INSERT INTO operations(op_key,method,phase,created_at,updated_at) VALUES(?,'setMessageReaction','prepared',0,0)",`${e.RECOVERY_EPOCH}:album:${e.ADMIN_CHAT_ID}:album-recover:album-reaction:2`);
    expect(await recoverPreparedReceipts(e)).toBe(1);
    expect(await all(e,'SELECT row_id FROM receipts')).toEqual([{row_id:1}]);
    await handleReaction(e,await job());
    expect(marker()).toEqual([[{type:'emoji',emoji:'👍'}]]);
  });

  it('advances a bounded recovery scan past ineligible records without mutating them', async () => {
    for (let id=100;id<=120;id++) {
      await message(id);
      await run(e,"INSERT INTO operations(op_key,method,phase,created_at,updated_at) VALUES(?,'setMessageReaction','prepared',0,0)",`${e.RECOVERY_EPOCH}:sent-reaction:${id}`);
    }
    await run(e,"INSERT INTO operations(op_key,method,phase,result,created_at,updated_at) VALUES(?,'copyMessage','success',?,0,0)",`${e.RECOVERY_EPOCH}:send:120`,JSON.stringify({message_id:920}));
    expect(await recoverPreparedReceipts(e)).toBe(0);
    expect(await recoverPreparedReceipts(e)).toBe(1);
    expect(await all(e,'SELECT row_id FROM receipts')).toEqual([{row_id:120}]);
    expect(await one(e,"SELECT COUNT(*) n FROM operations WHERE method='setMessageReaction' AND phase='prepared'")).toEqual({n:20});
  });
});
