import {env} from 'cloudflare:test';
import {beforeEach, describe, expect, it} from 'vitest';
import type {Env, Job, MessageRow, Params, TGMessage, UserRow} from '../src/types';
import {all, one, run, stmt, user} from '../src/db';
import {clearConversation, deleteSingle, handleDeliveryNotice, handleMessage, handleReaction} from '../src/messaging';
import {call, RetryLater, type Outcome} from '../src/operations';
import {accept} from '../src/ingress';
import {handleAdmin} from '../src/admin';
import {syncReceipt} from '../src/receipts';

interface Request {key: string; method: string; params: Params}
let e: Env;
let requests: Request[];
let handler: (request: Request) => Outcome | Promise<Outcome>;
let nextPrivate: number;
const privateMessage = (id: number, extra: Partial<TGMessage> = {}): TGMessage => ({message_id: id, date: 100, chat: {id: 42, type: 'private'}, from: {id: 42, first_name: 'Test'}, text: 'message', ...extra});
const adminMessage = (id: number, extra: Partial<TGMessage> = {}): TGMessage => ({message_id: id, date: 100, chat: {id: -1001234567890, type: 'supergroup'}, from: {id: 10001}, message_thread_id: 77, is_topic_message: true, text: 'reply', ...extra});
const job = (id: number, m?: TGMessage, edited = false): Job => ({id, dedupe: `job:${id}`, user_key: '42', kind: 'update', payload: JSON.stringify({update: {update_id: id, [edited ? 'edited_message' : 'message']: m}}), state: 'running', due_at: 0, attempts: 0, epoch: 'initial', created_at: 1});
async function record(m: TGMessage, direction = m.chat.type === 'private' ? 'in' : 'out') {
  await run(e, "INSERT INTO messages(user_id,direction,source_chat,source_id,admin_id,private_id,thread_id,business,media_group,received_at,payload) VALUES('42',?,?,?,?,?,?,1,?,0,?)", direction, String(m.chat.id), m.message_id, direction === 'out' ? m.message_id : null, direction === 'in' ? m.message_id : null, direction === 'out' ? 77 : null, m.media_group_id ?? null, JSON.stringify(m));
  return (await one<MessageRow>(e, 'SELECT * FROM messages WHERE source_chat=? AND source_id=?', String(m.chat.id), m.message_id))!;
}
async function snapshot(id: string) {
  await e.DB.batch([
    stmt(e, "INSERT INTO deletion_batches(id,user_id,boundary,created_at) SELECT ?,'42',MAX(id),0 FROM messages WHERE user_id='42'", id),
    stmt(e, "INSERT INTO deletion_items(batch_id,message_id) SELECT ?,id FROM messages WHERE user_id='42' AND id<=(SELECT boundary FROM deletion_batches WHERE id=?)", id, id),
    stmt(e, "UPDATE messages SET tombstone=1 WHERE user_id='42' AND id<=(SELECT boundary FROM deletion_batches WHERE id=?)", id),
  ]);
}

beforeEach(async () => {
  e = {...env} as unknown as Env;
  await e.DB.batch(['media_batches','media_groups','deletion_items','deletion_batches','operations','delivery_events','messages','jobs','audit_logs','users','settings'].map(t => e.DB.prepare(`DELETE FROM ${t}`)));
  await run(e, "INSERT INTO users(tgid,first_name,first_contact,thread_id,binding_version,card_id) VALUES('42','Test',1,77,1,99)");
  requests = [];
  nextPrivate = 800;
  handler = request => ({status: 'success', result: request.method === 'copyMessage' || request.method === 'sendMessage' ? {message_id: ++nextPrivate} : request.method === 'sendMediaGroup' ? (request.params.media as Params[]).map(() => ({message_id: ++nextPrivate})) : true});
  const journal = new Map<string, Outcome>();
  e.BOT = {
    idFromName: (name: string) => name,
    get: () => ({fetch: async (_url: string, init: RequestInit) => {
      const request = JSON.parse(init.body as string) as Request;
      let outcome = journal.get(request.key);
      if (!outcome) {requests.push(request); outcome = await handler(request); journal.set(request.key, outcome);}
      return Response.json(outcome);
    }}),
  } as unknown as DurableObjectNamespace;
});

describe('persistent messaging and deletion invariants', () => {
  it.each([
    {description:'defaults',custom:false,start:'你好，{name}。请直接发送消息，管理员会人工回复。',help:'请直接发送文字或媒体与管理员联系。',startReply:'你好，Test Example。请直接发送消息，管理员会人工回复。',helpReply:'请直接发送文字或媒体与管理员联系。'},
    {description:'saved custom templates',custom:true,start:'测试欢迎 {name}/{tgid}',help:'测试帮助 {username}',startReply:'测试欢迎 Test Example/42',helpReply:'测试帮助 @test_person'},
  ])('shows the same $description in the panel and actual fixed replies', async templates => {
    if (templates.custom) {
      await run(e,"INSERT INTO settings(key,value) VALUES('start_text',?),('help_text',?)",templates.start,templates.help);
    }
    const panel = adminMessage(170,{text:'/mb settings',message_thread_id:undefined,is_topic_message:false});
    expect(await handleAdmin(e,job(170,panel),{update_id:170,message:panel})).toBe(true);
    for (const [index,command] of (['start','help'] as const).entries()) {
      const label = command === 'start' ? '欢迎文案：\n' : '帮助文案：\n';
      const displayed = requests.find(request=>request.method==='sendMessage'&&request.params.chat_id===e.ADMIN_CHAT_ID&&String(request.params.text).startsWith(label));
      expect(displayed?.params.text).toBe(label+templates[command]);
      const m = privateMessage(171+index,{text:`/${command}`,from:{id:42,first_name:'Test',last_name:'Example',username:'test_person'}});
      await record(m);
      await handleMessage(e,job(171+index,m),m,false);
      expect(requests.filter(request=>request.method==='sendMessage'&&request.params.chat_id==='42').at(-1)?.params.text).toBe(command==='start'?templates.startReply:templates.helpReply);
    }
    expect(await all(e,"SELECT key,value FROM settings WHERE key IN ('start_text','help_text') ORDER BY key")).toEqual(templates.custom?[{key:'help_text',value:templates.help},{key:'start_text',value:templates.start}]:[]);
    expect((await all<{business:number}>(e,'SELECT business FROM messages')).every(row=>row.business===0)).toBe(true);
  });

  it('keeps confirmed delivery when reaction fails, and retries only the marker', async () => {
    const m = adminMessage(101);
    await record(m);
    const normal = handler;
    handler = request => request.method === 'setMessageReaction' ? {status: 'failed', reason: 'REACTION_INVALID', code: 400} : normal(request);
    await handleMessage(e, job(10, m), m, false);
    await handleMessage(e, job(10, m), m, false);
    expect(requests.filter(r => r.method === 'copyMessage')).toHaveLength(1);
    expect(requests.filter(r => r.method === 'setMessageReaction')).toHaveLength(0);
    const apply = await one<Job>(e, "SELECT * FROM jobs WHERE kind='reaction'");
    await handleReaction(e, apply!);
    expect(requests.filter(r => r.method === 'setMessageReaction')).toHaveLength(1);
    expect(await one(e, 'SELECT delivery,private_id FROM messages WHERE source_id=101')).toEqual({delivery: 'success', private_id: 801});
    expect(requests.some(r => r.method === 'deleteMessage')).toBe(false);
    const repair = await one<Job>(e, "SELECT * FROM jobs WHERE kind='reaction' AND json_extract(payload,'$.attempt')=2");
    expect(repair).not.toBeNull();
    handler = normal;
    await handleReaction(e, repair!);
    expect(requests.filter(r => r.method === 'setMessageReaction')).toHaveLength(2);
    expect(requests.filter(r => r.method === 'copyMessage')).toHaveLength(1);
  });

  it('preserves unknown delivery without blindly resending on repeated jobs', async () => {
    const m = adminMessage(102);
    await record(m);
    const normal = handler;
    handler = request => request.method === 'copyMessage' ? {status: 'unknown', reason: 'timeout'} : normal(request);
    await handleMessage(e, job(11, m), m, false);
    await handleMessage(e, job(12, m), m, false);
    expect(requests.filter(r => r.method === 'copyMessage')).toHaveLength(1);
    expect(await one(e, 'SELECT delivery,private_id FROM messages WHERE source_id=102')).toEqual({delivery: 'unknown', private_id: null});
    expect(requests.some(r => r.method === 'setMessageReaction')).toBe(false);
  });

  it('recovers a rate-limited failure notice independently without repeating content or its audit', async () => {
    const m = adminMessage(180);
    await record(m);
    const normal = handler;
    handler = request => request.method === 'copyMessage' ? {status:'failed',code:403,reason:'Forbidden: bot was blocked by the user'} : normal(request);
    await handleMessage(e,job(180,m),m,false);
    const notice = (await one<Job>(e,"SELECT * FROM jobs WHERE kind='delivery-notice'"))!;
    expect(notice.user_key).toMatch(/^notice:/);
    const bot = e.BOT;
    let delayed = false;
    e.BOT = {idFromName:(name:string)=>name,get:(id:DurableObjectId)=>({fetch:async (url:string,init:RequestInit)=>{
      const request = JSON.parse(init.body as string) as Request;
      if (!delayed && request.method === 'sendMessage') {delayed=true;return Response.json({due:Date.now()+1000},{status:429});}
      return bot.get(id).fetch(url,init);
    }})} as unknown as DurableObjectNamespace;
    await expect(handleDeliveryNotice(e,notice)).rejects.toBeInstanceOf(RetryLater);
    await handleMessage(e,job(180,m),m,false);
    await handleDeliveryNotice(e,notice);
    await handleDeliveryNotice(e,notice);
    expect(requests.filter(r=>r.method==='copyMessage')).toHaveLength(1);
    expect(requests.filter(r=>r.method==='sendMessage').map(r=>r.params.text)).toEqual(['对方已拉黑机器人']);
    expect(await all(e,"SELECT id FROM audit_logs WHERE event='delivery_failed'")).toHaveLength(1);
  });

  it('rebuilds an unknown delivery notice after interruption between mapping persistence and notice enqueue', async () => {
    const m = adminMessage(181);
    const row = await record(m);
    await run(e,"UPDATE messages SET delivery='unknown' WHERE id=?",row.id);
    await run(e,"INSERT INTO operations(op_key,method,phase,reason,created_at,updated_at) VALUES(?,'copyMessage','unknown','connection lost',0,0)",`${e.RECOVERY_EPOCH}:send:${row.id}`);
    await handleMessage(e,job(181,m),m,false);
    await handleDeliveryNotice(e,(await one<Job>(e,"SELECT * FROM jobs WHERE kind='delivery-notice'"))!);
    expect(requests.filter(r=>r.method==='copyMessage')).toHaveLength(0);
    expect(requests.filter(r=>r.method==='sendMessage').map(r=>r.params.text)).toEqual(['发生未知情况：投递结果未知，内容不会自动重发。']);
  });

  it('clears known blocked originals but excludes messages received after the snapshot', async () => {
    await run(e, "UPDATE users SET blocked=1 WHERE tgid='42'");
    const old = privateMessage(103);
    await record(old);
    await handleMessage(e, job(13, old), old, false);
    expect(requests).toHaveLength(0);
    await snapshot('blocked-clear');
    await record(privateMessage(104));
    await clearConversation(e, job(14), (await user(e, '42'))!, 'blocked-clear');
    const deletes = requests.filter(r => r.method === 'deleteMessage');
    expect(deletes.map(r => [r.params.chat_id, r.params.message_id])).toEqual([['42', 103]]);
    expect(await one(e, 'SELECT deletion,tombstone FROM messages WHERE source_id=104')).toEqual({deletion: 'none', tombstone: 0});
    expect(await one(e, 'SELECT state FROM deletion_batches WHERE id=?', 'blocked-clear')).toEqual({state: 'done'});
  });

  it('/sc on one album member deletes only that private message and reports a partial group', async () => {
    const first = await record(privateMessage(105, {media_group_id: 'album-delete'}));
    const second = await record(privateMessage(106, {media_group_id: 'album-delete'}));
    await run(e, "UPDATE messages SET admin_id=source_id+1000,thread_id=77,delivery='success' WHERE id IN (?,?)", first.id, second.id);
    const command = adminMessage(107, {text: '/sc', reply_to_message: adminMessage(1106)});
    await deleteSingle(e, job(15, command), command, (await user(e, '42'))!);
    expect(requests.filter(r => r.method === 'deleteMessage').map(r => r.params)).toEqual([{chat_id: '42', message_id: 106}]);
    expect(await one(e, 'SELECT * FROM receipts')).toBeNull();
    expect(await one(e, "SELECT * FROM jobs WHERE kind='reaction'")).toBeNull();
    expect(requests.filter(r => r.method === 'setMessageReaction')).toEqual([]);
    expect(requests.some(r => r.method === 'sendMessage' && String(r.params.text).includes('已确认1/2'))).toBe(true);
    expect(await one(e, 'SELECT deletion FROM messages WHERE id=?', first.id)).toEqual({deletion: 'none'});
  });

  it('clears an untracked legacy outgoing album marker after partial deletion',async()=>{
    const first=await record(adminMessage(189,{media_group_id:'legacy-album'}));
    const second=await record(adminMessage(190,{media_group_id:'legacy-album'}));
    await run(e,"UPDATE messages SET private_id=source_id+1000,delivery='success' WHERE id IN (?,?)",first.id,second.id);
    await run(e,"INSERT INTO operations(op_key,method,phase,result,created_at,updated_at) VALUES(?,'setMessageReaction','success','true',1,1)",`${e.RECOVERY_EPOCH}:album:${e.ADMIN_CHAT_ID}:legacy-album:album-reaction:2`);
    const command=adminMessage(191,{text:'/sc',reply_to_message:adminMessage(190)});
    await deleteSingle(e,job(192,command),command,(await user(e,'42'))!);
    await handleReaction(e,(await one<Job>(e,"SELECT * FROM jobs WHERE kind='reaction'"))!);
    expect(requests.filter(r=>r.method==='setMessageReaction').map(r=>r.params.reaction)).toEqual([[]]);
    expect(await one(e,"SELECT phase FROM operations WHERE op_key=?",`${e.RECOVERY_EPOCH}:album:${e.ADMIN_CHAT_ID}:legacy-album:album-reaction:2`)).toEqual({phase:'success'});
  });

  it.each(['success','unknown'] as const)('clears a tracked %s album marker after partial deletion while retiring its obsolete jobs',async status=>{
    const first=await record(adminMessage(185,{media_group_id:'tracked-album'}));
    const second=await record(adminMessage(186,{media_group_id:'tracked-album'}));
    await run(e,"UPDATE messages SET private_id=source_id+1000,delivery='success' WHERE id IN (?,?)",first.id,second.id);
    await syncReceipt(e,first.id);
    const original=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='reaction'"))!;
    const normal=handler;
    handler=request=>request.method==='setMessageReaction'&&status==='unknown'?{status:'unknown',reason:'synthetic marker result unknown'}:normal(request);
    await handleReaction(e,original);
    const prior=(await all<Job>(e,"SELECT * FROM jobs WHERE kind='reaction'"));
    handler=normal;
    const command=adminMessage(187,{text:'/sc',reply_to_message:adminMessage(186)});
    await deleteSingle(e,job(188,command),command,(await user(e,'42'))!);
    for(const old of prior)expect(await one(e,'SELECT state FROM jobs WHERE id=?',old.id)).toEqual({state:'done'});
    expect(await one(e,'SELECT version,emoji,phase FROM receipts WHERE row_id=?',first.id)).toEqual({version:2,emoji:null,phase:'pending'});
    const replacement=(await one<Job>(e,"SELECT * FROM jobs WHERE kind='reaction' AND json_extract(payload,'$.version')=2"))!;
    await handleReaction(e,replacement);
    for(const old of prior)await handleReaction(e,old);
    expect(requests.filter(r=>r.method==='setMessageReaction').map(r=>r.params.reaction)).toEqual([[{type:'emoji',emoji:'👍'}],[]]);
    expect(await one(e,'SELECT deletion FROM messages WHERE id=?',first.id)).toEqual({deletion:'none'});
    expect(await one(e,'SELECT deletion FROM messages WHERE id=?',second.id)).toEqual({deletion:'deleted'});
  });

  it('never revives deleted content and rejects stale edits, including ties within one second', async () => {
    const original = privateMessage(108);
    const row = await record(original);
    await run(e, "UPDATE messages SET admin_id=900,thread_id=77,delivery='success' WHERE id=?", row.id);
    const latest = privateMessage(108, {text: 'latest', edit_date: 110});
    await handleMessage(e, job(30, latest, true), latest, true);
    const stale = privateMessage(108, {text: 'stale', edit_date: 110});
    await handleMessage(e, job(29, stale, true), stale, true);
    await run(e, "UPDATE messages SET tombstone=1,deletion='deleted' WHERE id=?", row.id);
    const deletedEdit = privateMessage(108, {text: 'must not revive', edit_date: 120});
    await handleMessage(e, job(31, deletedEdit, true), deletedEdit, true);
    expect(requests.filter(r => r.method.startsWith('edit')).map(r => r.params.text)).toEqual(['latest']);
    expect(requests.some(r => r.method === 'copyMessage')).toBe(false);
  });

  it('waits for an original message instead of converting an early edit into a new send', async () => {
    const m = privateMessage(109, {text: 'edit first', edit_date: 120});
    const j = job(32, m, true);
    await run(e, 'INSERT INTO jobs(id,dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES(?,?,?,?,?,?,?,?,?)', j.id,j.dedupe,j.user_key,j.kind,j.payload,j.state,j.due_at,j.epoch,j.created_at);
    await handleMessage(e, j, m, true);
    expect(requests).toHaveLength(0);
    expect(await one(e, 'SELECT state FROM jobs WHERE id=?', j.id)).toEqual({state: 'waiting'});
  });

  it('recovers an album after mapping-write failure while retaining a late member separately', async () => {
    const first = privateMessage(110, {text: undefined, photo: [{file_id: 'photo-1'}], media_group_id: 'recover'});
    const second = privateMessage(111, {text: undefined, video: {file_id: 'video-2'}, media_group_id: 'recover'});
    await record(first); await record(second);
    const db = e.DB;
    let failMapping = true;
    e.DB = new Proxy(db, {get(target, property) {
      if (property === 'batch') return async (statements: D1PreparedStatement[]) => {
        if (failMapping && statements.length === 3 && requests.some(r => r.method === 'sendMediaGroup')) {failMapping = false; throw new Error('simulated D1 mapping interruption');}
        return target.batch(statements);
      };
      const value = Reflect.get(target, property);
      return typeof value === 'function' ? value.bind(target) : value;
    }});
    await expect(handleMessage(e, job(33, first), first, false)).rejects.toThrow('simulated D1');
    const late = privateMessage(112, {text: undefined, photo: [{file_id: 'photo-3'}], media_group_id: 'recover'});
    await record(late);
    await handleMessage(e, job(33, first), first, false);
    await handleMessage(e, job(34, late), late, false);
    expect(requests.filter(r => r.method === 'sendMediaGroup')).toHaveLength(1);
    expect(requests.filter(r => r.method === 'copyMessage')).toHaveLength(1);
    expect((await all<MessageRow>(e, "SELECT * FROM messages WHERE media_group='recover' ORDER BY source_id")).map(r => [r.admin_id,r.delivery])).toEqual([[801,'success'],[802,'success'],[803,'success']]);
  });

  it('captures an in-flight fixed reply in the clear snapshot and deletes real user-side ids', async () => {
    const m = privateMessage(113, {text: '/start'});
    await record(m);
    const normal = handler;
    let captured = false;
    handler = async request => {
      if (!captured && request.method === 'sendMessage' && request.params.chat_id === '42') {captured = true; await snapshot('during-fixed');}
      return normal(request);
    };
    await handleMessage(e, job(35, m), m, false);
    await clearConversation(e, job(36), (await user(e, '42'))!, 'during-fixed');
    expect(requests.filter(r => r.method === 'deleteMessage').map(r => r.params.message_id)).toEqual([113,801]);
    expect((await all<{result: string}>(e, "SELECT result FROM deletion_items WHERE batch_id='during-fixed'")).map(r => r.result)).toEqual(['deleted','deleted']);
  });

  it('does not turn an unknown deletion into success or retry it after a second /sc', async () => {
    const row = await record(adminMessage(114));
    await run(e, "UPDATE messages SET private_id=914,delivery='success' WHERE id=?", row.id);
    const normal = handler;
    handler = request => request.method === 'deleteMessage' ? {status: 'unknown', reason: 'connection lost'} : normal(request);
    const m = adminMessage(115, {text: '/sc', reply_to_message: adminMessage(114)});
    await deleteSingle(e, job(37, m), m, (await user(e, '42'))!);
    await deleteSingle(e, job(38, m), m, (await user(e, '42'))!);
    expect(requests.filter(r => r.method === 'deleteMessage')).toHaveLength(1);
    expect(requests.some(r => r.method === 'setMessageReaction')).toBe(false);
    expect(await one(e, 'SELECT deletion FROM messages WHERE id=?', row.id)).toEqual({deletion: 'unknown'});
  });

  it('recovers a delivered private target from the ledger when clear follows a mapping-write gap', async () => {
    const m = adminMessage(116);
    const row = await record(m);
    // This is the persistent state after an API success followed by a failed
    // messages write: the operation journal knows the ID, the mapping does not.
    await call(e, `send:${row.id}`, 'copyMessage', {chat_id: '42', from_chat_id: e.ADMIN_CHAT_ID, message_id: m.message_id});
    await snapshot('mapping-gap');
    await clearConversation(e, job(39), (await user(e, '42'))!, 'mapping-gap');
    expect(requests.filter(r => r.method === 'copyMessage')).toHaveLength(1);
    expect(requests.filter(r => r.method === 'deleteMessage').map(r => r.params)).toEqual([{chat_id: '42', message_id: 801}]);
    expect(await one(e, 'SELECT private_id,deletion FROM messages WHERE id=?', row.id)).toEqual({private_id: 801, deletion: 'deleted'});
  });

  it('continues a clear in bounded persisted batches without deleting later messages', async () => {
    for (let id = 120; id < 145; id++) await record(privateMessage(id));
    await snapshot('bounded');
    await record(privateMessage(145));
    await clearConversation(e, job(40), (await user(e, '42'))!, 'bounded');
    expect(requests.filter(r => r.method === 'deleteMessage')).toHaveLength(2);
    for (let round = 0; round < 20; round++) {
      const continuation = await one<Job>(e, "SELECT * FROM jobs WHERE kind='clear' AND state='pending' ORDER BY id LIMIT 1");
      if (!continuation) break;
      expect(JSON.parse(continuation.payload)).toEqual({batchId: 'bounded'});
      await clearConversation(e, continuation, (await user(e, '42'))!, 'bounded');
      await run(e, "UPDATE jobs SET state='done' WHERE id=?", continuation.id);
    }
    expect(requests.filter(r => r.method === 'deleteMessage')).toHaveLength(25);
    expect(requests.some(r => r.method === 'deleteMessage' && r.params.message_id === 145)).toBe(false);
    expect(await one(e, "SELECT state FROM deletion_batches WHERE id='bounded'")).toEqual({state: 'done'});
  });

  it.each([
    {label:'topic-created root',replyId:77,service:true,expectedAudits:0},
    {label:'unmapped ordinary root message',replyId:77,service:false,expectedAudits:1},
    {label:'another topic-created service message',replyId:78,service:true,expectedAudits:1},
  ])('omits $label from the private copy with the appropriate fallback audit', async sample => {
    const root=adminMessage(sample.replyId,{text:undefined,...(sample.service?{forum_topic_created:{name:'Synthetic topic',icon_color:0}}:{})});
    const outbound=adminMessage(155,{reply_to_message:root});
    await record(outbound);
    await handleMessage(e,job(46,outbound),outbound,false);
    expect(requests.filter(r=>r.method==='copyMessage')).toHaveLength(1);
    expect(requests.find(r=>r.method==='copyMessage')?.params.reply_parameters).toBeUndefined();
    expect(await all(e,"SELECT id FROM audit_logs WHERE event='reply_fallback'")).toHaveLength(sample.expectedAudits);
  });

  it('retains fallback accounting for a private message with a forged forum-root shape', async () => {
    const incoming=privateMessage(156,{message_thread_id:77,is_topic_message:true,reply_to_message:privateMessage(77,{text:undefined,forum_topic_created:{name:'Synthetic topic',icon_color:0}})});
    await record(incoming);
    await handleMessage(e,job(47,incoming),incoming,false);
    expect(requests.find(r=>r.method==='copyMessage')?.params.reply_parameters).toBeUndefined();
    expect(await all(e,"SELECT id FROM audit_logs WHERE event='reply_fallback'")).toHaveLength(1);
  });

  it.each(['quote','external_reply'])('retains fallback accounting when a service-root message includes explicit %s metadata', async field => {
    const root=adminMessage(77,{text:undefined,forum_topic_created:{name:'Synthetic topic',icon_color:0}});
    const outbound=adminMessage(157,{reply_to_message:root});
    Object.assign(outbound,{[field]:{text:'Synthetic explicit quote',position:0}});
    await record(outbound);
    await handleMessage(e,job(48,outbound),outbound,false);
    expect(requests.find(r=>r.method==='copyMessage')?.params.reply_parameters).toBeUndefined();
    expect(await all(e,"SELECT id FROM audit_logs WHERE event='reply_fallback'")).toHaveLength(1);
  });

  it('maps reply references across both directions without quoting another user', async () => {
    const incoming = await record(privateMessage(146));
    await run(e, "UPDATE messages SET admin_id=946,thread_id=77,delivery='success' WHERE id=?", incoming.id);
    const outbound = adminMessage(147, {reply_to_message: adminMessage(946)});
    const outgoing = await record(outbound);
    await handleMessage(e, job(41, outbound), outbound, false);
    expect(requests.find(r => r.method === 'copyMessage')?.params.reply_parameters).toEqual({message_id: 146, allow_sending_without_reply: true});
    const outgoingRow = (await one<MessageRow>(e, 'SELECT * FROM messages WHERE id=?', outgoing.id))!;
    const answer = privateMessage(148, {reply_to_message: privateMessage(outgoingRow.private_id!)});
    await record(answer);
    await handleMessage(e, job(42, answer), answer, false);
    expect(requests.filter(r => r.method === 'copyMessage')[1].params.reply_parameters).toEqual({message_id: 147, allow_sending_without_reply: true});
    await run(e, "INSERT INTO messages(user_id,direction,source_chat,source_id,private_id,admin_id,thread_id,business,received_at,delivery,payload) VALUES('777','in','777',1,999,1000,88,1,0,'success','{}')");
    const foreignQuote = privateMessage(149, {reply_to_message: privateMessage(999)});
    await record(foreignQuote);
    await handleMessage(e, job(43, foreignQuote), foreignQuote, false);
    expect(requests.filter(r => r.method === 'copyMessage')[2].params.reply_parameters).toBeUndefined();
  });

  it('does not create a topic or answer private service updates and failures', async () => {
    const service = privateMessage(150, {text: undefined});
    await record(service);
    await handleMessage(e, job(44, service), service, false);
    expect(requests).toHaveLength(0);
    expect(await one(e, 'SELECT delivery,business FROM messages WHERE source_id=150')).toEqual({delivery: 'ignored', business: 0});
    const m = privateMessage(151);
    await record(m);
    handler = () => ({status: 'failed', reason: 'copy rejected', code: 400});
    await handleMessage(e, job(45, m), m, false);
    expect(requests.filter(r => r.method === 'copyMessage')).toHaveLength(1);
    expect(requests.filter(r => r.params.chat_id === '42')).toHaveLength(0);
  });

  it('preserves the received business count when a binding switches before an admitted admin message runs', async () => {
    const m = adminMessage(152);
    const row = await record(m);
    await run(e, "UPDATE users SET thread_id=78,binding_version=binding_version+1 WHERE tgid='42'");
    await handleMessage(e, job(46, m), m, false);
    expect(requests.some(r => r.method === 'copyMessage')).toBe(false);
    expect(await one(e, 'SELECT business,delivery,thread_id FROM messages WHERE id=?', row.id)).toEqual({business: 1, delivery: 'suppressed', thread_id: 77});
  });

  it('preserves the original recovery provenance on fixed-command private replies', async () => {
    e = {...e, RECOVERY_EPOCH: 'new-generation'};
    const m = privateMessage(153, {text: '/help'});
    await accept(e, {update_id: 47, message: m});
    const originalJob = (await one<Job>(e, "SELECT * FROM jobs WHERE dedupe='u:47'"))!;
    await handleMessage(e, originalJob, m, false);
    expect((await all<MessageRow>(e, 'SELECT * FROM messages ORDER BY id')).map(r => [r.direction, r.epoch])).toEqual([['in', 'new-generation'], ['system', 'new-generation']]);
  });
});
