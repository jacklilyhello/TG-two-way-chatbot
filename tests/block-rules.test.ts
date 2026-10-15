import {env} from 'cloudflare:test';
import {describe,expect,it} from 'vitest';
import type {Env,Job,Params,TGMessage,TGUpdate} from '../src/types';
import {all,one,run} from '../src/db';
import {RetryLater} from '../src/operations';
import {handleAdmin} from '../src/admin';
import {accept} from '../src/ingress';
import {addBlockRule,changeBlockRule,compileRule,handleRuleAction,handleRuleCommand,matchBlockRules,RULE_LIMITS,rulesPanel,testCompiledRule,type BlockRule,type RuleAction} from '../src/block-rules';

const base=env as unknown as Env;
interface Request {key:string;method:string;params:Params}
function client(onRequest?:(request:Request)=>Response) {
 const requests:Request[]=[];let sequence=900;
 const e={...base,JOBS:{send:async()=>{}},CONVERSATIONS:{idFromName:()=>'',get:()=>({fetch:async()=>new Response('ok')})},BOT:{idFromName:()=>'',get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  const body=JSON.parse(String(init.body)) as Request;requests.push(body);return onRequest?.(body)??Response.json({status:'success',result:{message_id:++sequence}});
 }})}} as unknown as Env;
 return {e,requests};
}
const job=(id:number):Job=>({id,dedupe:`rule:${id}`,user_key:'admin',kind:'update',payload:'{}',state:'running',due_at:0,attempts:0,epoch:base.RECOVERY_EPOCH,created_at:1});
const test=(pattern:string,text:string)=>testCompiledRule(compileRule(pattern),text);

describe('bounded regular-language matcher',()=>{
 it.each([
  ['优惠|推广','领取优惠',true],['(优惠|推广)\\s*[0-9]+','限时推广 2026',true],['^广告[0-9]{2,4}$','广告123',true],['^广告[0-9]{2,4}$','前缀广告123',false],
  ['^a(bc)?d+$','addd',true],['^a(bc)?d+$','abcdd',true],['^a(bc)?d+$','abdd',false],['^x{2,}y$','xxxxy',true],['^x{2,}y$','xy',false],
  ['[^a-z]+','hello8',true],['[^a-z]+','hello',false],['广告\\.','广告.',true],['[😀-🙏]+','优惠😀',true],['\\w+@\\w+','abc@example',true],
  ['\\D+','ABC',true],['\\D+','123',false],['^line\\nline$','line\nline',true],['a.b','a\nb',false],['[a-]+','a--',true],['[-a]+','--a',true],
  ['\\S+','\t ',false],['^\\W+$','-?!',true],['\\[ad\\]','[ad]',true],['a{0,2}b','b',true]
 ])('matches %s against bounded text', (pattern,text,expected)=>{expect(test(pattern as string,text as string)).toBe(expected);});
 it.each(['','a*','^$','a|','()','(?=a)a','(?!a)b','(?:a)','(a)\\1','\\bword','a{41}','a{4,2}','a{2','[z-a]','[]','[a','a++','a*?','^+a','\\','/ad/i','x'.repeat(201),'('.repeat(13)+'a'+')'.repeat(13),'(abcde){40}'])('rejects invalid, empty matching or oversized syntax %s',pattern=>{expect(()=>compileRule(pattern)).toThrow();});
 it('simulates classic backtracking attacks over full allowed input without backtracking',()=>{
  const input='a'.repeat(RULE_LIMITS.field-1)+'!';
  expect(test('(a+)+$',input)).toBe(false);
  expect(test('(a|aa)+$',input)).toBe(false);
  expect(test('a+a+a+a+a+z',input)).toBe(false);
 });
 it('agrees with standard regular language semantics across short generated inputs',()=>{
  const patterns=['^a(b|c)+$','a{1,3}b?','(ab|a)+c','[a-c]{2,4}','^a[^b]*c$','(a+b)+c','^a{0,2}b+$','a(b|bc)*c','^a?b?c$','^(a|bc){1,3}$'];
  const strings=[''];
  for(let size=1;size<=5;size++){const previous=strings.filter(text=>text.length===size-1);for(const prefix of previous)for(const char of ['a','b','c'])strings.push(prefix+char);}
  for(const pattern of patterns){const machine=compileRule(pattern),standard=new RegExp(pattern,'u');for(const text of strings)expect(testCompiledRule(machine,text),`${pattern} / ${text}`).toBe(standard.test(text));}
 });
});

describe('persistent administrator block rules',()=>{
 it('matches case insensitive keyword text and media captions, and case sensitive regex independently',async()=>{
  const {e}=client();
  await addBlockRule(e,job(1),'10001','keyword','SPECIAL offer');
  await addBlockRule(e,job(2),'10001','regex','^CODE-[0-9]{3}$');
  expect(await matchBlockRules(e,{text:'Today special OFFER!'})).toMatchObject({id:1,kind:'keyword'});
  expect(await matchBlockRules(e,{caption:'Picture SPECIAL OFFER'})).toMatchObject({id:1});
  expect(await matchBlockRules(e,{text:'CODE-123'})).toMatchObject({id:2,kind:'regex'});
  expect(await matchBlockRules(e,{caption:'CODE-123'})).toMatchObject({id:2});
  expect(await matchBlockRules(e,{text:'code-123'})).toBeNull();
  expect(await matchBlockRules(e,{text:'CODE-',caption:'123'})).toBeNull();
  expect(await matchBlockRules(e,{})).toBeNull();
  // The cache is compilation-only; matching always reads this instance's DB.
  expect(await matchBlockRules({...e,VERSION:'after-restart'}, {text:'SPECIAL OFFER'})).toMatchObject({id:1});
  await changeBlockRule(e,job(3),'10001',1,'off');
  expect(await matchBlockRules(e,{text:'special offer'})).toBeNull();
  await changeBlockRule(e,job(4),'10001',1,'on');
  expect(await matchBlockRules(e,{text:'special offer'})).toMatchObject({id:1});
  await changeBlockRule(e,job(5),'10001',1,'delete');
  expect(await matchBlockRules(e,{text:'special offer'})).toBeNull();
  expect(await one(e,'SELECT deleted,enabled FROM block_rules WHERE id=1')).toEqual({deleted:1,enabled:0});
 });
 it('keeps add and change mutations idempotent under retries and concurrent repeats',async()=>{
  const {e}=client();
  await Promise.all([addBlockRule(e,job(10),'10001','keyword','repeat-ad'),addBlockRule(e,job(10),'10001','keyword','repeat-ad')]);
  expect(await all(e,'SELECT id FROM block_rules')).toHaveLength(1);
  expect(await all(e,"SELECT id FROM audit_logs WHERE event='block_rule_created'")).toHaveLength(1);
  await Promise.all([changeBlockRule(e,job(11),'10001',1,'off'),changeBlockRule(e,job(11),'10001',1,'off')]);
  expect(await one(e,'SELECT enabled,version FROM block_rules WHERE id=1')).toEqual({enabled:0,version:2});
  await changeBlockRule(e,job(12),'10001',1,'on');
  await addBlockRule(e,job(10),'10001','keyword','repeat-ad');
  await changeBlockRule(e,job(11),'10001',1,'off');
  expect(await one(e,'SELECT enabled,version FROM block_rules WHERE id=1')).toEqual({enabled:1,version:3});
  expect(await all(e,"SELECT id FROM audit_logs WHERE event='block_rule_changed'")).toHaveLength(2);
  expect(JSON.stringify(await all(e,'SELECT detail FROM audit_logs'))).not.toContain('repeat-ad');
 });
 it('does not repeat mutation when an administrator confirmation is rate limited',async()=>{
  let limited=true;const {e}=client(request=>{
   if(request.key.endsWith(':rules:add')&&limited){limited=false;return Response.json({due:Date.now()+1},{status:429});}
   return Response.json({status:'success',result:{message_id:900}});
  });
  await expect(handleRuleCommand(e,job(20),'10001','add keyword retry-ad')).rejects.toBeInstanceOf(RetryLater);
  await handleRuleCommand(e,job(20),'10001','add keyword retry-ad');
  expect(await all(e,'SELECT id FROM block_rules')).toHaveLength(1);
  expect(await all(e,"SELECT id FROM audit_logs WHERE event='block_rule_created'")).toHaveLength(1);
 });
 it('rolls rule mutations and audit markers back together on database failure',async()=>{
  const {e}=client();
  await run(e,"CREATE TRIGGER reject_rule_audit BEFORE INSERT ON audit_logs BEGIN SELECT RAISE(ABORT,'synthetic failure'); END");
  await expect(addBlockRule(e,job(30),'10001','keyword','failure-ad')).rejects.toThrow();
  expect(await all(e,'SELECT id FROM block_rules')).toHaveLength(0);
  expect(await all(e,'SELECT op_key FROM block_rule_mutations')).toHaveLength(0);
  await run(e,'DROP TRIGGER reject_rule_audit');
  await addBlockRule(e,job(30),'10001','keyword','failure-ad');
  await run(e,"CREATE TRIGGER reject_rule_audit BEFORE INSERT ON audit_logs BEGIN SELECT RAISE(ABORT,'synthetic failure'); END");
  await expect(changeBlockRule(e,job(31),'10001',1,'off')).rejects.toThrow();
  expect(await one(e,'SELECT enabled,version FROM block_rules WHERE id=1')).toEqual({enabled:1,version:1});
  expect(await one(e,"SELECT op_key FROM block_rule_mutations WHERE op_key LIKE '%:31:%'")).toBeNull();
 });
 it('rejects invalid rules and enforces count plus aggregate regex state limits',async()=>{
  const {e}=client();
  await expect(addBlockRule(e,job(40),'10001','regex','(?=bad)bad')).rejects.toThrow();
  await expect(addBlockRule(e,job(40),'10001','regex','.*')).rejects.toThrow();
  await expect(addBlockRule(e,job(40),'10001','keyword','  ')).rejects.toThrow();
  for(let i=0;i<10;i++)expect((await addBlockRule(e,job(41+i),'10001','regex',`ad${i}`)).result).toBe('created');
  expect((await addBlockRule(e,job(51),'10001','regex','extra')).result).toBe('limit');
  await changeBlockRule(e,job(52),'10001',1,'delete');
  expect((await addBlockRule(e,job(53),'10001','regex','extra')).result).toBe('created');
  for(let i=0;i<40;i++)await addBlockRule(e,job(60+i),'10001','keyword',`keyword${i}`);
  expect((await addBlockRule(e,job(101),'10001','keyword','overflow')).result).toBe('limit');
 });
 it('bounds matching and skips malformed saved expressions without breaking normal messages',async()=>{
  const {e}=client();await addBlockRule(e,job(110),'10001','keyword','endmarker');
  expect(await matchBlockRules(e,{text:'x'.repeat(RULE_LIMITS.field)+'endmarker'})).toBeNull();
  await addBlockRule(e,job(111),'10001','regex','^safe$');
  await run(e,"UPDATE block_rules SET pattern='(?=unsafe)' WHERE id=2");
  expect(await matchBlockRules(e,{text:'normal'})).toBeNull();
  expect(await matchBlockRules(e,{caption:'endmarker'})).toMatchObject({id:1});
 });
 it('limits aggregate compiled regex work including disabled rules and releases budget only on delete',async()=>{
  const {e}=client();
  for(let i=0;i<4;i++)expect((await addBlockRule(e,job(150+i),'10001','regex',`(abc){40}${i}`)).result).toBe('created');
  expect((await addBlockRule(e,job(154),'10001','regex','(abc){40}4')).result).toBe('limit');
  await changeBlockRule(e,job(155),'10001',1,'off');
  expect((await addBlockRule(e,job(156),'10001','regex','(abc){40}4')).result).toBe('limit');
  await changeBlockRule(e,job(157),'10001',1,'delete');
  expect((await addBlockRule(e,job(158),'10001','regex','(abc){40}4')).result).toBe('created');
 });
 it('restricts rules and buttons to configured administrators and stable list versions',async()=>{
  const {e,requests}=client();
  await expect(addBlockRule(e,job(120),'20001','keyword','bad')).rejects.toThrow('rule_admin_required');
  await handleRuleCommand(e,job(120),'20001','add keyword bad');await rulesPanel(e,job(120),'20001');
  expect(requests).toHaveLength(0);expect(await all(e,'SELECT id FROM block_rules')).toHaveLength(0);
  await addBlockRule(e,job(121),'10001','keyword','managed-ad');await rulesPanel(e,job(122),'10001');
  const actions=await all<RuleAction>(e,"SELECT * FROM callback_actions WHERE kind='rule_change'");
  expect(actions).toHaveLength(1);const action=actions[0],payload=JSON.parse(action.payload);
  expect(payload).toMatchObject({instance:e.INSTANCE_ID,id:1,version:1,messageId:901});
  expect(await handleRuleAction(e,job(123),action,payload,124,'20001')).toBe('无权操作。');
  await changeBlockRule(e,job(124),'10001',1,'off');
  expect(await handleRuleAction(e,job(125),action,payload,126,'10001')).toBe('规则已变化，请使用最新列表。');
  expect(await one(e,'SELECT enabled,version FROM block_rules WHERE id=1')).toEqual({enabled:0,version:2});
 });
 it('confirms deletion through an actor scoped original-message token and retains only a soft deleted rule',async()=>{
  const {e}=client();await addBlockRule(e,job(130),'10001','keyword','deleted-ad');await rulesPanel(e,job(131),'10001');
  const action=(await one<RuleAction>(e,"SELECT * FROM callback_actions WHERE kind='rule_delete_prompt'"))!;
  await handleRuleAction(e,job(132),action,JSON.parse(action.payload),133,'10001');
  const confirm=(await one<RuleAction>(e,"SELECT * FROM callback_actions WHERE kind='rule_delete_confirm'"))!;
  expect(JSON.parse(confirm.payload)).toMatchObject({messageId:902,instance:e.INSTANCE_ID});
  expect(await handleRuleAction(e,job(133),confirm,JSON.parse(confirm.payload),134,'10001')).toBe('规则已更新。');
  expect(await one<BlockRule>(e,'SELECT * FROM block_rules WHERE id=1')).toMatchObject({deleted:1,enabled:0});
  expect((await one<RuleAction>(e,'SELECT * FROM callback_actions WHERE token=?',confirm.token))!.used).toBe(134);
 });
 it('preserves internal whitespace in keyword command patterns and gives useful regex errors',async()=>{
  const {e,requests}=client();await handleRuleCommand(e,job(140),'10001','add keyword same  words\nnext');
  expect(await one(e,'SELECT pattern FROM block_rules')).toEqual({pattern:'same  words\nnext'});
  await handleRuleCommand(e,job(141),'10001','add regex (?!bad)');
  expect(String(requests.at(-1)!.params.text)).toContain('正则无效');
  expect(await all(e,'SELECT id FROM block_rules')).toHaveLength(1);
 });
});

describe('rule management actual administrator dispatcher',()=>{
 const message=(e:Env,id:number,text:string,actor=10001,thread?:number):TGMessage=>({message_id:id,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup',is_forum:true},from:{id:actor},text,...(thread?{is_topic_message:true,message_thread_id:thread}:{})});
 const callback=(e:Env,id:number,action:RuleAction,overrides:Partial<TGMessage>={},actor=10001):TGUpdate=>({update_id:id,callback_query:{id:`rules-cb:${id}`,from:{id:actor},data:`a:${action.token}`,message:{...message(e,JSON.parse(action.payload).messageId,''),from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true},...overrides}}});
 it('requires General and configured administrator for rule commands and exposes main panel entrance',async()=>{
  const {e,requests}=client();
  await handleAdmin(e,job(200),{update_id:200,message:message(e,200,'/mb rules add keyword no-access',20001)});
  await handleAdmin(e,job(201),{update_id:201,message:message(e,201,'/mb rules add keyword wrong-topic',10001,40)});
  expect(await all(e,'SELECT id FROM block_rules')).toHaveLength(0);
  expect(requests.some(request=>String(request.params.text).includes('no-access'))).toBe(false);
  await handleAdmin(e,job(202),{update_id:202,message:message(e,202,'/mb rules add keyword accepted-ad')});
  expect(await one(e,'SELECT pattern FROM block_rules')).toEqual({pattern:'accepted-ad'});
  await handleAdmin(e,job(203),{update_id:203,message:message(e,203,'/mb')});
  expect(JSON.stringify(requests.at(-1)?.params.reply_markup)).toContain('拦截规则');
  const entry=(await one<RuleAction>(e,"SELECT * FROM callback_actions WHERE kind='panel' AND json_extract(payload,'$.view')='rules'"))!;
  await handleAdmin(e,job(204),callback(e,204,entry));
  expect(requests.some(request=>String(request.params.text).includes('拦截规则 ·'))).toBe(true);
 });
 it('rejects copied, forwarded, other-instance and unauthorized callbacks before changing a rule',async()=>{
  const {e}=client();await addBlockRule(e,job(210),'10001','keyword','protected-ad');await rulesPanel(e,job(211),'10001');
  const action=(await one<RuleAction>(e,"SELECT * FROM callback_actions WHERE kind='rule_change'"))!;
  const mId=JSON.parse(action.payload).messageId;
  await handleAdmin(e,job(212),callback(e,212,action,{},20001));
  await handleAdmin(e,job(213),callback(e,213,action,{message_id:mId+10}));
  await handleAdmin(e,job(214),callback(e,214,action,{message_thread_id:40,is_topic_message:true}));
  await handleAdmin(e,job(215),callback(e,215,action,{chat:{id:-1009999999999,type:'supergroup'}}));
  await handleAdmin(e,job(216),callback(e,216,action,{from:{id:20001}}));
  await handleAdmin({...e,INSTANCE_ID:'other-instance'},job(217),callback(e,217,action));
  await handleAdmin({...e,ADMIN_IDS:'10001,10002'},job(218),callback(e,218,action,{},10002));
  expect(await one(e,'SELECT enabled,version FROM block_rules WHERE id=1')).toEqual({enabled:1,version:1});
  expect((await one<RuleAction>(e,'SELECT * FROM callback_actions WHERE token=?',action.token))!.used).toBe(0);
  await handleAdmin(e,job(219),callback(e,219,action));
  await handleAdmin(e,job(219),callback(e,219,action));
  expect(await one(e,'SELECT enabled,version FROM block_rules WHERE id=1')).toEqual({enabled:0,version:2});
  expect(await all(e,"SELECT id FROM audit_logs WHERE event='block_rule_changed'")).toHaveLength(1);
 });
 it('routes panel and rule callbacks onto a nonempty durable administrator lane',async()=>{
  const {e}=client();await rulesPanel(e,job(230),'10001');
  const action=(await one<RuleAction>(e,"SELECT * FROM callback_actions WHERE kind='rule_help'"))!;
  expect(await accept(e,callback(e,231,action))).toBe('admin:10001');
  expect(await one(e,"SELECT user_key FROM jobs WHERE dedupe='u:231'")).toEqual({user_key:'admin:10001'});
 });
});
