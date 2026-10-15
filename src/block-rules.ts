import type {Env, Job, Params, TGMessage} from './types';
import {all, logEpochStatements, logOutboxStatements, now, one, run, stmt, wakeLogs} from './db';
import {notify} from './operations';

export const RULE_LIMITS = {rules:50, pattern:200, field:8192, states:128, totalStates:512, regexRules:10, repeat:40, depth:12} as const;
export interface BlockRule {id:number;kind:'keyword'|'regex';pattern:string;enabled:number;deleted:number;version:number;state_count:number}
export interface RuleAction {token:string;kind:string;actor_id:string;thread_id:number|null;used:number;expires_at:number;payload:string}
type Button = {text:string;callback_data:string};
type CharTest = {kind:'literal';value:string}|{kind:'any'}|{kind:'range';from:number;to:number}|{kind:'set';items:CharTest[];negated:boolean}|{kind:'digit'|'word'|'space';negated:boolean};
type Node = {kind:'empty'}|{kind:'char';test:CharTest}|{kind:'start'|'end'}|{kind:'sequence'|'alternate';nodes:Node[]}|{kind:'repeat';node:Node;min:number;max:number|null};
interface State {kind:'char'|'split'|'start'|'end'|'match';next:number;other?:number;test?:CharTest}
export interface CompiledRule {states:State[];start:number}
const charNode=(value:string):Node=>({kind:'char',test:{kind:'literal',value}});

/** A small regular-language parser. User patterns never reach JavaScript's
 * backtracking RegExp implementation. Thompson simulation visits each state
 * once per character, including nested repetition and overlapping branches. */
export function compileRule(pattern:string):CompiledRule {
 if(!pattern||pattern.length>RULE_LIMITS.pattern)throw new Error('正则请输入 1–200 字符。');
 if(pattern.startsWith('/')&&/\/[a-z]*$/i.test(pattern))throw new Error('正则请直接填写表达式，不使用 / 分隔符或 flags。');
 const chars=Array.from(pattern);let cursor=0,depth=0;
 const fail=():never=>{throw new Error('正则无效或包含不支持的语法。支持字面量、.、[]、()、|、^、$、*、+、?、{m,n} 以及 \\d/\\w/\\s；不支持回溯引用、前后查找、内联标志或 /.../flags。');};
 function escaped(inClass=false):CharTest {
  const c=chars[cursor++];if(c===undefined)fail();
  const types:Record<string,'digit'|'word'|'space'>={d:'digit',D:'digit',w:'word',W:'word',s:'space',S:'space'};
  if(types[c])return {kind:types[c],negated:c===c.toUpperCase()};
  if(c==='n'||c==='r'||c==='t')return {kind:'literal',value:{n:'\n',r:'\r',t:'\t'}[c]!};
  if('\\.^$|?*+()[]{}-/'.includes(c)||(inClass&&c==='^'))return {kind:'literal',value:c};
  return fail();
 }
 function characterClass():CharTest {
  const negated=chars[cursor]==='^';if(negated)cursor++;
  const items:CharTest[]=[];
  const item=():CharTest=>{const c=chars[cursor++];if(c===undefined||c===']')return fail();return c==='\\'?escaped(true):{kind:'literal',value:c};};
  while(cursor<chars.length&&chars[cursor]!==']') {
   const first=item();
   if(chars[cursor]==='-'&&chars[cursor+1]!==']'&&chars[cursor+1]!==undefined) {
    cursor++;const last=item();
    if(first.kind!=='literal'||last.kind!=='literal')fail();
    const from=(first as {value:string}).value.codePointAt(0)!,to=(last as {value:string}).value.codePointAt(0)!;
    if(from>to)fail();items.push({kind:'range',from,to});
   } else items.push(first);
  }
  if(chars[cursor++]!==']'||!items.length)fail();
  return {kind:'set',items,negated};
 }
 function atom():Node {
  const c=chars[cursor++];if(c===undefined)return fail();
  if(c==='(') {
   if(++depth>RULE_LIMITS.depth||chars[cursor]==='?')fail();
   const node=alternate();if(chars[cursor++]!==')')fail();depth--;return node;
  }
  if(c==='[')return {kind:'char',test:characterClass()};
  if(c==='\\')return {kind:'char',test:escaped()};
  if(c==='.')return {kind:'char',test:{kind:'any'}};
  if(c==='^'||c==='$')return {kind:c==='^'?'start':'end'};
  if(')*+?{}]'.includes(c))return fail();
  return charNode(c);
 }
 function quantified():Node {
  const node=atom(),c=chars[cursor];let min:number,max:number|null;
  if(c==='*'||c==='+'||c==='?') {cursor++;min=c==='+'?1:0;max=c==='?'?1:null;}
  else if(c==='{') {
   cursor++;let left='';while(cursor<chars.length&&chars[cursor]>='0'&&chars[cursor]<='9')left+=chars[cursor++];
   if(!left)fail();min=Number(left);max=min;
   if(chars[cursor]===',') {cursor++;let right='';while(cursor<chars.length&&chars[cursor]>='0'&&chars[cursor]<='9')right+=chars[cursor++];max=right?Number(right):null;}
   if(chars[cursor++]!=='}'||min>RULE_LIMITS.repeat||(max!==null&&(max<min||max>RULE_LIMITS.repeat)))throw new Error('正则重复次数无效；有限重复上限为 40。');
  } else return node;
  if(node.kind==='start'||node.kind==='end'||'*+?{'.includes(chars[cursor]??'\0'))fail();
  return {kind:'repeat',node,min,max};
 }
 function sequence():Node {
  const nodes:Node[]=[];while(cursor<chars.length&&chars[cursor]!==')'&&chars[cursor]!=='|')nodes.push(quantified());
  return !nodes.length?{kind:'empty'}:nodes.length===1?nodes[0]:{kind:'sequence',nodes};
 }
 function alternate():Node {
  const nodes=[sequence()];while(chars[cursor]==='|'){cursor++;nodes.push(sequence());}
  return nodes.length===1?nodes[0]:{kind:'alternate',nodes};
 }
 const root=alternate();if(cursor!==chars.length)fail();
 const states:State[]=[];
 const push=(state:State)=>{if(states.length>=RULE_LIMITS.states)throw new Error('正则过于复杂；请缩短表达式或减少重复次数。');states.push(state);return states.length-1;};
 function build(node:Node,next:number):number {
  if(node.kind==='empty')return next;
  if(node.kind==='char')return push({kind:'char',test:node.test,next});
  if(node.kind==='start'||node.kind==='end')return push({kind:node.kind,next});
  if(node.kind==='sequence'){for(let i=node.nodes.length-1;i>=0;i--)next=build(node.nodes[i],next);return next;}
  if(node.kind==='alternate'){let start=build(node.nodes.at(-1)!,next);for(let i=node.nodes.length-2;i>=0;i--)start=push({kind:'split',next:build(node.nodes[i],next),other:start});return start;}
  const repetition=node as Extract<Node,{kind:'repeat'}>;
  if(repetition.max===null) {const loop=push({kind:'split',next:-1,other:next});states[loop].next=build(repetition.node,loop);next=loop;}
  else for(let i=repetition.min;i<repetition.max;i++)next=push({kind:'split',next:build(repetition.node,next),other:next});
  for(let i=0;i<repetition.min;i++)next=build(repetition.node,next);
  return next;
 }
 const match=push({kind:'match',next:-1}),start=build(root,match);
 const compiled={states,start};
 if(testCompiledRule(compiled,''))throw new Error('正则不能匹配空字符串；请加入需要实际出现的文字。');
 return compiled;
}
function accepts(test:CharTest,char:string):boolean {
 switch(test.kind) {
  case 'literal':return char===test.value;
  case 'any':return char!=='\n'&&char!=='\r'&&char!=='\u2028'&&char!=='\u2029';
  case 'range':{const point=char.codePointAt(0)!;return point>=test.from&&point<=test.to;}
  case 'set':return test.items.some(item=>accepts(item,char))!==test.negated;
  case 'digit':return (char>='0'&&char<='9')!==test.negated;
  case 'word':return ((char>='0'&&char<='9')||(char>='A'&&char<='Z')||(char>='a'&&char<='z')||char==='_')!==test.negated;
  case 'space':return /\s/u.test(char)!==test.negated;
 }
}
export function testCompiledRule(compiled:CompiledRule,text:string):boolean {
 const input=Array.from(text.slice(0,RULE_LIMITS.field)),{states,start}=compiled;
 let current:number[]=[],next:number[]=[],seen=new Uint8Array(states.length),nextSeen=new Uint8Array(states.length);
 function add(id:number,position:number,items:number[],marks:Uint8Array) {
  if(marks[id])return;marks[id]=1;const state=states[id];
  if(state.kind==='split'){add(state.next,position,items,marks);add(state.other!,position,items,marks);}
  else if(state.kind==='start'){if(position===0)add(state.next,position,items,marks);}
  else if(state.kind==='end'){if(position===input.length)add(state.next,position,items,marks);}
  else items.push(id);
 }
 for(let position=0;position<=input.length;position++) {
  add(start,position,current,seen);
  for(const id of current)if(states[id].kind==='match')return true;
  if(position===input.length)return false;
  for(const id of current){const state=states[id];if(state.kind==='char'&&accepts(state.test!,input[position]))add(state.next,position+1,next,nextSeen);}
  [current,next]=[next,current];next.length=0;[seen,nextSeen]=[nextSeen,seen];nextSeen.fill(0);
 }
 return false;
}
const compiledCache=new Map<string,CompiledRule>();
function compiled(pattern:string) {
 let result=compiledCache.get(pattern);if(!result){result=compileRule(pattern);if(compiledCache.size>=100)compiledCache.delete(compiledCache.keys().next().value!);compiledCache.set(pattern,result);}return result;
}
export async function matchBlockRules(e:Env,m:Pick<TGMessage,'text'|'caption'>):Promise<BlockRule|null> {
 const fields=[m.text,m.caption].filter((value):value is string=>typeof value==='string'&&value.length>0).map(value=>value.slice(0,RULE_LIMITS.field));
 if(!fields.length)return null;
 const rules=await all<BlockRule>(e,'SELECT * FROM block_rules WHERE deleted=0 AND enabled=1 ORDER BY id LIMIT ?',RULE_LIMITS.rules);
 let stateBudget=0,regexCount=0;
 for(const rule of rules) {
  if(rule.kind==='keyword') {if(fields.some(value=>value.toLowerCase().includes(rule.pattern.toLowerCase())))return rule;continue;}
  // Defense in depth for restored or manually modified databases: malformed or
  // oversized saved patterns cannot block the user lane or spend unbounded CPU.
  let machine:CompiledRule;try {machine=compiled(rule.pattern);}catch{continue;}
  if(++regexCount>RULE_LIMITS.regexRules||(stateBudget+=machine.states.length)>RULE_LIMITS.totalStates)continue;
  if(fields.some(value=>testCompiledRule(machine,value)))return rule;
 }
 return null;
}

const ruleId=(text:string)=>/^[1-9]\d{0,9}$/.test(text)&&Number.isSafeInteger(Number(text))?Number(text):null;
const allowed=(e:Env,actor:string)=>e.ADMIN_IDS.split(/[\s,]+/).includes(actor);
const mutationKey=(e:Env,job:Job,suffix:string)=>`${e.RECOVERY_EPOCH}:rule:${job.id}:${suffix}`;
interface Mutation {rule_id:number|null;result:string}
async function wakeMutation(e:Env,results:D1Result[],start:number,waitUntil?:((promise:Promise<unknown>)=>void)) {
 if(results.slice(start).some(result=>result.meta.changes>0)){const wake=wakeLogs(e);if(waitUntil)waitUntil(wake);else await wake;}
}
export async function addBlockRule(e:Env,job:Job,actor:string,kind:'keyword'|'regex',pattern:string,waitUntil?:((promise:Promise<unknown>)=>void)):Promise<Mutation> {
 if(!allowed(e,actor))throw new Error('rule_admin_required');
 pattern=pattern.trim();if(!pattern||pattern.length>RULE_LIMITS.pattern||/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/.test(pattern))throw new Error('规则请输入 1–200 个有效字符。');
 const stateCount=kind==='regex'?compiled(pattern).states.length:0,key=mutationKey(e,job,'add');
 const old=await one<Mutation>(e,'SELECT rule_id,result FROM block_rule_mutations WHERE op_key=?',key);if(old)return old;
 const pending='NOT EXISTS(SELECT 1 FROM block_rule_mutations WHERE op_key=?)';
 const epoch=logEpochStatements(e),time=now();
 const results=await e.DB.batch([
  ...epoch,
  stmt(e,`INSERT OR IGNORE INTO block_rules(kind,pattern,state_count,create_key,created_by,created_at,updated_at) SELECT ?,?,?,?,?,?,? WHERE ${pending} AND (SELECT COUNT(*) FROM block_rules WHERE deleted=0)<? AND (?='keyword' OR ((SELECT COUNT(*) FROM block_rules WHERE deleted=0 AND kind='regex')<? AND (SELECT COALESCE(SUM(state_count),0) FROM block_rules WHERE deleted=0 AND kind='regex')+?<=?))`,kind,pattern,stateCount,key,actor,time,time,key,RULE_LIMITS.rules,kind,RULE_LIMITS.regexRules,stateCount,RULE_LIMITS.totalStates),
  stmt(e,`INSERT OR IGNORE INTO block_rule_mutations(op_key,rule_id,result,created_at) SELECT ?,id,'created',? FROM block_rules WHERE create_key=?`,key,time,key),
  stmt(e,"INSERT OR IGNORE INTO block_rule_mutations(op_key,result,created_at) VALUES(?,'limit',?)",key,time),
  stmt(e,`INSERT INTO audit_logs(event,detail,created_at) SELECT 'block_rule_created',json_object('rule',id,'kind',kind,'actor',?,'mutation',?),? FROM block_rules WHERE create_key=? AND NOT EXISTS(SELECT 1 FROM audit_logs WHERE event='block_rule_created' AND json_extract(detail,'$.mutation')=?)`,actor,key,time,key,key),
  ...logOutboxStatements(e,time)
 ]);
 // Audit rows never contain rule patterns. Use the create marker rather than
 // callback delivery as the mutation's source of truth.
 await wakeMutation(e,results,epoch.length+4,waitUntil);
 return (await one<Mutation>(e,'SELECT rule_id,result FROM block_rule_mutations WHERE op_key=?',key))!;
}
export async function changeBlockRule(e:Env,job:Job,actor:string,id:number,operation:'on'|'off'|'delete',version?:number,action?:RuleAction,stamp?:number,waitUntil?:((promise:Promise<unknown>)=>void)):Promise<Mutation> {
 if(!allowed(e,actor))throw new Error('rule_admin_required');
 const key=mutationKey(e,job,action?.token??operation),prior=await one<Mutation>(e,'SELECT rule_id,result FROM block_rule_mutations WHERE op_key=?',key);if(prior)return prior;
 const pending='NOT EXISTS(SELECT 1 FROM block_rule_mutations WHERE op_key=?)';
 const actionGate=action?' AND EXISTS(SELECT 1 FROM callback_actions WHERE token=? AND used=0 AND actor_id=? AND thread_id IS NULL AND expires_at>=? AND json_extract(payload,\'$.instance\')=?)':'';
 const versionGate=version===undefined?'':' AND version=?';
 const args=[id,key,...(version===undefined?[]:[version]),...(action?[action.token,actor,now(),e.INSTANCE_ID]:[])];
 const gate=`id=? AND deleted=0 AND ${pending}${versionGate}${actionGate}`,epoch=logEpochStatements(e),time=now();
 const results=await e.DB.batch([
  ...epoch,
  stmt(e,`INSERT OR IGNORE INTO block_rule_mutations(op_key,rule_id,result,created_at) SELECT ?,id,'changed',? FROM block_rules WHERE ${gate}`,key,time,...args),
  stmt(e,`UPDATE block_rules SET ${operation==='delete'?'deleted=1,enabled=0':`enabled=${operation==='on'?1:0}`},version=version+1,updated_at=? WHERE id=? AND EXISTS(SELECT 1 FROM block_rule_mutations WHERE op_key=? AND result='changed')`,time,id,key),
  stmt(e,"INSERT INTO audit_logs(event,detail,created_at) SELECT 'block_rule_changed',?,? WHERE EXISTS(SELECT 1 FROM block_rule_mutations WHERE op_key=? AND result='changed')",JSON.stringify({rule:id,operation,actor}),time,key),
  stmt(e,"UPDATE block_rule_mutations SET result='finished' WHERE op_key=? AND result='changed'",key),
  stmt(e,"INSERT OR IGNORE INTO block_rule_mutations(op_key,rule_id,result,created_at) VALUES(?,?,'stale',?)",key,id,time),
  ...(action?[stmt(e,"UPDATE callback_actions SET used=? WHERE token=? AND used=0 AND EXISTS(SELECT 1 FROM block_rule_mutations WHERE op_key=? AND result='finished')",stamp,action.token,key)]:[]),
  ...logOutboxStatements(e,time)
 ]);
 await wakeMutation(e,results,epoch.length+5+(action?1:0),waitUntil);
 return (await one<Mutation>(e,'SELECT rule_id,result FROM block_rule_mutations WHERE op_key=?',key))!;
}

async function button(e:Env,job:Job,actor:string,label:string,kind:string,payload:Params):Promise<Button> {
 const context=`rules:${job.id}:${kind}:${JSON.stringify(payload)}`;
 const old=await one<{token:string}>(e,"SELECT token FROM callback_actions WHERE kind=? AND actor_id=? AND thread_id IS NULL AND json_extract(payload,'$.context')=?",kind,actor,context);
 const token=old?.token??crypto.randomUUID().replace(/-/g,'');
 if(!old)await run(e,'INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) VALUES(?,?,?, ?,NULL,0,?,?)',token,kind,'',actor,JSON.stringify({...payload,context,instance:e.INSTANCE_ID}),now()+15*60_000);
 return {text:label,callback_data:`a:${token}`};
}
export async function rulesPanel(e:Env,job:Job,actor:string,page=0):Promise<void> {
 if(!allowed(e,actor))return;
 const count=(await one<{n:number}>(e,'SELECT COUNT(*) n FROM block_rules WHERE deleted=0'))!.n;
 const maxPage=Math.max(0,Math.ceil(count/5)-1);page=Math.min(maxPage,Math.max(0,Number.isFinite(page)?Math.floor(page):0));
 const rows=await all<BlockRule>(e,'SELECT * FROM block_rules WHERE deleted=0 ORDER BY id LIMIT 5 OFFSET ?',page*5);
 const lines=rows.map(rule=>`#${rule.id} · ${rule.enabled?'启用':'停用'} · ${rule.kind==='keyword'?'关键词':'正则'}\n${rule.pattern}`);
 const buttons:Button[][]=[];
 for(const rule of rows)buttons.push([
  await button(e,job,actor,`#${rule.id} ${rule.enabled?'停用':'启用'}`,'rule_change',{id:rule.id,version:rule.version,operation:rule.enabled?'off':'on',page}),
  await button(e,job,actor,`#${rule.id} 删除`,'rule_delete_prompt',{id:rule.id,version:rule.version,page})
 ]);
 const navigation:Button[]=[];
 if(page>0)navigation.push(await button(e,job,actor,'上一页','rule_page',{page:page-1}));
 if(page<maxPage)navigation.push(await button(e,job,actor,'下一页','rule_page',{page:page+1}));
 if(navigation.length)buttons.push(navigation);
 buttons.push([await button(e,job,actor,'添加与用法','rule_help',{})]);
 const text=`拦截规则 · ${page+1}/${maxPage+1} · 共 ${count}/${RULE_LIMITS.rules} 条\n只匹配用户文字与媒体说明；关键词忽略大小写，正则区分大小写。\n\n${lines.join('\n\n')||'尚无规则。'}\n\n添加：/mb rules add keyword 广告关键词\n正则：/mb rules add regex (优惠|推广)\\s*[0-9]+\n命中即封禁；用户端静默。停用或删除规则不会自动解除已封禁用户。`;
 await sendRulePanel(e,`job:${job.id}:rules:${page}`,text,buttons);
}
async function sendRulePanel(e:Env,key:string,text:string,buttons:Button[][]) {
 const result=await notify(e,key,null,text,{reply_markup:{inline_keyboard:buttons}});
 if(result.status==='success')await e.DB.batch(buttons.flat().map(b=>stmt(e,"UPDATE callback_actions SET payload=json_set(payload,'$.messageId',?) WHERE token=?",result.result.message_id,b.callback_data.slice(2))));
}
async function ruleHelp(e:Env,job:Job) {
 await notify(e,`job:${job.id}:rules:help`,null,'拦截规则用法（仅限管理员在 General）\n\n/mb rules 查看规则\n/mb rules add keyword 广告关键词\n关键词按原文子串匹配，忽略大小写；空格及换行按实际文字保留。\n/mb rules add regex (优惠|推广)\\s*[0-9]+\n正则按子串匹配且区分大小写；^ / $ 表示整条文字的开头 / 结尾。支持 .、[]、()、|、*、+、?、{m,n}、\\d、\\w、\\s。无需 / 分隔符；不支持前后查找、回溯引用、内联标志或 flags。\n/mb rules on 规则编号\n/mb rules off 规则编号\n/mb rules delete 规则编号\n列表按钮也可启用、停用或确认删除。\n\n匹配用户每条文字和媒体说明（分别最多前 8192 字符），含编辑；不检查附件文件内容、图片文字、姓名或用户名。最多 50 条规则、10 条正则、每条 200 字符；有限重复最多 40 次，复杂度总量受限，拒绝能匹配空串的正则。规则及封禁状态保存在当前机器人的独立数据库。命中后用户端静默；解除封禁需在资料面板操作。');
}
export async function handleRuleCommand(e:Env,job:Job,actor:string,rawArgs:string,waitUntil?:((promise:Promise<unknown>)=>void)):Promise<void> {
 if(!allowed(e,actor))return;
 const args=rawArgs.trim();
 if(!args||/^\d+$/.test(args)){await rulesPanel(e,job,actor,args?Number(args)-1:0);return;}
 if(args==='help'||args==='add'){await ruleHelp(e,job);return;}
 const addition=/^add\s+(keyword|regex)\s+([\s\S]+)$/.exec(args);
 if(addition) {
  try {
   const result=await addBlockRule(e,job,actor,addition[1] as 'keyword'|'regex',addition[2],waitUntil);
   await notify(e,`job:${job.id}:rules:add`,null,result.result==='created'?`拦截规则 #${result.rule_id} 已添加并启用。`:'规则数量或正则复杂度总量已达上限；请删除不再需要的规则。');
  } catch(error) {
   // Only parser/validation errors are shown. Database and delivery errors
   // must remain retryable rather than being misreported as invalid rules.
   if(error instanceof Error&&(error.message.startsWith('规则请输入')||error.message.startsWith('正则')))await notify(e,`job:${job.id}:rules:invalid`,null,error.message);else throw error;
  }
  return;
 }
 const change=/^(on|off|delete)\s+(\d+)$/.exec(args),id=change?ruleId(change[2]):null;
 if(change&&id) {
  const result=await changeBlockRule(e,job,actor,id,change[1] as 'on'|'off'|'delete',undefined,undefined,undefined,waitUntil);
  await notify(e,`job:${job.id}:rules:change`,null,result.result==='finished'?`规则 #${id} 已${change[1]==='delete'?'删除':change[1]==='on'?'启用':'停用'}。`:'规则不存在或已删除。');return;
 }
 await ruleHelp(e,job);
}
/** Called only after the administrator callback's existing actor, bot, chat,
 * instance, expiry, General and original message checks have succeeded. */
export async function handleRuleAction(e:Env,job:Job,action:RuleAction,payload:Params,stamp:number,actor:string,waitUntil?:((promise:Promise<unknown>)=>void)):Promise<string> {
 if(!allowed(e,actor)||action.actor_id!==actor||action.thread_id!==null)return '无权操作。';
 const page=Number(payload.page)||0;
 if(action.kind==='rule_change'||action.kind==='rule_delete_confirm') {
  const id=ruleId(String(payload.id)),operation=action.kind==='rule_delete_confirm'?'delete':payload.operation;
  if(!id||!['on','off','delete'].includes(String(operation))||!Number.isSafeInteger(payload.version))return '按钮已失效。';
  const result=await changeBlockRule(e,job,actor,id,operation as 'on'|'off'|'delete',Number(payload.version),action,stamp,waitUntil);
  await rulesPanel(e,job,actor,page);return result.result==='finished'?'规则已更新。':'规则已变化，请使用最新列表。';
 }
 await run(e,'UPDATE callback_actions SET used=? WHERE token=? AND used=0',stamp,action.token);
 if(action.kind==='rule_page'){await rulesPanel(e,job,actor,page);return '已更新。';}
 if(action.kind==='rule_help'){await ruleHelp(e,job);return '请查看 General 中的用法。';}
 if(action.kind==='rule_delete_prompt') {
  const id=ruleId(String(payload.id)),rule=id?await one<BlockRule>(e,'SELECT * FROM block_rules WHERE id=? AND deleted=0 AND version=?',id,payload.version):null;
  if(!rule)return '规则已变化，请使用最新列表。';
  await sendRulePanel(e,`job:${job.id}:rules:delete-prompt`,`确认删除拦截规则 #${rule.id}？\n${rule.pattern}\n\n已有封禁不会自动解除。`,[[await button(e,job,actor,'确认删除','rule_delete_confirm',{id:rule.id,version:rule.version,page}),await button(e,job,actor,'取消','rule_page',{page})]]);return '请在 General 中确认删除。';
 }
 return '按钮已失效。';
}
