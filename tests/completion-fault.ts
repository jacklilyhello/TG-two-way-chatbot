/** Fail only the terminal operation write, preserving its already-issued
 * Telegram request and independent Bot journal for recovery assertions. */
export function failOperationCompletion(database:D1Database,message:string):D1Database {
 const completion=new WeakSet<D1PreparedStatement>();
 return new Proxy(database,{get(target,property){
  if(property==='prepare')return (sql:string)=>{
   const statement=target.prepare(sql);
   if(!sql.startsWith('UPDATE operations SET phase=?'))return statement;
   return new Proxy(statement,{get(prepared,key){
    if(key==='bind')return (...args:unknown[])=>{const bound=prepared.bind(...args);completion.add(bound);return bound;};
    const value=Reflect.get(prepared,key);return typeof value==='function'?value.bind(prepared):value;
   }});
  };
  if(property==='batch')return (statements:D1PreparedStatement[])=>{
   if(statements.some(statement=>completion.has(statement)))throw new Error(message);
   return target.batch(statements);
  };
  const value=Reflect.get(target,property);return typeof value==='function'?value.bind(target):value;
 }});
}
