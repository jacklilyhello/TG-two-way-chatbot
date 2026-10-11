// The only Node compatibility API used by the standalone drill bundle.
declare module 'node:async_hooks' {
 export class AsyncLocalStorage<T> {
  getStore():T|undefined;
  run<R>(store:T,callback:()=>R):R;
 }
}
