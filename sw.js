/* Only the public app shell is cached here. Farm data is isolated per user in IndexedDB. */
const CACHE='ardon-shell-1.1.0-3';
const BASE=new URL('./',self.location.href);
const assets=[
  ['./',null],['offline-store.js?v=1.1.0-3',null],
  ['https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js','sha384-NrKB+u6Ts6AtkIhwPixiKTzgSKNblyhlk0Sohlgar9UHUBzai/sgnNNWWd291xqt'],
  ['https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js','sha384-vtjasyidUo0kW94K5MXDXntzOJpQgBKXmE7e2Ga4LG0skTTLeBi97eFAXsqewJjw'],
  ['https://cdn.jsdelivr.net/npm/firebase@10.12.2/firebase-app-compat.js','sha384-AQ3POAMqIhwS81FrUH95ekxqBZHeP5tG2JfEL3+7GuTtfRLWnrRh32UxwzM+//A9'],
  ['https://cdn.jsdelivr.net/npm/firebase@10.12.2/firebase-firestore-compat.js','sha384-qn4Jh88HhJA8dplNQyGjOh9OGI4izVhrfj/qIFgaKcdgOE06pXqIKGlsCILZcLAC'],
  ['https://cdn.jsdelivr.net/npm/firebase@10.12.2/firebase-auth-compat.js','sha384-TnlRYaR4JYz/lpaGuaiU61PjNberSA4vjLjtF+oRC/IkohkJqnWxw5EmDSOt1vA1']
];
self.addEventListener('install',event=>event.waitUntil((async()=>{
  const cache=await caches.open(CACHE);
  await cache.addAll(assets.map(([url,integrity])=>new Request(new URL(url,BASE),{cache:'reload',...(integrity?{integrity,mode:'cors',credentials:'omit'}:{})})));
  await self.skipWaiting();
})()));
self.addEventListener('activate',event=>event.waitUntil((async()=>{
  for(const key of await caches.keys())if(key.startsWith('ardon-shell-')&&key!==CACHE)await caches.delete(key);
  await self.clients.claim();
})()));
self.addEventListener('fetch',event=>{
  const req=event.request,url=new URL(req.url);
  if(req.method!=='GET')return;
  const own=url.origin===BASE.origin&&url.pathname.startsWith(BASE.pathname);
  const resource=['cdn.jsdelivr.net','cdnjs.cloudflare.com','fonts.googleapis.com','fonts.gstatic.com'].includes(url.hostname);
  // Never intercept Firebase, authentication, or other sites.
  if(!own&&!resource)return;
  if(req.mode==='navigate'&&own){
    event.respondWith((async()=>{
      const cache=await caches.open(CACHE);
      const controller=new AbortController(),timeout=setTimeout(()=>controller.abort(),3500);
      try{const response=await fetch(req,{signal:controller.signal});if(!response.ok)throw Error('Application unavailable');await cache.put(new URL('./',BASE),response.clone());return response;}
      catch(e){const saved=await cache.match(new URL('./',BASE));if(saved)return saved;throw e;}
      finally{clearTimeout(timeout);}
    })());return;
  }
  if(own&&!url.pathname.endsWith('/offline-store.js'))return;
  event.respondWith((async()=>{
    const cache=await caches.open(CACHE),saved=await cache.match(req);if(saved)return saved;
    const response=await fetch(req);if(response.ok||response.type==='opaque')await cache.put(req,response.clone());return response;
  })());
});
