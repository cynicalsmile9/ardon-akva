/* Ардон Аква: durable per-account data and an ordered, conflict-checked outbox.
   No credentials are stored here. Firebase Auth owns the signed-in session. */
(function(root){
  'use strict';
  const copy=x=>x===undefined?null:JSON.parse(JSON.stringify(x));
  const stable=x=>Array.isArray(x)?'['+x.map(stable).join(',')+']':x&&typeof x==='object'?'{'+Object.keys(x).sort().map(k=>JSON.stringify(k)+':'+stable(x[k])).join(',')+'}':JSON.stringify(x);
  const same=(a,b)=>stable(a)===stable(b);
  const empty=()=>({version:1,collections:{},queue:[],profile:null,retryAt:0,lastSynced:0,error:'',seq:0});
  const uuid=()=>globalThis.crypto.randomUUID();
  const plotCollections=['stocking','mortality','feed','o2','temperature','treatments','transfers','incidents','naveski','population','plans','expenses'];
  const safeKey=k=>typeof k==='string'&&k.length>0&&!k.includes('/')&&!['__proto__','prototype','constructor'].includes(k);
  function audit(job,op){return {section:op.col.split('__')[0],docId:op.id,date:op.after?.date||op.before?.date||'',snapshot:op.before||{},action:op.after===null?'delete':op.before?'edit':'create',archivedAt:job.createdAt,plotId:op.col.split('__')[1]||'legacy',plotName:''};}
  function view(s,col){
    const docs=copy(s.collections[col]?.docs||{});
    for(const job of s.queue)for(const op of job.ops)if(op.col===col){if(op.after===null)delete docs[op.id];else docs[op.id]=copy(op.after);}
    if(col==='archive')for(const job of s.queue)job.ops.forEach((op,i)=>{docs['local-'+job.id+(i?'-'+i:'')]=audit(job,op);});
    return docs;
  }
  function dependentJobs(queue){
    // Transfers/costs can depend on previous operations in different documents.
    // Conservatively require review of the entire tail, not merely equal paths.
    return queue.map(j=>j.id);
  }
  class LocalDB {
    constructor(id,indexedDB=globalThis.indexedDB){this.name='ardon-local-v1-'+id;this.api=indexedDB;}
    async open(){
      this.db=await new Promise((resolve,reject)=>{const r=this.api.open(this.name,1);r.onupgradeneeded=()=>r.result.createObjectStore('state');r.onerror=()=>reject(r.error);r.onblocked=()=>reject(Error('Закройте старые вкладки приложения и повторите вход.'));r.onsuccess=()=>resolve(r.result);});
      this.db.onversionchange=()=>this.db.close();return this;
    }
    change(fn,write=true){
      return new Promise((resolve,reject)=>{
        let tx,result,error;
        try{tx=this.db.transaction('state',write?'readwrite':'readonly',write?{durability:'strict'}:undefined);}catch(e){reject(e);return;}
        const store=tx.objectStore('state'),r=store.get('root');
        r.onsuccess=()=>{try{const s=r.result||empty();result=fn(s);if(write)store.put(s,'root');}catch(e){error=e;tx.abort();}};
        tx.oncomplete=()=>resolve(result);tx.onerror=tx.onabort=()=>reject(error||tx.error||Error('Не удалось сохранить данные на ПК'));
      });
    }
    read(){return this.change(s=>s,false);}
    close(){this.db?.close();}
  }
  class OfflineStore {
    constructor({uid,db,onStatus=()=>{},onFatal=()=>{},indexedDB,locks=globalThis.navigator?.locks,online=()=>globalThis.navigator?.onLine!==false,auto=true}){
      this.uid=uid;this.remote=db;this.local=new LocalDB(uid,indexedDB);this.onStatus=onStatus;this.onFatal=onFatal;this.locks=locks;this.online=online;this.auto=auto;
      this.s=empty();this.watches=new Map();this.closed=false;this.syncing=false;this.failures=0;this.fatal='';
      this.hasReadLease=!auto;this.readLeasePending=false;
    }
    async open(){
      await this.local.open();await this.refresh();
      if(this.auto&&typeof BroadcastChannel!=='undefined'){this.channel=new BroadcastChannel(this.local.name);this.channel.onmessage=()=>this.refresh().catch(e=>this.storageError(e));}
      if(this.auto){this.timer=setInterval(()=>this.tick(),30000);this.onlineHandler=()=>this.retry();globalThis.addEventListener?.('online',this.onlineHandler);}
      return this;
    }
    async refresh(){if(this.closed)return;const s=await this.local.read();if(this.closed)return;this.s=s;this.notify();}
    notify(){
      for(const [col,w]of this.watches){const c=this.s.collections[col];if(!c?.complete&&!c?.usable)continue;const docs=view(this.s,col),signature=stable(docs);if(signature!==w.signature){w.signature=signature;for(const fn of w.cbs)fn(copy(docs));}}
      if(this.profileCB&&this.s.profile){const sig=stable(this.s.profile.record);if(sig!==this.profileSignature){this.profileSignature=sig;this.profileCB(copy(this.s.profile.record),false);}}
      this.onStatus(this.status());
    }
    status(){return {pending:this.s.queue.length,documents:this.s.queue.reduce((n,j)=>n+j.ops.length,0),conflict:this.s.queue[0]?.conflict||null,job:this.s.queue[0]||null,dependent:dependentJobs(this.s.queue).length,lastSynced:this.s.lastSynced,retryAt:this.s.retryAt,error:this.fatal||this.s.error,syncing:this.syncing,provisional:this.s.profile?.source==='backup',imported:this.s.imported||[],ready:Object.keys(this.s.collections).filter(k=>this.s.collections[k].complete),missing:[...this.watches.keys()].filter(k=>!this.s.collections[k]?.complete),offline:!this.online()};}
    async changed(){await this.refresh();this.channel?.postMessage('changed');}
    storageError(e){this.fatal='Локальное сохранение недоступно: '+(e.message||e);this.onStatus(this.status());this.onFatal(this.fatal);}
    async mutate(fn){if(this.closed||this.fatal)throw Error(this.fatal||'Сессия закрыта');try{const r=await this.local.change(fn);await this.changed();return r;}catch(e){if(!e.business)this.storageError(e);throw e;}}
    fail(message){const e=Error(message);e.business=true;throw e;}
    complete(col){return !!this.s.collections[col]?.complete;}
    async listCollection(col){await this.refresh();if(!this.complete(col)&&!this.s.collections[col]?.usable)this.fail('Раздел ещё не сохранён на ПК: '+col+'. Нужна первая загрузка с сервера.');return view(this.s,col);}
    watchCollection(col,cb,onError){
      let w=this.watches.get(col);if(!w){w={cbs:new Set(),errors:new Set(),stop:null,signature:null};this.watches.set(col,w);}
      w.cbs.add(cb);if(onError)w.errors.add(onError);
      if(this.complete(col)||this.s.collections[col]?.usable)cb(view(this.s,col));
      this.ensureReadLease();this.connectCollection(col,w);
      return ()=>{w.cbs.delete(cb);w.errors.delete(onError);if(!w.cbs.size){w.stop?.();this.watches.delete(col);}};
    }
    allowed(){return !this.closed&&!this.fatal&&this.online()&&Date.now()>=this.s.retryAt;}
    ensureReadLease(){
      if(!this.auto||this.closed||this.hasReadLease||this.readLeasePending||!this.locks)return;
      this.readLeasePending=true;
      // All plots are subscribed by the app, so one tab can supply the others.
      // The lock is released automatically if its tab/process crashes.
      this.locks.request(this.local.name+'-reader',{ifAvailable:true},async lock=>{
        if(!lock||this.closed)return;
        this.hasReadLease=true;
        this.connectProfile();for(const [col,w]of this.watches)this.connectCollection(col,w);
        await new Promise(resolve=>{this.releaseReadLease=resolve;if(this.closed)resolve();});
      }).catch(e=>this.storageError(e)).finally(()=>{this.hasReadLease=false;this.readLeasePending=false;});
    }
    connectCollection(col,w){
      if(!this.hasReadLease||!this.allowed()||w.stop||!this.remote)return;
      w.stop=this.remote.collection(col).onSnapshot({includeMetadataChanges:true},snap=>{
        // A cache snapshot may be empty or partial. It must never erase a complete copy.
        if(snap.metadata.fromCache||snap.metadata.hasPendingWrites||this.closed)return;
        const docs={};snap.docs.forEach(d=>{docs[d.id]=d.data();});
        this.mutate(s=>{s.collections[col]={docs,complete:true,at:Date.now()};}).catch(()=>{});
      },err=>{if(!this.complete(col))for(const fn of w.errors)fn(err);this.backoff(err);});
    }
    watchProfile(cb){this.profileCB=cb;if(this.s.profile){this.profileSignature=stable(this.s.profile.record);cb(copy(this.s.profile.record),false);}this.ensureReadLease();this.connectProfile();}
    connectProfile(){
      if(!this.hasReadLease||!this.allowed()||this.profileStop||!this.profileCB||!this.remote)return;
      this.profileStop=this.remote.doc('users/'+this.uid).onSnapshot({includeMetadataChanges:true},snap=>{
        if(snap.metadata.fromCache||snap.metadata.hasPendingWrites||this.closed)return;
        const record=snap.exists?snap.data():null;
        this.mutate(s=>{s.profile={record,at:Date.now(),source:'server'};}).then(()=>{if(!this.closed){this.profileCB(copy(record),true);if(this.auto)this.sync();}}).catch(()=>{});
      },err=>this.backoff(err));
    }
    stopNetwork(){this.profileStop?.();this.profileStop=null;for(const w of this.watches.values()){w.stop?.();w.stop=null;}}
    async backoff(err){
      if(this.closed)return;
      const code=String(err.code||'unavailable').replace(/^firestore\//,'');
      this.stopNetwork();
      const delay=code==='resource-exhausted'?15*60000:code==='permission-denied'||code==='unauthenticated'?24*3600000:Math.min(15*60000,30000*2**Math.min(this.failures++,5));
      await this.mutate(s=>{s.error=code;s.retryAt=Math.max(s.retryAt,Date.now()+delay);}).catch(()=>{});
    }
    async retry(){
      if(this.closed)return;
      await this.mutate(s=>{s.retryAt=0;s.error='';});
      return this.tick();
    }
    async tick(){
      if(this.closed)return;
      await this.refresh();this.ensureReadLease();if(!this.allowed())return;
      this.connectProfile();for(const [col,w]of this.watches)this.connectCollection(col,w);
      await this.sync();
    }
    async commit(operations,label='Изменение'){
      const id=uuid(),stamp=Date.now();
      await this.mutate(s=>{
        if(!['user','admin'].includes(s.profile?.record?.role))this.fail('Для сохранения нужен подтверждённый вход в приложение.');
        const seen=new Set(),ops=[];
        for(const input of operations){
          const {col,id:docId}=input;
          if(!safeKey(col)||!safeKey(docId)||col==='users'||col==='archive')this.fail('Недопустимая запись');
          const path=col+'/'+docId;if(seen.has(path))this.fail('Повтор записи в одной операции');seen.add(path);
          if(!s.collections[col]?.complete&&!s.collections[col]?.known?.[docId])this.fail('Раздел '+col+' ещё не загружен на ПК. Дождитесь первой загрузки с сервера.');
          const before=view(s,col)[docId]??null,after=copy(input.after);
          if('before'in input&&!same(before,input.before))this.fail('Запись изменилась в другой вкладке. Откройте форму заново: '+path);
          if(same(before,after))continue;
          if(after!==null&&(typeof after!=='object'||Array.isArray(after)))this.fail('Неверный формат записи');
          ops.push({col,id:docId,before:copy(before),after});
        }
        if(!ops.length)return;
        if(ops.length>100)this.fail('В одной операции допускается не более 100 записей');
        s.queue.push({id,seq:++s.seq,createdAt:stamp,label,ops});
        for(const op of ops)if(op.col==='plots'&&op.before===null&&op.after!==null&&op.id!=='legacy'){
          for(const base of plotCollections)s.collections[base+'__'+op.id]||={docs:{},complete:true,at:stamp};
        }
      });
      if(this.auto)queueMicrotask(()=>this.sync());return id;
    }
    setDoc(col,id,data){return this.commit([{col,id,after:data}]);}
    async addDoc(col,data){const id=uuid();await this.setDoc(col,id,data);return id;}
    deleteDoc(col,id){return this.commit([{col,id,after:null}],'Удаление');}
    async send(job){
      const receipt=this.remote.doc('archive/local-'+job.id);
      return this.remote.runTransaction(async tx=>{
        const done=await tx.get(receipt);
        const actual=[];
        for(const op of job.ops){const snap=await tx.get(this.remote.doc(op.col+'/'+op.id));actual.push(snap.exists?snap.data():null);}
        if(done.exists)return {already:true,actual};
        const conflicts=job.ops.flatMap((op,i)=>!same(actual[i],op.before)?[{col:op.col,id:op.id,before:op.before,local:op.after,server:actual[i]}]:[]);
        if(conflicts.length){const e=Error('Конфликт изменений');e.code='local/conflict';e.conflicts=conflicts;e.actual=actual;throw e;}
        for(let i=0;i<job.ops.length;i++){
          const op=job.ops[i],ref=this.remote.doc(op.col+'/'+op.id);
          if(op.after===null)tx.delete(ref);else tx.set(ref,op.after);
          tx.set(i===0?receipt:this.remote.doc('archive/local-'+job.id+'-'+i),audit(job,op));
        }
        return {already:false};
      },{maxAttempts:1});
    }
    async sync(){
      if(!this.allowed()||this.syncing||!this.remote||!this.s.queue.length||this.s.profile?.source==='backup'||!['user','admin'].includes(this.s.profile?.record?.role))return;
      if(!this.locks){this.fatal='Этот браузер не поддерживает безопасную синхронизацию. Используйте установленную программу или современный Chrome / Edge.';this.onStatus(this.status());return;}
      this.syncing=true;this.onStatus(this.status());
      try{
        await this.locks.request(this.local.name+'-sync',{ifAvailable:true},async lock=>{
          if(!lock)return;
          while(!this.closed&&this.online()){
            await this.refresh();if(!this.allowed())break;
            const job=this.s.queue[0];if(!job||job.conflict)break;
            try{
              const result=await this.send(copy(job));if(this.closed)break;
              await this.mutate(s=>{
                if(s.queue[0]?.id!==job.id)return;
                job.ops.forEach((op,i)=>{
                  const c=s.collections[op.col];if(!c)return;
                  const current=c.docs[op.id]??null,value=result.already?result.actual[i]:op.after;
                  if(result.already||same(current,op.before)||same(current,op.after)){if(value===null)delete c.docs[op.id];else c.docs[op.id]=value;}
                  if(s.collections.archive?.complete)s.collections.archive.docs['local-'+job.id+(i?'-'+i:'')]=audit(job,op);
                });
                s.queue.shift();s.lastSynced=Date.now();s.error='';s.retryAt=0;
              });this.failures=0;
            }catch(e){
              if(e.code==='local/conflict')await this.mutate(s=>{if(s.queue[0]?.id===job.id)s.queue[0].conflict={items:e.conflicts,actual:e.actual,at:Date.now()};});
              else await this.backoff(e);
              break;
            }
          }
        });
      }catch(e){if(!this.closed)await this.backoff(e);}
      finally{this.syncing=false;if(!this.closed)this.onStatus(this.status());}
    }
    async resolve(choice,expectedId){
      if(!['local','server'].includes(choice))this.fail('Выберите версию записи');
      await this.locks.request(this.local.name+'-sync',async()=>{
        await this.mutate(s=>{
          const job=s.queue[0];if(!job?.conflict||job.id!==expectedId)this.fail('Очередь уже изменилась. Откройте сравнение заново.');
          if(choice==='local'){
            job.ops.forEach((op,i)=>op.before=copy(job.conflict.actual[i]));delete job.conflict;
          }else{
            const remove=new Set(dependentJobs(s.queue));
            job.ops.forEach((op,i)=>{const docs=s.collections[op.col].docs,actual=job.conflict.actual[i];if(actual===null)delete docs[op.id];else docs[op.id]=actual;});
            // Preserve both alternatives and dependent edits in the local backup.
            s.resolutions ||= [];s.resolutions.push({at:Date.now(),jobs:s.queue.filter(j=>remove.has(j.id))});
            s.queue=s.queue.filter(j=>!remove.has(j.id));
          }
          s.error='';s.retryAt=0;
        });
      });
      if(this.auto)this.sync();
    }
    async backup(){await this.refresh();return {format:'ardon-offline-backup',version:1,uid:this.uid,exportedAt:new Date().toISOString(),state:copy(this.s)};}
    async importBackups(files){
      await this.mutate(s=>{
        if(!Array.isArray(files)||!files.length)this.fail('Выберите файл резервной копии');
        const put=(col,data,complete=true,known={})=>{
          if(!data||typeof data!=='object'||Array.isArray(data)||Object.keys(data).some(k=>!safeKey(k)))this.fail('Неверные записи в копии: '+col);
          if(s.collections[col]?.complete||s.queue.some(j=>j.ops.some(op=>op.col===col))){if(same(view(s,col),data))return;this.fail('Раздел '+col+' уже сохранён на ПК. Замена работающей копии запрещена.');}
          s.collections[col]={docs:copy(data),complete,known,usable:true,at:Date.now(),source:'backup'};
        };
        for(const file of files){
          if(file.version!==1)this.fail('Неподдерживаемый формат копии');
          if(file.plotId&&file.plot&&file.collections){
            if(!safeKey(file.plotId)||!Array.isArray(file.plot.pools)||!plotCollections.every(c=>file.collections[c]&&typeof file.collections[c]==='object'))this.fail('В копии участка отсутствуют журналы');
            for(const col of plotCollections)put(file.plotId==='legacy'?col:col+'__'+file.plotId,file.collections[col]);
            if(!s.collections.plots?.complete){const plots=copy(s.collections.plots?.docs||{});plots[file.plotId]=copy(file.plot);put('plots',plots,false,{[file.plotId]:true});}
          }else if(file.feeds&&'priceBook'in file){
            put('feeds',file.feeds);
            const settings=copy(s.collections.settings?.docs||{});if(file.priceBook)settings.f3PriceBook=copy(file.priceBook);
            if(!s.collections.settings?.complete)put('settings',settings,false,{f3PriceBook:true});
          }else if(file.format==='ardon-offline-backup'){
            if(file.uid!==this.uid)this.fail('Копия принадлежит другому аккаунту');
            if(s.queue.length)this.fail('На ПК уже есть очередь. Сначала синхронизируйте её или сохраните отдельную копию.');
            const saved=file.state;
            if(saved?.version!==1||!Array.isArray(saved.queue)||!saved.collections)this.fail('Повреждённая локальная копия');
            const allowed=col=>safeKey(col)&&['plots','catalog','archive','feeds','settings',...plotCollections].includes(col.split('__')[0]);
            if(Object.keys(saved.collections).some(col=>!allowed(col))||saved.queue.some(j=>!safeKey(j.id)||!Array.isArray(j.ops)||j.ops.some(op=>!allowed(op.col)||op.col==='archive'||!safeKey(op.id)||!('before'in op)||!('after'in op))))this.fail('Копия содержит недопустимые записи');
            for(const [col,c]of Object.entries(saved.collections)){
              if(!c.docs||typeof c.docs!=='object'||Object.keys(c.docs).some(k=>!safeKey(k)))this.fail('Повреждён раздел '+col);
              if(!s.collections[col]?.complete&&!s.collections[col]?.usable)s.collections[col]={...copy(c),source:'backup'};
            }
            // Import the same operation IDs, not newly created writes. If the source
            // PC also reconnects, Firestore receipts prevent double application.
            for(const job of saved.queue){
              for(const op of job.ops)if(!same(view(s,op.col)[op.id]??null,op.before))this.fail('Очередь из копии не совпадает с текущими данными: '+op.col+'/'+op.id+'. Сначала синхронизируйте исходную копию.');
              s.queue.push(copy(job));
            }
            s.seq=Math.max(s.seq,Number(saved.seq)||s.queue.length);s.resolutions=(s.resolutions||[]).concat(copy(saved.resolutions||[]));
            s.retryAt=Math.max(s.retryAt,Number(saved.retryAt)||0);if(saved.error)s.error=String(saved.error);
          }else this.fail('Это не резервная копия участка, кормов или локальной базы');
          s.imported ||= [];s.imported.push({at:Date.now(),exportedAt:file.exportedAt||'',plotId:file.plotId||null});
        }
        // A file cannot grant server/admin permissions. Imported copies remain local
        // until Firebase returns this account's actual role; every write still checks rules.
        if(!s.profile)s.profile={record:{role:'user'},at:Date.now(),source:'backup'};
      });
    }
    close(){this.closed=true;this.stopNetwork();this.releaseReadLease?.();clearInterval(this.timer);globalThis.removeEventListener?.('online',this.onlineHandler);this.channel?.close();this.local.close();}
  }
  const api={OfflineStore,LocalDB,view,same,dependentJobs};
  if(typeof module==='object'&&module.exports)module.exports=api;else root.ArdonOffline=api;
})(typeof window==='object'?window:globalThis);
