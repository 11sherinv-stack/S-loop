/* slnet.js — کانال اتاق آنلاین دو نفره: MQTT روی WebSocket + رمزنگاری AES-GCM با کد اتاق (بدون سرور اختصاصی) */
(function(){
const BR=['wss://broker.hivemq.com:8884/mqtt','wss://broker.emqx.io:8084/mqtt','wss://test.mosquitto.org:8081/mqtt'];
const enc=new TextEncoder(),dec=new TextDecoder(),ALPH='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const b64=u=>{let s='';for(let i=0;i<u.length;i++)s+=String.fromCharCode(u[i]);return btoa(s)},unb=s=>Uint8Array.from(atob(s),c=>c.charCodeAt(0));
const cid=Math.random().toString(36).slice(2,10);
const rl=n=>{const a=[];do{let d=n%128;n=Math.floor(n/128);if(n>0)d|=128;a.push(d)}while(n>0);return a};
const str=s=>{const b=enc.encode(s);return[b.length>>8,b.length&255,...b]};
const pkt=(h,body)=>Uint8Array.from([h,...rl(body.length),...body]);
const Q=new URLSearchParams(location.search);
const S={cid,room:null,host:false,name:(Q.get('n')||localStorage.getItem('sl-name')||'Player').slice(0,16),peer:null,peerName:'',last:0,state:'offline',cb:{},conns:[],key:null,seen:[],hi:0,q:[]};
S.newRoom=()=>Array.from({length:6},()=>ALPH[Math.floor(Math.random()*ALPH.length)]).join('');
S.base=()=>String(location.href).split(/[?#]/)[0].replace(/[^/]*$/,'');
S.url=(file,room,host)=>S.base()+file+'?room='+room+(host?'&host=1&n='+encodeURIComponent(S.name):'');
S.alive=()=>S.peer?Date.now()-S.last:1e9;
async function getKey(){if(S.key)return S.key;if(!(window.crypto&&crypto.subtle))return null;
 const m=await crypto.subtle.importKey('raw',enc.encode('sloop-game-v1|'+S.room),'PBKDF2',false,['deriveKey']);
 return S.key=await crypto.subtle.deriveKey({name:'PBKDF2',salt:enc.encode('sloop-game-room'),iterations:30000,hash:'SHA-256'},m,{name:'AES-GCM',length:256},false,['encrypt','decrypt'])}
async function pack(obj){const id=Math.random().toString(36).slice(2,9),k=await getKey(),j=JSON.stringify(obj);
 if(!k)return JSON.stringify({c:cid,i:id,p:obj});
 const iv=crypto.getRandomValues(new Uint8Array(12)),ct=new Uint8Array(await crypto.subtle.encrypt({name:'AES-GCM',iv},k,enc.encode(j)));
 return JSON.stringify({c:cid,i:id,iv:b64(iv),d:b64(ct)})}
function st(s){if(S.state!==s){S.state=s;S.cb.state&&S.cb.state(s)}}
function conn(i){const url=BR[i];let ws;try{ws=new WebSocket(url,'mqtt')}catch(e){return}
 const c={ws,ok:false,buf:new Uint8Array(0),ping:0};S.conns[i]=c;ws.binaryType='arraybuffer';
 ws.onopen=()=>ws.send(pkt(0x10,[...str('MQTT'),4,2,0,30,...str('slg-'+cid+'-'+i)]));
 ws.onmessage=e=>{const n=new Uint8Array(e.data),b=new Uint8Array(c.buf.length+n.length);b.set(c.buf);b.set(n,c.buf.length);c.buf=b;
  for(;;){if(c.buf.length<2)return;let p=1,mult=1,len=0,x;do{if(p>=c.buf.length)return;x=c.buf[p++];len+=(x&127)*mult;mult*=128}while(x&128);
   if(c.buf.length<p+len)return;const type=c.buf[0]>>4,body=c.buf.slice(p,p+len);c.buf=c.buf.slice(p+len);
   if(type===2&&body[1]===0){c.ok=true;ws.send(pkt(0x82,[0,1,...str('sloop-game/v1/'+S.room),0]));clearInterval(c.ping);c.ping=setInterval(()=>{try{ws.send(Uint8Array.from([0xC0,0]))}catch(e){}},20000);st('online');flush()}
   else if(type===3){const tl=(body[0]<<8)|body[1];incoming(dec.decode(body.slice(2+tl)))}}};
 const re=()=>{c.ok=false;clearInterval(c.ping);if(!S.conns.some(q=>q&&q.ok))st(S.room?'connecting':'offline');if(S.room)setTimeout(()=>{if(S.room&&S.conns[i]===c)conn(i)},3000)};
 ws.onclose=re;ws.onerror=()=>{try{ws.close()}catch(e){}}}
async function incoming(txt){let m;try{m=JSON.parse(txt)}catch(e){return}if(!m||m.c===cid||!m.i||S.seen.includes(m.i))return;S.seen.push(m.i);if(S.seen.length>300)S.seen.shift();
 let o=m.p;if(m.d){const k=await getKey();if(!k)return;try{o=JSON.parse(dec.decode(await crypto.subtle.decrypt({name:'AES-GCM',iv:unb(m.iv)},k,unb(m.d))))}catch(e){return}}
 if(!o)return;
 if(o._hi){if(!S.peer){S.peer=m.c;S.peerName=String(o.n||'Player').slice(0,16);S.last=Date.now();sendRaw({_hi:1,n:S.name});S.cb.peer&&S.cb.peer(S.peerName)}else if(S.peer===m.c)S.last=Date.now();return}
 if(m.c!==S.peer)return;S.last=Date.now();S.cb.msg&&S.cb.msg(o)}
async function sendRaw(o){const s=await pack(o);let sent=0;for(const c of S.conns)if(c&&c.ok){try{c.ws.send(pkt(0x30,[...str('sloop-game/v1/'+S.room),...enc.encode(s)]));sent++}catch(e){}}if(!sent)S.q.push(s)}
function flush(){const q=S.q;S.q=[];q.forEach(s=>{for(const c of S.conns)if(c&&c.ok){try{c.ws.send(pkt(0x30,[...str('sloop-game/v1/'+S.room),...enc.encode(s)]))}catch(e){}}})}
S.send=o=>{sendRaw(o)};
S.open=(room,cb)=>{S.room=room;S.cb=cb||{};S.host=!!(cb&&cb.host);S.key=null;st('connecting');for(let i=0;i<BR.length;i++)conn(i);
 clearInterval(S.hi);S.hi=setInterval(()=>{if(S.state==='online')sendRaw({_hi:1,n:S.name})},S.peer?3000:1200)};
S.close=()=>{const r=S.room;S.room=null;clearInterval(S.hi);S.conns.forEach(c=>{if(c){try{c.ws.close()}catch(e){}}});S.conns=[];S.peer=null;st('offline')};
// ---------- رابط‌کاربری مشترک
function ov(html){let d=document.getElementById('slnet-ov');if(!d){d=document.createElement('div');d.id='slnet-ov';d.style.cssText='position:fixed;inset:0;background:#000d;z-index:99;display:flex;align-items:center;justify-content:center;padding:16px;font-family:Tahoma,system-ui,sans-serif;color:#fff;direction:rtl';document.body.appendChild(d)}
 d.innerHTML='<div style="background:#1e1a2c;border:2px solid #ffd84a;border-radius:16px;padding:18px;max-width:340px;width:100%;text-align:center;line-height:1.9">'+html+'</div>';d.style.display='flex';return d}
const btn='style="margin:4px;padding:10px 16px;border:0;border-radius:10px;font:inherit;font-weight:800;background:#ffd84a;color:#222"',btn2='style="margin:4px;padding:10px 16px;border:1px solid #fff6;border-radius:10px;font:inherit;background:transparent;color:#fff"';
S.hide=()=>{const d=document.getElementById('slnet-ov');if(d)d.style.display='none'};
S.menu=(file,title)=>{const d=ov('<b style="font-size:1.1rem">🌐 '+title+' — بازی آنلاین ۲ نفره</b><div style="font-size:.8rem;opacity:.85">یک اتاق بساز و کد/لینک را به دوستت بده، یا با کد اتاق دوستت وارد شو.</div><button id="sn-c" '+btn+'>🆕 ساخت اتاق</button><div style="margin-top:6px"><input id="sn-code" maxlength="6" placeholder="کد اتاق" style="width:110px;padding:9px;border-radius:10px;border:1px solid #fff6;background:#0006;color:#fff;text-align:center;text-transform:uppercase;font:inherit;letter-spacing:3px;direction:ltr"> <button id="sn-j" '+btn2+'>ورود</button></div><div><button id="sn-x" '+btn2+'>بستن</button></div>');
 d.querySelector('#sn-c').onclick=()=>{location.href=S.url(file,S.newRoom(),true)};
 d.querySelector('#sn-j').onclick=()=>{const v=d.querySelector('#sn-code').value.trim().toUpperCase();if(/^[A-HJ-NP-Z2-9]{6}$/.test(v))location.href=S.url(file,v,false);else d.querySelector('#sn-code').style.borderColor='#f55'};
 d.querySelector('#sn-x').onclick=S.hide};
S.wait=(file,title)=>{const link=S.url(file,S.room,false);
 const d=ov('<b>🌐 '+title+'</b><div id="sn-s" style="font-size:.85rem;margin:4px 0">'+(S.host?'منتظر ورود حریف…':'در حال اتصال به اتاق…')+'</div>'+(S.host?'<div style="font-size:1.7rem;letter-spacing:6px;font-weight:800;color:#ffd84a;direction:ltr">'+S.room+'</div><div style="font-size:.74rem;opacity:.8;word-break:break-all;direction:ltr">'+link+'</div><button id="sn-cp" '+btn+'>📋 کپی لینک</button><button id="sn-sh" '+btn2+'>↗ ارسال</button>':'<div style="font-size:1.4rem;letter-spacing:5px;color:#ffd84a;direction:ltr">'+S.room+'</div>')+'<div><button id="sn-lv" '+btn2+'>خروج</button></div>');
 const cp=d.querySelector('#sn-cp');if(cp){cp.onclick=()=>{(navigator.clipboard?navigator.clipboard.writeText(link):Promise.reject()).then(()=>cp.textContent='✓ کپی شد').catch(()=>prompt('لینک:',link))};
  d.querySelector('#sn-sh').onclick=()=>{if(navigator.share)navigator.share({title:title,text:'بیا '+title+' آنلاین بازی کنیم!',url:link}).catch(()=>{});else cp.click()}}
 d.querySelector('#sn-lv').onclick=()=>{location.href=location.pathname};
 S.status=t=>{const e=document.getElementById('sn-s');if(e)e.textContent=t}};
window.SLNet=S;
})();
