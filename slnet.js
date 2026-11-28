/* S loop shared online layer: rooms over public MQTT brokers (all at once, de-duplicated) + records helper. */
(function(){
const LANG=localStorage.getItem('sl-lang')==='en'?'en':'fa',T=(e,f)=>LANG==='en'?e:f;
const BR=['wss://broker.hivemq.com:8884/mqtt','wss://broker.emqx.io:8084/mqtt','wss://test.mosquitto.org:8081/mqtt'];
const ALPH='ABCDEFGHJKLMNPQRSTUVWXYZ23456789',enc=new TextEncoder(),dec=new TextDecoder();
const str=s=>{const b=enc.encode(s);return[b.length>>8,b.length&255,...b]};
const pkt=(h,body)=>{const rl=[];let n=body.length;do{let d=n%128;n=Math.floor(n/128);if(n>0)d|=128;rl.push(d)}while(n>0);return Uint8Array.from([h,...rl,...body])};
const q=new URLSearchParams(location.search);
const S={me:'P'+Math.random().toString(36).slice(2,8),conns:[],seen:new Set(),seq:0,room:null,game:null,opt:null,host:false,seat:0,players:[],started:false,last:{},bc:null};
/* ---------- records ---------- */
S.rec=function(id,s,low){try{s=+s;if(!(s>0))return;const o=JSON.parse(localStorage.getItem('sl-lb-mine')||'{}'),c=o[id];if(!c||(low?s<c.s:s>c.s)){o[id]={s};localStorage.setItem('sl-lb-mine',JSON.stringify(o))}}catch(e){}};
/* ---------- transport ---------- */
const topic=()=>'sloop-game/v1/'+S.game+'/'+S.room;
function raw(m){const j=JSON.stringify(m);S.conns.forEach(c=>{if(c.ok)try{c.ws.send(pkt(0x30,[...str(topic()),...enc.encode(j)]))}catch(e){}});if(S.bc)S.bc.postMessage(j)}
function got(j){let m;try{m=JSON.parse(j)}catch(e){return}if(!m||m.f===S.me)return;const k=m.f+':'+m.q;if(S.seen.has(k))return;S.seen.add(k);if(S.seen.size>3000)S.seen.clear();S.last[m.f]=Date.now();handle(m)}
function open(){if(q.get('net')==='bc'){S.bc=new BroadcastChannel(topic());S.bc.onmessage=e=>got(e.data);return}
 BR.forEach((u,i)=>{let ws;try{ws=new WebSocket(u,'mqtt')}catch(e){return}const c={ws,ok:false,buf:new Uint8Array(0)};S.conns[i]=c;ws.binaryType='arraybuffer';
  ws.onopen=()=>ws.send(pkt(0x10,[...str('MQTT'),4,2,0,60,...str('slg-'+S.me+'-'+i)]));
  ws.onmessage=e=>{const n=new Uint8Array(e.data),b=new Uint8Array(c.buf.length+n.length);b.set(c.buf);b.set(n,c.buf.length);c.buf=b;
   for(;;){if(c.buf.length<2)return;let p=1,mult=1,len=0,x;do{if(p>=c.buf.length)return;x=c.buf[p++];len+=(x&127)*mult;mult*=128}while(x&128);if(c.buf.length<p+len)return;const type=c.buf[0]>>4,body=c.buf.slice(p,p+len);c.buf=c.buf.slice(p+len);
    if(type===2){c.ok=true;ws.send(pkt(0x82,[0,1,...str(topic()),0]));status();hello()}
    else if(type===3){const tl=(body[0]<<8)|body[1];got(dec.decode(body.slice(2+tl)))}}};
  ws.onclose=ws.onerror=()=>{c.ok=false;status()};c.ping=setInterval(()=>{if(c.ok)try{ws.send(Uint8Array.from([0xC0,0]))}catch(e){}},25000)})}
function close(){S.conns.forEach(c=>{try{clearInterval(c.ping);c.ws.close()}catch(e){}});S.conns=[];if(S.bc)S.bc.close();S.bc=null}
function send(m){m.f=S.me;m.q=++S.seq;raw(m)}
S.send=o=>{const m={t:'g',d:o,f:S.me,q:++S.seq};if(S.rtc&&S.rtc.open&&S.players.length===2&&S.rtc.dc&&S.rtc.dc.readyState==='open'){try{S.rtc.dc.send(JSON.stringify(m));return}catch(e){}}raw(m)};
S.sendTo=(seat,o)=>send({t:'g',d:o,to:seat});
/* ---------- room protocol ---------- */
/* ---------- optional direct WebRTC channel (faster); MQTT stays as fallback ---------- */
const ICE={iceServers:[{urls:'stun:stun.l.google.com:19302'}]};
function gatherDone(pc){return new Promise(r=>{if(pc.iceGatheringState==='complete')return r();const f=()=>{if(pc.iceGatheringState==='complete'){pc.removeEventListener('icegatheringstatechange',f);r()}};pc.addEventListener('icegatheringstatechange',f);setTimeout(r,4000)})}
function rtcNew(init){try{const pc=new RTCPeerConnection(ICE),p={pc,dc:null,open:false};const bind=dc=>{p.dc=dc;dc.onopen=()=>{p.open=true};dc.onclose=()=>{p.open=false};dc.onmessage=e=>got(e.data)};if(init)bind(pc.createDataChannel('g'));else pc.ondatachannel=e=>bind(e.channel);S.rtc=p;return p}catch(e){return null}}
async function rtcOffer(id){if(S.bc&&q.get('rtc')!=='1')return;const p=rtcNew(true);if(!p)return;try{await p.pc.setLocalDescription(await p.pc.createOffer());await gatherDone(p.pc);send({t:'rtc',to:id,d:JSON.stringify(p.pc.localDescription)})}catch(e){}}
async function rtcSig(m){if(m.to!==S.me)return;try{const d=JSON.parse(m.d);if(d.type==='offer'){const p=rtcNew(false);if(!p)return;await p.pc.setRemoteDescription(d);await p.pc.setLocalDescription(await p.pc.createAnswer());await gatherDone(p.pc);send({t:'rtc',to:m.f,d:JSON.stringify(p.pc.localDescription)})}else if(S.rtc){await S.rtc.pc.setRemoteDescription(d)}}catch(e){}}
function hello(){if(!S.room)return;send({t:'hi',n:S.name,h:S.host?1:0})}
function plist(){return S.players.map(p=>({id:p.id,n:p.n}))}
function handle(m){
 if(m.t==='hi'){if(S.host&&!S.started){let p=S.players.find(x=>x.id===m.f);if(!p){if(S.players.length>=(S.opt.max||2))return;S.players.push({id:m.f,n:String(m.n||'?').slice(0,16)});if(S.players.length===2)rtcOffer(m.f)}else p.n=String(m.n||'?').slice(0,16);send({t:'pl',l:plist()});ui()}else if(!S.host&&m.h)send({t:'hi',n:S.name,h:0})}
 else if(m.t==='pl'){if(!S.host){S.players=m.l;S.seat=Math.max(0,S.players.findIndex(p=>p.id===S.me));ui()}}
 else if(m.t==='go'){S.players=m.l;S.seat=Math.max(0,S.players.findIndex(p=>p.id===S.me));S.seed=m.seed;begin()}
 else if(m.t==='g'){if(m.to!==undefined&&m.to!==S.seat)return;const i=S.players.findIndex(p=>p.id===m.f);S.opt.msg&&S.opt.msg(m.d,i)}
 else if(m.t==='rtc'){rtcSig(m)}
 else if(m.t==='bye'){S.opt.left&&S.opt.left(S.players.findIndex(p=>p.id===m.f))}}
function begin(){S.started=true;S.isHost=S.host;ov&&ov.remove();ov=null;S.opt.start&&S.opt.start(S);btn.style.display='none'}
S.leave=()=>{try{send({t:'bye'})}catch(e){}try{S.rtc&&S.rtc.pc.close()}catch(e){}S.rtc=null;close();S.room=null;S.started=false};
addEventListener('pagehide',()=>{if(S.room)try{send({t:'bye'})}catch(e){}});
setInterval(()=>{if(S.started){send({t:'hb'});const now=Date.now();S.players.forEach((p,i)=>{if(p.id!==S.me&&S.last[p.id]&&now-S.last[p.id]>25000&&!p.gone){p.gone=1;S.opt.left&&S.opt.left(i)}})}},6000);
/* ---------- UI ---------- */
let ov=null,btn=null;
const css='position:fixed;inset:0;background:#000c;z-index:99999;display:flex;align-items:center;justify-content:center;padding:14px;font-family:system-ui,sans-serif;color:#fff;direction:'+(LANG==='en'?'ltr':'rtl');
function status(){const e=document.getElementById('sln-st');if(e)e.textContent=S.conns.some(c=>c.ok)||S.bc?T('🌐 Connected to server','🌐 به سرور وصل شدی'):T('⏳ Connecting…','⏳ در حال اتصال…')}
function link(){const u=new URL(location.href);u.search='?room='+S.room;if(q.get('net'))u.search+='&net='+q.get('net');return u.toString()}
function ui(){if(!ov)return;const b=ov.querySelector('#sln-b');if(!S.room){b.innerHTML=`<h3>🌐 ${T('Play online','بازی آنلاین')}</h3><input id="sln-n" maxlength="14" value="${(S.name||'').replace(/"/g,'')}" placeholder="${T('Your name','نام تو')}" style="width:100%;padding:10px;border-radius:10px;border:0;margin:10px 0;font:inherit;text-align:center"><button class="sb" id="sln-c">${T('➕ Create room','➕ ساخت اتاق')}</button><div style="margin:8px 0;opacity:.7">${T('or','یا')}</div><div style="display:flex;gap:6px"><input id="sln-j" maxlength="6" placeholder="CODE" style="flex:1;padding:10px;border-radius:10px;border:0;font:inherit;text-align:center;text-transform:uppercase"><button class="sb" id="sln-jb">${T('Join','ورود')}</button></div><button class="sx" id="sln-x">✖</button>`;
  b.querySelector('#sln-c').onclick=()=>{S.name=b.querySelector('#sln-n').value.trim()||'Player';start(true,gen())};b.querySelector('#sln-jb').onclick=()=>{const v=b.querySelector('#sln-j').value.trim().toUpperCase();if(v.length!==6)return;S.name=b.querySelector('#sln-n').value.trim()||'Player';start(false,v)};b.querySelector('#sln-x').onclick=()=>{ov.remove();ov=null}}
 else{const need=S.opt.min||2;b.innerHTML=`<h3>${T('Room','اتاق')}: <b style="letter-spacing:3px;font-size:1.4rem">${S.room}</b></h3><div id="sln-st" style="font-size:.85rem;opacity:.8"></div><div style="margin:10px 0;line-height:1.9">${S.players.map((p,i)=>`${i?'👤':'👑'} ${p.n.replace(/</g,'&lt;')}${p.id===S.me?' ('+T('you','تو')+')':''}`).join('<br>')||'…'}</div>
  ${S.host?`<button class="sb" id="sln-cp">${T('📋 Copy invite link','📋 کپی لینک دعوت')}</button><button class="sb" id="sln-go" ${S.players.length<need?'disabled':''}>${T('▶ Start','▶ شروع')} (${S.players.length}/${S.opt.max||2})</button>`:`<div>${T('Waiting for the host to start…','منتظر شروع میزبان…')}</div>`}<button class="sx" id="sln-x">✖</button>`;status();
  const cp=b.querySelector('#sln-cp');if(cp)cp.onclick=()=>{try{navigator.clipboard.writeText(link())}catch(e){prompt('Link',link())}cp.textContent='✓'};
  const go=b.querySelector('#sln-go');if(go)go.onclick=()=>{S.seed=Math.floor(Math.random()*2**31);send({t:'go',l:plist(),seed:S.seed});S.seat=0;begin()};
  b.querySelector('#sln-x').onclick=()=>{S.leave();ov.remove();ov=null}}}
function gen(){return Array.from({length:6},()=>ALPH[Math.random()*ALPH.length|0]).join('')}
function start(host,code){S.host=host;S.room=code;S.game=S.opt.game;S.started=false;S.players=host?[{id:S.me,n:S.name}]:[];S.seat=0;S.last={};open();ui();if(!host){let t=0;const iv=setInterval(()=>{if(S.started||!S.room){clearInterval(iv);return}hello();if(++t>20)clearInterval(iv)},1500)}}
function show(){if(ov)return;ov=document.createElement('div');ov.style.cssText=css;ov.innerHTML='<style>.sb{width:100%;padding:12px;border:0;border-radius:12px;background:#c6e03c;font-weight:800;font:inherit;font-weight:800;margin:6px 0}.sb:disabled{opacity:.4}.sx{position:absolute;top:10px;right:14px;background:none;border:0;color:#fff;font-size:1.4rem}</style><div id="sln-b" style="background:#1d2733;padding:22px;border-radius:16px;width:min(92vw,340px);text-align:center;position:relative"></div>';document.body.appendChild(ov);ui()}
S.init=function(opt){S.opt=opt;S.game=opt.game;let n='';try{n=(JSON.parse(localStorage.getItem('sl-profile')||'{}').name||'')}catch(e){}S.name=q.get('n')||n||'';
 btn=document.createElement('button');btn.textContent='🌐 '+T('Online','آنلاین');btn.style.cssText='position:fixed;top:calc(env(safe-area-inset-top,0px) + 6px);right:8px;z-index:9999;background:#2a7de1;color:#fff;border:0;border-radius:14px;padding:5px 12px;font:600 12px system-ui';btn.onclick=show;document.body.appendChild(btn);
 const r=q.get('room');if(r&&/^[A-HJ-NP-Z2-9]{6}$/.test(r)){S.name=S.name||'Player';start(q.get('host')==='1',r);show()}};
window.SLNet=S;
})();
