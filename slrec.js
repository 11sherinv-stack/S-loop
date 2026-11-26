/* S Loop shared helpers: records (Top Leader) + VIP flag */
(function(){
var KEY='sl-lb-mine',LOWER=['memory','minesweeper','jigsaw','sudoku','slidepuzzle'];
var id=(location.pathname.split('/').pop()||'').replace(/\.html?$/,'');
function load(){try{return JSON.parse(localStorage.getItem(KEY)||'{}')||{}}catch(e){return{}}}
function save(o){try{localStorage.setItem(KEY,JSON.stringify(o))}catch(e){}}
var vip=false;try{vip=!!JSON.parse(localStorage.getItem('sl-vip')).on}catch(e){}
window.SLV=vip;
window.SLRec={
  id:id,
  set:function(v,gid){gid=gid||id;v=+v;if(!(v>0)||!isFinite(v))return;var o=load(),old=o[gid]?+o[gid].s:null,low=LOWER.indexOf(gid)>=0;if(old===null||(low?v<old:v>old)){o[gid]={s:Math.round(v*100)/100,t:Date.now()};save(o)}},
  win:function(gid){gid=gid||id;var o=load();o[gid]={s:(o[gid]?+o[gid].s:0)+1,t:Date.now()};save(o)},
  get:function(gid){var o=load();return o[gid||id]?+o[gid||id].s:0}
};
if(window.SL_REC_WINS){
  var armed=true,tm=0,re=/تو بردی|برنده.{0,8}شدی|بردی!|You win|YOU WIN|You survived|بازی را بردی/;
  new MutationObserver(function(){clearTimeout(tm);tm=setTimeout(function(){var t=(document.body.innerText||'');var hit=re.test(t);if(hit&&armed){armed=false;window.SLRec.win()}else if(!hit)armed=true},350)}).observe(document.documentElement,{childList:true,subtree:true,characterData:true});
}
})();
