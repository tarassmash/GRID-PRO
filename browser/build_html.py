import sys
SP=__import__("os").path.dirname(__import__("os").path.abspath(__file__))+"/"
src=open(SP+'../bot.py',encoding='utf-8').read()
a=src.index('PAGE = r"""')+len('PAGE = r"""'); b=src.index('"""',a)
page=src[a:b]
engine=open(SP+'engine.js',encoding='utf-8').read()
def rep(old,new,cnt=1):
    global page
    assert page.count(old)==cnt,(old[:80],page.count(old)); page=page.replace(old,new)
rep('<meta name="viewport" content="width=device-width,initial-scale=1">','<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">')
rep('<title>Сеточный бот Bybit</title>','<title>Сеточный бот Bybit</title>')
# notice + resume banner + link check
rep('<nav class="tabs" id="tabs" aria-label="Сетки"></nav>','''<div class="tabnote" id="tabnote">Бот работает прямо в этой вкладке: пока сетки запущены, не закрывайте её и не давайте компьютеру уснуть. Ключ Bybit хранится только в этом браузере.</div>
<div class="resume" id="resume" hidden><div id="resumetext"></div><div class="row"><button type="button" id="resgo">Перезапустить эти сетки</button><button type="button" id="resno">Снять их ордера и не запускать</button></div><div class="note" id="resnote"></div></div>
<nav class="tabs" id="tabs" aria-label="Сетки"></nav>''')
rep('<p class="note">Ключ создайте на Bybit только с правом «Спот торговля», без вывода средств, лучше с привязкой к вашему IP. Сохраняется в gridbot_config.json рядом с программой.</p>',
    '''<div class="delrow"><button type="button" id="chkbtn">Проверить связь с Bybit</button><span class="note" id="chknote"></span></div>
<p class="note">Ключ создайте на Bybit с правами «Спот торговля» и «Контракты», <b>без вывода средств</b>. Он хранится только в этом браузере (localStorage) и уходит только на сервер Bybit. Не открывайте этот файл на чужом компьютере.</p>''')
rep('.take:disabled{opacity:.4;cursor:not-allowed}','''.take:disabled{opacity:.4;cursor:not-allowed}
.tabnote{font-size:13px;color:var(--muted);border-left:3px solid var(--warn);padding:6px 10px}
.resume{display:grid;gap:10px;padding:14px;border-radius:6px;border:2px solid var(--warn);background:var(--panel)}
.resume .row{display:flex;gap:8px;flex-wrap:wrap}
.resume button,#chkbtn{font:inherit;font-weight:600;font-size:14px;padding:9px 14px;border-radius:4px;border:0;cursor:pointer}
#resgo{background:var(--ink);color:var(--ink-fg)}#resno{background:transparent;color:var(--sell);box-shadow:inset 0 0 0 1px var(--sell)}
#chkbtn{background:var(--panel);color:var(--fg);box-shadow:inset 0 0 0 1px var(--line)}
.resume button:disabled,#chkbtn:disabled{opacity:.5;cursor:wait}''')
rep('.wrap{max-width:860px;margin:0 auto;padding:28px 16px 48px;display:grid;gap:22px}',
    '.wrap{max-width:860px;margin:0 auto;padding-block:28px 48px;padding-left:max(16px,env(safe-area-inset-left,0px));padding-right:max(16px,env(safe-area-inset-right,0px));display:grid;gap:22px}')
rep('<p class="note">При остановке бот снимает все свои ордера.','<p class="note">Пока вкладка закрыта, бот не работает, а ордера остаются на бирже: при следующем открытии бот предложит перезапустить сетки или снять их ордера. При остановке бот снимает все свои ордера.')
# extra UI code
extra='''
(function(){
 const list=GRIDPRO.wasRunning();if(!list.length)return;
 $("resume").hidden=false;
 $("resumetext").innerHTML="<b>Эти сетки работали, когда страница закрылась:</b> "+list.map(g=>esc(String(g.symbol).replace("USDT",""))+(g.market==="futures"?" (фьючерсы)":" (спот)")).join(", ")+
  ". Их ордера могли остаться на бирже без присмотра.";
 const go=async restart=>{$("resgo").disabled=$("resno").disabled=true;$("resnote").textContent=restart?"Снимаю старые ордера и запускаю…":"Снимаю ордера…";
  try{const r=await GRIDPRO.resume(restart);$("resnote").textContent=r.join(" · ");setTimeout(()=>{$("resume").hidden=true},6000);poll()}
  catch(e){$("resnote").textContent="Не получилось: "+e.message;$("resgo").disabled=$("resno").disabled=false}};
 $("resgo").onclick=()=>go(true);$("resno").onclick=()=>go(false);
})();
$("chkbtn").onclick=async()=>{const b=$("chkbtn");b.disabled=true;$("chknote").textContent="Проверяю…";await saveNow();
 try{const d=await (await fetch("/api/check")).json();
  $("chknote").textContent=(d.pub==="ok"?"Рынок Bybit: связь есть. ":"Рынок Bybit: "+d.pub+" ")+
   (d.key==="ok"?`Ключ работает: свободно ${fmt(d.usdt)} USDT, маржа ${fmt(d.margin)} USDT.`:"Ключ: "+d.key)}
 catch(e){$("chknote").textContent="Ошибка: "+e.message}
 b.disabled=false;spFunds()};
["api_key","api_secret","network"].forEach(k=>$(k).addEventListener("change",()=>setTimeout(spFunds,800)));
'''
rep('addEventListener("resize",draw);\n</script>','addEventListener("resize",draw);\n'+extra+'</script>')
rep('<script>\nconst $=id=>document.getElementById(id);','<script>\n'+engine+'\n</script>\n<script>\nconst $=id=>document.getElementById(id);')
out=sys.argv[1]
open(out,'w',encoding='utf-8').write(page)
print(out,len(page),'bytes')
