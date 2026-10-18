/* Operations pages share the existing theme and keep unfinished form edits. */
let watchView = null, opsView = null, accessGroups = [], accessKeys = [], accessRows = [];
const opTime = value => value ? new Date(Number(value) * 1000).toLocaleString('zh-CN') : '—';
const opList = value => (value || '').split(/[,\n]/).map(s => s.trim()).filter(Boolean);
const opBusy = id => document.getElementById(id)?.contains(document.activeElement) && document.activeElement.matches('input,select,textarea');
const opRows = (rows, empty, cols) => rows.length ? rows.join('') : `<tr><td colspan="${cols}">${empty}</td></tr>`;
async function accessAPI(path, options = {}) {
  return api('/upstream' + path, {...options, headers:{'content-type':'application/json','X-Master-Key':$('access-master').value}});
}
async function loadMarketWatch(force = false) {
  try {
    const value = await api('/watch'); watchView = value;
    const config = value.config || {}, state = value.state || {}, stats = state.stats || {};
    metric($('watch-metrics'), [['触发事件',stats.events || 0],['派发任务',stats.dispatched || 0],['注册成功',stats.success || 0],['收码估算',money(stats.cost || 0)]]);
    if (force || !opBusy('market-watch')) {
      $('watch-enabled').value = config.enabled ? '1':'0'; $('watch-interval').value = config.interval || 300;
      $('watch-want').value = config.per_trigger || 1; $('watch-cooldown').value = (config.cooldown_seconds || 86400) / 3600;
    }
    $('watch-status').textContent = value.paused || (config.enabled ? '值班已启用':'值班已暂停');
    $('watch-projects').innerHTML = opRows((config.projects || []).map((p,i) => `<tr><td>${esc(p.name)}</td><td>${esc(p.sid)}</td><td>${money(p.max_price)}</td><td>${esc(p.min_stock)}</td><td>${p.enabled ? '观察中':'暂停'}</td><td><button class="secondary" data-wtoggle="${i}">${p.enabled ? '暂停':'启用'}</button> <button class="danger" data-wremove="${i}">移除</button></td></tr>`),'尚未添加项目',6);
    $('watch-projects').querySelectorAll('[data-wtoggle]').forEach(b => b.onclick = () => updateWatchProject(+b.dataset.wtoggle, false));
    $('watch-projects').querySelectorAll('[data-wremove]').forEach(b => b.onclick = () => updateWatchProject(+b.dataset.wremove, true));
    const query = $('watch-search').value.toLowerCase();
    const events = [...(state.events || [])].reverse().filter(e => JSON.stringify(e).toLowerCase().includes(query));
    const names = {new_uid:'新码',restock:'补货',price_threshold:'降价进入区间',relisted:'重新上架',fetch_error:'拉表失败'};
    $('watch-events').innerHTML = opRows(events.slice(0,150).map(e => `<tr><td>${esc(opTime(e.at))}</td><td>${esc(e.sid)} / ${esc(e.uid)}</td><td>${esc(names[e.type] || e.type)}</td><td>${esc(e.task_count ?? e.want ?? '—')}</td><td>${esc(e.success ?? e.ok ?? '—')}</td><td>${money(e.cost ?? e.received_cost)} / ${money(e.uncertain_cost)}</td><td>${esc(e.status || e.error || '—')}</td></tr>`),'尚无触发事件',7);
    const cooldown = state.cooldown || {};
    $('watch-quality').innerHTML = Object.keys(cooldown).length ? '<h3>渠道冷却</h3>' + Object.entries(cooldown).map(([k,v]) => `<div>${esc(k)} · ${esc(opTime(typeof v === 'number' ? v : v.until))}</div>`).join('') : '<p style="color:var(--muted)">当前没有差渠道冷却记录</p>';
  } catch(e) { $('watch-status').textContent = e.message; }
}
async function saveWatch(config) {
  await api('/watch',{method:'PUT',body:JSON.stringify(config)}); await loadMarketWatch(true);
}
async function updateWatchProject(index, remove) {
  try {const config = structuredClone(watchView.config); if (remove) config.projects.splice(index,1); else config.projects[index].enabled = !config.projects[index].enabled; await saveWatch(config);} catch(e) {toast(e.message);}
}
async function loadOperations(force = false) {
  try {
    const [view, accounts] = await Promise.all([api('/gateway-operations/state'),api('/gateway-operations/accounts').catch(()=>({accounts:[]}))]);
    opsView = view; const config = view.config || {}, outbox = view.outbox || {};
    metric($('ops-metrics'),[['号池账号',(accounts.accounts || []).length],['入池完成',outbox.success || 0],['入池失败',outbox.failed || 0],['待处理',outbox.pending || 0]]);
    if(force || !opBusy('operations')) { $('ops-import').value = config.enable_auto_import === false ? '0':'1'; $('ops-maintenance').value = config.maintenance_enabled ? '1':'0'; $('ops-interval').value = config.maintenance_interval || 3600; }
    $('ops-status').textContent = view.health?.ok === false ? '网关离线；配置与入池队列仍保留' : view.maintenance?.last_error || '网关管理已连接；凭证导入与注册独立执行';
    const filter = $('ops-search').value.toLowerCase();
    $('ops-accounts').innerHTML = opRows((accounts.accounts || []).filter(a=>JSON.stringify(a).toLowerCase().includes(filter)).map(a=>`<tr><td>${esc(a.nickname || a.uid || a.id)}</td><td>${esc(a.provider || 'trae')}</td><td><span class="tag ${a.enabled === false ? 'bad':'ok'}">${a.enabled === false ? '禁用':'启用'}</span></td><td><button class="secondary" data-account="${esc(a.id)}" data-enabled="${a.enabled === false}">${a.enabled === false ? '启用':'禁用'}</button> <button class="secondary" data-usage="${esc(a.id)}">刷新额度</button> <button class="danger" data-delete-account="${esc(a.id)}">删除</button></td></tr>`),'暂无账号',4);
    $('ops-accounts').querySelectorAll('[data-account]').forEach(b=>b.onclick=()=>opsAction('/accounts/'+encodeURIComponent(b.dataset.account),'PUT',{enabled:b.dataset.enabled === 'true'}));
    $('ops-accounts').querySelectorAll('[data-usage]').forEach(b=>b.onclick=()=>opsAction('/accounts/'+encodeURIComponent(b.dataset.usage)+'/usage','POST',{}));
    $('ops-accounts').querySelectorAll('[data-delete-account]').forEach(b=>b.onclick=()=>{if(confirm('删除该网关账号？本地注册凭证仍保留。'))opsAction('/accounts/'+encodeURIComponent(b.dataset.deleteAccount),'DELETE');});
    $('ops-outbox').textContent = `已扫描 ${outbox.total || 0} 个凭证 · 成功 ${outbox.success || 0} · 失败 ${outbox.failed || 0} · 待处理 ${outbox.pending || 0}`;
     try {const p=await api('/gateway-operations/proxies'); const items=p.proxies || []; $('ops-proxy-status').innerHTML = items.map(v=>`<div>${esc(v.name || v.id || '请求出口')} · ${esc(v.protocol)}://${esc(v.host)}:${esc(v.port)} <button class="secondary" data-proxy-test="${esc(v.id)}">测试</button> <button class="secondary" data-proxy-bind="${esc(v.id)}">绑定账号</button> <button class="danger" data-proxy-delete="${esc(v.id)}">移除</button></div>`).join('') || '暂无请求出口';
       $('ops-proxy-status').querySelectorAll('[data-proxy-test]').forEach(b=>b.onclick=async()=>{try{const r=await api('/gateway-operations/proxies/'+encodeURIComponent(b.dataset.proxyTest)+'/test',{method:'POST'});toast(JSON.stringify(r).slice(0,500));}catch(e){toast(e.message);}});
       $('ops-proxy-status').querySelectorAll('[data-proxy-delete]').forEach(b=>b.onclick=()=>{if(confirm('移除此请求出口？'))opsAction('/proxies/'+encodeURIComponent(b.dataset.proxyDelete),'DELETE');});
       $('ops-proxy-status').querySelectorAll('[data-proxy-bind]').forEach(b=>b.onclick=()=>{const id=prompt('填写要绑定的账号 ID：\n'+(accounts.accounts || []).map(a=>a.id+' '+(a.name || a.uid || '')).join('\n'));if(id)opsAction('/accounts/'+encodeURIComponent(id.trim()),'PUT',{proxy:{source:'pool',proxyId:b.dataset.proxyBind}});});
     } catch(e){$('ops-proxy-status').textContent=e.message;}
  } catch(e) {$('ops-status').textContent=e.message;}
}
async function opsAction(path, method='POST', body) {
  try {const r=await api('/gateway-operations'+path,{method,body:body === undefined ? undefined:JSON.stringify(body)}); toast(r.imported !== undefined ? `本轮导入 ${r.imported}，失败 ${r.failed || 0}`:'操作完成'); await loadOperations();} catch(e){toast(e.message);}
}
async function loadAccess() {
  if(!$('access-master').value) return;
  try {
    const [keys,groups,requests]=await Promise.all([accessAPI('/keys'),accessAPI('/groups'),accessAPI('/requests?limit=150')]);
    accessKeys=keys.data || []; accessGroups=groups.data || []; accessRows=requests.data || [];
    $('access-status').textContent='管理连接已就绪；Key 仅保留在页面内存';
    $('access-keys').innerHTML=opRows(accessKeys.map(k=>`<tr><td>${esc(k.label)}</td><td>${esc(k.id.slice(0,10))}</td><td>${esc(accessGroups.find(g=>g.id===k.group_id)?.name || '默认')} / ${esc((k.models || []).join(', ') || '不限')}</td><td>${k.disabled ? '暂停':'启用'}${k.expires_at ? ' · '+esc(opTime(k.expires_at)):''}</td><td><button class="secondary" data-key-toggle="${esc(k.id)}">${k.disabled ? '启用':'暂停'}</button></td></tr>`),'尚无独立调用密钥',5);
    $('access-keys').querySelectorAll('[data-key-toggle]').forEach(b=>b.onclick=async()=>{try {const k=accessKeys.find(k=>k.id===b.dataset.keyToggle); await accessAPI('/keys/'+k.id,{method:'PATCH',body:JSON.stringify({disabled:!k.disabled})}); await loadAccess();}catch(e){toast(e.message);}});
    $('access-groups').innerHTML=accessGroups.map(g=>`<div>${esc(g.name)} · ${esc((g.models || []).join(', ') || '不限模型')}</div>`).join('') || '尚无模型策略分组';
    renderAccessRows();
  } catch(e){$('access-status').textContent=e.message;}
}
function renderAccessRows() {
  const filter=$('access-log-filter').value.toLowerCase();
  $('access-requests').innerHTML=opRows(accessRows.filter(r=>JSON.stringify(r).toLowerCase().includes(filter)).map(r=>{let usage=r.usage || {}; try{usage=JSON.parse(r.usage_json || '{}');}catch{} return `<tr><td>${esc(opTime(r.created_at))}</td><td>${esc(r.model)} / ${esc((r.key_id || '').slice(0,10))}</td><td>${esc(r.endpoint || (r.stream ? '流式':'同步'))}</td><td>${esc(r.status)}</td><td>${Math.round(r.ttfb_ms || 0)} / ${Math.round(r.latency_ms || 0)} ms</td><td>${esc(usage.total_tokens ?? ((usage.input_tokens || usage.prompt_tokens || 0)+(usage.output_tokens || usage.completion_tokens || 0)))}</td><td>${esc(r.error || '—')}</td></tr>`;}),'暂无匹配请求',7);
}
window.addEventListener('DOMContentLoaded',()=>{
  $('watch-save').onclick=async()=>{try{await saveWatch({...watchView?.config,enabled:$('watch-enabled').value==='1',interval:+$('watch-interval').value,per_trigger:+$('watch-want').value,cooldown_seconds:+$('watch-cooldown').value*3600});toast('监控规则已保存');}catch(e){toast(e.message);}};
  $('watch-tick').onclick=async()=>{try{await api('/watch/tick',{method:'POST'});await loadMarketWatch();}catch(e){toast(e.message);}};
  $('watch-project-add').onclick=async()=>{try{const projects=[...(watchView?.config?.projects || [])];projects.push({sid:$('watch-sid').value.trim(),hex_sid:$('watch-hex').value.trim(),name:$('watch-name').value.trim(),max_price:+$('watch-price').value,min_stock:+$('watch-stock').value,enabled:true});await saveWatch({...watchView?.config,projects});toast('项目已加入监控');}catch(e){toast(e.message);}};
  $('watch-use-project').onclick=()=>{$('watch-sid').value=$('hz-sid').value;$('watch-hex').value=$('hz-catalog-sid').value;toast('已填入当前项目，请核对名称和价格');};
  $('watch-refresh').onclick=()=>loadMarketWatch();$('watch-search').oninput=()=>loadMarketWatch();
  $('ops-refresh').onclick=()=>loadOperations();$('ops-search').oninput=()=>loadOperations();
  $('ops-save').onclick=()=>opsAction('/config','PUT',{enable_auto_import:$('ops-import').value==='1',maintenance_enabled:$('ops-maintenance').value==='1',maintenance_interval:+$('ops-interval').value});
  $('ops-sync').onclick=()=>opsAction('/import');$('ops-maintain').onclick=()=>opsAction('/maintenance');
  $('ops-proxy-add').onclick=()=>opsAction('/proxies','POST',{url:$('ops-proxy-url').value.trim(),name:'WebUI 请求出口'});
  $('access-unlock').onclick=loadAccess;$('access-refresh').onclick=loadAccess;$('access-log-filter').oninput=renderAccessRows;
  $('access-create').onclick=async()=>{try{const name=$('access-group').value.trim(),group=accessGroups.find(g=>g.name===name);if(name&&!group)throw Error('请先创建对应模型策略分组');const days=+$('access-days').value;const r=await accessAPI('/keys',{method:'POST',body:JSON.stringify({label:$('access-name').value.trim(),group_id:group?.id || null,models:opList($('access-models').value),expires_at:days ? Date.now()/1000+days*86400:null})});$('access-secret').textContent='仅显示一次，请复制保存：'+r.api_key;await loadAccess();}catch(e){toast(e.message);}};
  $('access-group-save').onclick=async()=>{try{await accessAPI('/groups',{method:'POST',body:JSON.stringify({name:$('access-group-name').value.trim(),models:opList($('access-group-models').value)})});await loadAccess();toast('分组已保存');}catch(e){toast(e.message);}};
});
