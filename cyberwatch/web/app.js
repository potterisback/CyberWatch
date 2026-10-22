'use strict';
const $ = id => document.getElementById(id);
const fmt = n => new Intl.NumberFormat('en-US').format(Number(n || 0));
let snapshot = null, paused = false, busy = false;
const el = (tag, text, cls) => {const node = document.createElement(tag); if(text !== undefined)node.textContent=text;if(cls)node.className=cls;return node;};
const svgNS = 'http://www.w3.org/2000/svg';
const svgel = (tag, attrs, text) => {const node=document.createElementNS(svgNS,tag);for(const [key,value] of Object.entries(attrs))node.setAttribute(key,String(value));if(text!==undefined)node.textContent=text;return node;};
const pct = value => (100 * Number(value || 0)).toFixed(1) + '%';
const bytes = value => {let n=Number(value||0),i=0;const units=['B','KB','MB','GB'];while(n>=1000&&i<3){n/=1000;i++;}return n.toFixed(i?1:0)+' '+units[i];};

function showView(view){
  document.querySelectorAll('.view').forEach(node=>node.hidden=node.id!==view);
  document.querySelectorAll('.nav').forEach(node=>{node.classList.toggle('active',node.dataset.view===view);node.setAttribute('aria-selected',String(node.dataset.view===view));});
  const names={overview:'Overview',alerts:'Alert explorer',model:'Model & data',architecture:'Architecture'};
  $('crumb').textContent=names[view];
  $('page-title').textContent={overview:'A clearer view of your traffic.',alerts:'Every signal. In context.',model:'Evidence behind the detection.',architecture:'Built for the complete data journey.'}[view];
}
document.querySelectorAll('[data-view]').forEach(button=>button.addEventListener('click',()=>showView(button.dataset.view)));
document.querySelectorAll('[data-go]').forEach(button=>button.addEventListener('click',()=>showView(button.dataset.go)));
$('pause').addEventListener('click',()=>{paused=!paused;$('pause').textContent=paused?'Resume refresh':'Pause refresh';});
$('refresh').addEventListener('click',()=>refresh());
$('protocol').addEventListener('change',()=>refresh());
$('search').addEventListener('input',()=>renderAlerts());

function table(rows){
 const root=el('table');const header=el('thead');const tr=el('tr');
 ['Event time (UTC)','Record','Protocol','Risk score','Detection reason'].forEach(name=>tr.append(el('th',name)));
 header.append(tr);root.append(header);const body=el('tbody');
 if(!rows.length){const tr=el('tr');const td=el('td','No suspicious connections match this view.','empty');td.colSpan=5;tr.append(td);body.append(tr);}
 for(const row of rows){const tr=el('tr');tr.append(el('td',String(row.event_time).replace('T',' ').slice(0,19)));tr.append(el('td',row.event_id.slice(0,12),'mono'));tr.append(el('td',row.proto.toUpperCase()));const score=el('td');score.append(el('span',Number(row.score).toFixed(2),'score'));tr.append(score);tr.append(el('td',row.reason,'reason'));body.append(tr);}
 root.append(body);return root;
}
function renderAlerts(){if(!snapshot)return;const search=$('search').value.toLowerCase();const rows=snapshot.alerts.filter(r=>[r.event_id,r.proto,r.reason,r.service].join(' ').toLowerCase().includes(search));$('alert-table').replaceChildren(table(rows));}

function drawTimeline(rows){
 const root=$('timeline');root.replaceChildren();const width=760,height=230,left=43,right=14,top=16,bottom=35;
 if(!rows.length){root.append(svgel('text',{x:380,y:110,'text-anchor':'middle',fill:'#8d9daf','font-size':13},'No traffic ingested yet'));return;}
 const max=Math.max(1,...rows.map(r=>Number(r.events)));const x=i=>left+i*(width-left-right)/Math.max(rows.length-1,1);const y=v=>height-bottom-(Number(v)/max)*(height-top-bottom);
 for(let i=0;i<=4;i++){const value=max*i/4;root.append(svgel('line',{x1:left,x2:width-right,y1:y(value),y2:y(value),stroke:'#223044','stroke-width':1,'stroke-dasharray':'3 5'}));root.append(svgel('text',{x:left-9,y:y(value)+3,'text-anchor':'end',fill:'#6f849c','font-size':9},fmt(Math.round(value))));}
 for(const [field,color] of [['events','#52dfbd'],['alerts','#ffb16a']]){
  const path=rows.map((r,i)=>(i?'L':'M')+x(i).toFixed(2)+','+y(r[field]).toFixed(2)).join(' ');
  if(field==='events'){root.append(svgel('path',{d:path+` L${x(rows.length-1)},${height-bottom} L${left},${height-bottom} Z`,fill:color,'fill-opacity':.06}));}
  root.append(svgel('path',{d:path,fill:'none',stroke:color,'stroke-width':2.5,'stroke-linejoin':'round','stroke-linecap':'round'}));
  if(rows.length===1)root.append(svgel('circle',{cx:x(0),cy:y(rows[0][field]),r:3,fill:color}));
 }
 const steps=Math.min(5,rows.length);for(let i=0;i<steps;i++){const index=Math.round(i*(rows.length-1)/Math.max(steps-1,1));const label=String(rows[index].minute).replace('T',' ').slice(11,16);root.append(svgel('text',{x:x(index),y:height-10,'text-anchor':'middle',fill:'#70859c','font-size':9},label));}
}

function protocolBars(rows,total){const root=$('protocol-bars');root.replaceChildren();for(const row of rows.slice(0,6)){const item=el('div');const label=el('div',undefined,'bar-label');label.append(el('span',row.proto.toUpperCase()));label.append(el('span',fmt(row.events)+' · '+pct(row.events/Math.max(total,1))));item.append(label);const svg=svgel('svg',{viewBox:'0 0 300 7',preserveAspectRatio:'none',class:'bar-svg'});svg.append(svgel('rect',{x:0,y:0,width:300,height:7,rx:3,fill:'#233044'}));svg.append(svgel('rect',{x:0,y:0,width:300*row.events/Math.max(total,1),height:7,rx:3,fill:'#52dfbd'}));item.append(svg);root.append(item);}}

function render(data){
 snapshot=data;const s=data.summary;$('total').textContent=fmt(s.total_events);$('alert-total').textContent=fmt(s.alert_count);$('nav-count').textContent=fmt(s.alert_count);$('alert-rate').textContent=pct(s.alert_rate)+' of processed connections';$('bytes').textContent=bytes(s.total_bytes);$('rejected').textContent=fmt(s.rejected_count);$('backend').textContent=data.backend==='trino'?'LAKEHOUSE':'LOCAL REFERENCE';
 const synthetic=Number(s.synthetic_count);$('data-mode').textContent=synthetic===Number(s.total_events)&&s.total_events?'Synthetic demonstration data':synthetic?'Mixed synthetic and dataset traffic':'UNSW-NB15 historical dataset replay';
 $('data-note').textContent=synthetic?'Illustrative records only. Synthetic results are not a benchmark of real-world detection.':Number(s.simulated_time_count)?'Original timestamps are unavailable in this split. Event times are simulated; alerts concern completed flow records.':'Historical flow records. Replaying events is a streaming demonstration, not live network monitoring.';
 $('timeline-note').textContent='Active engine: '+(data.engines.join(', ')||'none')+' · Showing the latest '+data.timeline.length+' event minutes.';
 drawTimeline(data.timeline);protocolBars(data.protocols,s.total_events);$('recent-table').replaceChildren(table(data.alerts.slice(0,6)));renderAlerts();
 $('last-update').textContent='Updated '+new Date(data.generated_at).toLocaleTimeString('en-GB')+' · Auto-refresh 6s';
 const root=$('provenance');root.replaceChildren();const list=el('div',undefined,'detail-list');for(const [label,value] of [['Backend',data.mode],['Raw unique records',fmt(s.bronze_count)],['Accepted records',fmt(s.total_events)],['Synthetic records',fmt(s.synthetic_count)],['Simulated event times',fmt(s.simulated_time_count)],['Detection engines',data.engines.join(', ')||'none']]){const item=el('div',label+': ');item.append(el('strong',value));list.append(item);}root.append(list);
 if(!$('protocol').value){const selected=$('protocol').value;$('protocol').replaceChildren(new Option('All protocols',''));for(const row of data.protocols)$('protocol').append(new Option(row.proto.toUpperCase(),row.proto));$('protocol').value=selected;}
}

async function loadModel(){try{const r=await fetch('/api/model');if(!r.ok)throw new Error('Evaluation unavailable');const data=await r.json();const root=$('evaluation');root.replaceChildren();if(data.available===false){root.append(el('p','No evaluation report yet. Train and evaluate a model to populate this panel.','empty'));return;}const intro=el('p',data.dataset+' · '+data.engine,'muted');root.append(intro);const grid=el('div',undefined,'metric-list');for(const key of ['precision','recall','f1','pr_auc']){const item=el('div',key==='pr_auc'&&data.pr_auc_definition==='average_precision_score'?'AVERAGE PRECISION':key.replace('_',' ').toUpperCase(),'metric-item');item.append(el('strong',pct(data[key])));grid.append(item);}root.append(grid);root.append(el('p',fmt(data.training_rows)+' training rows · '+fmt(data.testing_rows)+' held-out rows · threshold '+data.threshold,'muted'));root.append(el('h3','Confusion matrix'));const t=el('table',undefined,'cm');const head=el('tr');['Actual / predicted','Normal','Attack'].forEach(s=>head.append(el('th',s)));t.append(head);(data.confusion_matrix||[]).forEach((row,i)=>{const tr=el('tr');tr.append(el('td',i?'Attack':'Normal'));row.forEach(n=>tr.append(el('td',fmt(n))));t.append(tr);});root.append(t);root.append(el('h3','Most influential features'));const list=el('ol',undefined,'feature-list');for(const f of (data.feature_importance||[]).slice(0,6))list.append(el('li',f.feature+' · '+pct(f.importance)));root.append(list);}catch(e){$('evaluation').replaceChildren(el('p',e.message,'muted'));}}

async function refresh(){if(busy)return;busy=true;$('refresh').disabled=true;try{const query=$('protocol').value?'?proto='+encodeURIComponent($('protocol').value):'';const response=await fetch('/api/dashboard'+query);const data=await response.json();if(!response.ok)throw new Error(data.detail||data.error||'Unable to load data');render(data);$('error').hidden=true;$('connection').textContent='● Connected';}catch(error){$('error').textContent='Could not load data: '+error.message;$('error').hidden=false;$('connection').textContent='● Disconnected';}finally{busy=false;$('refresh').disabled=false;}}
refresh();loadModel();setInterval(()=>{if(!paused)refresh();},6000);
