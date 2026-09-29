(() => {
  'use strict';
  const CONFIG_KEY='riprapt-remote-config-v1', HISTORY_PREFIX='riprapt-remote-history-v1:';
  const defaultConfig={broker:{url:'wss://mqtt.example.com:8084/mqtt',username:'',password:''},devices:[{id:'F01',name:'Fermentor F01',inputTopic:'riprapt/F01/in',outputRoot:'riprapt/F01/out'}]};
  class MiniMqtt {
    constructor(callbacks={}){this.cb=callbacks;this.ws=null;this.timer=null;this.reconnectTimer=null;this.connected=false;this.packetId=1;this.manual=false;this.options=null}
    open(url,options={}){this.close(true);this.manual=false;this.options={url,...options};this._open()}
    _open(){const o=this.options;if(!o)return;try{this.ws=new WebSocket(o.url,['mqtt']);this.ws.binaryType='arraybuffer'}catch(e){this.cb.error?.(e);this._schedule();return}
      this.ws.onopen=()=>{try{this.ws.send(this._connectPacket(o))}catch(e){this.cb.error?.(e)}};
      this.ws.onmessage=e=>this._parse(new Uint8Array(e.data));
      this.ws.onerror=()=>this.cb.error?.(new Error('WebSocket-Verbindung fehlgeschlagen'));
      this.ws.onclose=()=>{this.connected=false;this._stopPing();this.cb.close?.();this._schedule()};
    }
    close(manual=false){this.manual=manual;clearTimeout(this.reconnectTimer);this.reconnectTimer=null;this._stopPing();if(this.ws){try{if(this.connected)this.ws.send(Uint8Array.from([0xE0,0x00]));this.ws.close()}catch{}this.ws=null}this.connected=false}
    _schedule(){if(this.manual||this.reconnectTimer)return;this.reconnectTimer=setTimeout(()=>{this.reconnectTimer=null;this._open()},3000)}
    subscribe(topic){if(!this.connected)throw new Error('MQTT ist nicht verbunden');const id=this.packetId++||1,payload=this._concat(this._u16(id),this._str(topic),Uint8Array.of(0));this.ws.send(this._packet(0x82,payload))}
    publish(topic,text){if(!this.connected)throw new Error('MQTT ist nicht verbunden');const body=this._concat(this._str(topic),new TextEncoder().encode(text));this.ws.send(this._packet(0x30,body))}
    _connectPacket(o){const clientId=o.clientId||('riprapt-web-'+crypto.randomUUID().replaceAll('-','').slice(0,16));let flags=0x02;if(o.username)flags|=0x80;if(o.password)flags|=0x40;const variable=Uint8Array.from([0,4,77,81,84,84,4,flags,0,30]);const parts=[variable,this._str(clientId)];if(o.username)parts.push(this._str(o.username));if(o.password)parts.push(this._str(o.password));return this._packet(0x10,this._concat(...parts))}
    _parse(data){let pos=0;while(pos<data.length){const header=data[pos++];let mult=1,remaining=0,b;do{if(pos>=data.length)return;b=data[pos++];remaining+=(b&127)*mult;mult*=128}while(b&128);if(pos+remaining>data.length)return;const body=data.slice(pos,pos+remaining);pos+=remaining;const type=header>>4;if(type===2){if(body[1]===0){this.connected=true;this._startPing();this.cb.connect?.()}else this.cb.error?.(new Error('MQTT CONNACK Fehler '+body[1]))}else if(type===3){if(body.length<2)continue;const n=(body[0]<<8)|body[1];if(2+n>body.length)continue;const topic=new TextDecoder().decode(body.slice(2,2+n));let off=2+n;const qos=(header>>1)&3;if(qos>0)off+=2;const payload=new TextDecoder().decode(body.slice(off));this.cb.message?.(topic,payload)}else if(type===9){this.cb.suback?.()}else if(type===13){/* PINGRESP */}}
    }
    _startPing(){this._stopPing();this.timer=setInterval(()=>{if(this.connected&&this.ws?.readyState===WebSocket.OPEN)try{this.ws.send(Uint8Array.from([0xC0,0x00]))}catch{}},15000)}
    _stopPing(){if(this.timer)clearInterval(this.timer);this.timer=null}
    _str(text){const b=new TextEncoder().encode(String(text));return this._concat(Uint8Array.of((b.length>>8)&255,b.length&255),b)}
    _u16(n){return Uint8Array.of((n>>8)&255,n&255)}
    _packet(header,body){return this._concat(Uint8Array.of(header),this._remaining(body.length),body)}
    _remaining(n){const a=[];do{let d=n%128;n=Math.floor(n/128);if(n>0)d|=128;a.push(d)}while(n>0);return Uint8Array.from(a)}
    _concat(...arrays){const len=arrays.reduce((n,a)=>n+a.length,0),out=new Uint8Array(len);let p=0;for(const a of arrays){out.set(a,p);p+=a.length}return out}
  }

  let config=loadConfig(), settingsDraft=null, selectedId=config.devices[0]?.id||'', states={}, client=null, brokerConnected=false;
  config.devices.forEach(d=>states[d.id]=blankState(d.id));
  const $=id=>document.getElementById(id);
  const safeJson=t=>{try{return JSON.parse(t)}catch{return null}};
  const fmt=(v,d=1,s='')=>Number.isFinite(v)?Number(v).toFixed(d)+s:'—';
  const duration=s=>{if(!Number.isFinite(s))return'—';s=Math.max(0,Math.floor(s));const d=Math.floor(s/86400),h=Math.floor(s%86400/3600),m=Math.floor(s%3600/60);return d?`${d}d ${h}h ${m}m`:`${h}h ${m}m`};
  function blankState(id){return{availability:'unknown',status:{},control:{},profile:{},network:{},alarm:{},profiles:{},lastAck:'',lastMessageAt:0,history:loadHistory(id)}}
  function loadConfig(){try{const p=JSON.parse(localStorage.getItem(CONFIG_KEY)||'null');return p?.broker?.url&&Array.isArray(p.devices)&&p.devices.length?p:structuredClone(defaultConfig)}catch{return structuredClone(defaultConfig)}}
  function saveConfig(){localStorage.setItem(CONFIG_KEY,JSON.stringify(config))}
  function validateConnectionConfig(candidate){
    if(!candidate||typeof candidate!=='object')throw new Error('Ungültige Konfiguration.');
    if(!candidate.broker||typeof candidate.broker!=='object')throw new Error('Broker-Konfiguration fehlt.');
    const urlText=String(candidate.broker.url||'').trim();
    let url;
    try{url=new URL(urlText)}catch{throw new Error('MQTT WebSocket URL ist ungültig.')}
    if(!['wss:','ws:'].includes(url.protocol))throw new Error('MQTT URL muss ws:// oder wss:// verwenden.');
    if(location.protocol==='https:'&&url.protocol!=='wss:')throw new Error('Über HTTPS ist ausschließlich wss:// zulässig.');
    if(!Array.isArray(candidate.devices)||candidate.devices.length<1)throw new Error('Mindestens ein Fermenter ist erforderlich.');
    if(candidate.devices.length>32)throw new Error('Maximal 32 Fermenter sind zulässig.');
    const ids=new Set();
    const devices=candidate.devices.map((raw,index)=>{
      if(!raw||typeof raw!=='object')throw new Error(`Fermenter ${index+1} ist ungültig.`);
      const id=String(raw.id||'').trim();
      const name=String(raw.name||id||`Fermentor ${index+1}`).trim();
      const inputTopic=String(raw.inputTopic||'').trim();
      const outputRoot=String(raw.outputRoot||'').trim().replace(/\/+$/,'');
      if(!id)throw new Error(`Fermenter ${index+1}: ID fehlt.`);
      if(ids.has(id))throw new Error(`Fermenter-ID "${id}" ist doppelt vorhanden.`);
      ids.add(id);
      if(!inputTopic||!outputRoot)throw new Error(`Fermenter ${id}: MQTT-Topics fehlen.`);
      if(/[#+]/.test(inputTopic)||/[#+]/.test(outputRoot))throw new Error(`Fermenter ${id}: Wildcards (#/+) sind in den konfigurierten Topics nicht zulässig.`);
      return{id,name,inputTopic,outputRoot};
    });
    return{
      broker:{
        url:urlText,
        username:String(candidate.broker.username??''),
        password:String(candidate.broker.password??'')
      },
      devices
    };
  }
  function parseCredentialsDocument(document){
    if(!document||document.format!=='riprapt-remote-credentials')throw new Error('Unbekanntes Credentials-Format.');
    if(document.version!==1)throw new Error(`Nicht unterstützte Credentials-Version: ${document.version??'fehlt'}.`);
    return validateConnectionConfig({broker:document.broker,devices:document.devices});
  }
  function setImportStatus(message,isError=false){
    const node=$('credentials-import-status');
    node.textContent=message||'';
    node.classList.toggle('hidden',!message);
    node.style.color=isError?'#ff9a9a':'#76e3ad';
  }
  function fillSettingsForm(){
    if(!settingsDraft)return;
    $('broker-url').value=settingsDraft.broker.url;
    $('broker-user').value=settingsDraft.broker.username;
    $('broker-password').value=settingsDraft.broker.password;
    renderDeviceEditors();
  }
  async function importCredentialsFile(file){
    setImportStatus('');
    if(!file)return;
    if(file.size>65536)throw new Error('Credentials-Datei ist größer als 64 KiB.');
    let document;
    try{document=JSON.parse(await file.text())}catch{throw new Error('Credentials-Datei enthält kein gültiges JSON.')}
    settingsDraft=parseCredentialsDocument(document);
    fillSettingsForm();
    setImportStatus(`Credentials aus "${file.name}" geprüft. Zum Übernehmen "Speichern & verbinden" klicken.`);
  }
  function loadHistory(id){try{const p=JSON.parse(localStorage.getItem(HISTORY_PREFIX+id)||'[]');return Array.isArray(p)?p.slice(-360):[]}catch{return[]}}
  function saveHistory(id,h){localStorage.setItem(HISTORY_PREFIX+id,JSON.stringify(h.slice(-360)))}
  function selectedDevice(){return config.devices.find(d=>d.id===selectedId)||config.devices[0]}
  function selectedState(){const d=selectedDevice();return d?(states[d.id]||(states[d.id]=blankState(d.id))):null}
  function setError(text){$('error-notice').textContent=text||'';$('error-notice').classList.toggle('hidden',!text)}
  function updateBrokerPill(){const el=$('broker-pill');el.textContent=brokerConnected?'Broker verbunden':'Broker offline';el.className='pill '+(brokerConnected?'good':'bad')}
  function addHistory(state,id){const t=state.status.temperature_valid===false?null:(Number.isFinite(state.status.temperature_c)?state.status.temperature_c:null);const sp=Number.isFinite(state.status.setpoint_c)?state.status.setpoint_c:(Number.isFinite(state.profile.setpoint_c)?state.profile.setpoint_c:null);if(t===null&&sp===null)return;const now=Date.now(),last=state.history[state.history.length-1];if(last&&now-last.ts<4000)return;state.history=[...state.history,{ts:now,temperature:t,setpoint:sp}].slice(-360);saveHistory(id,state.history)}
  function connect(){if(client){try{client.close(true)}catch{}client=null}brokerConnected=false;updateBrokerPill();setError('');
    client=new MiniMqtt({
      connect:()=>{brokerConnected=true;updateBrokerPill();setError('');try{config.devices.forEach(d=>client.subscribe(`${d.outputRoot}/#`))}catch(e){setError(String(e))}render()},
      close:()=>{brokerConnected=false;updateBrokerPill()},
      error:e=>setError('MQTT: '+(e?.message||e)),
      message:(topic,payload)=>{const d=config.devices.find(x=>topic.startsWith(x.outputRoot+'/'));if(!d)return;const suffix=topic.slice(d.outputRoot.length+1),s=states[d.id]||(states[d.id]=blankState(d.id));s.lastMessageAt=Date.now();if(suffix==='availability')s.availability=payload==='online'?'online':'offline';else if(suffix==='status')s.status=safeJson(payload)||s.status;else if(suffix==='control')s.control=safeJson(payload)||s.control;else if(suffix==='profile')s.profile=safeJson(payload)||s.profile;else if(suffix==='network')s.network=safeJson(payload)||s.network;else if(suffix==='alarm')s.alarm=safeJson(payload)||s.alarm;else if(suffix==='profiles/index')s.profiles=safeJson(payload)||s.profiles;else if(suffix==='ack')s.lastAck=payload;if(suffix==='status'||suffix==='temperature')addHistory(s,d.id);if(d.id===selectedId)render()}
    });
    try{client.open(config.broker.url,{username:config.broker.username,password:config.broker.password,clientId:'riprapt-web-'+crypto.randomUUID().replaceAll('-','').slice(0,16)})}catch(e){setError(String(e))}
  }
  function publish(obj){const d=selectedDevice();if(!d||!client?.connected){setError('MQTT ist nicht verbunden.');return}client.publish(d.inputTopic,JSON.stringify({id:crypto.randomUUID(),...obj}))}
  function renderTabs(){const box=$('device-tabs');box.innerHTML='';config.devices.forEach(d=>{const b=document.createElement('button');b.textContent=d.name;b.className=d.id===selectedId?'active':'';b.onclick=()=>{selectedId=d.id;render()};box.appendChild(b)})}
  function render(){renderTabs();const d=selectedDevice(),s=selectedState();if(!d||!s)return;const online=brokerConnected&&s.availability==='online';$('availability').textContent=online?'ONLINE':String(s.availability||'unknown').toUpperCase();$('availability').className='status-dot '+(online?'online':'offline');$('temperature').textContent=fmt(s.status.temperature_c,1,' °C');$('setpoint').textContent='Sollwert '+fmt(s.status.setpoint_c,1,' °C');$('heat').classList.toggle('on',!!s.status.heat_on);$('cool').classList.toggle('on',!!s.status.cool_on);$('output').textContent='Output '+fmt(s.status.output_percent??s.control.output_percent,1,' %');$('mode').textContent=s.status.mode??s.control.mode??'—';$('regulation').textContent=s.control.regulation??'—';$('density').textContent=s.status.density_valid?fmt(s.status.density_sg,5):'—';$('density-change').textContent=s.status.density_change_per_day_valid?fmt(s.status.density_change_per_day,5):'—';
    $('profile-name').textContent=s.status.profile_name||s.profile.profile_name||'Kein Profil gewählt';$('profile-step').textContent=`Schritt ${s.status.profile_step??s.profile.step??0} / ${s.status.profile_steps??s.profile.steps??0}`;$('profile-remaining').textContent=duration(s.status.profile_remaining_s??s.profile.remaining_s);const flags=$('profile-flags');flags.innerHTML='';[['waiting_for_target','Wartet auf Zieltemperatur','warn'],['waiting_for_condition','Wartet auf Bedingung','warn'],['paused_sensor','Sensorpause','bad']].forEach(([k,t,c])=>{if(s.profile[k]){const e=document.createElement('span');e.className='pill '+c;e.textContent=t;flags.appendChild(e)}});
    const sel=$('profile-select'),cur=String(s.profiles.selected_id??s.profile.profile_id??'');sel.innerHTML='<option value="">Profil wählen</option>';for(const p of s.profiles.profiles||[]){const o=document.createElement('option');o.value=String(p.id);o.textContent=`${p.name} (${p.steps} Schritte)`;sel.appendChild(o)}sel.value=cur;
    $('hostname').textContent=s.network.hostname??'—';$('ip').textContent=s.network.ip??s.status.wifi?.ip??'—';$('rssi').textContent=Number.isFinite(s.network.rssi_dbm)?`${s.network.rssi_dbm} dBm`:'—';$('mac').textContent=s.network.mac??'—';$('firmware').textContent=(s.status.firmware_version??'—')+(Number.isFinite(s.status.firmware_build)?` (${s.status.firmware_build})`:'');$('last-message').textContent=s.lastMessageAt?new Date(s.lastMessageAt).toLocaleTimeString():'—';$('last-ack').textContent=s.lastAck||'—';
    const a=s.alarm||{},active=!!(a.temperature?.active||a.sensor?.active);$('alarm-notice').classList.toggle('hidden',!active);$('alarm-text').textContent=a.sensor?.active?`${a.sensor.trigger_role||'Sensor'}: ${a.sensor.trigger_reason||'Fehler'}`:`Temperaturabweichung ${fmt(a.temperature?.trigger_deviation_c,1,' °C')}`;drawChart(s.history)
  }
  function drawChart(points){const svg=$('chart'),valid=(points||[]).filter(p=>p.temperature!==null||p.setpoint!==null);svg.innerHTML='';if(valid.length<2){svg.innerHTML='<text x="450" y="130" text-anchor="middle" class="chart-empty">Noch keine Verlaufsdaten</text>';return}const vals=valid.flatMap(p=>[p.temperature,p.setpoint].filter(Number.isFinite)),min=Math.min(...vals)-.5,max=Math.max(...vals)+.5,range=Math.max(1,max-min),W=900,H=260,P=22,x=i=>P+i/Math.max(1,valid.length-1)*(W-2*P),y=v=>H-P-(v-min)/range*(H-2*P);const ns='http://www.w3.org/2000/svg';const line=document.createElementNS(ns,'line');line.setAttribute('x1',P);line.setAttribute('y1',H-P);line.setAttribute('x2',W-P);line.setAttribute('y2',H-P);line.setAttribute('class','axis');svg.appendChild(line);for(const [key,cls] of [['setpoint','setpoint-line'],['temperature','temperature-line']]){let path='',drawing=false;valid.forEach((p,i)=>{const v=p[key];if(!Number.isFinite(v)){drawing=false;return}path+=`${drawing?' L':'M'} ${x(i).toFixed(1)} ${y(v).toFixed(1)}`;drawing=true});const el=document.createElementNS(ns,'path');el.setAttribute('d',path);el.setAttribute('class',cls);svg.appendChild(el)}for(const [yy,text] of [[18,max.toFixed(1)+' °C'],[H-5,min.toFixed(1)+' °C']]){const el=document.createElementNS(ns,'text');el.setAttribute('x',P);el.setAttribute('y',yy);el.setAttribute('class','chart-label');el.textContent=text;svg.appendChild(el)}}
  function openSettings(){settingsDraft=structuredClone(config);setImportStatus('');fillSettingsForm();$('settings-modal').classList.remove('hidden')}
  function renderDeviceEditors(){const box=$('device-editor-list');box.innerHTML='';if(!settingsDraft)return;settingsDraft.devices.forEach((d,i)=>{const e=document.createElement('div');e.className='device-editor';e.innerHTML=`<div class="form-grid"><label>ID<input data-field="id" value="${esc(d.id)}"></label><label>Anzeigename<input data-field="name" value="${esc(d.name)}"></label></div><label>Input Topic<input data-field="inputTopic" value="${esc(d.inputTopic)}"></label><label>Output Root<input data-field="outputRoot" value="${esc(d.outputRoot)}"></label>${settingsDraft.devices.length>1?'<button class="ghost danger remove-device">Fermenter entfernen</button>':''}`;e.querySelectorAll('input').forEach(inp=>inp.oninput=()=>{settingsDraft.devices[i][inp.dataset.field]=inp.value});const rm=e.querySelector('.remove-device');if(rm)rm.onclick=()=>{settingsDraft.devices.splice(i,1);renderDeviceEditors()};box.appendChild(e)})}
  function esc(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
  $('open-settings').onclick=openSettings;
  $('close-settings').onclick=()=>{settingsDraft=null;$('settings-modal').classList.add('hidden')};
  $('import-credentials').onclick=()=>{$('credentials-file').value='';$('credentials-file').click()};
  $('credentials-file').onchange=async event=>{try{await importCredentialsFile(event.target.files?.[0])}catch(error){setImportStatus(error?.message||'Credentials-Import fehlgeschlagen.',true)}};
  $('save-settings').onclick=()=>{try{
    if(!settingsDraft)settingsDraft=structuredClone(config);
    settingsDraft.broker.url=$('broker-url').value.trim();
    settingsDraft.broker.username=$('broker-user').value;
    settingsDraft.broker.password=$('broker-password').value;
    config=validateConnectionConfig(settingsDraft);
    settingsDraft=null;
    saveConfig();
    const old=states;states={};
    config.devices.forEach(d=>states[d.id]=old[d.id]||blankState(d.id));
    if(!config.devices.some(d=>d.id===selectedId))selectedId=config.devices[0].id;
    $('settings-modal').classList.add('hidden');
    connect();render();
  }catch(error){setImportStatus(error?.message||'Konfiguration ist ungültig.',true)}};
  $('add-device').onclick=()=>{if(!settingsDraft)settingsDraft=structuredClone(config);const id=`F${String(settingsDraft.devices.length+1).padStart(2,'0')}`;settingsDraft.devices.push({id,name:`Fermentor ${id}`,inputTopic:`riprapt/${id}/in`,outputRoot:`riprapt/${id}/out`});renderDeviceEditors()};
  $('reset-config').onclick=()=>{localStorage.removeItem(CONFIG_KEY);location.reload()};$('ack-alarm').onclick=()=>publish({command:'alarm',action:'acknowledge',target:'all'});$('setpoint-send').onclick=()=>{const v=Number($('setpoint-input').value);if(Number.isFinite(v)){publish({command:'setpoint',value:v});$('setpoint-input').value=''}};document.querySelectorAll('[data-mode]').forEach(b=>b.onclick=()=>publish({command:'mode',value:b.dataset.mode}));$('status-request').onclick=()=>publish({command:'status'});$('profile-select').onchange=e=>{if(e.target.value!=='')publish({command:'profile',action:'select',profile_id:Number(e.target.value)})};$('profile-start').onclick=()=>{const v=$('profile-select').value;publish({command:'profile',action:'start',...(v!==''?{profile_id:Number(v)}:{})})};$('profile-stop').onclick=()=>publish({command:'profile',action:'stop'});
  render();connect();
})();
