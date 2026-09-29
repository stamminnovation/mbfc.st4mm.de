(() => {
  'use strict';

  const CONFIG_KEY='riprapt-remote-config-v2';
  const LEGACY_CONFIG_KEY='riprapt-remote-config-v1';
  const DEVICE_CACHE_KEY='riprapt-remote-device-cache-v1';
  const HISTORY_PREFIX='riprapt-remote-history-v1:';
  const DISCOVERY_FILTER='riprapt/+/out/#';
  const defaultConfig={broker:{host:'',username:'',password:''}};

  class MiniMqtt {
    constructor(callbacks={}){this.cb=callbacks;this.ws=null;this.timer=null;this.reconnectTimer=null;this.connected=false;this.packetId=1;this.manual=false;this.options=null}
    open(url,options={}){this.close(true);this.manual=false;this.options={url,...options};this._open()}
    _open(){
      const o=this.options;if(!o)return;
      try{this.ws=new WebSocket(o.url,['mqtt']);this.ws.binaryType='arraybuffer'}
      catch(e){this.cb.error?.(e);this._schedule();return}
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
    _parse(data){
      let pos=0;
      while(pos<data.length){
        const header=data[pos++];let mult=1,remaining=0,b;
        do{if(pos>=data.length)return;b=data[pos++];remaining+=(b&127)*mult;mult*=128}while(b&128);
        if(pos+remaining>data.length)return;
        const body=data.slice(pos,pos+remaining);pos+=remaining;
        const type=header>>4;
        if(type===2){
          if(body[1]===0){this.connected=true;this._startPing();this.cb.connect?.()}
          else this.cb.error?.(new Error('MQTT CONNACK Fehler '+body[1]));
        }else if(type===3){
          if(body.length<2)continue;
          const n=(body[0]<<8)|body[1];if(2+n>body.length)continue;
          const topic=new TextDecoder().decode(body.slice(2,2+n));
          let off=2+n;const qos=(header>>1)&3;if(qos>0)off+=2;
          const payload=new TextDecoder().decode(body.slice(off));
          this.cb.message?.(topic,payload);
        }else if(type===9){this.cb.suback?.()}
      }
    }
    _startPing(){this._stopPing();this.timer=setInterval(()=>{if(this.connected&&this.ws?.readyState===WebSocket.OPEN)try{this.ws.send(Uint8Array.from([0xC0,0x00]))}catch{}},15000)}
    _stopPing(){if(this.timer)clearInterval(this.timer);this.timer=null}
    _str(text){const b=new TextEncoder().encode(String(text));return this._concat(Uint8Array.of((b.length>>8)&255,b.length&255),b)}
    _u16(n){return Uint8Array.of((n>>8)&255,n&255)}
    _packet(header,body){return this._concat(Uint8Array.of(header),this._remaining(body.length),body)}
    _remaining(n){const a=[];do{let d=n%128;n=Math.floor(n/128);if(n>0)d|=128;a.push(d)}while(n>0);return Uint8Array.from(a)}
    _concat(...arrays){const len=arrays.reduce((n,a)=>n+a.length,0),out=new Uint8Array(len);let p=0;for(const a of arrays){out.set(a,p);p+=a.length}return out}
  }

  const $=id=>document.getElementById(id);
  const safeJson=t=>{try{return JSON.parse(t)}catch{return null}};
  const fmt=(v,d=1,s='')=>Number.isFinite(v)?Number(v).toFixed(d)+s:'—';
  const duration=s=>{if(!Number.isFinite(s))return'—';s=Math.max(0,Math.floor(s));const d=Math.floor(s/86400),h=Math.floor(s%86400/3600),m=Math.floor(s%3600/60);return d?`${d}d ${h}h ${m}m`:`${h}h ${m}m`};

  let config=loadConfig();
  let settingsDraft=null;
  let devices=loadDeviceCache().map(deviceFromId);
  let selectedId=devices[0]?.id||'';
  let states={};
  let client=null;
  let brokerConnected=false;
  devices.forEach(d=>states[d.id]=blankState(d.id));

  function normalizeBrokerHost(value){
    const raw=String(value||'').trim();
    if(!raw)return'';
    try{
      const url=new URL(/^[a-z]+:\/\//i.test(raw)?raw:`wss://${raw}`);
      return url.hostname;
    }catch{return''}
  }

  function buildBrokerUrl(broker){
    const host=normalizeBrokerHost(broker?.host);
    if(!host)throw new Error('HiveMQ Brokerkennung / Cluster-Host ist ungültig.');
    return `wss://${host}:8884/mqtt`;
  }

  function migrateLegacyConfig(parsed){
    if(parsed?.broker?.host)return{broker:{host:normalizeBrokerHost(parsed.broker.host),username:String(parsed.broker.username??''),password:String(parsed.broker.password??'')}};
    if(parsed?.broker?.url){
      return{broker:{host:normalizeBrokerHost(parsed.broker.url),username:String(parsed.broker.username??''),password:String(parsed.broker.password??'')}};
    }
    return null;
  }

  function loadConfig(){
    for(const key of [CONFIG_KEY,LEGACY_CONFIG_KEY]){
      try{
        const parsed=JSON.parse(localStorage.getItem(key)||'null');
        const migrated=migrateLegacyConfig(parsed);
        if(migrated?.broker?.host)return migrated;
      }catch{}
    }
    return structuredClone(defaultConfig);
  }

  function saveConfig(){
    localStorage.setItem(CONFIG_KEY,JSON.stringify(config));
    localStorage.removeItem(LEGACY_CONFIG_KEY);
  }

  function loadDeviceCache(){
    try{
      const ids=JSON.parse(localStorage.getItem(DEVICE_CACHE_KEY)||'[]');
      return Array.isArray(ids)?ids.filter(validDeviceId).slice(0,64):[];
    }catch{return[]}
  }

  function saveDeviceCache(){
    localStorage.setItem(DEVICE_CACHE_KEY,JSON.stringify(devices.map(d=>d.id)));
  }

  function validDeviceId(id){
    return typeof id==='string'&&id.length>0&&id.length<=64&&!/[\/# +]/.test(id)&&!id.includes('+');
  }

  function deviceFromId(id){
    return{id,name:`Fermentor ${id}`,inputTopic:`riprapt/${id}/in`,outputRoot:`riprapt/${id}/out`};
  }

  function discoverDevice(id){
    if(!validDeviceId(id))return null;
    let device=devices.find(d=>d.id===id);
    if(device)return device;
    device=deviceFromId(id);
    devices.push(device);
    devices.sort((a,b)=>a.id.localeCompare(b.id,undefined,{numeric:true,sensitivity:'base'}));
    states[id]=blankState(id);
    if(!selectedId)selectedId=id;
    saveDeviceCache();
    renderTabs();
    return device;
  }

  function blankState(id){
    return{availability:'unknown',status:{},control:{},profile:{},network:{},alarm:{},profiles:{},temperature:{},density:{},lastAck:'',lastMessageAt:0,history:loadHistory(id)};
  }

  function loadHistory(id){
    try{const p=JSON.parse(localStorage.getItem(HISTORY_PREFIX+id)||'[]');return Array.isArray(p)?p.slice(-360):[]}
    catch{return[]}
  }

  function saveHistory(id,h){localStorage.setItem(HISTORY_PREFIX+id,JSON.stringify(h.slice(-360)))}
  function selectedDevice(){return devices.find(d=>d.id===selectedId)||devices[0]||null}
  function selectedState(){const d=selectedDevice();return d?(states[d.id]||(states[d.id]=blankState(d.id))):null}
  function setError(text){$('error-notice').textContent=text||'';$('error-notice').classList.toggle('hidden',!text)}
  function updateBrokerPill(){const el=$('broker-pill');el.textContent=brokerConnected?'Broker verbunden':'Broker offline';el.className='pill '+(brokerConnected?'good':'bad')}

  function validateBrokerConfig(candidate){
    if(!candidate||typeof candidate!=='object'||!candidate.broker)throw new Error('Broker-Konfiguration fehlt.');
    const host=normalizeBrokerHost(candidate.broker.host);
    if(!host)throw new Error('HiveMQ Brokerkennung / Cluster-Host ist ungültig.');
    const username=String(candidate.broker.username??'');
    const password=String(candidate.broker.password??'');
    if(!username)throw new Error('MQTT Benutzername fehlt.');
    if(!password)throw new Error('MQTT Passwort fehlt.');
    return{broker:{host,username,password}};
  }

  function parseCredentialsDocument(document){
    if(!document||document.format!=='riprapt-remote-credentials')throw new Error('Unbekanntes Credentials-Format.');
    if(document.version===2){
      return validateBrokerConfig({broker:document.broker});
    }
    if(document.version===1){
      const migrated=migrateLegacyConfig({broker:document.broker});
      if(!migrated)throw new Error('Brokerdaten der Version-1-Datei sind ungültig.');
      return validateBrokerConfig(migrated);
    }
    throw new Error(`Nicht unterstützte Credentials-Version: ${document.version??'fehlt'}.`);
  }

  function setImportStatus(message,isError=false){
    const node=$('credentials-import-status');
    node.textContent=message||'';
    node.classList.toggle('hidden',!message);
    node.style.color=isError?'#ff9a9a':'#76e3ad';
  }

  function fillSettingsForm(){
    if(!settingsDraft)return;
    $('broker-host').value=settingsDraft.broker.host;
    $('broker-user').value=settingsDraft.broker.username;
    $('broker-password').value=settingsDraft.broker.password;
  }

  async function importCredentialsFile(file){
    setImportStatus('');
    if(!file)return;
    if(file.size>65536)throw new Error('Credentials-Datei ist größer als 64 KiB.');
    let document;
    try{document=JSON.parse(await file.text())}
    catch{throw new Error('Credentials-Datei enthält kein gültiges JSON.')}
    settingsDraft=parseCredentialsDocument(document);
    fillSettingsForm();
    setImportStatus(`Credentials aus "${file.name}" geprüft. Zum Übernehmen "Speichern & verbinden" klicken.`);
  }

  function addHistory(state,id){
    const t=state.status.temperature_valid===false?null:
      (Number.isFinite(state.status.temperature_c)?state.status.temperature_c:
      (state.temperature.valid&&Number.isFinite(state.temperature.temperature_c)?state.temperature.temperature_c:null));
    const sp=Number.isFinite(state.status.setpoint_c)?state.status.setpoint_c:
      (Number.isFinite(state.temperature.setpoint_c)?state.temperature.setpoint_c:
      (Number.isFinite(state.profile.setpoint_c)?state.profile.setpoint_c:null));
    if(t===null&&sp===null)return;
    const now=Date.now(),last=state.history[state.history.length-1];
    if(last&&now-last.ts<4000)return;
    state.history=[...state.history,{ts:now,temperature:t,setpoint:sp}].slice(-360);
    saveHistory(id,state.history);
  }

  function handleMessage(topic,payload){
    const match=topic.match(/^riprapt\/([^/]+)\/out\/(.+)$/);
    if(!match)return;
    const id=match[1],suffix=match[2];
    const device=discoverDevice(id);
    if(!device)return;
    const state=states[id]||(states[id]=blankState(id));
    state.lastMessageAt=Date.now();

    if(suffix==='availability')state.availability=payload==='online'?'online':'offline';
    else if(suffix==='status')state.status=safeJson(payload)||state.status;
    else if(suffix==='temperature')state.temperature=safeJson(payload)||state.temperature;
    else if(suffix==='density')state.density=safeJson(payload)||state.density;
    else if(suffix==='control')state.control=safeJson(payload)||state.control;
    else if(suffix==='profile')state.profile=safeJson(payload)||state.profile;
    else if(suffix==='network')state.network=safeJson(payload)||state.network;
    else if(suffix==='alarm')state.alarm=safeJson(payload)||state.alarm;
    else if(suffix==='profiles/index')state.profiles=safeJson(payload)||state.profiles;
    else if(suffix==='ack')state.lastAck=payload;

    if(suffix==='status'||suffix==='temperature')addHistory(state,id);
    if(id===selectedId)render();
  }

  function connect(){
    if(client){try{client.close(true)}catch{}client=null}
    brokerConnected=false;updateBrokerPill();setError('');
    if(!config.broker.host){
      setError('Noch keine HiveMQ-Verbindung konfiguriert. Öffne "Verbindung".');
      return;
    }

    let url;
    try{url=buildBrokerUrl(config.broker)}
    catch(e){setError(e.message);return}

    client=new MiniMqtt({
      connect:()=>{
        brokerConnected=true;updateBrokerPill();setError('');
        try{client.subscribe(DISCOVERY_FILTER)}
        catch(e){setError(String(e))}
        render();
      },
      close:()=>{brokerConnected=false;updateBrokerPill()},
      error:e=>setError('MQTT: '+(e?.message||e)),
      message:handleMessage
    });

    try{
      client.open(url,{
        username:config.broker.username,
        password:config.broker.password,
        clientId:'riprapt-web-'+crypto.randomUUID().replaceAll('-','').slice(0,16)
      });
    }catch(e){setError(String(e))}
  }

  function publish(obj){
    const device=selectedDevice();
    if(!device||!client?.connected){setError('MQTT ist nicht verbunden oder es wurde noch kein Fermenter erkannt.');return}
    client.publish(device.inputTopic,JSON.stringify({id:crypto.randomUUID(),...obj}));
  }

  function renderTabs(){
    const box=$('device-tabs');box.innerHTML='';
    if(!devices.length){
      const placeholder=document.createElement('span');
      placeholder.className='hint';
      placeholder.textContent=brokerConnected?'Warte auf Fermenter-Telemetrie …':'Noch keine Fermenter erkannt';
      box.appendChild(placeholder);
      return;
    }
    devices.forEach(d=>{
      const button=document.createElement('button');
      button.textContent=d.name;
      button.className=d.id===selectedId?'active':'';
      button.onclick=()=>{selectedId=d.id;render()};
      box.appendChild(button);
    });
  }

  function renderEmpty(){
    $('availability').textContent='UNKNOWN';$('availability').className='status-dot offline';
    $('temperature').textContent='—';$('setpoint').textContent='Sollwert —';
    $('heat').classList.remove('on');$('cool').classList.remove('on');$('output').textContent='Output —';
    $('mode').textContent='—';$('regulation').textContent='—';$('density').textContent='—';$('density-change').textContent='—';
    $('profile-name').textContent='Kein Fermenter erkannt';$('profile-step').textContent='Schritt 0 / 0';$('profile-remaining').textContent='—';$('profile-flags').innerHTML='';
    $('profile-select').innerHTML='<option value="">Profil wählen</option>';
    ['hostname','ip','rssi','mac','firmware','last-message','last-ack'].forEach(id=>$(id).textContent='—');
    $('alarm-notice').classList.add('hidden');drawChart([]);
  }

  function render(){
    renderTabs();
    const device=selectedDevice(),state=selectedState();
    if(!device||!state){renderEmpty();return}

    const online=brokerConnected&&state.availability==='online';
    $('availability').textContent=online?'ONLINE':String(state.availability||'unknown').toUpperCase();
    $('availability').className='status-dot '+(online?'online':'offline');

    const temperature=Number.isFinite(state.status.temperature_c)?state.status.temperature_c:state.temperature.temperature_c;
    const setpoint=Number.isFinite(state.status.setpoint_c)?state.status.setpoint_c:state.temperature.setpoint_c;
    const densityValid=state.status.density_valid??state.density.valid;
    const density=Number.isFinite(state.status.density_sg)?state.status.density_sg:state.density.density_sg;
    const densityChangeValid=state.status.density_change_per_day_valid??state.density.change_per_day_valid;
    const densityChange=Number.isFinite(state.status.density_change_per_day)?state.status.density_change_per_day:state.density.change_per_day;

    $('temperature').textContent=fmt(temperature,1,' °C');
    $('setpoint').textContent='Sollwert '+fmt(setpoint,1,' °C');
    $('heat').classList.toggle('on',!!(state.status.heat_on??state.control.heat_on));
    $('cool').classList.toggle('on',!!(state.status.cool_on??state.control.cool_on));
    $('output').textContent='Output '+fmt(state.status.output_percent??state.control.output_percent,1,' %');
    $('mode').textContent=state.status.mode??state.control.mode??'—';
    $('regulation').textContent=state.control.regulation??'—';
    $('density').textContent=densityValid?fmt(density,5):'—';
    $('density-change').textContent=densityChangeValid?fmt(densityChange,5):'—';

    $('profile-name').textContent=state.status.profile_name||state.profile.profile_name||'Kein Profil gewählt';
    $('profile-step').textContent=`Schritt ${state.status.profile_step??state.profile.step??0} / ${state.status.profile_steps??state.profile.steps??0}`;
    $('profile-remaining').textContent=duration(state.status.profile_remaining_s??state.profile.remaining_s);

    const flags=$('profile-flags');flags.innerHTML='';
    [['waiting_for_target','Wartet auf Zieltemperatur','warn'],['waiting_for_condition','Wartet auf Bedingung','warn'],['paused_sensor','Sensorpause','bad']].forEach(([key,text,cls])=>{
      if(state.profile[key]){const e=document.createElement('span');e.className='pill '+cls;e.textContent=text;flags.appendChild(e)}
    });

    const select=$('profile-select');
    const current=String(state.profiles.selected_id??state.profile.profile_id??'');
    select.innerHTML='<option value="">Profil wählen</option>';
    for(const profile of state.profiles.profiles||[]){
      const option=document.createElement('option');option.value=String(profile.id);option.textContent=`${profile.name} (${profile.steps} Schritte)`;select.appendChild(option);
    }
    select.value=current;

    $('hostname').textContent=state.network.hostname??'—';
    $('ip').textContent=state.network.ip??state.status.wifi?.ip??'—';
    $('rssi').textContent=Number.isFinite(state.network.rssi_dbm)?`${state.network.rssi_dbm} dBm`:'—';
    $('mac').textContent=state.network.mac??'—';
    $('firmware').textContent=(state.status.firmware_version??'—')+(Number.isFinite(state.status.firmware_build)?` (${state.status.firmware_build})`:'');
    $('last-message').textContent=state.lastMessageAt?new Date(state.lastMessageAt).toLocaleTimeString():'—';
    $('last-ack').textContent=state.lastAck||'—';

    const alarm=state.alarm||{},active=!!(alarm.temperature?.active||alarm.sensor?.active);
    $('alarm-notice').classList.toggle('hidden',!active);
    $('alarm-text').textContent=alarm.sensor?.active?
      `${alarm.sensor.trigger_role||'Sensor'}: ${alarm.sensor.trigger_reason||'Fehler'}`:
      `Temperaturabweichung ${fmt(alarm.temperature?.trigger_deviation_c,1,' °C')}`;

    drawChart(state.history);
  }

  function drawChart(points){
    const svg=$('chart'),valid=(points||[]).filter(p=>p.temperature!==null||p.setpoint!==null);
    svg.innerHTML='';
    if(valid.length<2){svg.innerHTML='<text x="450" y="130" text-anchor="middle" class="chart-empty">Noch keine Verlaufsdaten</text>';return}
    const vals=valid.flatMap(p=>[p.temperature,p.setpoint].filter(Number.isFinite));
    const min=Math.min(...vals)-.5,max=Math.max(...vals)+.5,range=Math.max(1,max-min),W=900,H=260,P=22;
    const x=i=>P+i/Math.max(1,valid.length-1)*(W-2*P),y=v=>H-P-(v-min)/range*(H-2*P);
    const ns='http://www.w3.org/2000/svg';
    const line=document.createElementNS(ns,'line');line.setAttribute('x1',P);line.setAttribute('y1',H-P);line.setAttribute('x2',W-P);line.setAttribute('y2',H-P);line.setAttribute('class','axis');svg.appendChild(line);
    for(const [key,cls] of [['setpoint','setpoint-line'],['temperature','temperature-line']]){
      let path='',drawing=false;
      valid.forEach((p,i)=>{const value=p[key];if(!Number.isFinite(value)){drawing=false;return}path+=`${drawing?' L':'M'} ${x(i).toFixed(1)} ${y(value).toFixed(1)}`;drawing=true});
      const el=document.createElementNS(ns,'path');el.setAttribute('d',path);el.setAttribute('class',cls);svg.appendChild(el);
    }
    for(const [yy,text] of [[18,max.toFixed(1)+' °C'],[H-5,min.toFixed(1)+' °C']]){
      const el=document.createElementNS(ns,'text');el.setAttribute('x',P);el.setAttribute('y',yy);el.setAttribute('class','chart-label');el.textContent=text;svg.appendChild(el);
    }
  }

  function openSettings(){
    settingsDraft=structuredClone(config);
    setImportStatus('');
    fillSettingsForm();
    $('settings-modal').classList.remove('hidden');
  }

  $('open-settings').onclick=openSettings;
  $('close-settings').onclick=()=>{settingsDraft=null;$('settings-modal').classList.add('hidden')};
  $('import-credentials').onclick=()=>{$('credentials-file').value='';$('credentials-file').click()};
  $('credentials-file').onchange=async event=>{try{await importCredentialsFile(event.target.files?.[0])}catch(error){setImportStatus(error?.message||'Credentials-Import fehlgeschlagen.',true)}};

  $('save-settings').onclick=()=>{
    try{
      if(!settingsDraft)settingsDraft=structuredClone(config);
      settingsDraft.broker.host=$('broker-host').value.trim();
      settingsDraft.broker.username=$('broker-user').value;
      settingsDraft.broker.password=$('broker-password').value;
      config=validateBrokerConfig(settingsDraft);
      settingsDraft=null;
      saveConfig();
      $('settings-modal').classList.add('hidden');
      connect();render();
    }catch(error){setImportStatus(error?.message||'Konfiguration ist ungültig.',true)}
  };

  $('reset-config').onclick=()=>{
    localStorage.removeItem(CONFIG_KEY);
    localStorage.removeItem(LEGACY_CONFIG_KEY);
    localStorage.removeItem(DEVICE_CACHE_KEY);
    location.reload();
  };

  $('ack-alarm').onclick=()=>publish({command:'alarm',action:'acknowledge',target:'all'});
  $('setpoint-send').onclick=()=>{const value=Number($('setpoint-input').value);if(Number.isFinite(value)){publish({command:'setpoint',value});$('setpoint-input').value=''}};
  document.querySelectorAll('[data-mode]').forEach(button=>button.onclick=()=>publish({command:'mode',value:button.dataset.mode}));
  $('status-request').onclick=()=>publish({command:'status'});
  $('profile-select').onchange=event=>{if(event.target.value!=='')publish({command:'profile',action:'select',profile_id:Number(event.target.value)})};
  $('profile-start').onclick=()=>{const value=$('profile-select').value;publish({command:'profile',action:'start',...(value!==''?{profile_id:Number(value)}:{})})};
  $('profile-stop').onclick=()=>publish({command:'profile',action:'stop'});

  render();
  connect();
})();
