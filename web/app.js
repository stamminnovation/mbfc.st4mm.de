(() => {
  'use strict';

  const CONFIG_KEY='riprapt-remote-config-v2';
  const LEGACY_CONFIG_KEY='riprapt-remote-config-v1';
  const DEVICE_CACHE_KEY='riprapt-remote-device-cache-v2';
  const HISTORY_PREFIX='riprapt-remote-history-v1:';

  const TOPICS={
    globalAvailability:'fermentorcontrol/availability',
    state:'fermentorcontrol/+/state',
    availability:'fermentorcontrol/+/availability',
    profiles:'fermentorcontrol/+/profiles',
    commandResult:'fermentorcontrol/+/command_result'
  };

  const defaultConfig={broker:{host:'',username:'',password:''}};

  class MiniMqtt {
    constructor(callbacks={}) {
      this.cb=callbacks;
      this.ws=null;
      this.timer=null;
      this.reconnectTimer=null;
      this.connected=false;
      this.packetId=1;
      this.manual=false;
      this.options=null;
      this.pendingSubscriptions=new Map();
    }

    open(url,options={}) {
      this.close(true);
      this.manual=false;
      this.options={url,...options};
      this._open();
    }

    _open() {
      const o=this.options;
      if(!o)return;
      try{
        this.ws=new WebSocket(o.url,['mqtt']);
        this.ws.binaryType='arraybuffer';
      }catch(error){
        this.cb.error?.(error);
        this._schedule();
        return;
      }

      this.ws.onopen=()=>{
        try{this.ws.send(this._connectPacket(o))}
        catch(error){this.cb.error?.(error)}
      };

      this.ws.onmessage=event=>this._parse(new Uint8Array(event.data));
      this.ws.onerror=()=>this.cb.error?.(new Error('WebSocket-Verbindung fehlgeschlagen'));
      this.ws.onclose=()=>{
        this.connected=false;
        this.pendingSubscriptions.clear();
        this._stopPing();
        this.cb.close?.();
        this._schedule();
      };
    }

    close(manual=false) {
      this.manual=manual;
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer=null;
      this._stopPing();
      this.pendingSubscriptions.clear();

      if(this.ws){
        try{
          if(this.connected)this.ws.send(Uint8Array.from([0xE0,0x00]));
          this.ws.close();
        }catch{}
        this.ws=null;
      }

      this.connected=false;
    }

    _schedule() {
      if(this.manual||this.reconnectTimer)return;
      this.reconnectTimer=setTimeout(()=>{
        this.reconnectTimer=null;
        this._open();
      },3000);
    }

    subscribe(topic) {
      if(!this.connected)throw new Error('MQTT ist nicht verbunden');
      const id=this.packetId++||1;
      this.pendingSubscriptions.set(id,topic);
      const payload=this._concat(this._u16(id),this._str(topic),Uint8Array.of(0));
      this.ws.send(this._packet(0x82,payload));
    }

    publish(topic,text) {
      if(!this.connected)throw new Error('MQTT ist nicht verbunden');
      const body=this._concat(this._str(topic),new TextEncoder().encode(text));
      this.ws.send(this._packet(0x30,body));
    }

    _connectPacket(o) {
      const clientId=o.clientId||('mbfc-web-'+crypto.randomUUID().replaceAll('-','').slice(0,16));
      let flags=0x02;
      if(o.username)flags|=0x80;
      if(o.password)flags|=0x40;

      const variable=Uint8Array.from([0,4,77,81,84,84,4,flags,0,30]);
      const parts=[variable,this._str(clientId)];
      if(o.username)parts.push(this._str(o.username));
      if(o.password)parts.push(this._str(o.password));

      return this._packet(0x10,this._concat(...parts));
    }

    _parse(data) {
      let pos=0;

      while(pos<data.length){
        const header=data[pos++];
        let mult=1,remaining=0,b;

        do{
          if(pos>=data.length)return;
          b=data[pos++];
          remaining+=(b&127)*mult;
          mult*=128;
        }while(b&128);

        if(pos+remaining>data.length)return;

        const body=data.slice(pos,pos+remaining);
        pos+=remaining;
        const type=header>>4;

        if(type===2){
          if(body[1]===0){
            this.connected=true;
            this._startPing();
            this.cb.connect?.();
          }else{
            this.cb.error?.(new Error('MQTT CONNACK Fehler '+body[1]));
          }
        }else if(type===3){
          if(body.length<2)continue;
          const topicLength=(body[0]<<8)|body[1];
          if(2+topicLength>body.length)continue;

          const topic=new TextDecoder().decode(body.slice(2,2+topicLength));
          let offset=2+topicLength;
          const qos=(header>>1)&3;
          if(qos>0)offset+=2;

          const payload=new TextDecoder().decode(body.slice(offset));
          this.cb.message?.(topic,payload);
        }else if(type===9){
          if(body.length<3)continue;
          const id=(body[0]<<8)|body[1];
          const codes=[...body.slice(2)];
          const topic=this.pendingSubscriptions.get(id)||'';
          this.pendingSubscriptions.delete(id);
          this.cb.suback?.(topic,codes);
        }
      }
    }

    _startPing() {
      this._stopPing();
      this.timer=setInterval(()=>{
        if(this.connected&&this.ws?.readyState===WebSocket.OPEN){
          try{this.ws.send(Uint8Array.from([0xC0,0x00]))}catch{}
        }
      },15000);
    }

    _stopPing() {
      if(this.timer)clearInterval(this.timer);
      this.timer=null;
    }

    _str(text) {
      const bytes=new TextEncoder().encode(String(text));
      return this._concat(Uint8Array.of((bytes.length>>8)&255,bytes.length&255),bytes);
    }

    _u16(n) {
      return Uint8Array.of((n>>8)&255,n&255);
    }

    _packet(header,body) {
      return this._concat(Uint8Array.of(header),this._remaining(body.length),body);
    }

    _remaining(n) {
      const result=[];
      do{
        let digit=n%128;
        n=Math.floor(n/128);
        if(n>0)digit|=128;
        result.push(digit);
      }while(n>0);
      return Uint8Array.from(result);
    }

    _concat(...arrays) {
      const length=arrays.reduce((sum,array)=>sum+array.length,0);
      const output=new Uint8Array(length);
      let offset=0;
      for(const array of arrays){
        output.set(array,offset);
        offset+=array.length;
      }
      return output;
    }
  }

  const $=id=>document.getElementById(id);
  const safeJson=text=>{try{return JSON.parse(text)}catch{return null}};
  const fmt=(value,digits=1,suffix='')=>Number.isFinite(value)?Number(value).toFixed(digits)+suffix:'—';
  const duration=seconds=>{
    if(!Number.isFinite(seconds))return'—';
    seconds=Math.max(0,Math.floor(seconds));
    const days=Math.floor(seconds/86400);
    const hours=Math.floor(seconds%86400/3600);
    const minutes=Math.floor(seconds%3600/60);
    return days?`${days}d ${hours}h ${minutes}m`:`${hours}h ${minutes}m`;
  };

  let config=loadConfig();
  let settingsDraft=null;
  let devices=loadDeviceCache().map(deviceFromId);
  let selectedId=devices[0]?.id||'';
  let states={};
  let client=null;
  let brokerConnected=false;
  let fermentControlOnline=false;

  devices.forEach(device=>states[device.id]=blankState(device.id));

  function normalizeBrokerHost(value) {
    const raw=String(value||'').trim();
    if(!raw)return'';

    try{
      const url=new URL(/^[a-z]+:\/\//i.test(raw)?raw:`wss://${raw}`);
      return url.hostname;
    }catch{
      return'';
    }
  }

  function buildBrokerUrl(broker) {
    const host=normalizeBrokerHost(broker?.host);
    if(!host)throw new Error('HiveMQ Brokerkennung / Cluster-Host ist ungültig.');
    return `wss://${host}:8884/mqtt`;
  }

  function migrateLegacyConfig(parsed) {
    if(parsed?.broker?.host){
      return{broker:{
        host:normalizeBrokerHost(parsed.broker.host),
        username:String(parsed.broker.username??''),
        password:String(parsed.broker.password??'')
      }};
    }

    if(parsed?.broker?.url){
      return{broker:{
        host:normalizeBrokerHost(parsed.broker.url),
        username:String(parsed.broker.username??''),
        password:String(parsed.broker.password??'')
      }};
    }

    return null;
  }

  function loadConfig() {
    for(const key of [CONFIG_KEY,LEGACY_CONFIG_KEY]){
      try{
        const parsed=JSON.parse(localStorage.getItem(key)||'null');
        const migrated=migrateLegacyConfig(parsed);
        if(migrated?.broker?.host)return migrated;
      }catch{}
    }
    return structuredClone(defaultConfig);
  }

  function saveConfig() {
    localStorage.setItem(CONFIG_KEY,JSON.stringify(config));
    localStorage.removeItem(LEGACY_CONFIG_KEY);
  }

  function loadDeviceCache() {
    try{
      const ids=JSON.parse(localStorage.getItem(DEVICE_CACHE_KEY)||'[]');
      return Array.isArray(ids)?ids.filter(validDeviceId).slice(0,64):[];
    }catch{
      return[];
    }
  }

  function saveDeviceCache() {
    localStorage.setItem(DEVICE_CACHE_KEY,JSON.stringify(devices.map(device=>device.id)));
  }

  function validDeviceId(id) {
    return typeof id==='string'&&id.length>0&&id.length<=64&&!/[\/# +]/.test(id)&&!id.includes('+');
  }

  function deviceFromId(id) {
    return{id,name:`Fermenter ${id}`};
  }

  function discoverDevice(id,name='') {
    if(!validDeviceId(id))return null;

    let device=devices.find(item=>item.id===id);
    if(device){
      if(name&&device.name!==name)device.name=name;
      return device;
    }

    device={id,name:name||`Fermenter ${id}`};
    devices.push(device);
    devices.sort((a,b)=>a.id.localeCompare(b.id,undefined,{numeric:true,sensitivity:'base'}));
    states[id]=blankState(id);

    if(!selectedId)selectedId=id;

    saveDeviceCache();
    renderTabs();
    return device;
  }

  function blankState(id) {
    return{
      availability:'unknown',
      state:{},
      profiles:{},
      lastCommandResult:'',
      lastMessageAt:0,
      history:loadHistory(id)
    };
  }

  function loadHistory(id) {
    try{
      const parsed=JSON.parse(localStorage.getItem(HISTORY_PREFIX+id)||'[]');
      return Array.isArray(parsed)?parsed.slice(-360):[];
    }catch{
      return[];
    }
  }

  function saveHistory(id,history) {
    localStorage.setItem(HISTORY_PREFIX+id,JSON.stringify(history.slice(-360)));
  }

  function selectedDevice() {
    return devices.find(device=>device.id===selectedId)||devices[0]||null;
  }

  function selectedState() {
    const device=selectedDevice();
    return device?(states[device.id]||(states[device.id]=blankState(device.id))):null;
  }

  function setError(text) {
    $('error-notice').textContent=text||'';
    $('error-notice').classList.toggle('hidden',!text);
  }

  function updateBrokerPill() {
    const element=$('broker-pill');

    if(!brokerConnected){
      element.textContent='Broker offline';
      element.className='pill bad';
      return;
    }

    if(!fermentControlOnline){
      element.textContent='Broker verbunden';
      element.className='pill warn';
      return;
    }

    element.textContent='FermentControl online';
    element.className='pill good';
  }

  function validateBrokerConfig(candidate) {
    if(!candidate||typeof candidate!=='object'||!candidate.broker)throw new Error('Broker-Konfiguration fehlt.');

    const host=normalizeBrokerHost(candidate.broker.host);
    if(!host)throw new Error('HiveMQ Brokerkennung / Cluster-Host ist ungültig.');

    const username=String(candidate.broker.username??'');
    const password=String(candidate.broker.password??'');

    if(!username)throw new Error('MQTT Benutzername fehlt.');
    if(!password)throw new Error('MQTT Passwort fehlt.');

    return{broker:{host,username,password}};
  }

  function parseCredentialsDocument(document) {
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

  function setImportStatus(message,isError=false) {
    const node=$('credentials-import-status');
    node.textContent=message||'';
    node.classList.toggle('hidden',!message);
    node.style.color=isError?'#ff9a9a':'#76e3ad';
  }

  function fillSettingsForm() {
    if(!settingsDraft)return;
    $('broker-host').value=settingsDraft.broker.host;
    $('broker-user').value=settingsDraft.broker.username;
    $('broker-password').value=settingsDraft.broker.password;
  }

  async function importCredentialsFile(file) {
    setImportStatus('');
    if(!file)return;
    if(file.size>65536)throw new Error('Credentials-Datei ist größer als 64 KiB.');

    let document;
    try{
      document=JSON.parse(await file.text());
    }catch{
      throw new Error('Credentials-Datei enthält kein gültiges JSON.');
    }

    settingsDraft=parseCredentialsDocument(document);
    fillSettingsForm();
    setImportStatus(`Credentials aus "${file.name}" geprüft. Zum Übernehmen "Speichern & verbinden" klicken.`);
  }

  function addHistory(state,id) {
    const data=state.state||{};
    const temperature=Number(data.temperatureC);
    const setpoint=Number(data.setpointC);

    if(!Number.isFinite(temperature)&&!Number.isFinite(setpoint))return;

    const now=Date.now();
    const last=state.history[state.history.length-1];
    if(last&&now-last.ts<4000)return;

    state.history=[
      ...state.history,
      {
        ts:now,
        temperature:Number.isFinite(temperature)?temperature:null,
        setpoint:Number.isFinite(setpoint)?setpoint:null
      }
    ].slice(-360);

    saveHistory(id,state.history);
  }

  function handleExternalMessage(topic,payload) {
    if(topic===TOPICS.globalAvailability){
      fermentControlOnline=payload==='online';
      updateBrokerPill();
      return;
    }

    const match=topic.match(/^fermentorcontrol\/([^/]+)\/(state|availability|profiles|command_result)$/);
    if(!match)return;

    const id=match[1];
    const suffix=match[2];

    if(!validDeviceId(id))return;

    const parsed=suffix==='availability'?null:safeJson(payload);
    const statePayload=suffix==='state'&&parsed&&typeof parsed==='object'?parsed:null;
    const device=discoverDevice(id,statePayload?.name||'');
    if(!device)return;

    const state=states[id]||(states[id]=blankState(id));
    state.lastMessageAt=Date.now();

    if(suffix==='availability'){
      state.availability=payload==='online'?'online':'offline';
    }else if(suffix==='state'){
      if(statePayload){
        state.state=statePayload;
        if(typeof statePayload.name==='string'&&statePayload.name)device.name=statePayload.name;
        if(statePayload.controller?.online===true)state.availability='online';
        else if(statePayload.controller?.online===false)state.availability='offline';
        addHistory(state,id);
      }
    }else if(suffix==='profiles'){
      if(parsed&&typeof parsed==='object')state.profiles=parsed;
    }else if(suffix==='command_result'){
      state.lastCommandResult=payload;
    }

    if(id===selectedId)render();
    else renderTabs();
  }

  function connect() {
    if(client){
      try{client.close(true)}catch{}
      client=null;
    }

    brokerConnected=false;
    fermentControlOnline=false;
    updateBrokerPill();
    setError('');

    if(!config.broker.host){
      setError('Noch keine HiveMQ-Verbindung konfiguriert. Öffne "Verbindung".');
      return;
    }

    let url;
    try{
      url=buildBrokerUrl(config.broker);
    }catch(error){
      setError(error.message);
      return;
    }

    client=new MiniMqtt({
      connect:()=>{
        brokerConnected=true;
        updateBrokerPill();
        setError('');

        try{
          client.subscribe(TOPICS.globalAvailability);
          client.subscribe(TOPICS.state);
          client.subscribe(TOPICS.availability);
          client.subscribe(TOPICS.profiles);
          client.subscribe(TOPICS.commandResult);
        }catch(error){
          setError(String(error));
        }

        render();
      },
      close:()=>{
        brokerConnected=false;
        fermentControlOnline=false;
        updateBrokerPill();
      },
      error:error=>setError('MQTT: '+(error?.message||error)),
      suback:(topic,codes)=>{
        if(codes.some(code=>code===0x80)){
          setError(`HiveMQ verweigert SUBSCRIBE auf "${topic}". External-MQTT-Berechtigungen des Benutzers prüfen.`);
        }
      },
      message:handleExternalMessage
    });

    try{
      client.open(url,{
        username:config.broker.username,
        password:config.broker.password,
        clientId:'mbfc-web-'+crypto.randomUUID().replaceAll('-','').slice(0,16)
      });
    }catch(error){
      setError(String(error));
    }
  }

  function publishExternal(path,payload={}) {
    const device=selectedDevice();

    if(!device||!client?.connected){
      setError('MQTT ist nicht verbunden oder es wurde noch kein Fermenter erkannt.');
      return;
    }

    client.publish(
      `fermentorcontrol/${device.id}/command/${path}`,
      JSON.stringify(payload)
    );
  }

  function renderTabs() {
    const box=$('device-tabs');
    box.innerHTML='';

    if(!devices.length){
      const placeholder=document.createElement('span');
      placeholder.className='hint';
      placeholder.textContent=brokerConnected?
        'Warte auf FermentControl-Telemetrie …':
        'Noch keine Fermenter erkannt';
      box.appendChild(placeholder);
      return;
    }

    devices.forEach(device=>{
      const button=document.createElement('button');
      button.textContent=device.name;
      button.className=device.id===selectedId?'active':'';
      button.onclick=()=>{
        selectedId=device.id;
        render();
      };
      box.appendChild(button);
    });
  }

  function renderEmpty() {
    $('availability').textContent='UNKNOWN';
    $('availability').className='status-dot offline';
    $('temperature').textContent='—';
    $('setpoint').textContent='Sollwert —';
    $('heat').classList.remove('on');
    $('cool').classList.remove('on');
    $('output').textContent='Output —';
    $('mode').textContent='—';
    $('regulation').textContent='—';
    $('density').textContent='—';
    $('density-change').textContent='—';
    $('profile-name').textContent='Kein Fermenter erkannt';
    $('profile-step').textContent='Schritt 0 / 0';
    $('profile-remaining').textContent='—';
    $('profile-flags').innerHTML='';
    $('profile-select').innerHTML='<option value="">Profil wählen</option>';
    ['hostname','ip','rssi','mac','firmware','last-message','last-ack'].forEach(id=>$(id).textContent='—');
    $('alarm-notice').classList.add('hidden');
    drawChart([]);
  }

  function profileOptions(state) {
    const raw=state.profiles;
    if(Array.isArray(raw))return raw;
    if(Array.isArray(raw?.profiles))return raw.profiles;
    return[];
  }

  function render() {
    renderTabs();

    const device=selectedDevice();
    const state=selectedState();

    if(!device||!state){
      renderEmpty();
      return;
    }

    const data=state.state||{};
    const controller=data.controller||{};
    const profile=data.profile||{};
    const alarms=data.alarms||data.alarm||{};

    const online=brokerConnected&&(state.availability==='online'||controller.online===true);
    $('availability').textContent=online?'ONLINE':String(state.availability||'unknown').toUpperCase();
    $('availability').className='status-dot '+(online?'online':'offline');

    $('temperature').textContent=fmt(Number(data.temperatureC),1,' °C');
    $('setpoint').textContent='Sollwert '+fmt(Number(data.setpointC),1,' °C');
    $('heat').classList.toggle('on',data.heating===true);
    $('cool').classList.toggle('on',data.cooling===true);
    $('output').textContent='Output '+fmt(Number(data.outputPercent),1,' %');
    $('mode').textContent=data.mode??'—';
    $('regulation').textContent=data.regulation??'—';
    $('density').textContent=fmt(Number(data.densitySG),5);
    $('density-change').textContent=fmt(Number(data.densityChangePerDay),5);

    $('profile-name').textContent=profile.name||'Kein Profil gewählt';
    $('profile-step').textContent=`Schritt ${profile.step??0} / ${profile.steps??0}`;
    $('profile-remaining').textContent=duration(Number(profile.remainingSeconds));

    const flags=$('profile-flags');
    flags.innerHTML='';

    const flagDefinitions=[
      ['waitingForTarget','Wartet auf Zieltemperatur','warn'],
      ['waitingForCondition','Wartet auf Bedingung','warn'],
      ['pausedSensor','Sensorpause','bad'],
      ['complete','Profil abgeschlossen','good']
    ];

    for(const [key,text,cls] of flagDefinitions){
      if(profile[key]){
        const element=document.createElement('span');
        element.className='pill '+cls;
        element.textContent=text;
        flags.appendChild(element);
      }
    }

    const select=$('profile-select');
    const options=profileOptions(state);
    const currentId=profile.id??state.profiles?.selectedId??state.profiles?.selected_id??'';

    select.innerHTML='<option value="">Profil wählen</option>';

    for(const item of options){
      const id=item.id??item.profileId??item.profile_id;
      if(id===undefined||id===null)continue;

      const option=document.createElement('option');
      option.value=String(id);
      option.textContent=item.name?`${item.name} (ID ${id})`:`Profil ID ${id}`;
      select.appendChild(option);
    }

    select.value=currentId===undefined||currentId===null?'':String(currentId);

    $('hostname').textContent=controller.hostname??controller.hostName??'—';
    $('ip').textContent=controller.ip??controller.ipAddress??'—';
    $('rssi').textContent=Number.isFinite(Number(controller.rssiDbm))?`${Number(controller.rssiDbm)} dBm`:'—';
    $('mac').textContent=controller.mac??controller.macAddress??'—';
    $('firmware').textContent=controller.firmware??controller.firmwareVersion??'—';
    $('last-message').textContent=state.lastMessageAt?new Date(state.lastMessageAt).toLocaleTimeString():'—';
    $('last-ack').textContent=state.lastCommandResult||'—';

    const temperatureAlarm=alarms.temperature||alarms.temperatureAlarm||{};
    const sensorAlarm=alarms.sensor||alarms.sensorAlarm||{};
    const alarmActive=
      alarms.active===true||
      temperatureAlarm.active===true||
      sensorAlarm.active===true;

    $('alarm-notice').classList.toggle('hidden',!alarmActive);

    if(alarmActive){
      const text=
        alarms.text||
        alarms.alarmText||
        (sensorAlarm.active?
          `${sensorAlarm.triggerRole||'Sensor'}: ${sensorAlarm.triggerReason||'Fehler'}`:
          'Temperaturalarm aktiv');
      $('alarm-text').textContent=text;
    }

    drawChart(state.history);
  }

  function drawChart(points) {
    const svg=$('chart');
    const valid=(points||[]).filter(point=>point.temperature!==null||point.setpoint!==null);
    svg.innerHTML='';

    if(valid.length<2){
      svg.innerHTML='<text x="450" y="130" text-anchor="middle" class="chart-empty">Noch keine Verlaufsdaten</text>';
      return;
    }

    const values=valid.flatMap(point=>[point.temperature,point.setpoint].filter(Number.isFinite));
    const min=Math.min(...values)-.5;
    const max=Math.max(...values)+.5;
    const range=Math.max(1,max-min);
    const width=900,height=260,padding=22;
    const x=index=>padding+index/Math.max(1,valid.length-1)*(width-2*padding);
    const y=value=>height-padding-(value-min)/range*(height-2*padding);
    const ns='http://www.w3.org/2000/svg';

    const axis=document.createElementNS(ns,'line');
    axis.setAttribute('x1',padding);
    axis.setAttribute('y1',height-padding);
    axis.setAttribute('x2',width-padding);
    axis.setAttribute('y2',height-padding);
    axis.setAttribute('class','axis');
    svg.appendChild(axis);

    for(const [key,cls] of [['setpoint','setpoint-line'],['temperature','temperature-line']]){
      let path='',drawing=false;

      valid.forEach((point,index)=>{
        const value=point[key];
        if(!Number.isFinite(value)){
          drawing=false;
          return;
        }

        path+=`${drawing?' L':'M'} ${x(index).toFixed(1)} ${y(value).toFixed(1)}`;
        drawing=true;
      });

      const element=document.createElementNS(ns,'path');
      element.setAttribute('d',path);
      element.setAttribute('class',cls);
      svg.appendChild(element);
    }

    for(const [yy,text] of [[18,max.toFixed(1)+' °C'],[height-5,min.toFixed(1)+' °C']]){
      const element=document.createElementNS(ns,'text');
      element.setAttribute('x',padding);
      element.setAttribute('y',yy);
      element.setAttribute('class','chart-label');
      element.textContent=text;
      svg.appendChild(element);
    }
  }

  function openSettings() {
    settingsDraft=structuredClone(config);
    setImportStatus('');
    fillSettingsForm();
    $('settings-modal').classList.remove('hidden');
  }

  $('open-settings').onclick=openSettings;

  $('close-settings').onclick=()=>{
    settingsDraft=null;
    $('settings-modal').classList.add('hidden');
  };

  $('import-credentials').onclick=()=>{
    $('credentials-file').value='';
    $('credentials-file').click();
  };

  $('credentials-file').onchange=async event=>{
    try{
      await importCredentialsFile(event.target.files?.[0]);
    }catch(error){
      setImportStatus(error?.message||'Credentials-Import fehlgeschlagen.',true);
    }
  };

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
      connect();
      render();
    }catch(error){
      setImportStatus(error?.message||'Konfiguration ist ungültig.',true);
    }
  };

  $('reset-config').onclick=()=>{
    localStorage.removeItem(CONFIG_KEY);
    localStorage.removeItem(LEGACY_CONFIG_KEY);
    localStorage.removeItem(DEVICE_CACHE_KEY);
    location.reload();
  };

  $('ack-alarm').onclick=()=>publishExternal('alarm/acknowledge',{});

  $('setpoint-send').onclick=()=>{
    const value=Number($('setpoint-input').value);
    if(Number.isFinite(value)){
      publishExternal('setpoint',{value});
      $('setpoint-input').value='';
    }
  };

  document.querySelectorAll('[data-mode]').forEach(button=>{
    button.onclick=()=>publishExternal('mode',{value:button.dataset.mode});
  });

  $('status-request').onclick=()=>publishExternal('status',{});

  $('profile-select').onchange=event=>{
    if(event.target.value!==''){
      publishExternal('profile/select',{profile_id:Number(event.target.value)});
    }
  };

  $('profile-start').onclick=()=>{
    const value=$('profile-select').value;
    publishExternal('profile/start',value!==''?{profile_id:Number(value)}:{});
  };

  $('profile-stop').onclick=()=>publishExternal('profile/stop',{});

  render();
  connect();
})();
