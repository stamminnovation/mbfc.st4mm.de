(() => {
  'use strict';

  const CONFIG_KEY='riprapt-remote-config-v2';
  const LEGACY_CONFIG_KEY='riprapt-remote-config-v1';
  const LEGACY_DEVICE_CACHE_KEY='riprapt-remote-device-cache-v2';
  const HISTORY_PREFIX='riprapt-remote-history-v1:';
  const OVERVIEW_TAB='__overview__';
  const ARCHIVE_TAB='__profile_archive__';

  const TOPICS={
    devices:'fermentorcontrol/devices',
    globalAvailability:'fermentorcontrol/availability',
    archiveState:'fermentorcontrol/profile-archive/state'
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

    unsubscribe(topic) {
      if(!this.connected)return;
      const id=this.packetId++||1;
      const payload=this._concat(this._u16(id),this._str(topic));
      this.ws.send(this._packet(0xA2,payload));
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
  const finite=value=>Number.isFinite(Number(value))?Number(value):null;
  const fmt=(value,digits=1,suffix='')=>finite(value)!==null?finite(value).toFixed(digits)+suffix:'—';
  const duration=seconds=>{
    const value=finite(seconds);
    if(value===null)return'—';
    const total=Math.max(0,Math.floor(value));
    const days=Math.floor(total/86400);
    const hours=Math.floor(total%86400/3600);
    const minutes=Math.floor(total%3600/60);
    return days?`${days}d ${hours}h ${minutes}m`:`${hours}h ${minutes}m`;
  };

  function calculateProfileProgress(profile){
    if(!profile||typeof profile!=='object')return null;

    const published=finite(profile.progressPercent);
    if(published!==null){
      return Math.min(100,Math.max(0,Math.round(published*10)/10));
    }

    const steps=finite(profile.steps);
    if(steps===null||steps<=0)return null;
    if(profile.complete===true)return 100;
    if(profile.active!==true)return 0;

    const rawStep=finite(profile.step);
    const step=rawStep===null?1:Math.min(Math.max(Math.trunc(rawStep),1),Math.trunc(steps));
    let withinStep=0;

    const elapsed=finite(profile.elapsedSeconds);
    const remaining=finite(profile.remainingSeconds);
    const timeBased=!profile.advanceMode||profile.advanceMode==='time';

    if(
      timeBased
      && elapsed!==null
      && remaining!==null
      && elapsed>=0
      && remaining>=0
      && elapsed+remaining>0
    ){
      withinStep=Math.min(1,Math.max(0,elapsed/(elapsed+remaining)));
    }

    return Math.min(
      100,
      Math.max(0,Math.round((((step-1)+withinStep)/steps)*1000)/10)
    );
  }

  function renderProfileProgress(profile){
    const panel=$('profile-progress-panel');
    const track=panel.querySelector('.profile-progress-track');
    const bar=$('profile-progress-bar');
    const value=$('profile-progress-value');
    const note=$('profile-progress-note');
    const progress=calculateProfileProgress(profile);

    const visible=progress!==null&&(profile?.active===true||profile?.complete===true);
    panel.classList.toggle('hidden',!visible);

    if(!visible){
      bar.style.width='0%';
      value.textContent='0,0 %';
      note.textContent='';
      track.setAttribute('aria-valuenow','0');
      return;
    }

    bar.style.width=`${progress}%`;
    value.textContent=`${progress.toFixed(1).replace('.',',')} %`;
    track.setAttribute('aria-valuenow',String(progress));

    if(profile.complete===true){
      note.textContent='Profil abgeschlossen';
    }else if(profile.advanceMode&&profile.advanceMode!=='time'){
      note.textContent='Aktueller Schritt endet nach Profilbedingung; kein fiktiver Zeitfortschritt.';
    }else if(finite(profile.remainingSeconds)!==null&&finite(profile.remainingSeconds)>0){
      note.textContent=`Restzeit aktueller Schritt: ${duration(profile.remainingSeconds)}`;
    }else{
      note.textContent='';
    }
  }

  let config=loadConfig();
  let settingsDraft=null;
  let devices=[];
  let selectedId=OVERVIEW_TAB;
  let hasDeviceRegistry=false;
  let states={};
  let archiveState={count:0,profiles:[],targets:[]};
  let client=null;
  let brokerConnected=false;
  let fermentControlOnline=false;
  let subscribedDeviceIds=new Set();

  localStorage.removeItem(LEGACY_DEVICE_CACHE_KEY);

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

    if(document.version===2)return validateBrokerConfig({broker:document.broker});

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

  function blankState(id) {
    return{
      availability:'unknown',
      publicState:null,
      profiles:null,
      lastCommandResult:'',
      lastMessageAt:0,
      history:loadHistory(id)
    };
  }

  function selectedDevice() {
    return devices.find(device=>device.id===selectedId)||null;
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
    }else if(!fermentControlOnline){
      element.textContent='Broker verbunden';
      element.className='pill warn';
    }else{
      element.textContent='FermentControl online';
      element.className='pill good';
    }
  }

  function topicsForDevice(id) {
    const root=`fermentorcontrol/${id}`;
    return[
      `${root}/state`,
      `${root}/availability`,
      `${root}/profiles`,
      `${root}/command_result`
    ];
  }

  function syncDeviceSubscriptions(nextIds) {
    if(!client?.connected)return;

    for(const id of subscribedDeviceIds){
      if(nextIds.has(id))continue;
      for(const topic of topicsForDevice(id))client.unsubscribe(topic);
      subscribedDeviceIds.delete(id);
    }

    for(const id of nextIds){
      if(subscribedDeviceIds.has(id))continue;
      for(const topic of topicsForDevice(id))client.subscribe(topic);
      subscribedDeviceIds.add(id);
    }
  }

  function syncCurrentDevices(targets) {
    const clean=(Array.isArray(targets)?targets:[])
      .filter(target=>target&&typeof target.id==='string'&&target.id&&target.id!=='profile-archive')
      .map(target=>({
        id:target.id,
        name:String(target.name||target.id),
        online:target.online===true,
        writable:target.writable===true
      }));

    const nextIds=new Set(clean.map(device=>device.id));
    syncDeviceSubscriptions(nextIds);

    const nextStates={};
    for(const device of clean){
      nextStates[device.id]=states[device.id]||blankState(device.id);
    }

    states=nextStates;
    devices=clean;

    if(selectedId!==OVERVIEW_TAB&&selectedId!==ARCHIVE_TAB&&!nextIds.has(selectedId)){
      selectedId=OVERVIEW_TAB;
    }

    renderTabs();
    render();
  }

  function addHistory(state,id) {
    const data=state.publicState;
    if(!data)return;

    const temperature=data.temperature?.valid?finite(data.temperature.beerC):null;
    const setpoint=finite(data.temperature?.setpointC);

    if(temperature===null&&setpoint===null)return;

    const now=Date.now();
    const last=state.history[state.history.length-1];
    if(last&&now-last.ts<4000)return;

    state.history=[
      ...state.history,
      {ts:now,temperature,setpoint}
    ].slice(-360);

    saveHistory(id,state.history);
  }

  function handleMessage(topic,payload) {
    if(topic===TOPICS.devices){
      const parsed=safeJson(payload);
      if(parsed?.schemaVersion!==1||!Array.isArray(parsed.devices)||
          parsed.devices.some(device=>!device||typeof device.id!=='string'||
            !/^[A-Za-z0-9_-]{1,48}$/.test(device.id)||typeof device.online!=='boolean')||
          new Set(parsed.devices.map(device=>device.id)).size!==parsed.devices.length){
        setError('Ungültige Fermenterliste empfangen; bisherige Liste bleibt erhalten.');
        return;
      }
      hasDeviceRegistry=true;
      syncCurrentDevices(parsed.devices);
      return;
    }
    if(topic===TOPICS.globalAvailability){
      fermentControlOnline=payload==='online';
      updateBrokerPill();
      render();
      return;
    }

    if(topic===TOPICS.archiveState){
      const parsed=safeJson(payload);
      if(!parsed||typeof parsed!=='object'){
        setError('Profilarchiv-State konnte nicht gelesen werden. External-MQTT-Recht "Profile anzeigen" prüfen.');
        return;
      }

      archiveState=parsed;
      if(!hasDeviceRegistry)syncCurrentDevices(parsed.targets);
      else render();
      return;
    }

    const match=topic.match(/^fermentorcontrol\/([^/]+)\/(state|availability|profiles|command_result)$/);
    if(!match)return;

    const id=match[1];
    const suffix=match[2];

    if(!devices.some(device=>device.id===id))return;

    const state=states[id]||(states[id]=blankState(id));
    state.lastMessageAt=Date.now();

    if(suffix==='availability'){
      state.availability=payload==='online'?'online':'offline';
    }else if(suffix==='state'){
      const parsed=safeJson(payload);
      if(parsed&&typeof parsed==='object'){
        state.publicState=parsed;
        state.availability=parsed.connectionStatus||state.availability;
        const device=devices.find(item=>item.id===id);
        if(device&&parsed.name)device.name=String(parsed.name);
        addHistory(state,id);
      }
    }else if(suffix==='profiles'){
      const parsed=safeJson(payload);
      if(parsed&&typeof parsed==='object')state.profiles=parsed;
    }else if(suffix==='command_result'){
      state.lastCommandResult=payload;
    }

    if(selectedId===id||selectedId===OVERVIEW_TAB)render();
    else renderTabs();
  }

  function connect() {
    if(client){
      try{client.close(true)}catch{}
      client=null;
    }

    brokerConnected=false;
    fermentControlOnline=false;
    subscribedDeviceIds.clear();
    devices=[];
    states={};
    archiveState={count:0,profiles:[],targets:[]};
    selectedId=OVERVIEW_TAB;
    hasDeviceRegistry=false;

    updateBrokerPill();
    setError('');
    render();

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
          client.subscribe(TOPICS.devices);
          client.subscribe(TOPICS.archiveState);
        }catch(error){
          setError(String(error));
        }

        render();
      },
      close:()=>{
        brokerConnected=false;
        fermentControlOnline=false;
        subscribedDeviceIds.clear();
        updateBrokerPill();
        render();
      },
      error:error=>setError('MQTT: '+(error?.message||error)),
      suback:(topic,codes)=>{
        if(codes.some(code=>code===0x80)){
          setError(`HiveMQ verweigert SUBSCRIBE auf "${topic}". MQTT-Berechtigungen prüfen.`);
        }
      },
      message:handleMessage
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
      setError('MQTT ist nicht verbunden oder es wurde kein aktueller Fermenter ausgewählt.');
      return;
    }

    client.publish(
      `fermentorcontrol/${device.id}/command/${path}`,
      JSON.stringify({id:crypto.randomUUID(),...payload})
    );
  }

  function renderTabs() {
    const box=$('device-tabs');
    box.innerHTML='';
    const overview=document.createElement('button');
    overview.textContent=`Fermenterübersicht (${devices.length})`;
    overview.className=selectedId===OVERVIEW_TAB?'active':'';
    overview.onclick=()=>{selectedId=OVERVIEW_TAB;render();};
    box.appendChild(overview);

    for(const device of devices){
      const button=document.createElement('button');
      button.textContent=device.name;
      button.className=device.id===selectedId?'active':'';
      button.onclick=()=>{
        selectedId=device.id;
        render();
      };
      box.appendChild(button);
    }

    const archiveButton=document.createElement('button');
    archiveButton.textContent='Profilarchiv';
    archiveButton.className=selectedId===ARCHIVE_TAB?'active':'';
    archiveButton.onclick=()=>{
      selectedId=ARCHIVE_TAB;
      render();
    };
    box.appendChild(archiveButton);
  }

  function setView(archive) {
    $('fermenter-overview').classList.add('hidden');
    document.querySelectorAll('.hero-grid,.content-grid').forEach(element=>{
      element.classList.toggle('hidden',archive);
    });
    $('profile-archive-view').classList.toggle('hidden',!archive);
  }

  function renderArchive() {
    setView(true);

    const profiles=Array.isArray(archiveState.profiles)?archiveState.profiles:[];
    $('archive-count').textContent=`${profiles.length} ${profiles.length===1?'Profil':'Profile'}`;
    $('archive-selected').textContent=archiveState.selectedProfileName||'—';
    $('archive-target').textContent=archiveState.targetFermenterName||'—';

    const list=$('archive-profile-list');
    list.innerHTML='';

    if(!profiles.length){
      const empty=document.createElement('p');
      empty.className='hint';
      empty.textContent='Keine Archivprofile vorhanden.';
      list.appendChild(empty);
      return;
    }

    for(const profile of profiles){
      const item=document.createElement('div');
      item.className='archive-item';

      const text=document.createElement('div');
      const title=document.createElement('strong');
      const meta=document.createElement('span');

      title.textContent=profile.name||'Unbenanntes Profil';
      meta.textContent=`${profile.steps??0} Schritte · ${profile.archiveId||'—'}`;

      text.appendChild(title);
      text.appendChild(meta);
      item.appendChild(text);

      if(profile.archiveId===archiveState.selectedArchiveId){
        const badge=document.createElement('span');
        badge.className='pill good';
        badge.textContent='Ausgewählt';
        item.appendChild(badge);
      }

      list.appendChild(item);
    }
  }

  function renderEmptyFermenter() {
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
    $('profile-name').textContent='Keine aktuellen Fermenter';
    $('profile-step').textContent='Schritt 0 / 0';
    $('profile-remaining').textContent='—';
    renderProfileProgress(null);
    $('profile-flags').innerHTML='';
    $('profile-select').innerHTML='<option value="">Profil wählen</option>';
    ['hostname','ip','rssi','mac','firmware','last-message','last-ack'].forEach(id=>$(id).textContent='—');
    $('alarm-notice').classList.add('hidden');
    drawChart([]);
  }

  function renderFermenter() {
    setView(false);

    const device=selectedDevice();
    const state=selectedState();

    if(!device||!state||!state.publicState){
      renderEmptyFermenter();
      return;
    }

    const data=state.publicState;
    const temperature=data.temperature||{};
    const gravity=data.gravity||{};
    const control=data.control||{};
    const profile=data.profile||{};
    const network=data.controller?.network||{};
    const firmware=data.controller?.firmware||{};
    const alarm=data.alarm||{};

    const online=data.connectionStatus==='online'||state.availability==='online';

    $('availability').textContent=online?'ONLINE':String(data.connectionStatus||state.availability||'unknown').toUpperCase();
    $('availability').className='status-dot '+(online?'online':'offline');

    $('temperature').textContent=temperature.valid?fmt(temperature.beerC,1,' °C'):'—';
    $('setpoint').textContent='Sollwert '+fmt(temperature.setpointC,1,' °C');
    $('heat').classList.toggle('on',control.heating===true);
    $('cool').classList.toggle('on',control.cooling===true);
    $('output').textContent='Output '+fmt(control.outputPercent,1,' %');
    $('mode').textContent=control.mode??'—';
    $('regulation').textContent=control.regulation??'—';
    $('density').textContent=gravity.valid?fmt(gravity.sg,5):'—';
    $('density-change').textContent=gravity.changePerDayValid?fmt(gravity.changePerDay,5):'—';

    $('profile-name').textContent=profile.name||'Kein Profil gewählt';
    $('profile-step').textContent=`Schritt ${profile.step??0} / ${profile.steps??0}`;
    $('profile-remaining').textContent=duration(profile.remainingSeconds);
    renderProfileProgress(profile);

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

    const library=state.profiles;
    const select=$('profile-select');
    const options=Array.isArray(library?.profiles)?library.profiles:[];
    const currentId=library?.selectedId??profile.id??'';

    select.innerHTML='<option value="">Profil wählen</option>';

    for(const item of options){
      if(item?.id===undefined||item?.id===null)continue;
      const option=document.createElement('option');
      option.value=String(item.id);
      option.textContent=item.name?`${item.name} (ID ${item.id})`:`Profil ID ${item.id}`;
      select.appendChild(option);
    }

    select.value=currentId===undefined||currentId===null?'':String(currentId);

    $('hostname').textContent=network.hostname??'—';
    $('ip').textContent=network.ip??'—';
    $('rssi').textContent=finite(network.rssiDbm)!==null?`${finite(network.rssiDbm)} dBm`:'—';
    $('mac').textContent=network.mac??'—';
    $('firmware').textContent=
      (firmware.version??'—')+
      (finite(firmware.build)!==null?` (${finite(firmware.build)})`:'');
    $('last-message').textContent=state.lastMessageAt?new Date(state.lastMessageAt).toLocaleTimeString():'—';
    $('last-ack').textContent=state.lastCommandResult||'—';

    const tempAlarm=alarm.temperature||{};
    const sensorAlarm=alarm.sensor||{};
    const alarmActive=tempAlarm.active===true||sensorAlarm.active===true||(Array.isArray(data.alarms)&&data.alarms.length>0);

    $('alarm-notice').classList.toggle('hidden',!alarmActive);

    if(alarmActive){
      $('alarm-text').textContent=
        Array.isArray(data.alarms)&&data.alarms.length?
          data.alarms.join(', '):
          sensorAlarm.active?
            `${sensorAlarm.triggerRole||'Sensor'}: ${sensorAlarm.triggerReason||'Fehler'}`:
            'Temperaturalarm aktiv';
    }

    drawChart(state.history);
  }

  function renderOverview() {
    setView(true);
    $('profile-archive-view').classList.add('hidden');
    $('fermenter-overview').classList.remove('hidden');
    $('alarm-notice').classList.add('hidden');
    const live=brokerConnected&&fermentControlOnline;
    const online=devices.filter(device=>device.online).length;
    $('overview-count').textContent=`${devices.length} Fermenter · ${live?online:0} online`;
    $('overview-note').textContent=!live
      ? 'Verbindung unterbrochen – angezeigte Werte sind der letzte bekannte Stand.'
      : hasDeviceRegistry?'Aktuelle Geräteliste aus Fermentor Control.':'Geräteliste aus Profilarchiv; für unabhängige Erkennung Fermentor Control aktualisieren.';
    const grid=$('overview-grid');
    grid.innerHTML='';
    $('overview-empty').classList.toggle('hidden',devices.length>0);
    for(const device of devices){
      const data=states[device.id]?.publicState;
      const card=document.createElement('article');
      card.className='card overview-card';
      const title=document.createElement('h3');title.textContent=device.name;
      const id=document.createElement('p');id.className='hint';id.textContent=device.id;
      const status=document.createElement('span');
      status.className='pill '+(live&&device.online?'good':'bad');
      status.textContent=live?(device.online?'ONLINE':'OFFLINE'):'VERBINDUNG FEHLT';
      card.append(title,id,status);
      const readings=document.createElement('dl');readings.className='details';
      const alarm=data?.alarm;
      const rows=[
        ['Temperatur',data?.temperature?.valid?fmt(data.temperature.beerC,1,' °C'):'—'],
        ['Sollwert',fmt(data?.temperature?.setpointC,1,' °C')],
        ['Dichte',data?.gravity?.valid?fmt(data.gravity.sg,5):'—'],
        ['Modus',data?.control?.mode||'—'],
        ['Profil',data?.profile?.name||'—'],
        ['Alarm',alarm?(alarm.temperature?.active||alarm.sensor?.active?'Aktiv':'Keiner'):'—'],
      ];
      for(const [label,value] of rows){
        const row=document.createElement('div'),dt=document.createElement('dt'),dd=document.createElement('dd');
        dt.textContent=label;dd.textContent=value;row.append(dt,dd);readings.appendChild(row);
      }
      const open=document.createElement('button');open.className='secondary full';open.textContent='Details öffnen';
      open.onclick=()=>{selectedId=device.id;render();};
      card.append(readings,open);grid.appendChild(card);
    }
  }

  function render() {
    renderTabs();
    if(selectedId===OVERVIEW_TAB){renderOverview();return;}

    if(selectedId===ARCHIVE_TAB||(!selectedId&&devices.length===0)){
      selectedId=ARCHIVE_TAB;
      renderTabs();
      renderArchive();
      return;
    }

    renderFermenter();
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
    localStorage.removeItem(LEGACY_DEVICE_CACHE_KEY);
    location.reload();
  };

  $('ack-alarm').onclick=()=>publishExternal('alarm/acknowledge',{});

  $('setpoint-send').onclick=()=>{
    const value=finite($('setpoint-input').value);
    if(value!==null){
      publishExternal('setpoint',{value});
      $('setpoint-input').value='';
    }
  };

  document.querySelectorAll('[data-mode]').forEach(button=>{
    button.onclick=()=>{
      const value=button.dataset.mode==='temp'?'temperature':button.dataset.mode;
      publishExternal('mode',{value});
    };
  });

  $('status-request').onclick=()=>publishExternal('status',{});

  $('profile-select').onchange=event=>{
    if(event.target.value!==''){
      publishExternal('profile/select',{profileId:Number(event.target.value)});
    }
  };

  $('profile-start').onclick=()=>{
    const value=$('profile-select').value;
    publishExternal('profile/start',value!==''?{profileId:Number(value)}:{});
  };

  $('profile-stop').onclick=()=>publishExternal('profile/stop',{});

  render();
  connect();
})();

