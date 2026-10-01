const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

function app() {
  class Element {
    constructor() { this.children=[]; this.classes=new Set(); this.textContent=''; this.value=''; this.style={};
      this.classList={add:x=>this.classes.add(x),remove:x=>this.classes.delete(x),toggle:(x,on)=>on?this.classes.add(x):this.classes.delete(x)};
    }
    set innerHTML(value) { this.children=[]; }
    append(...children) { this.children.push(...children); }
    appendChild(child) { this.append(child); }
    setAttribute() {}
    querySelector() { return new Element(); }
  }
  const elements=new Map(), storage=new Map();
  const element=id=>{if(!elements.has(id))elements.set(id,new Element());return elements.get(id);};
  const context={console,URL,structuredClone,crypto:{randomUUID:()=> 'test-command'},document:{getElementById:element,querySelectorAll:()=>[],createElement:()=>new Element(),createElementNS:()=>new Element()},
    localStorage:{getItem:key=>storage.get(key)||null,setItem:(key,value)=>storage.set(key,value),removeItem:key=>storage.delete(key)}};
  let source=fs.readFileSync('web/app.js','utf8');
  source=source.replace('  render();\n  connect();\n})();', '  globalThis.api={handleMessage,parseCredentialsDocument,validateBrokerConfig,topicsForDevice,publishExternal,setConfig:value=>{config=validateBrokerConfig(value);},setClient:value=>{client=value;},select:id=>{selectedId=id;},openSettings,snapshot:()=>({devices,states,selectedId}),online:()=>{brokerConnected=true;fermentControlOnline=true;render();}};\n  render();\n})();');
  vm.runInNewContext(source,context);
  return {api:context.api,element,storage};
}

test('authoritative list removes stale devices, preserves history, and restores returning devices',()=>{
  const {api,element,storage}=app();
  const send=(topic,value)=>api.handleMessage('fermentorcontrol/'+topic,JSON.stringify(value));
  const registry=devices=>send('devices',{schemaVersion:1,devices});
  const device={id:'F01',name:'Tank 1',online:true};
  registry([device]);
  send('F01/state',{temperature:{valid:true,beerC:18,setpointC:19},control:{mode:'temperature'}});
  assert.equal(element('overview-grid').children.length,1);
  const history=storage.get('riprapt-remote-history-v1:F01');
  assert.ok(history);
  registry([]);
  assert.equal(element('overview-grid').children.length,0);
  assert.equal(storage.get('riprapt-remote-history-v1:F01'),history);
  send('F01/state',{name:'old retained device'});
  send('profile-archive/state',{targets:[device]});
  assert.equal(api.snapshot().devices.length,0);
  registry([device]);
  assert.equal(api.snapshot().states.F01.history.length,1);
  assert.equal(element('overview-grid').children.length,1);
});

test('configured nested base topic routes registry, subscriptions, state, and commands',()=>{
  const {api,element}=app();
  const base='brewery.v2/fermentercontrol';
  const config=api.parseCredentialsDocument({format:'riprapt-remote-credentials',version:2,
    broker:{host:'example.hivemq.cloud',username:'test',password:'test'},topics:{baseTopic:' /'+base+'/ '}});
  assert.equal(config.topics.baseTopic,base);
  api.setConfig(config);
  const subscribed=[],published=[];
  api.setClient({connected:true,subscribe:t=>subscribed.push(t),unsubscribe:()=>{},publish:(...args)=>published.push(args)});
  api.handleMessage(base+'/devices',JSON.stringify({schemaVersion:1,devices:[{id:'F01',online:true}]}));
  assert.equal(element('overview-grid').children.length,1);
  assert.deepEqual(subscribed,[base+'/F01/state',base+'/F01/availability',base+'/F01/profiles',base+'/F01/command_result']);
  api.handleMessage(base+'/F01/state',JSON.stringify({temperature:{valid:true,beerC:18}}));
  assert.equal(api.snapshot().states.F01.publicState.temperature.beerC,18);
  api.handleMessage('fermentorcontrol/devices',JSON.stringify({schemaVersion:1,devices:[]}));
  assert.equal(api.snapshot().devices.length,1);
  api.select('F01');
  api.publishExternal('setpoint',{value:19});
  assert.equal(published[0][0],base+'/F01/command/setpoint');
  assert.equal(JSON.parse(published[0][1]).value,19);
  api.openSettings();
  assert.equal(element('mqtt-base-topic').value,base);
  assert.ok(element('mqtt-topic-preview').textContent.includes(base+'/devices'));
});

test('legacy credentials keep their topic; configured prefixes are normalized and validated',()=>{
  const {api}=app();
  const broker={host:'example.hivemq.cloud',username:'test',password:'test'};
  assert.equal(api.parseCredentialsDocument({format:'riprapt-remote-credentials',version:2,broker}).topics.baseTopic,'fermentorcontrol');
  assert.equal(api.parseCredentialsDocument({format:'riprapt-remote-credentials',version:1,broker:{url:'wss://example.hivemq.cloud:8884/mqtt',username:'test',password:'test'}}).topics.baseTopic,'fermentorcontrol');
  assert.equal(api.validateBrokerConfig({broker,topics:{baseTopic:' /fermentercontrol/ '}}).topics.baseTopic,'fermentercontrol');
  for(const baseTopic of ['', '/', 'test/+', 'test/#', 'test\u0000', 42]){
    assert.throws(()=>api.validateBrokerConfig({broker,topics:{baseTopic}}));
  }
  assert.throws(()=>api.validateBrokerConfig({broker,topics:[]}));
});

test('archive targets never populate overview; malformed lists keep current devices',()=>{
  const {api,element}=app();
  const device={id:'F02',name:'<img src=x>',online:true};
  api.handleMessage('fermentorcontrol/profile-archive/state',JSON.stringify({targets:[device]}));
  assert.equal(api.snapshot().devices.length,0);
  assert.equal(element('overview-grid').children.length,0);
  assert.match(element('overview-note').textContent,/Warte auf aktuelle Geräteliste/);
  api.handleMessage('fermentorcontrol/devices',JSON.stringify({schemaVersion:1,devices:[device]}));
  assert.equal(element('overview-grid').children[0].children[0].textContent,'<img src=x>');
  api.handleMessage('fermentorcontrol/devices',JSON.stringify({schemaVersion:1,devices:[{id:'bad/ID'}]}));
  assert.equal(api.snapshot().devices[0].id,'F02');
  api.online();
  assert.match(element('overview-count').textContent,/1 online/);
  api.handleMessage('fermentorcontrol/availability','offline');
  assert.match(element('overview-count').textContent,/0 online/);
});

test('removed F99 stays absent despite stale archive targets and individual state',()=>{
  const {api,element}=app();
  const stale={id:'F99',name:'F99',online:true};
  const archive=()=>api.handleMessage('fermentorcontrol/profile-archive/state',JSON.stringify({targets:[stale]}));
  archive();
  assert.equal(element('overview-grid').children.length,0);
  api.handleMessage('fermentorcontrol/devices',JSON.stringify({schemaVersion:1,devices:[{id:'F01',name:'F01',online:true}]}));
  archive();
  api.handleMessage('fermentorcontrol/F99/state',JSON.stringify({name:'F99',connectionStatus:'online'}));
  assert.equal(api.snapshot().devices.length,1);
  assert.equal(api.snapshot().devices[0].id,'F01');
  assert.equal(api.snapshot().states.F99,undefined);
  assert.equal(element('overview-grid').children.length,1);
});
