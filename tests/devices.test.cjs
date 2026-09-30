const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

function app() {
  class Element {
    constructor() { this.children=[]; this.classes=new Set(); this.textContent=''; this.value='';
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
  const context={console,URL,structuredClone,document:{getElementById:element,querySelectorAll:()=>[],createElement:()=>new Element(),createElementNS:()=>new Element()},
    localStorage:{getItem:key=>storage.get(key)||null,setItem:(key,value)=>storage.set(key,value),removeItem:key=>storage.delete(key)}};
  let source=fs.readFileSync('web/app.js','utf8');
  source=source.replace('  render();\n  connect();\n})();', '  globalThis.api={handleMessage,snapshot:()=>({devices,states,selectedId}),online:()=>{brokerConnected=true;fermentControlOnline=true;render();}};\n  render();\n})();');
  vm.runInNewContext(source,context);
  return {api:context.api,element,storage};
}

test('authoritative list removes stale devices, preserves history, and restores returning devices',()=>{
  const {api,element,storage}=app();
  const send=(topic,value)=>api.handleMessage('fermentercontrol/'+topic,JSON.stringify(value));
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

test('archive fallback works until valid registry arrives; malformed lists keep current devices',()=>{
  const {api,element}=app();
  const device={id:'F02',name:'<img src=x>',online:true};
  api.handleMessage('fermentercontrol/profile-archive/state',JSON.stringify({targets:[device]}));
  assert.equal(api.snapshot().devices.length,1);
  api.handleMessage('fermentercontrol/devices',JSON.stringify({schemaVersion:1,devices:[device]}));
  assert.equal(element('overview-grid').children[0].children[0].textContent,'<img src=x>');
  api.handleMessage('fermentercontrol/devices',JSON.stringify({schemaVersion:1,devices:[{id:'bad/ID'}]}));
  assert.equal(api.snapshot().devices[0].id,'F02');
  api.online();
  assert.match(element('overview-count').textContent,/1 online/);
  api.handleMessage('fermentercontrol/availability','offline');
  assert.match(element('overview-count').textContent,/0 online/);
});
