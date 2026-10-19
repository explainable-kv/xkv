// Copyright xkv.org 2026
// SPDX-License-Identifier: Apache-2.0
//
// Loads the WebAssembly build in Node and runs the demo's calls once.
//
//   node wasm/smoke.cjs path/to/wasm_exec.js path/to/xkv.wasm
const fs = require('fs');
require(process.argv[2]);
const go = new Go();
const fail = (msg) => { console.error('wasm smoke: ' + msg); process.exit(1); };
globalThis.xkvReady = () => {
  const x = globalThis.xkv;
  const p = (s) => JSON.parse(s);
  const cb = p(x.put('chargeback:cb_991', 'card dispute filed', 'bank notice', 'billing-worker', ''));
  if (!cb.ok || cb.ok.rev !== 1) fail('put: ' + JSON.stringify(cb));
  const risk = p(x.put('risk:42:score', '0.82', 'new chargeback', 'risk-model', '[{"key":"chargeback:cb_991","rev":0}]'));
  if (!risk.ok || risk.ok.causes[0].rev !== 1) fail('put with cause: ' + JSON.stringify(risk));
  const why = p(x.why('risk:42:score', 0));
  if (!why.ok || why.ok.causes[0].revision.key !== 'chargeback:cb_991') fail('why: ' + JSON.stringify(why));
  const strict = p(x.put('a', 'b', '', 'me', ''));
  if (!strict.error) fail('a write without a reason was accepted');
  console.log('wasm smoke passed: xkv ' + x.version);
  process.exit(0);
};
WebAssembly.instantiate(fs.readFileSync(process.argv[3]), go.importObject)
  .then((r) => go.run(r.instance))
  .catch((e) => fail(String(e)));
