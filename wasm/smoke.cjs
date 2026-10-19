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
  // Watch: the two revisions so far, then one written after it began.
  const seen = [];
  if (!p(x.watch(0, (r) => seen.push(JSON.parse(r)))).ok) fail('watch');
  setTimeout(() => {
    p(x.put('account:42:limit', '50', 'risk policy v3', 'limits-service', '[{"key":"risk:42:score","rev":0}]'));
    setTimeout(() => {
      const keys = seen.map((r) => r.key).join(',');
      if (keys !== 'chargeback:cb_991,risk:42:score,account:42:limit') fail('watch saw ' + keys);
      if (!p(x.watch(-1, () => {})).error) fail('watch from -1 was accepted');
      // unwatch, or reset, inside the callback stops the deliveries at once.
      let n = 0;
      p(x.watch(0, () => { n++; x.unwatch(); }));
      setTimeout(() => {
        if (n !== 1) fail('calls after unwatch in the callback: ' + (n - 1));
        let q = 0;
        p(x.watch(0, () => { q++; x.reset(); }));
        setTimeout(() => {
          if (q !== 1) fail('calls after reset in the callback: ' + (q - 1));
          if (p(x.stats()).ok.revisions !== 0) fail('reset left revisions');
          console.log('wasm smoke passed: xkv ' + x.version);
          process.exit(0);
        }, 50);
      }, 50);
    }, 50);
  }, 50);
};
WebAssembly.instantiate(fs.readFileSync(process.argv[3]), go.importObject)
  .then((r) => go.run(r.instance))
  .catch((e) => fail(String(e)));
