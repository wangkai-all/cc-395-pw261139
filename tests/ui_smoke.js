/* Minimal DOM stub smoke test: executes app.js against a fake DOM + fetch,
   asserting the render paths produce expected markup and actions call the
   right endpoints. Run: node tests/ui_smoke.js
*/
"use strict";

const calls = [];
const routes = {
  "/api/overview": {
    imports: { total: 3, processed: 1, revoked: 1 },
    products: { active: 5, removed: 2, invalidated: 3, externalized: 4 },
    events: 42, pending_deliveries: 1,
    verify: { ok: true, problems: [], ground_truth_total_cents: 23165,
      event_ledger_total_cents: 23165, product_total_cents: 23165 },
  },
  "/api/imports": [
    { id: "imp_x1", name: "batch_1.json", status: "revoked", row_count: 12,
      source_path: "raw/imp_x1__batch_1.json", file_sha256: "abc123def456" },
    { id: "imp_x2", name: "batch_2.json", status: "processed", row_count: 10,
      source_path: "raw/x2", file_sha256: "fed987" },
  ],
  "/api/products": [
    { id: "pa1", type: "aggregate", title: "账户 ACC-100 汇总", account: "ACC-100",
      status: "active", current_amount: 5000, current_qty: 9, externalized: 1 },
    { id: "pd1", type: "detail", title: "明细 UID-00001", account: "ACC-100",
      status: "removed", current_amount: 0, current_qty: 0, externalized: 0 },
  ],
  "/api/contributions": [
    { product_id: "pa1", import_id: "imp_x2", kind: "PARTIAL", active: 1,
      amount: 5000, record_count: 4 },
    { product_id: "pd1", import_id: "imp_x1", kind: "FULL", active: 0,
      amount: 1200, record_count: 1 },
  ],
  "/api/consumers": [
    { id: "con1", name: "在线风控", account_filter: "*", status: "online" },
  ],
  "/api/messages": [
    { id: "msg1", type: "aggregate.corrected", account: "ACC-100", fanout: 2,
      delivered: 2, pending: 0, idempotency_key: "msg:corr:rev1:pa1" },
  ],
  "/api/revocations": [
    { id: "rev1", import_id: "imp_x1", strategy: "compensate", status: "done",
      reason: "坏数据", created_at: "2026-11-17T00:00:00Z" },
  ],
  "/api/oplog": [
    { ts: "2026-11-17T00:00:00.000Z", actor: "api", action: "import.revoked",
      detail_json: "{}" },
  ],
};

function makeEl(tag) {
  const el = {
    tag, children: [], style: {}, dataset: {}, classList: {
      add() {}, remove() {}, toggle() {},
    },
    _text: "", _html: "",
    set textContent(v) { this._text = v; }, get textContent() { return this._text; },
    set innerHTML(v) { this._html = v; }, get innerHTML() { return this._html; },
    set onclick(fn) { this._onclick = fn; }, get onclick() { return this._onclick; },
    addEventListener(ev, fn) { (this._listeners ||= {})[ev] = fn; },
    appendChild(c) { this.children.push(c); },
    remove() {}, click() { this._onclick && this._onclick(); },
    querySelector() { return makeEl("div"); },
    showModal() { this.open = true; }, close() { this.open = false; },
    files: [], value: "",
  };
  return el;
}

const registry = {};
function $(sel) {
  return registry[sel] ||= makeEl(sel);
}
globalThis.document = {
  querySelector: $,
  querySelectorAll: (sel) => {
    if (sel.includes("product-tabs") || sel.includes("audit-tabs")) {
      return [makeEl("button"), makeEl("button")].map((b) => {
        b.addEventListener = () => {}; b.classList = { add() {}, remove() {} };
        return b;
      });
    }
    return [];
  },
  createElement: (t) => makeEl(t),
};
const document = globalThis.document;
globalThis.fetch = async (path, opts = {}) => {
  calls.push({ path: String(path), method: opts.method || "GET" });
  const key = String(path).split("?")[0];
  let body = routes[key];
  if (key.startsWith("/api/imports/") && key.endsWith("/revocation-preview")) {    body = { import: { name: "batch_1.json" }, affected_count: 3,
      full_dependent: [{ title: "明细", type: "detail", account: "ACC-100",
        kind: "FULL", externalized: false, contrib_amount: 100 }],
      partial_dependent: [{ title: "汇总", type: "aggregate", account: "ACC-100",
        kind: "PARTIAL", externalized: true, contrib_amount: 200 }],
      externalized: [{}] };
  }
  if (key.startsWith("/api/messages/") && key.endsWith("/redrive")) {
    body = { message_id: key.split("/")[3], results: { con1: { delivered: [], duplicates: ["msg1"] } } };
  }
  if (body === undefined) body = [{ seq: 1, event_type: "ADJUSTMENT",
    account: "ACC-100", amount_delta: -100, qty_delta: -1,
    idempotency_key: "k1" }];
  return { ok: true, status: 200, json: async () => body };
};
globalThis.setInterval = () => {};
globalThis.window = globalThis;

const fs = require("fs");
const path = require("path");
const src = fs.readFileSync(path.join(__dirname, "..", "app", "static", "app.js"), "utf8");
eval(src.replace('"use strict";', ""));

(async () => {
  await new Promise((r) => setTimeout(r, 30));
  const stats = registry["#stats"].innerHTML;
  const impHtml = registry["#imports-table tbody"].innerHTML;
  const prodHtml = registry["#products-table tbody"].innerHTML;
  const msgHtml = registry["#messages-table tbody"].innerHTML;
  const asserts = [
    ["stats render import count", stats.includes(">3<")],
    ["stats render revoked count", stats.includes(">1<")],
    ["imports show revoked badge", impHtml.includes("b-revoked")],
    ["products show PARTIAL badge", prodHtml.includes("b-PARTIAL")],
    ["products show externalized badge", prodHtml.includes("已外发")],
    ["messages render redrive button", msgHtml.includes("redrive")],
    ["verify box rendered", registry["#verify-box"].innerHTML.includes("校验通过")],
  ];
  let fail = 0;
  for (const [name, ok] of asserts) {
    console.log(`${ok ? "PASS" : "FAIL"}  ${name}`);
    if (!ok) fail++;
  }
  // action wiring: redrive posts to the right endpoint
  await window.redrive("msg1");
  const redriveCall = calls.find((c) => c.path.includes("/redrive"));
  console.log(redriveCall && redriveCall.method === "POST"
    ? "PASS  redrive action posts to redrive endpoint" : "FAIL  redrive action");
  if (!redriveCall || redriveCall.method !== "POST") fail++;
  process.exit(fail ? 1 : 0);
})();
