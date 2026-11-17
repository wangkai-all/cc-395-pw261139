/* Pointwise revocation console — vanilla JS SPA, no build step. */
"use strict";

const $ = (sel) => document.querySelector(sel);
const api = async (path, opts = {}) => {
  const res = await fetch(path, {
    headers: opts.body && !opts.rawBody
      ? { "Content-Type": "application/json" } : {},
    ...opts,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
};
const esc = (s) => String(s ?? "").replace(/[&<>"]/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const money = (cents) =>
  (cents / 100).toLocaleString("zh-CN", { style: "currency", currency: "CNY" }) +
  ` <span class="muted">(${cents} 分)</span>`;

let state = { productsFilter: "all", auditTab: "revoc", current: {} };

function toast(msg, kind = "info") {
  const el = document.createElement("div");
  el.className = `toast ${kind}`;
  el.textContent = msg;
  $("#toasts").appendChild(el);
  setTimeout(() => el.remove(), 4200);
}

function badge(text, cls) {
  return `<span class="badge b-${cls}">${esc(text)}</span>`;
}

// ------------------------------------------------------------- rendering
async function refresh() {
  try {
    const [overview, imports, products, contributions, consumers, messages,
      revocations] = await Promise.all([
      api("/api/overview"), api("/api/imports"), api("/api/products"),
      api("/api/contributions"), api("/api/consumers"), api("/api/messages"),
      api("/api/revocations"),
    ]);
    state.current = { overview, imports, products, contributions, consumers, messages, revocations };
    renderStats(overview);
    renderImports(imports);
    renderProducts(products, contributions);
    renderConsumers(consumers);
    renderMessages(messages);
    renderAudit();
  } catch (e) {
    toast(`刷新失败：${e.message}`, "err");
  }
}

function renderStats(o) {
  const v = o.verify;
  const stats = [
    ["导入总数", o.imports.total], ["已撤销", o.imports.revoked],
    ["活跃产物", o.products.active], ["已外发", o.products.externalized],
    ["已失效", o.products.invalidated], ["事件数", o.events],
    ["挂起投递", o.pending_deliveries],
  ];
  $("#stats").innerHTML = stats.map(([l, val]) =>
    `<div class="stat"><div class="v">${val}</div><div class="l">${l}</div></div>`).join("");
  $("#verify-box").innerHTML = `
    <div class="guarantee ${v.ok ? "good" : "bad"}">
      <strong>${v.ok ? "✓ 校验通过" : "✗ 校验失败"}</strong> —
      ground truth 总额 <span class="mono">${v.ground_truth_total_cents}</span> 分 ·
      事件账本汇总 <span class="mono">${v.event_ledger_total_cents}</span> 分 ·
      产物汇总 <span class="mono">${v.product_total_cents}</span> 分，三者一致
      ${v.problems.length ? `<br>问题：${esc(v.problems.join("; "))}` : ""}
    </div>`;
}

function renderImports(imports) {
  $("#imports-table tbody").innerHTML = imports.map((im) => `
    <tr>
      <td><div><strong>${esc(im.name)}</strong></div>
          <div class="muted mono" style="font-size:11px">${esc(im.id)}</div>
          <div class="muted" style="font-size:11px">${esc(im.source_path)}</div></td>
      <td>${badge(im.status, im.status)}</td>
      <td class="num">${im.row_count}</td>
      <td class="mono" style="font-size:10.5px">${esc(im.file_sha256.slice(0, 16))}…</td>
      <td><div class="row-actions">
        ${im.status === "registered"
          ? `<button onclick="processImport('${im.id}')">加工</button>` : ""}
        ${["processed", "registered"].includes(im.status)
          ? `<button class="warn" onclick="openRevoke('${im.id}')">撤销</button>` : ""}
        <button class="ghost" onclick='showImportDetail(${JSON.stringify(im.id)})'>证据</button>
      </div></td>
    </tr>`).join("") || `<tr><td colspan="5" class="muted">暂无导入</td></tr>`;
}

function renderProducts(products, contributions) {
  const byProduct = {};
  for (const c of contributions) {
    (byProduct[c.product_id] ||= []).push(c);
  }
  const rows = products
    .filter((p) => state.productsFilter === "all" || p.type === state.productsFilter)
    .map((p) => {
      const links = (byProduct[p.id] || []).filter((c) => c.active);
      const contribHtml = links.length
        ? links.map((c) =>
            `<div class="deppair">${badge(c.kind, c.kind)}
               <span class="mono muted">${esc(c.import_id.slice(4, 18))}…</span>
               <span class="num mono">${c.amount} 分 / ${c.record_count} 行</span></div>`).join("")
        : `<span class="muted">无活跃贡献</span>`;
      return `<tr>
        <td><strong>${esc(p.title)}</strong><div class="muted mono" style="font-size:10.5px">${esc(p.id)}</div></td>
        <td>${esc(p.type)}</td>
        <td>${esc(p.account)}</td>
        <td>${badge(p.status, p.status)}</td>
        <td class="num">${p.current_amount}</td>
        <td class="num">${p.current_qty}</td>
        <td>${p.externalized ? badge("已外发", "invalidated") : '<span class="muted">内部</span>'}</td>
        <td>${contribHtml}</td>
      </tr>`;
    }).join("");
  $("#products-table tbody").innerHTML =
    rows || `<tr><td colspan="8" class="muted">暂无产物</td></tr>`;
}

function renderConsumers(consumers) {
  $("#consumers").innerHTML = consumers.map((c) => `
    <div style="border:1px solid var(--border);border-radius:8px;padding:10px 12px;margin-bottom:8px;background:var(--panel-2)">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap">
        <div><strong>${esc(c.name)}</strong>
          <span class="muted mono" style="font-size:11px">${esc(c.id)}</span>
          <div class="muted" style="font-size:11.5px">过滤：${esc(c.account_filter)}</div></div>
        <div class="row-actions">
          ${badge(c.status, c.status)}
          ${c.status === "online"
            ? `<button class="ghost" onclick="setOffline('${c.id}')">置为离线</button>`
            : `<button onclick="setOnline('${c.id}')">上线并补发</button>`}
          <button class="ghost" onclick="deliver('${c.id}')">投递挂起</button>
          <button class="ghost" onclick="inbox('${c.id}')">收件箱</button>
        </div>
      </div>
    </div>`).join("") || `<p class="muted">暂无订阅方</p>`;
}

function renderMessages(messages) {
  $("#messages-table tbody").innerHTML = messages.map((m) => `
    <tr>
      <td>${esc(m.type)}</td>
      <td>${esc(m.account)}</td>
      <td class="num">${m.fanout}</td>
      <td class="num"><span class="pill-ok">${m.delivered}</span></td>
      <td class="num">${m.pending ? `<span class="pill-bad">${m.pending}</span>` : 0}</td>
      <td class="mono" style="font-size:10.5px;max-width:180px;overflow:hidden;text-overflow:ellipsis">${esc(m.idempotency_key)}</td>
      <td><div class="row-actions">
        <button class="ghost" onclick='showMsgDetail(${JSON.stringify(m.id)})'>内容</button>
        <button class="warn" onclick="redrive('${m.id}')">重复发送</button>
      </div></td>
    </tr>`).join("") || `<tr><td colspan="7" class="muted">暂无消息</td></tr>`;
}

async function renderAudit() {
  const body = $("#audit-body");
  if (state.auditTab === "revoc") {
    body.innerHTML = state.current.revocations.length
      ? state.current.revocations.map((r) => `
        <div style="border:1px solid var(--border);border-radius:8px;padding:10px 12px;margin-bottom:8px;background:var(--panel-2)">
          <div style="display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap">
            <strong>${esc(r.strategy === "compensate" ? "精确补偿" : "重建")} · ${esc(r.import_id)}</strong>
            ${badge(r.status, r.status === "done" ? "processed" : "revoked")}
          </div>
          <div class="muted" style="font-size:11.5px;margin:4px 0">${esc(r.reason)} · ${esc(r.created_at)}</div>
          <button class="ghost" onclick='showRevocReport(${JSON.stringify(r.id)})'>查看撤销报告（证据）</button>
        </div>`).join("")
      : `<p class="muted">暂无撤销记录</p>`;
  } else if (state.auditTab === "oplog") {
    const rows = await api("/api/oplog");
    body.innerHTML = `<table><thead><tr><th>时间</th><th>执行者</th><th>动作</th><th>详情</th></tr></thead>
      <tbody>${rows.map((o) => `<tr>
        <td class="mono" style="font-size:10.5px">${esc(o.ts.slice(11, 23))}</td>
        <td>${esc(o.actor)}</td><td>${esc(o.action)}</td>
        <td class="mono" style="font-size:10.5px;max-width:380px;overflow:hidden;text-overflow:ellipsis">${esc(o.detail_json)}</td>
      </tr>`).join("")}</tbody></table>`;
  } else {
    const rows = await api("/api/events?limit=80");
    body.innerHTML = `<table><thead><tr><th>#</th><th>事件类型</th><th>账户</th>
      <th class="num">Δ金额</th><th class="num">Δ数量</th><th>幂等键</th></tr></thead>
      <tbody>${rows.map((e) => `<tr>
        <td class="num mono">${e.seq}</td><td>${esc(e.event_type)}</td><td>${esc(e.account)}</td>
        <td class="num mono">${e.amount_delta}</td><td class="num mono">${e.qty_delta}</td>
        <td class="mono" style="font-size:10px;max-width:260px;overflow:hidden;text-overflow:ellipsis">${esc(e.idempotency_key)}</td>
      </tr>`).join("")}</tbody></table>`;
  }
}

// ------------------------------------------------------------- actions
window.processImport = async (id) => {
  try {
    const job = await api(`/api/imports/${id}/process`, {
      method: "POST", body: JSON.stringify({ chunk_size: 5 }),
    });
    toast(`加工完成：${job.processed_rows} 行，作业 ${job.id}`, "ok");
    refresh();
  } catch (e) { toast(e.message, "err"); }
};

window.openRevoke = async (id) => {
  const preview = await api(`/api/imports/${id}/revocation-preview`);
  state.revokeTarget = id;
  $("#revoke-title").textContent = `撤销导入：${preview.import.name}`;
  const row = (x) => `<li>${esc(x.title)} <span class="muted">(${x.type} / ${x.account})</span>
    ${badge(x.kind, x.kind)}
    ${x.externalized ? badge("已外发→仅能通知失效", "invalidated") : badge("内部→可保证修正", "active")}
    <span class="mono">贡献 ${x.contrib_amount} 分</span></li>`;
  $("#revoke-preview").innerHTML = `
    <p>受影响产物 <strong>${preview.affected_count}</strong> 个：</p>
    <strong>完全依赖 FULL（${preview.full_dependent.length}）</strong>
    <ul style="margin:4px 0 10px">${preview.full_dependent.map(row).join("") || "<li class='muted'>无</li>"}</ul>
    <strong>部分依赖 PARTIAL（${preview.partial_dependent.length}，已与其他批次合并）</strong>
    <ul style="margin:4px 0 10px">${preview.partial_dependent.map(row).join("") || "<li class='muted'>无</li>"}</ul>
    <div class="guarantee ${preview.externalized.length ? "bad" : "good"}">
      ${preview.externalized.length
        ? `⚠ ${preview.externalized.length} 个产物已外发：物理撤回不可能，系统将下发失效/更正通知，是否采纳以消费者 ack 为准。`
        : "当前无外发产物：撤销后内部余额可保证与原始记录完全一致。"}
    </div>`;
  updateStrategyNote();
  $("#revoke-dlg").showModal();
};

function updateStrategyNote() {
  const s = $("#revoke-strategy").value;
  $("#strategy-note").innerHTML = s === "compensate"
    ? "精确补偿保证：按贡献图生成等额反向 ADJUSTMENT 事件，未外发产物余额严格正确；已外发结果只发失效通知，无法保证对方删除。"
    : "重建保证：丢弃受影响产物的现有值，从所有未撤销原始记录重算，可自愈历史漂移；成本为全量扫描。已外发结果同样只能通知失效。";
}
$("#revoke-strategy").addEventListener("change", updateStrategyNote);

$("#revoke-confirm").onclick = async () => {
  try {
    const report = await api(`/api/imports/${state.revokeTarget}/revoke`, {
      method: "POST",
      body: JSON.stringify({
        strategy: $("#revoke-strategy").value,
        reason: $("#revoke-reason").value,
      }),
    });
    $("#revoke-dlg").close();
    toast(`撤销完成：失效 ${report.invalidated.length}，调整 ${report.adjusted.length}，重建 ${report.rebuilt.length}`, "ok");
    refresh();
  } catch (e) { toast(e.message, "err"); }
};
$("#revoke-cancel").onclick = () => $("#revoke-dlg").close();
$("#revoke-close").onclick = () => $("#revoke-dlg").close();

window.setOffline = async (id) => {
  await api(`/api/consumers/${id}/offline`, { method: "POST" });
  toast("订阅方已离线，新消息将挂起", "info"); refresh();
};
window.setOnline = async (id) => {
  const r = await api(`/api/consumers/${id}/online`, { method: "POST" });
  toast(`上线补发 ${r.backlog_replay.delivered.length} 条挂起消息`, "ok"); refresh();
};
window.deliver = async (id) => {
  const r = await api(`/api/consumers/${id}/deliver`, { method: "POST" });
  toast(`投递 ${r.delivered.length} 条，重复拦截 ${r.duplicates.length} 条`, "ok"); refresh();
};
window.inbox = async (id) => {
  const view = await api(`/api/consumers/${id}`);
  openDetail(`订阅方收件箱：${view.consumer.name}`, view.messages.map((m) => ({
    type: m.type, status: m.delivery_status, attempts: m.attempts,
    acked: !!m.acked, payload: JSON.parse(m.payload_json),
  })));
};
window.redrive = async (id) => {
  const r = await api(`/api/messages/${id}/redrive`, { method: "POST" });
  const dups = Object.values(r.results).reduce((n, x) => n + x.duplicates.length, 0);
  toast(`重发完成：消费者侧重复 ack ${dups} 条，效果不会被应用第二次`, "ok"); refresh();
};
window.showMsgDetail = async (id) => {
  const m = state.current.messages.find((x) => x.id === id);
  openDetail(`消息 ${m.type}`, { ...m, payload: JSON.parse(m.payload_json) });
};
window.showImportDetail = async (id) => {
  const im = state.current.imports.find((x) => x.id === id);
  openDetail(`导入证据：${im.name}`, im);
};
window.showRevocReport = async (id) => {
  const r = state.current.revocations.find((x) => x.id === id);
  openDetail(`撤销报告 ${r.id}`, { ...r, report: JSON.parse(r.report_json) });
};

function openDetail(title, obj) {
  $("#detail-title").textContent = title;
  $("#detail-json").textContent = JSON.stringify(obj, null, 2);
  $("#detail-dlg").showModal();
}
$("#detail-close").onclick = () => $("#detail-dlg").close();

// ------------------------------------------------------------- forms
$("#btn-register").onclick = async () => {
  const file = $("#file-input").files[0];
  const sample = $("#sample-pick").value;
  try {
    if (file) {
      const buf = await file.arrayBuffer();
      const im = await api("/api/imports", {
        method: "POST", rawBody: true,
        headers: { "Content-Type": "application/octet-stream", "X-Import-Name": file.name },
        body: buf,
      });
      toast(`已登记 ${im.name}（${im.row_count} 行），原件已入证据库`, "ok");
    } else if (sample) {
      const paths = await api("/api/samples");
      const im = await api("/api/imports", {
        method: "POST", body: JSON.stringify({ name: sample }),
      });
      toast(`已登记内置样例 ${im.name}`, "ok");
    } else {
      toast("请先选择文件或内置样例", "err"); return;
    }
    refresh();
  } catch (e) { toast(e.message, "err"); }
};

$("#btn-add-consumer").onclick = async () => {
  try {
    const c = await api("/api/consumers", {
      method: "POST", body: JSON.stringify({
        name: $("#c-name").value || "未命名订阅方",
        account_filter: $("#c-filter").value || "*",
        status: $("#c-status").value,
      }),
    });
    toast(`订阅方 ${c.name} 已创建（${c.status}）`, "ok");
    $("#c-name").value = ""; refresh();
  } catch (e) { toast(e.message, "err"); }
};

$("#btn-demo").onclick = async () => {
  toast("正在运行完整演示（含并发、重复、离线场景）…", "info");
  try {
    const r = await api("/api/demo/run", {
      method: "POST", body: JSON.stringify({ scenario: "all" }),
    });
    const lines = Object.entries(r).filter(([k]) => !k.startsWith("_"))
      .map(([k, v]) => `${k}: verify=${v.final_verify?.ok}`).join("; ");
    toast(`演示完成 → ${lines}`, "ok");
    refresh();
  } catch (e) { toast(e.message, "err"); }
};
$("#btn-refresh").onclick = refresh;

document.querySelectorAll("#product-tabs button").forEach((b) =>
  b.addEventListener("click", () => {
    document.querySelectorAll("#product-tabs button").forEach((x) => x.classList.remove("active"));
    b.classList.add("active");
    state.productsFilter = b.dataset.t;
    renderProducts(state.current.products, state.current.contributions);
  }));
document.querySelectorAll("#audit-tabs button").forEach((b) =>
  b.addEventListener("click", () => {
    document.querySelectorAll("#audit-tabs button").forEach((x) => x.classList.remove("active"));
    b.classList.add("active");
    state.auditTab = b.dataset.t;
    renderAudit();
  }));

refresh();
setInterval(refresh, 8000);
