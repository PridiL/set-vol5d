/**
 * %Vol5D scan — SET and MAI top 20 (stocks + funds) by today's volume vs the
 * average volume of the last 5 days each name actually traded.
 *
 *   node vol5d_scan.mjs <outDir> [YYYY-MM-DD|today] [morning|afternoon|render]
 *
 *   Session defaults from the Bangkok clock: before 14:30 = morning, else afternoon.
 *   "render" skips the scan and only rebuilds the page from saved session files.
 *
 *   %Vol5D = (today's volume / mean(volume of the previous 5 traded bars) - 1) * 100
 *   Days with no trade are skipped, not counted as 0 (matches the broker screen:
 *   HPT 2026-11-02 = +303.97%, zero-filling gives +2,939%).
 *
 * Universe: TradingView scanner, market "thailand", type stock + fund (closed-end
 * funds and ETFs). MAI = members of index SET:MAI, everything else = SET.
 * Bars: TradingView chart websocket, guest token (SET data 15 min delayed — a
 * 13:00 run sees the whole morning session, a 17:00 run the final volumes).
 * Each run writes vol5d_<date>_<session>.json/.csv; the page vol5d_<date>.html
 * (+ latest.html) shows the Morning and Afternoon sessions of that date.
 *
 * Exit: 0 ok, 3 market closed today (too few names with a bar dated today),
 *       1 failure (scanner down, or too many symbols failed to fetch).
 */
import { writeFileSync, readFileSync, existsSync, mkdirSync } from 'fs';
import { join } from 'path';

const OUT_DIR = process.argv[2] || '.';
const BKK_NOW = new Date(Date.now() + 7 * 3600e3).toISOString();        // run start, Bangkok
const TODAY = !process.argv[3] || process.argv[3] === 'today' ? BKK_NOW.slice(0, 10) : process.argv[3];
const SESSION = process.argv[4] || (BKK_NOW.slice(11, 16) < '14:30' ? 'morning' : 'afternoon');
if (!['morning', 'afternoon', 'render'].includes(SESSION)) { console.error(`bad session ${SESSION}`); process.exit(1); }
const TOP_N = 20, PREV = 5, BARS = 15, PARALLEL = 4, TIMEOUT_MS = 20000;
const MIN_TODAY = 200;      // fewer names with a bar today = holiday / no session
const MAX_FAIL_PCT = 3;     // more fetch failures than this = treat the run as broken

// ---------------------------------------------------------------- universe
async function universe() {
  const res = await fetch('https://scanner.tradingview.com/thailand/scan', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Origin: 'https://www.tradingview.com' },
    body: JSON.stringify({
      filter: [{ left: 'type', operation: 'in_range', right: ['stock', 'fund'] }],
      columns: ['name', 'type', 'subtype', 'indexes'],
      range: [0, 3000],
    }),
  });
  if (!res.ok) throw new Error(`scanner HTTP ${res.status}`);
  const rows = (await res.json()).data || [];
  return rows.map(r => {
    const [name, type, subtype, indexes] = r.d;
    const mai = (indexes || []).some(i => i.proname === 'SET:MAI');
    const kind = type === 'stock' ? 'stock' : subtype === 'etf' ? 'ETF' : 'fund';
    return { sym: r.s, name, mkt: mai ? 'MAI' : 'SET', kind };
  });
}

// ---------------------------------------------------------------- bars
const WS_URL = 'wss://data.tradingview.com/socket.io/websocket?from=chart%2F&type=chart';
const frame = o => { const s = JSON.stringify(o); return `~m~${s.length}~m~${s}`; };
function unframe(data) {
  const out = []; let i = 0;
  while (i < data.length) {
    const m = /^~m~(\d+)~m~/.exec(data.slice(i)); if (!m) break;
    const st = i + m[0].length; out.push(data.slice(st, st + +m[1])); i = st + +m[1];
  }
  return out;
}
const rand = () => Math.random().toString(36).slice(2, 14);

function fetchBars(sym) {   // -> [[t,o,h,l,c,v], ...] oldest first
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(WS_URL, { headers: { Origin: 'https://www.tradingview.com' } });
    const cs = 'cs_' + rand(), bars = new Map();
    const timer = setTimeout(() => { ws.close(); reject(new Error('timeout')); }, TIMEOUT_MS);
    const done = (e, v) => { clearTimeout(timer); try { ws.close(); } catch {} e ? reject(e) : resolve(v); };
    const send = (m, p) => ws.send(frame({ m, p }));
    ws.onerror = () => done(new Error('socket error'));
    ws.onmessage = ev => {
      for (const msg of unframe(String(ev.data))) {
        if (msg.startsWith('~h~')) { ws.send(`~m~${msg.length}~m~${msg}`); continue; }
        let j; try { j = JSON.parse(msg); } catch { continue; }
        if (j.session_id) {
          send('set_auth_token', ['unauthorized_user_token']);
          send('chart_create_session', [cs, '']);
          send('resolve_symbol', [cs, 'sym1', '=' + JSON.stringify({ symbol: sym, adjustment: 'splits' })]);
          send('create_series', [cs, 's1', 's1', 'sym1', '1D', BARS, '']);
          continue;
        }
        if (['symbol_error', 'series_error', 'protocol_error', 'critical_error'].includes(j.m))
          return done(new Error(j.m));
        if (j.m === 'timescale_update' || j.m === 'du')
          for (const b of j.p?.[1]?.s1?.s || []) bars.set(b.v[0], b.v);
        if (j.m === 'series_completed')
          return done(null, [...bars.values()].sort((a, b) => a[0] - b[0]));
      }
    };
  });
}

async function fetchAll(syms, parallel) {
  const got = {}, bad = [];
  let k = 0;
  await Promise.all(Array.from({ length: parallel }, async () => {
    while (k < syms.length) {
      const s = syms[k++];
      try { got[s] = await fetchBars(s); } catch { bad.push(s); }
    }
  }));
  return { got, bad };
}

// ---------------------------------------------------------------- scan
const bkkDate = t => new Date((t + 7 * 3600) * 1000).toISOString().slice(0, 10);
const sessionFile = s => join(OUT_DIR, `vol5d_${TODAY}_${s}.json`);
mkdirSync(OUT_DIR, { recursive: true });

if (SESSION !== 'render') {
  const t0 = Date.now();
  const uni = await universe();
  console.log(`universe ${uni.length}: SET ${uni.filter(u => u.mkt === 'SET').length}, MAI ${uni.filter(u => u.mkt === 'MAI').length}`);

  let { got, bad } = await fetchAll(uni.map(u => u.sym), PARALLEL);
  if (bad.length) {
    const retry = await fetchAll(bad, 2);
    Object.assign(got, retry.got); bad = retry.bad;
  }
  console.log(`bars ${Object.keys(got).length}/${uni.length} in ${((Date.now() - t0) / 1000).toFixed(0)}s` + (bad.length ? `, failed: ${bad.join(' ')}` : ''));
  if (bad.length > uni.length * MAX_FAIL_PCT / 100) { console.log('FAIL: too many fetch failures'); process.exit(1); }

  const rows = [];
  for (const u of uni) {
    const bars = got[u.sym];
    if (!bars?.length || bkkDate(bars.at(-1)[0]) !== TODAY) continue;
    const today = bars.at(-1), prior = bars.slice(0, -1);
    const prev = prior.slice(-PREV);
    if (!prev.length) continue;
    const avg = prev.reduce((s, b) => s + b[5], 0) / prev.length;
    if (!avg) continue;
    const pc = prior.at(-1)[4];
    rows.push({ ...u, last: today[4], chg: today[4] - pc, chgPct: pc ? (today[4] / pc - 1) * 100 : 0,
                vol: today[5], avg5: avg, nPrev: prev.length, pct: (today[5] / avg - 1) * 100 });
  }
  console.log(`names with a bar dated ${TODAY}: ${rows.length}`);
  if (rows.length < MIN_TODAY) { console.log('market closed / no session today'); process.exit(3); }

  const result = { date: TODAY, session: SESSION, built: BKK_NOW.slice(11, 16), tops: {}, counts: {} };
  for (const m of ['SET', 'MAI']) {
    const r = rows.filter(x => x.mkt === m);
    result.tops[m] = r.sort((a, b) => b.pct - a.pct).slice(0, TOP_N);
    result.counts[m] = { up: r.filter(x => x.chg > 0).length, down: r.filter(x => x.chg < 0).length, flat: r.filter(x => x.chg === 0).length };
  }
  writeFileSync(sessionFile(SESSION), JSON.stringify(result));

  const csv = ['market,rank,symbol,type,last,change,pct_change,volume,avg_vol_prev5,prev_bars,pct_vol5d'];
  for (const m of ['SET', 'MAI'])
    result.tops[m].forEach((r, i) => csv.push([m, i + 1, r.name, r.kind, r.last, r.chg.toFixed(2), r.chgPct.toFixed(2),
      r.vol, r.avg5.toFixed(1), r.nPrev, r.pct.toFixed(2)].join(',')));
  writeFileSync(join(OUT_DIR, `vol5d_${TODAY}_${SESSION}.csv`), csv.join('\n') + '\n');
  for (const m of ['SET', 'MAI'])
    console.log(`${SESSION} ${m}: ` + result.tops[m].map(r => `${r.name} ${r.pct.toFixed(2)}`).join(', '));
}

// ---------------------------------------------------------------- page (both sessions of TODAY)
const load = s => existsSync(sessionFile(s)) ? JSON.parse(readFileSync(sessionFile(s), 'utf8')) : null;
const fmtN = (x, d = 0) => x.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
const sgn = (x, d = 2) => (x > 0 ? '+' : '') + fmtN(x, d);
const cls = x => (x > 0 ? 'up' : x < 0 ? 'dn' : 'fl');
const table = (res, m) => {
  const c = res.counts[m];
  return `<section><h3>${m} <span class="cnt">[<b class="up">${c.up}+</b> <b class="dn">${c.down}−</b> <b class="fl">${c.flat}=</b>]</span></h3>
<table><thead><tr><th>#</th><th>Symbol</th><th class="r">Last</th><th class="r">Change</th><th class="r">%Chg</th><th class="r">Volume</th><th class="r">%Vol5D</th></tr></thead><tbody>
${res.tops[m].map((r, i) => `<tr><td class="n">${i + 1}</td><td class="sym">${r.name}${r.kind !== 'stock' ? ` <span class="tag">${r.kind}</span>` : ''}</td>`
  + `<td class="r">${fmtN(r.last, 2)}</td><td class="r ${cls(r.chg)}">${sgn(r.chg)}</td><td class="r ${cls(r.chg)}">${sgn(r.chgPct)}%</td>`
  + `<td class="r">${fmtN(r.vol)}</td><td class="r">${sgn(r.pct)}%</td></tr>`).join('\n')}
</tbody></table></section>`;
};
const SESSIONS = [
  { key: 'morning', title: 'Morning Session', due: '13:00', note: 'volume up to the midday break' },
  { key: 'afternoon', title: 'Afternoon Session', due: '17:00', note: 'full-day volume at the close' },
];
const block = s => {
  const res = load(s.key);
  const dl = res ? `<span class="dl" data-html2canvas-ignore>`
    + `<button type="button" onclick="saveImg(this,'png')">Download PNG</button>`
    + `<button type="button" onclick="saveImg(this,'jpeg')">Download JPEG</button></span>` : '';
  return `<div class="sess" data-name="vol5d_${TODAY}_${s.key}"><h2><span>${s.title} <span class="when">${res ? `${TODAY} · updated ${res.built} Bangkok · ${s.note}` : `not run yet · updates at ${s.due}`}</span></span>${dl}</h2>
${res ? `<div class="grid">${table(res, 'SET')}${table(res, 'MAI')}</div>` : ''}</div>`;
};
const html = `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>%Vol5D ${TODAY}</title><style>
:root{--bg:#fafaf8;--fg:#000;--mut:#6b6a66;--line:#e3e2de;--up:#0a7d32;--dn:#c0262d;--fl:#9a7400;--tag:#ecebe7;--rule:#cfcdc7}
@media (prefers-color-scheme:dark){:root{--bg:#161615;--fg:#eceae4;--mut:#9b9992;--line:#2e2d2a;--up:#3ccf6e;--dn:#ff6b6b;--fl:#e8c547;--tag:#2a2927;--rule:#45433f}}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.4 system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:1100px;margin:0 auto;padding:20px 16px}h1{font-size:20px;margin:0 0 4px}p.sub{color:var(--mut);margin:0 0 8px}
.sess{border-top:2px solid var(--rule);padding-top:14px;margin-top:22px}
h2{font-size:18px;margin:0 0 12px;display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:8px}
.when{font-size:13px;font-weight:400;color:var(--mut);margin-left:6px}
.dl{display:flex;gap:6px}.dl button{font:500 13px system-ui,-apple-system,"Segoe UI",sans-serif;padding:5px 10px;border-radius:6px;
border:1px solid var(--rule);background:var(--tag);color:var(--fg);cursor:pointer}.dl button:hover{border-color:var(--mut)}
.dl button:disabled{opacity:.5;cursor:wait}
.sess.shot{width:1068px;padding:16px;margin-top:0;background:var(--bg)}.sess.shot section{overflow:visible}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(440px,100%),1fr));gap:24px}h3{font-size:16px;margin:0 0 8px}
.cnt{font-weight:400;font-size:14px}table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}
th,td{padding:5px 8px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}th{color:var(--mut);font-weight:500}
.sym{font-weight:700;color:var(--fg)}.r{text-align:right}.n{color:var(--mut)}.up{color:var(--up)}.dn{color:var(--dn)}.fl{color:var(--fl)}
td.r{font-weight:700}td.fl{color:#808080}section{min-width:0;overflow-x:auto}
.tag{font-size:11px;background:var(--tag);color:var(--mut);padding:1px 5px;border-radius:4px}
@media (max-width:520px){.grid{grid-template-columns:1fr}th,td{padding:4px 5px}}
</style></head><body><main>
<h1>%Vol5D — top ${TOP_N} stocks + funds</h1>
<p class="sub">${TODAY} · TradingView daily bars (15-min delayed feed) ·
%Vol5D = today's volume ÷ average of the last 5 traded days − 1</p>
${SESSIONS.map(block).join('\n')}
</main>
<script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
<script>
// Save one session (title + SET and MAI tables) as an image, always at desktop width
// so the two tables sit side by side whatever the window size.
async function saveImg(btn, fmt) {
  if (!window.html2canvas) { alert('Image export needs an internet connection (html2canvas from cdnjs).'); return; }
  const sess = btn.closest('.sess'), bg = getComputedStyle(document.body).backgroundColor;
  btn.disabled = true;
  try {
    const canvas = await html2canvas(sess, {
      scale: 2, backgroundColor: bg, windowWidth: 1200,
      onclone: doc => doc.querySelector('[data-name="' + sess.dataset.name + '"]').classList.add('shot'),
    });
    const a = document.createElement('a');
    a.download = sess.dataset.name + (fmt === 'png' ? '.png' : '.jpg');
    a.href = canvas.toDataURL('image/' + fmt, 0.95);
    document.body.appendChild(a); a.click(); a.remove();
  } catch (e) { alert('Could not create the image: ' + e.message); }
  finally { btn.disabled = false; }
}
</script>
</body></html>`;
writeFileSync(join(OUT_DIR, `vol5d_${TODAY}.html`), html);
writeFileSync(join(OUT_DIR, 'latest.html'), html);
console.log(`wrote vol5d_${TODAY}.html (` + SESSIONS.map(s => `${s.key} ${load(s.key) ? 'yes' : 'no'}`).join(', ') + ')');
