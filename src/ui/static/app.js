function setOptions(select, items, placeholder, labelMap) {
  const isMultiple = select.multiple;
  const current = isMultiple ? Array.from(select.selectedOptions).map(o => o.value) : select.value;
  const uniq = Array.from(new Set(items)).filter(Boolean);
  const head = isMultiple ? '' : `<option value="">${placeholder}</option>`;
  select.innerHTML = head + uniq.map(v => {
    const label = labelMap && labelMap[v] ? `${labelMap[v]}(${v})` : v;
    return `<option value="${v}">${label}</option>`;
  }).join('');
  if (isMultiple && Array.isArray(current)) {
    current.forEach(val => {
      if (uniq.includes(val)) {
        const opt = Array.from(select.options).find(o => o.value === val);
        if (opt) opt.selected = true;
      }
    });
  } else if (!isMultiple && current && uniq.includes(current)) {
    select.value = current;
  }
}
function formatTime(ts) {
  if (!ts) return '';
  const t = (ts.split('T')[1] || ts.split(' ')[1] || '');
  return t.slice(0,5);
}
function levelArrowCount(level) {
  const m = String(level || '').match(/^Lv(\d)$/);
  if (!m) return 2;
  return Math.max(0, Math.min(5, Number(m[1])));
}
function formatSignalMessage(msg, level) {
  const text = msg || '';
  const parts = text.split('\n');
  if (!parts.length) return '';
  const head = parts[0];
  let headHtml = head;
  const arrowCount = levelArrowCount(level);
  if (head.startsWith('▼')) {
    const label = head.replace(/^▼+\s*/, '');
    const prefix = arrowCount > 0 ? '▼'.repeat(arrowCount) : '•';
    headHtml = `<span class="msg-down">${prefix} ${label}</span>`;
  } else if (head.startsWith('▲')) {
    const label = head.replace(/^▲+\s*/, '');
    const prefix = arrowCount > 0 ? '▲'.repeat(arrowCount) : '•';
    headHtml = `<span class="msg-up">${prefix} ${label}</span>`;
  }
  const tail = parts.slice(1).map(p => p.trim()).filter(Boolean);
  return [headHtml, ...tail].join('<br>');
}
function timeKey(ts) {
  if (!ts) return '';
  const t = (ts.split('T')[1] || ts.split(' ')[1] || '');
  return t.slice(0,5);
}
function levelSize(level) {
  const map = { Lv0: 3, Lv1: 4, Lv2: 5, Lv3: 6, Lv4: 7, Lv5: 8 };
  return map[level] || 4;
}
function triggerLabel(name) {
  const map = {
    open_range_breakout: '开盘区间突破',
    vwap_deviation: 'VWAP偏离',
    squeeze_breakout: '波动率快速放大',
    rsi_overbought: 'RSI超买',
    rsi_oversold: 'RSI超卖',
    break_retest: '突破+回踩确认',
    volume_price_divergence: '量价背离',
    prev_day_break: '昨日高低突破',
  };
  return map[name] || name;
}
function normalizeTf(tf) {
  return String(tf || '').toLowerCase().trim();
}

function resetPages() {
  pageBySymbol.clear();
}

function getSelectedSymbols(symbols) {
  const menu = document.getElementById('symbolMenu');
  const checked = Array.from(menu.querySelectorAll('input[type="checkbox"]:checked'))
    .map(el => el.value)
    .filter(Boolean)
    .slice(0, 2);
  return checked.length ? checked : symbols.slice(0, 2);
}

function buildSymbolMenu(symbols, nameMap) {
  const menu = document.getElementById('symbolMenu');
  const toggle = document.getElementById('symbolToggle');
  const existing = new Set(Array.from(menu.querySelectorAll('input')).filter(i => i.checked).map(i => i.value));
  if (!existing.size && symbols.length) {
    symbols.slice(0, 2).forEach(sym => existing.add(sym));
  }
  menu.innerHTML = symbols.map(sym => {
    const label = nameMap && nameMap[sym] ? `${nameMap[sym]}(${sym})` : sym;
    const checked = existing.has(sym) ? 'checked' : '';
    return `<label class="multi-item"><input type="checkbox" value="${sym}" ${checked}/> ${label}</label>`;
  }).join('');
  const selected = getSelectedSymbols(symbols);
  toggle.textContent = selected.length ? `已选 ${selected.length} 支` : '选择股票';
}

function updateLevelToggle() {
  const lv1m = document.getElementById('filterLevel1m').value;
  const lv5m = document.getElementById('filterLevel5m').value;
  const parts = [];
  if (lv1m) parts.push(`1m ${lv1m}+`);
  if (lv5m) parts.push(`5m ${lv5m}+`);
  document.getElementById('levelToggle').textContent = parts.length ? parts.join(' / ') : '全部等级';
}

// 筛选与计算已迁移到后端

let chartBars = [];
let chartSignals = [];
let chartPoints = [];
let highlightKey = null;
let chartViewCount = 80;
let chartOffset = 0;

function drawChart() {
  const canvas = document.getElementById('chart');
  const ctx = canvas.getContext('2d');
  const rect = canvas.getBoundingClientRect();
  canvas.width = rect.width * devicePixelRatio;
  canvas.height = 320 * devicePixelRatio;
  ctx.scale(devicePixelRatio, devicePixelRatio);

  ctx.clearRect(0,0,rect.width,320);
  ctx.fillStyle = '#0f1217';
  ctx.fillRect(0,0,rect.width,320);

  if (!chartBars.length) {
    ctx.fillStyle = '#94a3b8';
    ctx.fillText('暂无当日K线', 10, 20);
    return;
  }

  const end = Math.max(0, chartBars.length - chartOffset);
  const start = Math.max(0, end - chartViewCount);
  const view = chartBars.slice(start, end);

  const highs = view.map(b => b.high);
  const lows = view.map(b => b.low);
  const maxH = Math.max(...highs);
  const minL = Math.min(...lows);
  const pad = (maxH - minL) * 0.05 || 0.5;
  const top = maxH + pad;
  const bot = minL - pad;

  const w = rect.width;
  const h = 320;
  const barW = Math.max(3, Math.floor(w / view.length));
  const pxPer = (top - bot) === 0 ? 1 : (h - 20) / (top - bot);

  // grid
  ctx.strokeStyle = '#1f2937';
  ctx.lineWidth = 1;
  for (let i=0;i<5;i++) {
    const y = 10 + i * ((h-20)/4);
    ctx.beginPath();
    ctx.moveTo(0,y);
    ctx.lineTo(w,y);
    ctx.stroke();
    const price = (top - (i * ((top - bot)/4))).toFixed(2);
    ctx.fillStyle = '#94a3b8';
    ctx.fillText(price, 4, y - 2);
  }

  const showCurve = document.getElementById('toggleCurve').checked;
  if (showCurve) {
    ctx.strokeStyle = '#e2e8f0';
    ctx.lineWidth = 2;
    ctx.beginPath();
    view.forEach((b, i) => {
      const x = i * barW + barW / 2;
      const y = 10 + (top - b.close) * pxPer;
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.stroke();
  } else {
    view.forEach((b, i) => {
      const x = i * barW + 2;
      const openY = 10 + (top - b.open) * pxPer;
      const closeY = 10 + (top - b.close) * pxPer;
      const highY = 10 + (top - b.high) * pxPer;
      const lowY = 10 + (top - b.low) * pxPer;
      const up = b.close >= b.open;
      ctx.strokeStyle = up ? '#34d399' : '#f87171';
      ctx.fillStyle = up ? '#34d399' : '#f87171';
      // wick
      ctx.beginPath();
      ctx.moveTo(x + barW/2, highY);
      ctx.lineTo(x + barW/2, lowY);
      ctx.stroke();
      // body
      const bodyY = Math.min(openY, closeY);
      const bodyH = Math.max(1, Math.abs(openY - closeY));
      ctx.fillRect(x, bodyY, barW-2, bodyH);
    });
  }

  // VWAP line
  const showVwap = document.getElementById('toggleVwap').checked;
  if (showVwap) {
    ctx.strokeStyle = '#60a5fa';
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    let started = false;
    view.forEach((b, i) => {
      if (b.vwap === null || b.vwap === undefined) return;
      const x = i * barW + barW / 2;
      const y = 10 + (top - b.vwap) * pxPer;
      if (!started) {
        ctx.moveTo(x, y);
        started = true;
      } else {
        ctx.lineTo(x, y);
      }
    });
    ctx.stroke();
  }

  // Signal markers
  const signalMap = new Map();
  chartSignals.forEach(s => {
    const k = timeKey(s.ts);
    if (k) signalMap.set(k, s);
  });
  chartPoints = [];
  view.forEach((b, i) => {
    const key = timeKey(b.ts);
    const sig = signalMap.get(key);
    if (!sig) return;
    const x = i * barW + barW / 2;
    const y = 10 + (top - b.close) * pxPer;
    const isUp = sig.direction === 'up';
    const isDown = sig.direction === 'down';
    const color = isUp ? '#34d399' : (isDown ? '#f87171' : '#94a3b8');
    const size = levelSize(sig.level);
    ctx.fillStyle = color;
    ctx.font = `${size + 6}px ui-monospace, Menlo, monospace`;
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    const glyph = isUp ? '↑' : (isDown ? '↓' : '•');
    if (highlightKey && highlightKey === `${sig.symbol}|${sig._tf || normalizeTf(sig.timeframe)}|${sig.ts}`) {
      ctx.strokeStyle = '#fbbf24';
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.arc(x, y, size + 6, 0, Math.PI * 2);
      ctx.stroke();
    }
    ctx.fillText(glyph, x, y);
    chartPoints.push({ x, y, sig });
  });

  // X-axis time labels
  ctx.fillStyle = '#94a3b8';
  const step = Math.max(1, Math.floor(view.length / 10));
  for (let i=0;i<view.length;i+=step) {
    const b = view[i];
    const label = b.ts.split('T')[1]?.slice(0,5) || '';
    const x = i * barW + 2;
    ctx.fillText(label, x, h - 4);
  }
}

function attachChartInteractions() {
  const canvas = document.getElementById('chart');
  const tooltip = document.getElementById('tooltip');
  let dragging = false;
  let lastX = 0;
  canvas.addEventListener('mousedown', (e) => {
    dragging = true;
    lastX = e.clientX;
  });
  window.addEventListener('mouseup', () => dragging = false);
  window.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    const dx = e.clientX - lastX;
    lastX = e.clientX;
    const bars = chartBars.length;
    const step = Math.max(1, Math.round(Math.abs(dx) / 8));
    if (dx > 0) {
      chartOffset = Math.max(0, chartOffset - step);
    } else if (dx < 0) {
      chartOffset = Math.min(bars, chartOffset + step);
    }
    drawChart();
  });
  canvas.addEventListener('wheel', (e) => {
    e.preventDefault();
    if (e.deltaY < 0) {
      chartViewCount = Math.max(20, chartViewCount - 10);
    } else {
      chartViewCount = Math.min(300, chartViewCount + 10);
    }
    drawChart();
  }, { passive: false });

  canvas.addEventListener('click', (e) => {
    const rect = canvas.getBoundingClientRect();
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    let hit = null;
    let minDist = 999;
    chartPoints.forEach(p => {
      const dx = p.x - x;
      const dy = p.y - y;
      const d = Math.sqrt(dx*dx + dy*dy);
      if (d < 8 && d < minDist) {
        minDist = d;
        hit = p;
      }
    });
    if (hit) {
      const s = hit.sig;
      const name = s.symbol_name ? `${s.symbol_name}(${s.symbol})` : s.symbol;
      tooltip.style.display = 'block';
      tooltip.style.left = `${Math.min(x + 10, rect.width - 200)}px`;
      tooltip.style.top = `${Math.max(10, y - 10)}px`;
      const upList = (s.triggers || []).filter(t => t.direction === 'up').map(t => triggerLabel(t.name));
      const downList = (s.triggers || []).filter(t => t.direction === 'down').map(t => triggerLabel(t.name));
      const neutralList = (s.triggers || []).filter(t => t.direction !== 'up' && t.direction !== 'down').map(t => triggerLabel(t.name));
      const trig = `<br>向上指标: ${upList.length ? upList.join(',') : '-'}`
        + `<br>向下指标: ${downList.length ? downList.join(',') : '-'}`
        + (neutralList.length ? `<br>中性指标: ${neutralList.join(',')}` : '');
      const tfVal = s._tf || normalizeTf(s.timeframe);
      const tfBadge = `<span class="tf-badge tf-${tfVal}">${tfVal}</span>`;
      tooltip.innerHTML = `<strong>${name}</strong> ${tfBadge} ${s.level || ''}<br>${s.rule}<br>${formatSignalMessage(s.message, s.level)}${trig}`;
    } else {
      tooltip.style.display = 'none';
    }
  });
}

async function refreshChart(force = false) {
  const symbol = document.getElementById('chartSymbol').value;
  const tf = normalizeTf(document.getElementById('chartTf').value);
  if (!symbol || !tf) return;
  const dayKey = new Date().toLocaleDateString('sv-SE');
  const cacheKey = `chart:${symbol}:${tf}:${dayKey}`;
  const cached = localStorage.getItem(cacheKey);
  Object.keys(localStorage).forEach(k => {
    if (k.startsWith(`chart:${symbol}:${tf}:`) && k !== cacheKey) localStorage.removeItem(k);
  });
  if (cached && !force) {
    try {
      chartBars = JSON.parse(cached) || [];
    } catch {
      chartBars = [];
    }
  }
  let lastTs = chartBars.length ? chartBars[chartBars.length - 1].ts : '';
  const res = await fetch(`/api/day_bars?symbol=${symbol}&tf=${tf}${lastTs ? `&since=${encodeURIComponent(lastTs)}` : ''}`);
  const newBars = await res.json();
  if (newBars && newBars.length) {
    const existing = new Set(chartBars.map(b => b.ts));
    const merged = chartBars.concat(newBars.filter(b => !existing.has(b.ts)));
    chartBars = merged;
    localStorage.setItem(cacheKey, JSON.stringify(chartBars));
  } else if (!cached) {
    chartBars = [];
  }

  const sigParams = new URLSearchParams({ limit: String(SIGNAL_LIMIT), symbol, tf });
  const sigRes = await fetch(`/api/signals?${sigParams.toString()}`);
  const sigData = await sigRes.json();
  const today = chartBars.length ? (chartBars[0].ts.split('T')[0] || chartBars[0].ts.split(' ')[0]) : dayKey;
  chartSignals = sigData
    .filter(s => s.symbol === symbol && normalizeTf(s.timeframe) === tf && (!today || (s.ts || '').startsWith(today)))
    .map(s => ({ ...s, _tf: normalizeTf(s.timeframe) }));
  const status = document.getElementById('chartStatus');
  status.textContent = `bars: ${chartBars.length} • signals: ${chartSignals.length}`;
  if (newBars && newBars.length || force || !cached) {
    drawChart();
  }
}

const SIGNAL_LIMIT = 400;
const PAGE_SIZE = 20;
const pageBySymbol = new Map();
let lastSymbolsKey = '';
async function refresh() {
  const ft = normalizeTf(document.getElementById('filterTf').value);
  const fl1m = document.getElementById('filterLevel1m').value;
  const fl5m = document.getElementById('filterLevel5m').value;
  const ftext = document.getElementById('filterText').value.trim();

  const warm = await fetch('/api/warmup');
  const warmData = await warm.json();
  const meta = await fetch('/api/meta');
  const metaData = await meta.json();
  const list = document.getElementById('list');
  const status = document.getElementById('status');
  const warmup = document.getElementById('warmup');
  if (warmData && warmData.total !== undefined) {
    warmup.textContent = `warmup: ${warmData.total} bars`;
  } else {
    warmup.textContent = '';
  }

  const symbols = (metaData.symbols || []).length ? metaData.symbols : [];
  const tfsRaw = (metaData.timeframes || []).length ? metaData.timeframes : [];
  const tfs = tfsRaw.map(normalizeTf).filter(Boolean);
  const symbolsKey = JSON.stringify(symbols) + JSON.stringify(tfs);
  if (symbolsKey !== lastSymbolsKey) {
    setOptions(document.getElementById('filterTf'), tfs, '全部周期');
    lastSymbolsKey = symbolsKey;
    buildSymbolMenu(symbols, metaData.symbol_names || {});
  }

  const chosen = getSelectedSymbols(symbols);

  const baseParams = new URLSearchParams({
    limit: String(PAGE_SIZE),
  });
  if (ft) baseParams.set('tf', ft);
  if (fl1m) baseParams.set('level_1m', fl1m);
  if (fl5m) baseParams.set('level_5m', fl5m);
  if (ftext) baseParams.set('text', ftext);

  const results = await Promise.all(chosen.map(sym => {
    const params = new URLSearchParams(baseParams.toString());
    params.set('symbol', sym);
    const page = pageBySymbol.get(sym) || 1;
    params.set('offset', String((page - 1) * PAGE_SIZE));
    return fetch(`/api/signals?${params.toString()}`)
      .then(r => r.json())
      .then(items => ({ symbol: sym, items, page }));
  }));

  const totalCount = results.reduce((sum, r) => sum + r.items.length, 0);
  status.textContent = `signals: ${totalCount}`;

  list.classList.toggle('two-cols', chosen.length > 1);
  list.classList.toggle('one-col', chosen.length <= 1);
  list.innerHTML = results.map(result => {
    const items = result.items.map(item => ({ ...item, _tf: normalizeTf(item.timeframe) }));
    items.sort((a, b) => (b.ts || '').localeCompare(a.ts || ''));
    const title = items[0]?.symbol_name ? `${items[0].symbol_name}(${result.symbol})` : result.symbol;
    const cards = items.map(item => {
      const msg = formatSignalMessage(item.message, item.level);
      const upList = (item.triggers || []).filter(t => t.direction === 'up').map(t => triggerLabel(t.name));
      const downList = (item.triggers || []).filter(t => t.direction === 'down').map(t => triggerLabel(t.name));
      const neutralList = (item.triggers || []).filter(t => t.direction !== 'up' && t.direction !== 'down').map(t => triggerLabel(t.name));
      const extraRules =
        `向上指标: ${upList.length ? upList.join(',') : '-'}`
        + `<br>向下指标: ${downList.length ? downList.join(',') : '-'}`
        + (neutralList.length ? `<br>中性指标: ${neutralList.join(',')}` : '');
      return `
      <div class="card" data-symbol="${item.symbol}" data-tf="${item._tf}" data-ts="${item.ts}">
        <div class="row">
          <div><strong>${item.symbol_name || item.symbol}</strong> <span class="tf-badge tf-${item._tf}">${item._tf}</span></div>
          <div class="level">${item.level || 'Lv?'}</div>
        </div>
        <div class="meta">${formatTime(item.ts)} • ${item.level || 'Lv?'} • ${item.rule}</div>
        <div class="msg">${msg}<br>${extraRules}</div>
      </div>`;
    }).join('');
    const page = result.page || 1;
    const hasMore = result.items.length >= PAGE_SIZE;
    return `
    <div class="col">
      <div class="col-head">
        <div class="meta" style="margin: 6px 0 10px; font-weight:700;">${title}</div>
        <div class="col-pager">
          <button class="page-btn" data-symbol="${result.symbol}" data-dir="-1" ${page <= 1 ? 'disabled' : ''}>上一页</button>
          <span class="meta">第 ${page} 页</span>
          <button class="page-btn" data-symbol="${result.symbol}" data-dir="1" ${hasMore ? '' : 'disabled'}>下一页</button>
        </div>
      </div>
      ${cards || '<div class="meta">暂无信号</div>'}
    </div>`;
  }).join('');

  const chartSymbol = document.getElementById('chartSymbol');
  const chartTf = document.getElementById('chartTf');
  setOptions(chartSymbol, symbols, '选择股票', metaData.symbol_names || {});
  setOptions(chartTf, tfs, '选择周期');
  if (!chartSymbol.value && symbols.length) chartSymbol.value = symbols[0];
  if (!chartTf.value && tfs.length) chartTf.value = tfs[0];
}

refresh();
attachChartInteractions();
document.getElementById('filterTf').addEventListener('change', () => { resetPages(); refresh(); });
document.getElementById('filterText').addEventListener('input', () => { resetPages(); refresh(); });
document.getElementById('refreshBtn').addEventListener('click', () => { refresh(); });
document.getElementById('levelToggle').addEventListener('click', () => {
  const menu = document.getElementById('levelMenu');
  menu.classList.toggle('open');
});
document.getElementById('filterLevel1m').addEventListener('change', () => {
  updateLevelToggle();
  resetPages();
  refresh();
});
document.getElementById('filterLevel5m').addEventListener('change', () => {
  updateLevelToggle();
  resetPages();
  refresh();
});
document.getElementById('symbolToggle').addEventListener('click', () => {
  const menu = document.getElementById('symbolMenu');
  menu.classList.toggle('open');
});
document.getElementById('symbolMenu').addEventListener('change', () => {
  const symbols = (document.getElementById('symbolMenu').querySelectorAll('input') || []);
  const all = Array.from(symbols).map(i => i.value);
  const selected = getSelectedSymbols(all);
  const toggle = document.getElementById('symbolToggle');
  toggle.textContent = selected.length ? `已选 ${selected.length} 支` : '选择股票';
  resetPages();
  refresh();
});
document.addEventListener('click', (e) => {
  const menu = document.getElementById('symbolMenu');
  const toggle = document.getElementById('symbolToggle');
  const levelMenu = document.getElementById('levelMenu');
  const levelToggle = document.getElementById('levelToggle');
  if (!menu.contains(e.target) && !toggle.contains(e.target)) {
    menu.classList.remove('open');
  }
  if (!levelMenu.contains(e.target) && !levelToggle.contains(e.target)) {
    levelMenu.classList.remove('open');
  }
});
document.getElementById('list').addEventListener('click', (e) => {
  const btn = e.target.closest('.page-btn');
  if (btn) {
    const sym = btn.getAttribute('data-symbol');
    const dir = parseInt(btn.getAttribute('data-dir') || '0', 10);
    const curr = pageBySymbol.get(sym) || 1;
    const next = Math.max(1, curr + dir);
    pageBySymbol.set(sym, next);
    refresh();
    return;
  }
});
document.getElementById('chartSymbol').addEventListener('change', () => { chartOffset = 0; refreshChart(true); });
document.getElementById('chartTf').addEventListener('change', () => { chartOffset = 0; refreshChart(true); });
document.getElementById('toggleVwap').addEventListener('change', () => drawChart());
document.getElementById('toggleCurve').addEventListener('change', () => drawChart());
document.getElementById('list').addEventListener('click', (e) => {
  const card = e.target.closest('.card');
  if (!card) return;
  document.querySelectorAll('.card.selected').forEach(el => el.classList.remove('selected'));
  card.classList.add('selected');
  const symbol = card.getAttribute('data-symbol');
  const tf = card.getAttribute('data-tf');
  const ts = card.getAttribute('data-ts');
  highlightKey = `${symbol}|${tf}|${ts}`;
  const chartSymbol = document.getElementById('chartSymbol');
  const chartTf = document.getElementById('chartTf');
  if (chartSymbol.value !== symbol) chartSymbol.value = symbol;
  if (chartTf.value !== tf) chartTf.value = tf;
  refreshChart();
});
setInterval(refreshChart, 60000);
setInterval(refresh, 60000);
