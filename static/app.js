// ═══════════════════════════════════════════
//  Polymarket BTC Trader — Client App
// ═══════════════════════════════════════════

const socket = io();

// DOM Elements
const statusBadge = document.getElementById('status-badge');
const statusText = document.getElementById('status-text');
const eventName = document.getElementById('event-name');
const eventTimer = document.getElementById('event-timer');
const priceUp = document.getElementById('price-up');
const priceDown = document.getElementById('price-down');
const btnUp = document.getElementById('btn-up');
const btnDown = document.getElementById('btn-down');
const ordersBody = document.getElementById('orders-body');
const ordersCount = document.getElementById('orders-count');
const ordersEmpty = document.getElementById('orders-empty');
const logContent = document.getElementById('log-content');
const cfgBuyOffset = document.getElementById('cfg-buy-offset');
const cfgSellOffset = document.getElementById('cfg-sell-offset');
const balanceValue = document.getElementById('balance-value');
const balancePnlDay = document.getElementById('balance-pnl-day');
const orderSizeInput = document.getElementById('order-size-input');
const buyOffsetInput = document.getElementById('buy-offset-input');
const sellOffsetInput = document.getElementById('sell-offset-input');
const navTrade = document.getElementById('nav-trade');
const navGrafic = document.getElementById('nav-grafic');
const viewTrade = document.getElementById('view-trade');
const viewGrafic = document.getElementById('view-grafic');
const graficLegend = document.getElementById('grafic-legend');

// State
let windowEnd = 0;
let timerInterval = null;
let currentBalance = null;

// Grafic: Chart.js, ~7.5 мин истории при опросе 1 Гц
const GRAFIC_MAX_POINTS = 450;
let graficChart = null;
let lastGraficWindowStart = 0;
let historicalPatterns = [];
let signalPatterns = [];

// Подгрузка паттернов из API (вызовем при старте или смене таба)
async function loadHistoricalPatterns() {
    try {
        const res = await fetch('/api/patterns');
        historicalPatterns = await res.json();
    } catch(e) {
        console.error("Pattern load error", e);
    }
    try {
        const res2 = await fetch('/api/signals');
        signalPatterns = await res2.json();
    } catch(e) {
        console.error("Signal load error", e);
    }
}
loadHistoricalPatterns();


// ═══════════════════════════════════════════
//  SOCKET EVENTS
// ═══════════════════════════════════════════

socket.on('connect', () => {
    statusBadge.className = 'status-badge connected';
    statusText.textContent = 'Подключено';
    addLog('info', '🔗', 'WebSocket подключение установлено');
});

socket.on('disconnect', () => {
    statusBadge.className = 'status-badge disconnected';
    statusText.textContent = 'Отключено';
    btnUp.disabled = true;
    btnDown.disabled = true;
    addLog('error', '❌', 'WebSocket отключен');
});

// Balance update
socket.on('balance', (data) => {
    if (data.balance !== null && data.balance !== undefined) {
        currentBalance = parseFloat(data.balance);
        balanceValue.textContent = '$' + currentBalance.toFixed(2);
        // Animate balance update
        balanceValue.classList.remove('balance-flash');
        void balanceValue.offsetWidth;
        balanceValue.classList.add('balance-flash');
    }
    if (balancePnlDay) {
        const pnl = data.pnl_day;
        if (pnl === null || pnl === undefined || Number.isNaN(parseFloat(pnl))) {
            balancePnlDay.textContent = 'PnL за сутки: --';
            balancePnlDay.className = 'balance-pnl neutral';
        } else {
            const v = parseFloat(pnl);
            const sign = v > 0 ? '+' : '';
            balancePnlDay.textContent = `PnL за сутки: ${sign}$${v.toFixed(2)}`;
            balancePnlDay.className = v > 0 ? 'balance-pnl positive' : v < 0 ? 'balance-pnl negative' : 'balance-pnl neutral';
        }
    }
});

// Config received from server
socket.on('config', (data) => {
    orderSizeInput.value = data.order_size;
    if (data.min_trade_offset != null && buyOffsetInput) {
        buyOffsetInput.min = data.min_trade_offset;
        sellOffsetInput.min = data.min_trade_offset;
    }
    if (data.max_trade_offset != null && buyOffsetInput) {
        buyOffsetInput.max = data.max_trade_offset;
        sellOffsetInput.max = data.max_trade_offset;
    }
    if (buyOffsetInput && data.buy_offset != null) {
        buyOffsetInput.value = data.buy_offset;
    }
    if (sellOffsetInput && data.sell_offset != null) {
        sellOffsetInput.value = data.sell_offset;
    }
    cfgBuyOffset.textContent = Math.round(data.buy_offset * 100) + '¢';
    cfgSellOffset.textContent = '+' + Math.round(data.sell_offset * 100) + '¢';
});

// Price update (every 1 second)
socket.on('prices', (data) => {
    if (data.error) {
        eventName.textContent = data.error;
        priceUp.textContent = '--¢';
        priceDown.textContent = '--¢';
        btnUp.disabled = true;
        btnDown.disabled = true;
        return;
    }

    // Update event name
    eventName.textContent = data.event_name || 'BTC Up/Down 5m';

    // Update timer
    if (data.window_end) {
        windowEnd = data.window_end;
        startTimer();
    }

    // Update UP price
    const upPrice = data.up_price;
    const downPrice = data.down_price;

    if (upPrice !== null && upPrice !== undefined) {
        const upText = Math.round(upPrice * 100) + '¢';
        if (priceUp.textContent !== upText) {
            priceUp.textContent = upText;
            flashElement(priceUp);
        }
        btnUp.disabled = false;
    }

    if (downPrice !== null && downPrice !== undefined) {
        const downText = Math.round(downPrice * 100) + '¢';
        if (priceDown.textContent !== downText) {
            priceDown.textContent = downText;
            flashElement(priceDown);
        }
        btnDown.disabled = false;
    }

    appendGraficSample(data);
});

// Log message from server
socket.on('log', (data) => {
    addLog(data.level || 'info', data.icon || 'ℹ️', data.message);
});

// Orders update
socket.on('orders', (data) => {
    renderOrders(data.orders || []);
});

// Trade placed confirmation
socket.on('trade_result', (data) => {
    if (data.success) {
        addLog('success', '✅', data.message);
    } else {
        addLog('error', '❌', data.message);
    }
    // Re-enable buttons
    btnUp.disabled = false;
    btnDown.disabled = false;
});

let lastSignaledMatch = null;
socket.on('pattern_match', (data) => {
    const sigKey = data.match_id + '_' + data.side;
    if (lastSignaledMatch === sigKey) return; // Не флудим одним и тем же 
    lastSignaledMatch = sigKey;
    
    addLog('profit', '🔥', `ПАТТЕРН-СИГНАЛ: Текущий график ${data.confidence}% совпадает с успешной сделкой ${data.side} (прирост: +$${data.profit.toFixed(2)})!`);
    
    // Вспышка на кнопках
    if (data.side === 'UP') {
        btnUp.style.boxShadow = "0 0 20px #22c55e";
        setTimeout(() => btnUp.style.boxShadow = "none", 3000);
    } else {
        btnDown.style.boxShadow = "0 0 20px #ef4444";
        setTimeout(() => btnDown.style.boxShadow = "none", 3000);
    }
});

let aiRecTimeout = null;
socket.on('ai_recommendation', (data) => {
    const aiPanel = document.getElementById('ai-recommender');
    const aiText = document.getElementById('ai-recommender-text');
    
    if (aiPanel && aiText) {
        aiPanel.style.display = 'block';
        if (data.side === 'UP') {
            aiPanel.style.backgroundColor = 'rgba(34, 197, 94, 0.15)';
            aiPanel.style.border = '1px solid rgba(34, 197, 94, 0.4)';
            aiPanel.style.color = '#4ade80';
        } else {
            aiPanel.style.backgroundColor = 'rgba(239, 68, 68, 0.15)';
            aiPanel.style.border = '1px solid rgba(239, 68, 68, 0.4)';
            aiPanel.style.color = '#f87171';
        }
        
        aiText.innerText = `🧠 AI Analysis: Сходство ${data.confidence.toFixed(1)}% с выигрышным ${data.side}-паттерном`;
        
        clearTimeout(aiRecTimeout);
        aiRecTimeout = setTimeout(() => {
            aiPanel.style.display = 'none';
        }, 15000);
    }
});

socket.on('cancel_result', (data) => {
    if (data.success) {
        addLog('success', '✅', data.message);
    } else {
        addLog('error', '❌', data.message);
    }
});

// ═══════════════════════════════════════════
//  VIEWS: Trade / Grafic
// ═══════════════════════════════════════════

function switchView(name) {
    const isGrafic = name === 'grafic';
    if (viewTrade) viewTrade.classList.toggle('is-hidden', isGrafic);
    if (viewGrafic) viewGrafic.classList.toggle('is-hidden', !isGrafic);
    if (navTrade) navTrade.classList.toggle('active', !isGrafic);
    if (navGrafic) navGrafic.classList.toggle('active', isGrafic);
    if (isGrafic) {
        initGraficChart();
        if (graficChart) graficChart.resize();
    }
}

if (navTrade) {
    navTrade.addEventListener('click', () => switchView('trade'));
}
if (navGrafic) {
    navGrafic.addEventListener('click', () => switchView('grafic'));
}

function initGraficChart() {
    const canvas = document.getElementById('grafic-chart');
    if (!canvas || typeof Chart === 'undefined' || graficChart) return;

    const grid = 'rgba(255,255,255,0.06)';
    const tick = '#64748b';

    graficChart = new Chart(canvas, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'P(UP) %',
                    data: [],
                    borderColor: '#22c55e',
                    backgroundColor: 'rgba(34,197,94,0.08)',
                    fill: false,
                    tension: 0.12,
                    yAxisID: 'yProb',
                    pointRadius: 0,
                    borderWidth: 2,
                    order: 10,
                },
                {
                    label: 'P(DOWN) %',
                    data: [],
                    borderColor: '#ef4444',
                    backgroundColor: 'rgba(239,68,68,0.06)',
                    fill: false,
                    tension: 0.12,
                    yAxisID: 'yProb',
                    pointRadius: 0,
                    borderWidth: 2,
                    order: 10,
                },
                {
                    label: 'BTC spot (USDT)',
                    data: [],
                    borderColor: '#f97316',
                    backgroundColor: 'transparent',
                    fill: false,
                    tension: 0.12,
                    yAxisID: 'yPrice',
                    pointRadius: 0,
                    borderWidth: 2,
                    order: 10,
                },
                {
                    label: 'Таргет (5m open)',
                    data: [],
                    borderColor: '#60a5fa',
                    borderDash: [],
                    fill: false,
                    tension: 0,
                    yAxisID: 'yPrice',
                    pointRadius: 0,
                    borderWidth: 2,
                    order: 10,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: {
                    display: false,
                },
                tooltip: {
                    callbacks: {
                        title(items) {
                            if (!items.length) return '';
                            const x = items[0].parsed.x;
                            return new Date(x).toLocaleTimeString();
                        },
                    },
                },
            },
            scales: {
                x: {
                    type: 'time',
                    time: { displayFormats: { second: 'mm:ss', minute: 'HH:mm' } },
                    grid: { color: grid },
                    ticks: { color: tick, maxRotation: 0 },
                },
                yProb: {
                    type: 'linear',
                    position: 'left',
                    min: 0,
                    max: 100,
                    title: { display: true, text: 'Вероятность %', color: '#94a3b8', font: { size: 11 } },
                    grid: { color: grid },
                    ticks: { color: tick },
                },
                yPrice: {
                    type: 'linear',
                    position: 'right',
                    title: { display: true, text: 'BTC USDT', color: '#94a3b8', font: { size: 11 } },
                    grid: { drawOnChartArea: false },
                    ticks: { color: tick },
                },
            },
        },
    });
}

function resetGraficData() {
    if (!graficChart) return;
    graficChart.data.datasets.forEach((d) => {
        d.data = [];
    });
    graficChart.update('none');
}

function trimDataset(ds) {
    while (ds.data.length > GRAFIC_MAX_POINTS) ds.data.shift();
}

function appendGraficSample(data) {
    if (data.error) return;
    initGraficChart();
    if (!graficChart) return;

    const ws = data.window_start || 0;
    const tgt = data.btc_target_price;

    if (ws && ws !== lastGraficWindowStart) {
        lastGraficWindowStart = ws;
        resetGraficData();

        // -------------------------
        // Отрисовка Тепловой Карты (паттернов)
        // -------------------------
        graficChart.data.datasets.splice(4); // Удалить старые исторические накладывания
        if (tgt != null && tgt > 0) {
            if (historicalPatterns.length > 0) {
                historicalPatterns.forEach((patt, i) => {
                    const vector = patt.data;
                    const pathSpot = [];
                    vector.forEach(point => {
                        const timeMs = (ws + point.t) * 1000;
                        pathSpot.push({ x: timeMs, y: tgt + point.p });
                    });
                    
                    graficChart.data.datasets.push({
                        label: `Hist ${patt.event_id.substring(0, 5)} ${patt.outcome || ''}`,
                        data: pathSpot,
                        borderColor: patt.outcome === 'UP' ? 'rgba(34,197,94,0.15)' : patt.outcome === 'DOWN' ? 'rgba(239,68,68,0.15)' : 'rgba(255,255,255,0.15)',
                        fill: false,
                        tension: 0.12,
                        yAxisID: 'yPrice',
                        pointRadius: 0,
                        borderWidth: 1,
                        order: 20, 
                    });
                });
            }
            if (signalPatterns.length > 0) {
                signalPatterns.forEach((sig, i) => {
                    const vector = sig.data;
                    const pathSpot = [];
                    vector.forEach(point => {
                        const timeMs = (ws + point.t) * 1000;
                        pathSpot.push({ x: timeMs, y: tgt + point.p });
                    });
                    
                    graficChart.data.datasets.push({
                        label: `Signal ${sig.side} (+$${sig.profit.toFixed(2)})`,
                        data: pathSpot,
                        borderColor: 'rgba(234, 179, 8, 0.4)', // Яркий золотой/желтый цвет для прибыльных паттернов
                        borderDash: [5, 5],
                        fill: false,
                        tension: 0.12,
                        yAxisID: 'yPrice',
                        pointRadius: 0,
                        borderWidth: 2,
                        order: 15,
                    });
                });
            }
            graficChart.update('none');
        }
    }

    const tMs = (data.ts != null ? Number(data.ts) : Date.now() / 1000) * 1000;
    const up = data.up_price;
    const down = data.down_price;
    const spot = data.btc_spot;

    const [dsUp, dsDown, dsSpot, dsTgt] = graficChart.data.datasets;

    if (up != null && !Number.isNaN(Number(up))) {
        dsUp.data.push({ x: tMs, y: Number(up) * 100 });
        trimDataset(dsUp);
    }
    if (down != null && !Number.isNaN(Number(down))) {
        dsDown.data.push({ x: tMs, y: Number(down) * 100 });
        trimDataset(dsDown);
    }
    if (spot != null && !Number.isNaN(Number(spot))) {
        dsSpot.data.push({ x: tMs, y: Number(spot) });
        trimDataset(dsSpot);
    }
    if (tgt != null && !Number.isNaN(Number(tgt))) {
        dsTgt.data.push({ x: tMs, y: Number(tgt) });
        trimDataset(dsTgt);
    }

    if (graficLegend) {
        const u = up != null ? (Number(up) * 100).toFixed(1) : '—';
        const d = down != null ? (Number(down) * 100).toFixed(1) : '—';
        const s = spot != null ? Number(spot).toLocaleString('en-US', { maximumFractionDigits: 0 }) : '—';
        const tg = tgt != null ? Number(tgt).toLocaleString('en-US', { maximumFractionDigits: 0 }) : '—';
        graficLegend.innerHTML = `
            <span class="grafic-tag prob-up">P(UP) ${u}%</span>
            <span class="grafic-tag prob-down">P(DOWN) ${d}%</span>
            <span class="grafic-tag spot">Spot ${s}</span>
            <span class="grafic-tag target">Таргет ${tg}</span>
        `;
    }

    // Показываем фоновые паттерны в графике только при совпадении >= 60%
    const conf = data.current_confidence || 0;
    const showPatterns = conf >= 60;
    let visibilityChanged = false;

    if (graficChart.data.datasets.length > 4) {
        for (let i = 4; i < graficChart.data.datasets.length; i++) {
            if (graficChart.data.datasets[i].hidden !== !showPatterns) {
                graficChart.data.datasets[i].hidden = !showPatterns;
                visibilityChanged = true;
            }
        }
    }

    if (visibilityChanged) {
        graficChart.update('normal');
    } else {
        graficChart.update('none');
    }
}

// ═══════════════════════════════════════════
//  ACTIONS
// ═══════════════════════════════════════════

function clampOffsetInput(el, fallback) {
    if (!el) return fallback;
    let v = parseFloat(el.value);
    if (Number.isNaN(v)) v = fallback;
    const min = parseFloat(el.min);
    const max = parseFloat(el.max);
    const lo = Number.isNaN(min) ? 0.01 : min;
    const hi = Number.isNaN(max) ? 0.5 : max;
    v = Math.max(lo, Math.min(hi, v));
    el.value = v;
    return v;
}

function placeTrade(side) {
    // Disable both buttons while processing
    btnUp.disabled = true;
    btnDown.disabled = true;

    const size = parseFloat(orderSizeInput.value) || 10;
    const buyOff = clampOffsetInput(buyOffsetInput, 0.04);
    const sellOff = clampOffsetInput(sellOffsetInput, 0.10);
    addLog('trade', '📤', `Отправка ордера ${side}: $${size}, отступ −$${buyOff.toFixed(2)}, профит +$${sellOff.toFixed(2)}...`);
    socket.emit('place_order', {
        side: side,
        size: size,
        buy_offset: buyOff,
        sell_offset: sellOff,
    });
}

function adjustSize(delta) {
    let current = parseFloat(orderSizeInput.value) || 10;
    current = Math.max(1, Math.min(10000, current + delta));
    orderSizeInput.value = current;
}

if (buyOffsetInput) {
    buyOffsetInput.addEventListener('change', () => clampOffsetInput(buyOffsetInput, 0.04));
}
if (sellOffsetInput) {
    sellOffsetInput.addEventListener('change', () => clampOffsetInput(sellOffsetInput, 0.10));
}

// ═══════════════════════════════════════════
//  TIMER
// ═══════════════════════════════════════════

function startTimer() {
    if (timerInterval) clearInterval(timerInterval);
    updateTimer();
    timerInterval = setInterval(updateTimer, 1000);
}

function updateTimer() {
    const now = Math.floor(Date.now() / 1000);
    const remaining = windowEnd - now;

    if (remaining <= 0) {
        eventTimer.textContent = '00:00';
        eventTimer.style.color = 'var(--red-400)';
        return;
    }

    const mins = Math.floor(remaining / 60);
    const secs = remaining % 60;
    eventTimer.textContent = String(mins).padStart(2, '0') + ':' + String(secs).padStart(2, '0');
    eventTimer.style.color = remaining <= 30 ? 'var(--red-400)' : 'var(--amber-400)';
}

// ═══════════════════════════════════════════
//  LOG RENDERING
// ═══════════════════════════════════════════

function addLog(level, icon, message) {
    const entry = document.createElement('div');
    entry.className = `log-entry ${level}`;

    const now = new Date();
    const timeStr = [
        String(now.getHours()).padStart(2, '0'),
        String(now.getMinutes()).padStart(2, '0'),
        String(now.getSeconds()).padStart(2, '0')
    ].join(':');

    entry.innerHTML = `
        <span class="log-time">${timeStr}</span>
        <span class="log-icon">${icon}</span>
        <span class="log-msg">${escapeHtml(message)}</span>
    `;

    logContent.appendChild(entry);
    logContent.scrollTop = logContent.scrollHeight;

    // Keep max 300 entries
    while (logContent.children.length > 300) {
        logContent.removeChild(logContent.firstChild);
    }
}

function clearLogs() {
    logContent.innerHTML = '';
    addLog('info', '🗑️', 'Логи очищены');
}

// ═══════════════════════════════════════════
//  ORDERS RENDERING
// ═══════════════════════════════════════════

function renderOrders(orders) {
    ordersCount.textContent = orders.length;

    if (orders.length === 0) {
        ordersBody.innerHTML = '<div class="orders-empty">Нет активных ордеров</div>';
        return;
    }

    let html = `
        <table class="orders-table">
            <thead>
                <tr>
                    <th>Сторона</th>
                    <th>Покупка</th>
                    <th>Продажа</th>
                    <th>Размер</th>
                    <th>Статус</th>
                    <th class="orders-col-action"></th>
                </tr>
            </thead>
            <tbody>
    `;

    for (const o of orders) {
        const sideClass = o.side === 'UP' ? 'up' : 'down';
        const statusClass = o.status.toLowerCase();
        const statusLabels = {
            'pending': '⏳ Ожидание покупки',
            'bought': '✅ Куплено',
            'selling': '📋 На продаже',
            'sold': '💰 Продано',
            'cancelled': '❌ Отменён',
            'failed': '⚠️ Ошибка'
        };

        const canCancel = (!o.is_external) && (o.status === 'pending' || o.status === 'selling');
        const canMarket = o.status === 'bought' || o.status === 'selling';
        const oid = o.id != null ? String(o.id) : '';
        
        let actionButtons = '';

        if (canCancel && oid) {
            actionButtons += `<button type="button" class="order-cancel-btn" title="Отменить ордер" aria-label="Отменить ордер" data-order-id="${escapeHtml(oid)}">×</button>`;
        }

        html += `
            <tr class="order-row ${statusClass} ${o.is_external ? 'external-pos' : ''}">
                <td><span class="order-side ${sideClass}">${o.side}${o.is_external ? ' (Внеш.)' : ''}</span></td>
                <td>${o.buy_price ? '$' + o.buy_price.toFixed(2) : '—'}</td>
                <td>${o.sell_price ? '$' + o.sell_price.toFixed(2) : '—'}</td>
                <td>${o.size}</td>
                <td><span class="order-status ${statusClass}">${o.is_external ? '💰 Позиция' : (statusLabels[statusClass] || o.status)}</span></td>
                <td class="orders-cell-action">${actionButtons}</td>
            </tr>
        `;
    }

    html += '</tbody></table>';
    ordersBody.innerHTML = html;
}

ordersBody.addEventListener('click', (e) => {
        const cancelBtn = e.target.closest('.order-cancel-btn');
    if (cancelBtn) {
        e.preventDefault();
        const id = cancelBtn.getAttribute('data-order-id');
        if (id) {
            addLog('trade', '📦', 'Отмена ордера...');
            socket.emit('cancel_order', { order_id: id });
        }
        return;
    }

    const marketBtn = e.target.closest('.market-close-btn');
    if (marketBtn) {
        e.preventDefault();
        const id = marketBtn.getAttribute('data-order-id');
        const side = marketBtn.getAttribute('data-side');

        addLog('trade', '📦', 'Закрытие по маркету...');
        if (id) {
            socket.emit('close_position', { order_id: id });
        } else if (side) {
            socket.emit('close_position', { side: side });
        }
        return;
    }
});

// ═══════════════════════════════════════════
//  HELPERS
// ═══════════════════════════════════════════

function flashElement(el) {
    el.classList.remove('flash');
    // Force reflow
    void el.offsetWidth;
    el.classList.add('flash');
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

window.setMaxSize = function() {
    if (currentBalance !== null) {
        orderSizeInput.value = Math.floor(currentBalance);
    }
}
