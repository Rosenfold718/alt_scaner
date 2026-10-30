/**
 * pump-service — сканер пампов альткоинов Binance
 *
 * Источник данных: публичный поток Binance !miniTicker@arr (все спот-рынки, ~1 раз в секунду).
 * Это те же цены, которые TradingView показывает по символам BINANCE:XXXUSDT.
 *
 * Логика детекции:
 *   • Для каждой USDT-пары ведётся скользящее окно цены (по умолчанию 5 минут).
 *   • Рост = (текущая цена − цена ~5 минут назад) / цена 5 минут назад × 100%.
 *   • Рост ≥ minPct → сигнал «ПАМП»; рост > maxPct → сигнал с меткой «СИЛЬНЫЙ ПАМП».
 *   • Повторный сигнал по той же монете — не чаще, чем раз в 3 минуты (кулдаун).
 *   • На старте окно мгратно заполняется историями 1-секундных свечей (REST klines),
 *     поэтому сканер начинает ловить пампы сразу, без 5-минутного прогрева.
 */

import { createServer } from 'http';
import { Server } from 'socket.io';
import { PrismaClient } from '@prisma/client';

// База SQLite: путь берётся из DATABASE_URL, иначе — <корень проекта>/db/custom.db
process.env.DATABASE_URL ??= `file:${new URL('../../db/custom.db', import.meta.url).pathname}`;
const prisma = new PrismaClient();

// ============================ Типы ============================

type PumpKind = 'pump' | 'strong';

interface PumpEvent {
  id: string;
  symbol: string;
  base: string;
  changePct: number;
  price: number;
  refPrice: number;
  windowSec: number;
  quoteVol24h: number | null;
  kind: PumpKind;
  message: string;
  at: string;
}

interface EngineConfig {
  minPct: number;
  maxPct: number;
  windowSec: number;
}

interface EngineStatus {
  binanceWs: boolean;
  binanceHost: string;
  symbolsLoaded: boolean;
  symbolsTracked: number;
  windowReadyPct: number;
  windowSec: number;
  eventsTotal: number;
  dbTotal: number;
  tickersLastSec: number;
  lastTickAt: string | null;
  uptimeSec: number;
}

interface SymbolInfo {
  symbol: string;
  base: string;
}

interface MiniTicker {
  e: string;
  E: number;
  s: string;
  c: string;
  o: string;
  h: string;
  l: string;
  v: string;
  q: string;
}

interface BinanceSymbol {
  status: string;
  symbol: string;
  baseAsset: string;
  quoteAsset: string;
  isSpotTradingAllowed?: boolean;
}

// ============================ Константы ============================

const PORT = 3003;

const CONFIG: EngineConfig = { minPct: 3, maxPct: 10, windowSec: 300 };
const MIN_WINDOW_SEC = 60;
const MAX_WINDOW_SEC = 3600;

const COOLDOWN_MS = 3 * 60_000; // повторный сигнал по монете не чаще раза в 3 минуты
const KEEP_MARGIN_MS = 30_000; // запас глубины буфера поверх окна
const SYMBOLS_REFRESH_MS = 10 * 60_000; // период обновления списка торговых пар
const STALE_WS_MS = 20_000; // нет данных столько — считаем поток мёртвым
const RECONNECT_MIN_MS = 3_000;
const RECONNECT_MAX_MS = 30_000;
const HISTORY_LIMIT = 300;
const INIT_HISTORY = 100;

const REST_HOSTS = ['https://api.binance.com', 'https://data-api.binance.vision'];
const WS_HOSTS = ['wss://stream.binance.com:9443', 'wss://data-stream.binance.vision'];
const KLINE_BATCH = 40;

// Не альткоины: стейблкоины, токенизированный fiat и золото
const EXCLUDED_BASES = new Set([
  'USDC', 'TUSD', 'FDUSD', 'USDP', 'DAI', 'BUSD', 'AEUR', 'EUR', 'EURI',
  'BRL', 'ARS', 'TRY', 'PAXG', 'USDS', 'USD1', 'USDG', 'USDE', 'PYUSD', 'XUSD',
]);
// BTC — не альткоин по определению задачи (легко убрать: удалите 'BTCUSDT' из набора)
const EXCLUDED_SYMBOLS = new Set(['BTCUSDT']);
// Плечевые токены (leveraged tokens) Binance
const LEVERAGED_SUFFIX = /(UP|DOWN|BULL|BEAR)USDT$/;

// ============================ Состояние ============================

let tracked = new Map<string, SymbolInfo>();
const buffers = new Map<string, Array<{ t: number; p: number }>>();
const quoteVol = new Map<string, number>();
const lastAlertAt = new Map<string, number>();

const history: PumpEvent[] = [];

// ============================ Сохранение истории в базу (SQLite + Prisma) ============================

let dbOk = true;
let dbTotalCache = 0;

/** Загружает последние 200 сохранённых сигналов из базы в память (при старте сервиса). */
async function loadHistoryFromDb(): Promise<void> {
  try {
    const rows = await prisma.pumpSignal.findMany({
      orderBy: { detectedAt: 'desc' },
      take: 200,
    });
    history.length = 0;
    for (const r of rows) {
      history.push({
        id: r.id,
        symbol: r.symbol,
        base: r.base,
        changePct: r.changePct,
        price: r.price,
        refPrice: r.refPrice,
        windowSec: r.windowSec,
        quoteVol24h: r.quoteVol24h,
        kind: r.kind as PumpKind,
        message: r.message,
        at: r.detectedAt.toISOString(),
      });
    }
    console.log(`[db] в память загружено ${history.length} сохранённых сигналов`);
  } catch (e) {
    console.error('[db] не удалось загрузить историю:', (e as Error).message);
  }
}

/** Сохраняет сигнал в базу; при недоступности базы сканер продолжает работать в памяти. */
async function saveSignal(ev: PumpEvent): Promise<void> {
  try {
    await prisma.pumpSignal.create({
      data: {
        id: ev.id,
        symbol: ev.symbol,
        base: ev.base,
        changePct: ev.changePct,
        price: ev.price,
        refPrice: ev.refPrice,
        windowSec: ev.windowSec,
        quoteVol24h: ev.quoteVol24h,
        kind: ev.kind,
        message: ev.message,
        detectedAt: new Date(ev.at),
      },
    });
    dbTotalCache++;
    if (!dbOk) {
      dbOk = true;
      console.log('[db] сохранение восстановлено');
    }
  } catch (e) {
    if (dbOk) {
      dbOk = false;
      console.error('[db] сигнал не сохранён:', (e as Error).message);
    }
  }
}

/** Общее количество сигналов в базе (-1 = база недоступна). */
async function countDb(): Promise<number> {
  try {
    return await prisma.pumpSignal.count();
  } catch {
    return -1;
  }
}

let io: Server;
let symbolsLoaded = false;
let seededOnce = false;
let eventsTotal = 0;
let tickersLastSec = 0;
let lastTickAt: number | null = null;
const engineStartedAt = Date.now();

let restHostIdx = 0;
let wsHostIdx = 0;
let ws: WebSocket | null = null;
let wsConnected = false;
let reconnectAttempts = 0;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
let lastWsMsgAt = 0;

const windowMs = () => CONFIG.windowSec * 1000;

// ============================ Вспомогательное ============================

function fmtPrice(p: number): string {
  if (p >= 1000) return p.toLocaleString('en-US', { maximumFractionDigits: 2 });
  if (p >= 1) return p.toFixed(Math.min(4, 2 + (p < 10 ? 2 : 0)));
  if (p >= 0.01) return p.toFixed(5).replace(/0+$/, '').replace(/\.$/, '');
  return p.toPrecision(4);
}

function fmtVolume(v: number): string {
  if (v >= 1e9) return `${(v / 1e9).toFixed(2)} млрд`;
  if (v >= 1e6) return `${(v / 1e6).toFixed(2)} млн`;
  if (v >= 1e3) return `${(v / 1e3).toFixed(1)} тыс`;
  return v.toFixed(0);
}

function currentRestHost(): string {
  return REST_HOSTS[restHostIdx % REST_HOSTS.length];
}

function currentWsHost(): string {
  return WS_HOSTS[wsHostIdx % WS_HOSTS.length];
}

// ============================ Список торговых пар ============================

async function loadSymbols(): Promise<boolean> {
  for (let attempt = 0; attempt < REST_HOSTS.length; attempt++) {
    const host = currentRestHost();
    try {
      const res = await fetch(`${host}/api/v3/exchangeInfo?permissions=SPOT`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = (await res.json()) as { symbols?: BinanceSymbol[] };
      const fresh = new Map<string, SymbolInfo>();
      for (const s of data.symbols ?? []) {
        if (s.status !== 'TRADING' || s.quoteAsset !== 'USDT') continue;
        if (s.isSpotTradingAllowed === false) continue;
        if (EXCLUDED_SYMBOLS.has(s.symbol) || LEVERAGED_SUFFIX.test(s.symbol)) continue;
        if (EXCLUDED_BASES.has(s.baseAsset)) continue;
        fresh.set(s.symbol, { symbol: s.symbol, base: s.baseAsset });
        if (!buffers.has(s.symbol)) buffers.set(s.symbol, []);
      }
      // убрать пары, которых больше нет на бирже
      for (const key of [...buffers.keys()]) {
        if (!fresh.has(key)) {
          buffers.delete(key);
          quoteVol.delete(key);
          lastAlertAt.delete(key);
        }
      }
      const added = [...fresh.keys()].filter((k) => !tracked.has(k));
      tracked = fresh;
      symbolsLoaded = true;
      console.log(`[symbols] отслеживается ${tracked.size} USDT-пар (добавлено новых: ${added.length})`);
      if (seededOnce && added.length > 0 && added.length <= 50) {
        await Promise.all(added.map((s) => seedSymbol(s)));
        console.log(`[symbols] досеяна история для ${added.length} новых пар`);
      }
      return true;
    } catch (e) {
      console.warn(`[symbols] не удалось получить список с ${host}: ${(e as Error).message}`);
      restHostIdx++;
    }
  }
  console.error('[symbols] все REST-хосты недоступны, повторю попытку через 30 с');
  return false;
}

// ============================ Засев истории окна ============================

/**
 * Заполняет буфер монеты 1-секундными свечами, чтобы детекция работала сразу, без прогрева.
 * При mergeExisting=true свежие точки из живого потока сохраняются: история сливается и сортируется.
 */
async function seedSymbol(sym: string, mergeExisting = false): Promise<void> {
  const buf = buffers.get(sym);
  if (!buf) return;
  if (!mergeExisting && buf.length > 0) return;
  const limit = Math.min(1000, Math.max(420, Math.ceil(CONFIG.windowSec * 1.5)));
  try {
    const res = await fetch(`${currentRestHost()}/api/v3/klines?symbol=${sym}&interval=1s&limit=${limit}`);
    if (!res.ok) return;
    const rows = (await res.json()) as unknown;
    if (!Array.isArray(rows)) return;
    const pts: Array<{ t: number; p: number }> = [];
    for (const row of rows as Array<[number, string, string, string, string]>) {
      const t = Number(row[0]);
      const p = parseFloat(row[4]);
      if (Number.isFinite(t) && Number.isFinite(p) && p > 0) pts.push({ t, p });
    }
    if (pts.length === 0) return;
    if (mergeExisting && buf.length > 0) {
      const merged = [...buf, ...pts].sort((a, b) => a.t - b.t);
      buf.length = 0;
      for (let i = 0; i < merged.length; i++) {
        if (i + 1 < merged.length && merged[i + 1].t === merged[i].t) continue; // дедуп по времени
        buf.push(merged[i]);
      }
    } else {
      buf.push(...pts);
    }
  } catch {
    // тихо: монета прогреется живым потоком за 5 минут
  }
}

async function seedAllSymbols(mergeExisting = false): Promise<void> {
  if (!symbolsLoaded) return;
  const symbols = [...tracked.keys()];
  const startedAt = Date.now();
  let ok = 0;
  for (let i = 0; i < symbols.length; i += KLINE_BATCH) {
    const batch = symbols.slice(i, i + KLINE_BATCH);
    const results = await Promise.all(batch.map((s) => seedSymbol(s, mergeExisting).then(() => true, () => false)));
    const batchOk = results.filter(Boolean).length;
    ok += batchOk;
    if (batchOk === 0) {
      // REST недоступен (например, лимит 418) — не долбим, живой поток прогреет окно сам
      console.warn(`[seed] батч ${i / KLINE_BATCH + 1} не засеян — останавливаем засев, продолжаем живым потоком`);
      break;
    }
    await new Promise((r) => setTimeout(r, 150)); // мягкий темп запросов
  }
  if (ok > 0) {
    seededOnce = true;
    console.log(`[seed] история окна засеяна для ${ok}/${symbols.length} пар за ${((Date.now() - startedAt) / 1000).toFixed(1)} с`);
  }
}

// ============================ Поток Binance ============================

function connectBinance(): void {
  if (ws) return;
  const host = currentWsHost();
  const url = `${host}/ws/!miniTicker@arr`;
  console.log(`[binance] подключение к ${url} …`);
  try {
    ws = new WebSocket(url);
  } catch (e) {
    console.error('[binance] ошибка создания WebSocket:', (e as Error).message);
    ws = null;
    scheduleReconnect();
    return;
  }

  ws.onopen = () => {
    wsConnected = true;
    reconnectAttempts = 0;
    lastWsMsgAt = Date.now();
    console.log('[binance] поток !miniTicker@arr подключён');
  };

  ws.onmessage = (event: MessageEvent) => {
    lastWsMsgAt = Date.now();
    if (!wsConnected) {
      wsConnected = true;
      reconnectAttempts = 0;
    }
    try {
      const data = JSON.parse(String(event.data)) as MiniTicker[];
      if (Array.isArray(data)) processTickers(data);
    } catch (e) {
      console.warn('[binance] неразборчивое сообщение:', (e as Error).message);
    }
  };

  ws.onclose = () => {
    console.warn('[binance] поток отключён');
    ws = null;
    wsConnected = false;
    scheduleReconnect();
  };

  ws.onerror = () => {
    // закрытие обработается в onclose
  };
}

function scheduleReconnect(): void {
  if (reconnectTimer) return;
  reconnectAttempts++;
  if (reconnectAttempts % 2 === 0) wsHostIdx++; // чередуем хосты при проблемах
  const delay = Math.min(RECONNECT_MAX_MS, RECONNECT_MIN_MS * reconnectAttempts);
  console.log(`[binance] переподключение через ${delay / 1000} с (попытка ${reconnectAttempts}, хост ${currentWsHost()})`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectBinance();
  }, delay);
}

function processTickers(arr: MiniTicker[]): void {
  tickersLastSec = arr.length;
  lastTickAt = Date.now();
  const wMs = windowMs();
  for (const t of arr) {
    const sym = t.s;
    if (!tracked.has(sym)) continue;
    const price = parseFloat(t.c);
    if (!Number.isFinite(price) || price <= 0) continue;
    const q = parseFloat(t.q);
    if (Number.isFinite(q)) quoteVol.set(sym, q);

    const buf = buffers.get(sym);
    if (!buf) continue;
    buf.push({ t: t.E, p: price });
    const keepFrom = t.E - wMs - KEEP_MARGIN_MS;
    while (buf.length > 2 && buf[0].t < keepFrom) buf.shift();
    detectPump(sym, t.E);
  }
}

// ============================ Детекция пампов ============================

function detectPump(sym: string, nowMs: number): void {
  const buf = buffers.get(sym);
  const info = tracked.get(sym);
  if (!buf || !info || buf.length < 2) return;

  const wMs = windowMs();
  // окно должно быть фактически заполнено реальными данными
  const span = buf[buf.length - 1].t - buf[0].t;
  if (span < wMs * 0.98) return;

  // цена ~windowSec минут назад: последний сэмпл не новее cutoff
  const cutoff = nowMs - wMs;
  let refPrice: number | null = null;
  for (let i = 0; i < buf.length; i++) {
    if (buf[i].t <= cutoff) refPrice = buf[i].p;
    else break;
  }
  if (refPrice === null || refPrice <= 0) return;

  const price = buf[buf.length - 1].p;
  const changePct = ((price - refPrice) / refPrice) * 100;
  if (changePct < CONFIG.minPct) return;

  const last = lastAlertAt.get(sym) ?? 0;
  if (nowMs - last < COOLDOWN_MS) return;
  lastAlertAt.set(sym, nowMs);

  const kind: PumpKind = changePct > CONFIG.maxPct ? 'strong' : 'pump';
  const rounded = Math.round(changePct * 100) / 100;
  const icon = kind === 'strong' ? '🔥' : '🚀';
  const vol = quoteVol.get(sym) ?? null;
  const message = `${icon} ${sym} +${rounded.toFixed(2)}% за ${CONFIG.windowSec / 60} мин | ${fmtPrice(refPrice)} → ${fmtPrice(price)} USDT`;

  const ev: PumpEvent = {
    id: crypto.randomUUID(),
    symbol: sym,
    base: info.base,
    changePct: rounded,
    price,
    refPrice,
    windowSec: CONFIG.windowSec,
    quoteVol24h: vol,
    kind,
    message,
    at: new Date(nowMs).toISOString(),
  };

  history.unshift(ev);
  if (history.length > HISTORY_LIMIT) history.length = HISTORY_LIMIT;
  eventsTotal++;
  io.emit('pump', ev);
  void saveSignal(ev);
  console.log(`[pump] ${message} (объём 24ч: ${vol === null ? '—' : fmtVolume(vol)} USDT)`);
}

// ============================ Статус ============================

function buildStatus(): EngineStatus {
  let ready = 0;
  let counted = 0;
  const wMs = windowMs();
  for (const [, buf] of buffers) {
    if (buf.length < 2) continue;
    counted++;
    if (buf[buf.length - 1].t - buf[0].t >= wMs * 0.98) ready++;
  }
  return {
    binanceWs: wsConnected,
    binanceHost: currentWsHost(),
    symbolsLoaded,
    symbolsTracked: tracked.size,
    windowReadyPct: counted === 0 ? 0 : Math.round((ready / counted) * 100),
    windowSec: CONFIG.windowSec,
    eventsTotal,
    dbTotal: dbTotalCache,
    tickersLastSec,
    lastTickAt: lastTickAt ? new Date(lastTickAt).toISOString() : null,
    uptimeSec: Math.floor((Date.now() - engineStartedAt) / 1000),
  };
}

// ============================ Socket.io ============================

function validateConfigPatch(patch: unknown): { ok: true; config: EngineConfig } | { ok: false; error: string } {
  if (typeof patch !== 'object' || patch === null) return { ok: false, error: 'Некорректный формат настроек' };
  const p = patch as Record<string, unknown>;
  const next: EngineConfig = { ...CONFIG };
  if (p.minPct !== undefined) {
    const v = Number(p.minPct);
    if (!Number.isFinite(v) || v < 0.1 || v > 99) return { ok: false, error: 'Нижний порог: число от 0.1 до 99' };
    next.minPct = Math.round(v * 100) / 100;
  }
  if (p.maxPct !== undefined) {
    const v = Number(p.maxPct);
    if (!Number.isFinite(v) || v < 0.1 || v > 1000) return { ok: false, error: 'Верхняя граница: число от 0.1 до 1000' };
    next.maxPct = Math.round(v * 100) / 100;
  }
  if (p.windowSec !== undefined) {
    const v = Number(p.windowSec);
    if (!Number.isFinite(v) || v < MIN_WINDOW_SEC || v > MAX_WINDOW_SEC) {
      return { ok: false, error: `Окно: от ${MIN_WINDOW_SEC} до ${MAX_WINDOW_SEC} секунд` };
    }
    next.windowSec = Math.round(v);
  }
  if (next.maxPct <= next.minPct) return { ok: false, error: 'Верхняя граница должна быть больше нижнего порога' };
  return { ok: true, config: next };
}

function setupSocket(): void {
  const httpServer = createServer();
  io = new Server(httpServer, {
    // путь менять нельзя: его использует шлюз Caddy
    path: '/',
    cors: { origin: '*', methods: ['GET', 'POST'] },
    pingTimeout: 60000,
    pingInterval: 25000,
  });

  io.on('connection', (socket) => {
    console.log(`[socket] клиент подключился: ${socket.id}`);
    socket.emit('init', {
      config: { ...CONFIG },
      history: history.slice(0, INIT_HISTORY),
      status: buildStatus(),
    });

    socket.on('config:update', (patch: unknown, ack?: (r: unknown) => void) => {
      const result = validateConfigPatch(patch);
      if (!result.ok) {
        console.warn(`[config] отклонено: ${result.error}`);
        if (typeof ack === 'function') ack({ ok: false, error: result.error });
        return;
      }
      const windowChanged = result.config.windowSec !== CONFIG.windowSec;
      Object.assign(CONFIG, result.config);
      console.log(`[config] новые пороги: min ${CONFIG.minPct}% / max ${CONFIG.maxPct}% / окно ${CONFIG.windowSec} с`);
      if (windowChanged) {
        // при смене окна сбрасываем буферы и мгновенно пересеиваем историю под новое окно
        for (const [, buf] of buffers) buf.length = 0;
        void seedAllSymbols(true);
      }
      io.emit('config', { ...CONFIG });
      if (typeof ack === 'function') ack({ ok: true, config: { ...CONFIG } });
    });

    socket.on('disconnect', () => console.log(`[socket] клиент отключился: ${socket.id}`));
    socket.on('error', (e: Error) => console.error(`[socket] ошибка (${socket.id}):`, e.message));
  });

  httpServer.listen(PORT, () => console.log(`pump-service запущен на порту ${PORT}`));
}

// ============================ Запуск ============================

async function main(): Promise<void> {
  // восстановление истории сигналов из базы — до открытия сокета клиентам
  await loadHistoryFromDb();
  void countDb().then((n) => {
    if (n >= 0) dbTotalCache = n;
  });

  setupSocket();

  // поток Binance подключаем сразу — он не зависит от REST-лимитов
  connectBinance();

  // периодический контроль живости потока
  setInterval(() => {
    if (wsConnected && Date.now() - lastWsMsgAt > STALE_WS_MS) {
      console.warn('[binance] поток молчит дольше 20 с — принудительный рестарт');
      wsConnected = false;
      const sock = ws as unknown as { close?: () => void; terminate?: () => void } | null;
      try {
        if (sock?.terminate) sock.terminate();
        else if (sock?.close) sock.close();
      } catch {
        /* игнорируем */
      }
      ws = null;
      scheduleReconnect();
    }
  }, 5_000);

  // статус клиентам раз в 3 секунды (с периодическим обновлением счётчика базы)
  setInterval(() => {
    void countDb().then((n) => {
      if (n >= 0) dbTotalCache = n;
    });
    if (io) io.emit('status', buildStatus());
  }, 3_000);

  // загрузка пар, затем мягкий засев истории окна (в режиме слияния с живыми данными)
  let loaded = await loadSymbols();
  while (!loaded) {
    await new Promise((r) => setTimeout(r, 30_000));
    loaded = await loadSymbols();
  }
  void seedAllSymbols(true);

  // обновление списка пар + страховочный пересев
  setInterval(() => void loadSymbols(), SYMBOLS_REFRESH_MS);

  process.on('SIGTERM', () => {
    console.log('SIGTERM — останавливаемся…');
    process.exit(0);
  });
  process.on('SIGINT', () => process.exit(0));
}

void main();
