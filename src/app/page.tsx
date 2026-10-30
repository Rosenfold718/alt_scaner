'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { io, Socket } from 'socket.io-client';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '@/components/ui/accordion';
import {
  Activity,
  Check,
  Coins,
  Copy,
  Database,
  ExternalLink,
  Gauge,
  Signal,
  Wifi,
  WifiOff,
} from 'lucide-react';

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

interface InitPayload {
  config: EngineConfig;
  history: PumpEvent[];
  status: EngineStatus;
}

interface ConfigAck {
  ok: boolean;
  config?: EngineConfig;
  error?: string;
}

// ============================ Помощники ============================

/** Адрес socket-сервиса: в песочнице — через шлюз Caddy, локально — напрямую. */
function socketUrl(): string {
  if (typeof window !== 'undefined') {
    const { hostname } = window.location;
    if (hostname === 'localhost' || hostname === '127.0.0.1') {
      return 'http://localhost:3003';
    }
  }
  return '/?XTransformPort=3003';
}

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

function fmtTime(iso: string): string {
  return new Date(iso).toLocaleTimeString('ru-RU', { hour12: false });
}

function tradingViewUrl(symbol: string): string {
  return `https://www.tradingview.com/chart/?symbol=BINANCE%3A${encodeURIComponent(symbol)}`;
}

const MAX_ROWS = 150;

// ============================ Страница ============================

export default function PumpScannerPage() {
  const [connected, setConnected] = useState(false);
  const [ready, setReady] = useState(false);
  const [events, setEvents] = useState<PumpEvent[]>([]);
  const [status, setStatus] = useState<EngineStatus | null>(null);
  const [cfg, setCfg] = useState<EngineConfig | null>(null);
  const [minDraft, setMinDraft] = useState('');
  const [maxDraft, setMaxDraft] = useState('');
  const [configError, setConfigError] = useState<string | null>(null);
  const [savedFlash, setSavedFlash] = useState(false);
  const [copiedId, setCopiedId] = useState<string | null>(null);
  const socketRef = useRef<Socket | null>(null);

  useEffect(() => {
    const socket = io(socketUrl(), {
      transports: ['websocket', 'polling'],
      forceNew: true,
      reconnection: true,
      reconnectionAttempts: 20,
      reconnectionDelay: 1500,
      timeout: 10000,
    });
    socketRef.current = socket;

    socket.on('connect', () => setConnected(true));
    socket.on('disconnect', () => setConnected(false));

    socket.on('init', (data: InitPayload) => {
      setReady(true);
      setEvents(data.history);
      setStatus(data.status);
      setCfg(data.config);
      setMinDraft(String(data.config.minPct));
      setMaxDraft(String(data.config.maxPct));
      setConfigError(null);
    });

    socket.on('status', (s: EngineStatus) => setStatus(s));

    socket.on('config', (c: EngineConfig) => {
      setCfg(c);
      setMinDraft(String(c.minPct));
      setMaxDraft(String(c.maxPct));
    });

    socket.on('pump', (ev: PumpEvent) => {
      setEvents((prev) => [ev, ...prev].slice(0, 300));
    });

    return () => {
      socket.disconnect();
      socketRef.current = null;
    };
  }, []);

  const uniqueCoins = useMemo(() => new Set(events.map((e) => e.symbol)).size, [events]);
  const best = useMemo(
    () => events.reduce<PumpEvent | null>((b, e) => (!b || e.changePct > b.changePct ? e : b), null),
    [events],
  );

  const draftValid = useMemo(() => {
    const min = parseFloat(minDraft.replace(',', '.'));
    const max = parseFloat(maxDraft.replace(',', '.'));
    return Number.isFinite(min) && Number.isFinite(max) && min >= 0.1 && max > min;
  }, [minDraft, maxDraft]);

  const applyConfig = () => {
    const socket = socketRef.current;
    if (!socket || !draftValid) return;
    setConfigError(null);
    socket.emit(
      'config:update',
      { minPct: parseFloat(minDraft.replace(',', '.')), maxPct: parseFloat(maxDraft.replace(',', '.')) },
      (res: ConfigAck) => {
        if (res.ok && res.config) {
          setCfg(res.config);
          setMinDraft(String(res.config.minPct));
          setMaxDraft(String(res.config.maxPct));
          setSavedFlash(true);
          setTimeout(() => setSavedFlash(false), 2000);
        } else {
          setConfigError(res.error ?? 'Не удалось применить настройки');
        }
      },
    );
  };

  const copyMessage = async (ev: PumpEvent) => {
    try {
      await navigator.clipboard.writeText(ev.message);
      setCopiedId(ev.id);
      setTimeout(() => setCopiedId(null), 1500);
    } catch {
      /* буфер обмена недоступен */
    }
  };

  const windowMin = cfg ? Math.round(cfg.windowSec / 60) : 5;

  return (
    <div className="min-h-screen flex flex-col bg-[#0b0e11] text-zinc-100">
      {/* ======================= Шапка ======================= */}
      <header className="border-b border-zinc-800/80 bg-[#101318]">
        <div className="max-w-6xl w-full mx-auto px-4 sm:px-6 py-4 flex flex-wrap items-center gap-x-4 gap-y-2">
          <div className="flex items-center gap-3 mr-auto">
            <span className="text-2xl sm:text-3xl" aria-hidden>🚀</span>
            <div>
              <h1 className="text-lg sm:text-xl font-bold leading-tight">Альткоин-сканер пампов</h1>
              <p className="text-xs sm:text-sm text-zinc-400">
                Скачки +{cfg ? cfg.minPct : 3}%…+{cfg ? cfg.maxPct : 10}% за {windowMin} мин · все USDT-пары Binance · цены как на TradingView
              </p>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <Badge
              className={
                connected
                  ? 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/30 gap-1.5'
                  : 'bg-red-500/15 text-red-400 border border-red-500/30 gap-1.5'
              }
            >
              {connected ? <Signal className="w-3 h-3" aria-hidden /> : <WifiOff className="w-3 h-3" aria-hidden />}
              {connected ? 'в эфире' : 'нет связи'}
            </Badge>
            <Badge
              className={
                status?.binanceWs
                  ? 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/30 gap-1.5'
                  : 'bg-amber-500/15 text-amber-400 border border-amber-500/30 gap-1.5'
              }
            >
              {status?.binanceWs ? <Wifi className="w-3 h-3" aria-hidden /> : <WifiOff className="w-3 h-3" aria-hidden />}
              поток Binance
            </Badge>
          </div>
        </div>
      </header>

      <main className="flex-1 w-full max-w-6xl mx-auto px-4 sm:px-6 py-6 space-y-6">
        {/* ======================= Показатели ======================= */}
        <section aria-label="Показатели" className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4">
          <Card className="bg-[#12161c] border-zinc-800/80">
            <CardHeader className="p-4 pb-1">
              <CardDescription className="text-xs text-zinc-500 flex items-center gap-1.5">
                <Database className="w-3.5 h-3.5" aria-hidden /> Сигналов в базе
              </CardDescription>
              <CardTitle className="text-2xl sm:text-3xl font-bold text-zinc-100 font-mono">
                {status ? status.dbTotal : events.length}
              </CardTitle>
            </CardHeader>
            <CardContent className="p-0" />
          </Card>

          <Card className="bg-[#12161c] border-zinc-800/80">
            <CardHeader className="p-4 pb-1">
              <CardDescription className="text-xs text-zinc-500 flex items-center gap-1.5">
                <Coins className="w-3.5 h-3.5" aria-hidden /> Монет с пампами
              </CardDescription>
              <CardTitle className="text-2xl sm:text-3xl font-bold text-zinc-100 font-mono">{uniqueCoins}</CardTitle>
            </CardHeader>
            <CardContent className="p-0" />
          </Card>

          <Card className="bg-[#12161c] border-zinc-800/80">
            <CardHeader className="p-4 pb-1">
              <CardDescription className="text-xs text-zinc-500 flex items-center gap-1.5">
                <Activity className="w-3.5 h-3.5" aria-hidden /> Лучший памп
              </CardDescription>
              <CardTitle className="text-xl sm:text-2xl font-bold font-mono text-emerald-400 truncate">
                {best ? `${best.base} +${best.changePct.toFixed(2)}%` : '—'}
              </CardTitle>
            </CardHeader>
            <CardContent className="p-0" />
          </Card>

          <Card className="bg-[#12161c] border-zinc-800/80">
            <CardHeader className="p-4 pb-1">
              <CardDescription className="text-xs text-zinc-500 flex items-center gap-1.5">
                <Gauge className="w-3.5 h-3.5" aria-hidden /> Пар в скане
              </CardDescription>
              <CardTitle className="text-2xl sm:text-3xl font-bold text-zinc-100 font-mono">
                {status ? status.symbolsTracked : '…'}
              </CardTitle>
            </CardHeader>
            <CardContent className="p-0" />
          </Card>
        </section>

        {/* ======================= Настройки ======================= */}
        <Card className="bg-[#12161c] border-zinc-800/80">
          <CardHeader className="p-4 sm:p-6 pb-2 sm:pb-3">
            <CardTitle className="text-base sm:text-lg text-zinc-100">Пороги детекции</CardTitle>
            <CardDescription className="text-xs sm:text-sm text-zinc-400">
              Сигнал приходит, когда монета растёт минимум на нижний порог за {windowMin} мин. Рост выше верхней границы
              помечается как «СИЛЬНЫЙ ПАМП». Повторов нет: один сигнал на эпизод роста — новый возможен, только когда
              рост монеты откатится ниже порога.
            </CardDescription>
          </CardHeader>
          <CardContent className="p-4 sm:p-6 pt-0 flex flex-col sm:flex-row sm:items-end gap-3 sm:gap-4">
            <div className="space-y-1.5">
              <Label htmlFor="minPct" className="text-xs text-zinc-400">Нижний порог, %</Label>
              <Input
                id="minPct"
                inputMode="decimal"
                value={minDraft}
                onChange={(e) => setMinDraft(e.target.value)}
                className="w-32 bg-[#0b0e11] border-zinc-700 text-zinc-100 font-mono"
                disabled={!connected}
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="maxPct" className="text-xs text-zinc-400">Верхняя граница, %</Label>
              <Input
                id="maxPct"
                inputMode="decimal"
                value={maxDraft}
                onChange={(e) => setMaxDraft(e.target.value)}
                className="w-32 bg-[#0b0e11] border-zinc-700 text-zinc-100 font-mono"
                disabled={!connected}
              />
            </div>
            <Button
              onClick={applyConfig}
              disabled={!connected || !draftValid}
              className="bg-emerald-500 hover:bg-emerald-600 text-black font-semibold min-h-11"
            >
              {savedFlash ? <Check className="w-4 h-4 mr-1" aria-hidden /> : null}
              {savedFlash ? 'Сохранено' : 'Применить'}
            </Button>
            {configError && (
              <p className="text-sm text-red-400 sm:mb-1" role="alert">{configError}</p>
            )}
          </CardContent>
        </Card>

        {/* ======================= Журнал пампов ======================= */}
        <Card className="bg-[#12161c] border-zinc-800/80">
          <CardHeader className="p-4 sm:p-6 pb-2 sm:pb-3">
            <div className="flex flex-wrap items-center gap-2">
              <CardTitle className="text-base sm:text-lg text-zinc-100 mr-auto">Журнал пампов</CardTitle>
              <Badge variant="outline" className="text-zinc-400 border-zinc-700">новые сверху</Badge>
              {status && status.windowReadyPct < 100 && (
                <Badge className="bg-amber-500/15 text-amber-400 border border-amber-500/30">
                  окно готово для {status.windowReadyPct}% пар
                </Badge>
              )}
            </div>
            <CardDescription className="text-xs sm:text-sm text-zinc-400">
              Скачки за последние {windowMin} минут. Каждый сигнал сохраняется в базу — история не теряется при
              перезапуске. Клик по тикеру открывает график монеты на TradingView.
            </CardDescription>
          </CardHeader>
          <CardContent className="p-0">
            {!ready ? (
              <div className="px-4 sm:px-6 py-10 text-center text-sm text-zinc-500">
                Подключение к сканеру…
              </div>
            ) : events.length === 0 ? (
              <div className="px-4 sm:px-6 py-10 text-center space-y-2">
                <p className="text-3xl" aria-hidden>📡</p>
                <p className="text-sm text-zinc-400">
                  Сигналов пока нет. Сканер следит за {status?.symbolsTracked ?? '…'} парами в реальном времени.
                </p>
                <p className="text-xs text-zinc-500 max-w-md mx-auto">
                  Пампов ≥{cfg?.minPct ?? 3}% за {windowMin} минут сейчас не зафиксировано — это нормально для спокойного
                  рынка. Как только какая-то монета резко вырастет, сигнал появится здесь мгновенно.
                </p>
              </div>
            ) : (
              <div
                className="pump-scroll max-h-[560px] overflow-y-auto"
                role="list"
                aria-label="Список пампов"
              >
                {events.slice(0, MAX_ROWS).map((ev) => {
                  const strong = ev.kind === 'strong';
                  return (
                    <div
                      key={ev.id}
                      role="listitem"
                      className="pump-flash px-4 sm:px-6 py-3 border-b border-zinc-800/60 hover:bg-zinc-800/20 transition-colors"
                    >
                      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                        <span className="text-xs text-zinc-500 font-mono w-16 shrink-0">{fmtTime(ev.at)}</span>
                        <a
                          href={tradingViewUrl(ev.symbol)}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="font-bold text-sm sm:text-base text-zinc-100 hover:text-emerald-400 transition-colors inline-flex items-center gap-1"
                          title={`Открыть ${ev.symbol} на TradingView`}
                        >
                          {ev.symbol}
                          <ExternalLink className="w-3 h-3 opacity-50" aria-hidden />
                        </a>
                        {strong ? (
                          <Badge className="bg-amber-500/15 text-amber-400 border border-amber-500/30 text-[11px]">
                            🔥 СИЛЬНЫЙ ПАМП
                          </Badge>
                        ) : (
                          <Badge className="bg-emerald-500/15 text-emerald-400 border border-emerald-500/30 text-[11px]">
                            ПАМП
                          </Badge>
                        )}
                        <span
                          className={`font-bold text-lg font-mono ml-auto ${strong ? 'text-amber-400' : 'text-emerald-400'}`}
                        >
                          +{ev.changePct.toFixed(2)}%
                        </span>
                      </div>
                      <div className="flex flex-wrap items-center gap-x-4 gap-y-0.5 mt-1 text-xs sm:text-sm">
                        <span className="text-zinc-300">
                          цена <span className="font-mono text-zinc-100">{fmtPrice(ev.price)}</span> USDT
                        </span>
                        <span className="text-zinc-500">
                          было <span className="font-mono">{fmtPrice(ev.refPrice)}</span> {windowMin} мин назад
                        </span>
                        {ev.quoteVol24h !== null && (
                          <span className="text-zinc-500">
                            объём 24ч <span className="font-mono">{fmtVolume(ev.quoteVol24h)}</span> USDT
                          </span>
                        )}
                        <button
                          onClick={() => copyMessage(ev)}
                          className="ml-auto inline-flex items-center gap-1 text-zinc-500 hover:text-zinc-300 transition-colors min-h-8"
                          title="Скопировать сообщение"
                        >
                          {copiedId === ev.id ? (
                            <Check className="w-3.5 h-3.5 text-emerald-400" aria-hidden />
                          ) : (
                            <Copy className="w-3.5 h-3.5" aria-hidden />
                          )}
                          <span className="text-xs">{copiedId === ev.id ? 'скопировано' : 'копировать'}</span>
                        </button>
                      </div>
                      <p className="mt-1 text-xs text-zinc-500 font-mono break-all">{ev.message}</p>
                    </div>
                  );
                })}
              </div>
            )}
            {events.length > MAX_ROWS && (
              <p className="px-4 sm:px-6 py-2 text-xs text-zinc-600 border-t border-zinc-800/60">
                Показаны последние {MAX_ROWS} из {events.length} сигналов сессии.
              </p>
            )}
          </CardContent>
        </Card>

        {/* ======================= Справка ======================= */}
        <Card className="bg-[#12161c] border-zinc-800/80">
          <CardContent className="p-4 sm:p-6">
            <Accordion type="single" collapsible>
              <AccordionItem value="how" className="border-zinc-800/60">
                <AccordionTrigger className="text-sm sm:text-base text-zinc-200">Как ищутся пампы?</AccordionTrigger>
                <AccordionContent className="text-sm text-zinc-400 space-y-2">
                  <p>
                    Для каждой монеты сканер непрерывно сравнивает текущую цену с ценой {windowMin} минут назад. Если рост
                    достигает нижнего порога (сейчас {cfg?.minPct ?? 3}%) — формируется сигнал: тикер, цена, процент роста
                    и готовое сообщение.
                  </p>
                  <p>
                    Рост выше верхней границы (сейчас {cfg?.maxPct ?? 10}%) помечается как «СИЛЬНЫЙ ПАМП». По каждой монете
                    приходит один сигнал на эпизод роста: пока рост держится выше нижнего порога, повторов не будет — даже
                    если монета продолжает дорожать. Новый сигнал возможен только после того, как рост откатится ниже
                    порога и продержится там минимум 30 секунд.
                  </p>
                </AccordionContent>
              </AccordionItem>
              <AccordionItem value="coins" className="border-zinc-800/60">
                <AccordionTrigger className="text-sm sm:text-base text-zinc-200">Какие монеты сканируются?</AccordionTrigger>
                <AccordionContent className="text-sm text-zinc-400 space-y-2">
                  <p>
                    Все спотовые пары к USDT на Binance со статусом TRADING — сейчас это {status?.symbolsTracked ?? '…'}{' '}
                    инструментов. Исключены стейблкоины, плечевые токены и BTC (он не альткоин).
                  </p>
                  <p>Список пар автоматически обновляется каждые 10 минут: новые листинги подхватываются сразу.</p>
                </AccordionContent>
              </AccordionItem>
              <AccordionItem value="source" className="border-zinc-800/60">
                <AccordionTrigger className="text-sm sm:text-base text-zinc-200">Откуда данные и при чём тут TradingView?</AccordionTrigger>
                <AccordionContent className="text-sm text-zinc-400 space-y-2">
                  <p>
                    Цены берутся из официального публичного потока биржи Binance — это ровно те же котировки, которые
                    TradingView показывает по символам <span className="font-mono text-zinc-300">BINANCE:XXXUSDT</span>.
                    У TradingView нет бесплатного публичного API, поэтому прямое подключение к бирже — стандартное решение.
                  </p>
                  <p>
                    Клик по тикеру в журнале открывает график этой монеты на TradingView, чтобы сразу оценить памп
                    глазами.
                  </p>
                </AccordionContent>
              </AccordionItem>
            </Accordion>
          </CardContent>
        </Card>
      </main>

      {/* ======================= Подвал ======================= */}
      <footer className="mt-auto border-t border-zinc-800/80 bg-[#101318]">
        <div className="max-w-6xl w-full mx-auto px-4 sm:px-6 py-3 text-xs text-zinc-500 flex flex-wrap gap-x-4 gap-y-1">
          <span>Данные: публичный поток Binance (спот)</span>
          <span className="hidden sm:inline">·</span>
          <span>Цены соответствуют символам BINANCE:XXX на TradingView</span>
          <span className="hidden sm:inline">·</span>
          <span>Не является инвестиционной рекомендацией</span>
        </div>
      </footer>
    </div>
  );
}
