/**
 * Симуляция анти-повтора сигналов — проверка логики detectPump()
 * из mini-services/pump-service/index.ts на синтетических ценах
 * (тик раз в секунду, окно 5 минут, порог 3%).
 *
 * Сценарии:
 *  1. Непрерывный рост 20 минут (+3% → +12%)          → ровно 1 сигнал
 *  2. Рост → плато (откат ниже порога) → новый рост   → 2 сигнала, второй только после отката
 *  3. Дребезг у порога (короткие просадки по 2 с)     → ровно 1 сигнал
 *
 * Запуск: bun scripts/test-dedup.mjs
 */

const WINDOW_MS = 300_000;
const KEEP_MARGIN_MS = 30_000;
const COOLDOWN_MS = 180_000;
const REARM_HOLD_MS = 30_000;
const MIN_PCT = 3;
const MAX_PCT = 10;

// ——— точная копия решения из detectPump() ———
function makeDetector() {
  const pumping = new Set();
  const pumpingBelowSince = new Map();
  const lastAlertAt = new Map();
  const signals = [];

  return {
    signals,
    detect(sym, nowMs, changePct) {
      if (pumping.has(sym)) {
        if (changePct >= MIN_PCT) {
          pumpingBelowSince.delete(sym);
          return;
        }
        const belowSince = pumpingBelowSince.get(sym) ?? nowMs;
        if (nowMs - belowSince < REARM_HOLD_MS) {
          pumpingBelowSince.set(sym, belowSince);
          return;
        }
        pumping.delete(sym);
        pumpingBelowSince.delete(sym);
      }
      if (changePct < MIN_PCT) return;
      const last = lastAlertAt.get(sym) ?? 0;
      if (nowMs - last < COOLDOWN_MS) return;
      lastAlertAt.set(sym, nowMs);
      pumping.add(sym);
      signals.push({ at: nowMs, changePct: Math.round(changePct * 100) / 100 });
    },
  };
}

// ——— мини-движок: скользящее окно цены {t, p}, как в pump-service ———
function makeEngine() {
  const buf = [];
  return {
    push(t, p) {
      buf.push({ t, p });
      const keepFrom = t - WINDOW_MS - KEEP_MARGIN_MS;
      while (buf.length > 2 && buf[0].t < keepFrom) buf.shift();
    },
    changePct(nowMs) {
      if (buf.length < 2) return null;
      if (buf[buf.length - 1].t - buf[0].t < WINDOW_MS * 0.98) return null;
      const cutoff = nowMs - WINDOW_MS;
      let ref = null;
      for (let i = 0; i < buf.length; i++) {
        if (buf[i].t <= cutoff) ref = buf[i].p;
        else break;
      }
      if (ref === null || ref <= 0) return null;
      const price = buf[buf.length - 1].p;
      return ((price - ref) / ref) * 100;
    },
  };
}

function runScenario(name, priceAt, totalSec) {
  const engine = makeEngine();
  const detector = makeDetector();
  const t0 = 1_700_000_000_000;
  for (let s = 0; s <= totalSec; s++) {
    const t = t0 + s * 1000;
    engine.push(t, priceAt(s));
    const ch = engine.changePct(t);
    if (ch !== null) detector.detect('TESTUSDT', t, ch);
  }
  return detector.signals;
}

let failed = 0;
function check(name, cond, details) {
  console.log(`${cond ? '✓' : '✗'} ${name}${details ? ` — ${details}` : ''}`);
  if (!cond) failed++;
}

// Сценарий 1: непрерывный рост +0.01%/с — после первого сигнала монета продолжает дорожать
const s1 = runScenario('непрерывный рост', (s) => 100 * (1 + 0.0001 * s), 1200);
check(
  'Непрерывный рост 20 мин → 1 сигнал',
  s1.length === 1,
  `сигналов: ${s1.length} (${s1.map((x) => `+${x.changePct}%`).join(', ')})`
);

// Сценарий 2: рост до +3% → плато 7 мин (рост откатывается ниже порога) → новый рост
const s2 = runScenario(
  'рост → плато → рост',
  (s) => {
    if (s <= 300) return 100 * (1 + 0.0001 * s); // разгон до +3%
    if (s <= 720) return 103; // плато: рост в окне тает ниже порога
    return 103 * (1 + 0.0001 * (s - 720)); // новый эпизод роста
  },
  1800
);
const s2gap = s2.length >= 2 ? s2[1].at - s2[0].at : 0;
check('Рост → откат → новый рост → 2 сигнала', s2.length === 2, `сигналов: ${s2.length}`);
check('Второй сигнал только после отката (пауза > 5 мин)', s2gap > 300_000, `пауза: ${Math.round(s2gap / 1000)} с`);

// Сценарий 3: дребезг у порога — базовый рост +0.01%/с, каждые 30 с просадка на 2 с
const s3 = runScenario(
  'дребезг у порога',
  (s) => {
    const base = 100 * (1 + 0.0001 * s);
    const dip = s > 320 && s % 30 < 2 ? 0.995 : 1;
    return base * dip;
  },
  1200
);
check('Дребезг у порога (просадки по 2 с) → 1 сигнал', s3.length === 1, `сигналов: ${s3.length}`);

console.log(failed === 0 ? '\nВСЕ ПРОВЕРКИ ПРОЙДЕНЫ' : `\nПРОВАЛЕНО ПРОВЕРОК: ${failed}`);
process.exit(failed === 0 ? 0 : 1);
