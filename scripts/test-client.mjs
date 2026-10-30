/**
 * Тестовый клиент pump-service:
 * 1) подключается к сервису, показывает init и статус;
 * 2) временно ставит нижний порог 0.3%, чтобы проверить поток событий пампов;
 * 3) через 45 секунд возвращает пороги 3% / 10% и выходит.
 *
 * Запуск: bun /home/z/my-project/scripts/test-client.mjs
 */
import { io } from 'socket.io-client';

const socket = io('http://localhost:3003', {
  path: '/',
  transports: ['websocket', 'polling'],
  forceNew: true,
  timeout: 10000,
});

let pumpCount = 0;
const seen = new Set();

socket.on('connect', () => {
  console.log('✓ подключено к pump-service, id:', socket.id);
  socket.emit('config:update', { minPct: 0.3 }, (res) => {
    console.log('✓ тестовый порог 0.3% применён:', JSON.stringify(res.config ?? res));
  });
});

socket.on('connect_error', (e) => {
  console.error('✗ ошибка подключения:', e.message);
  process.exit(1);
});

socket.on('init', (data) => {
  console.log('✓ init:', JSON.stringify({
    config: data.config,
    historyLen: data.history.length,
    status: data.status,
  }, null, 2));
});

socket.on('config', (cfg) => console.log('· конфиг:', JSON.stringify(cfg)));

socket.on('status', (s) => {
  console.log(`· статус: поток=${s.binanceWs} пар=${s.symbolsTracked} окно готово=${s.windowReadyPct}% тикеров/с=${s.tickersLastSec}`);
});

socket.on('pump', (ev) => {
  pumpCount++;
  seen.add(ev.symbol);
  console.log(`🚀 [${pumpCount}] ${ev.message} | объём 24ч: ${ev.quoteVol24h ?? '—'}`);
});

setTimeout(() => {
  console.log(`\n--- итог: событий ${pumpCount}, уникальных монет ${seen.size} ---`);
  socket.emit('config:update', { minPct: 3, maxPct: 10 }, (res) => {
    console.log('✓ пороги возвращены к 3% / 10%:', JSON.stringify(res.config ?? res));
    socket.disconnect();
    process.exit(0);
  });
}, 45000);
