/**
 * Проверка сохранения истории: подключается к pump-service в режиме «только чтение»
 * и показывает, сколько сигналов загружено из базы при старте сервиса.
 *
 * Запуск: bun /home/z/my-project/scripts/test-history.mjs
 */
import { io } from 'socket.io-client';

const socket = io('http://localhost:3003', {
  path: '/',
  transports: ['websocket', 'polling'],
  forceNew: true,
  timeout: 10000,
});

socket.on('connect_error', (e) => {
  console.error('✗ ошибка подключения:', e.message);
  process.exit(1);
});

socket.on('init', (data) => {
  console.log('✓ init:');
  console.log('  сигналов из базы в init.history:', data.history.length);
  console.log('  счётчик в базе (status.dbTotal):', data.status.dbTotal);
  console.log('  конфиг:', JSON.stringify(data.config));
  if (data.history.length > 0) {
    const first = data.history[0];
    console.log('  самый свежий сохранённый сигнал:');
    console.log('   ', first.message, '|', first.at);
    const last = data.history[data.history.length - 1];
    console.log('  самый старый из загруженных:');
    console.log('   ', last.message, '|', last.at);
  }
  socket.disconnect();
  process.exit(0);
});

setTimeout(() => {
  console.error('✗ init не получен за 15 с');
  process.exit(1);
}, 15000);
