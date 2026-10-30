/**
 * Автостарт ядра-сканера (pump-service, порт 3003).
 *
 * Модуль импортируется только в Node-рантайме сервера (см. src/instrumentation.ts).
 * Зачем: сканер — долгоживущий фоновый процесс (WebSocket Binance + socket.io),
 * и он должен подниматься автоматически вместе с дашбордом, а не запускаться вручную.
 * Если порт 3003 уже отвечает (сервис запущен вручную), автостарт пропускается.
 */

import { spawn } from 'node:child_process';
import { openSync } from 'node:fs';
import { join } from 'node:path';

export async function startPumpService(): Promise<void> {
  // сервис уже отвечает на порту 3003? — второй экземпляр не нужен
  try {
    const res = await fetch('http://127.0.0.1:3003/?EIO=4&transport=polling', {
      signal: AbortSignal.timeout(1500),
    });
    await res.text().catch(() => {});
    console.log('[instrumentation] pump-service уже работает — автостарт не нужен');
    return;
  } catch {
    // порт не отвечает — запускаем сервис
  }

  try {
    const svcDir = join(process.cwd(), 'mini-services', 'pump-service');
    const logPath = join(process.cwd(), 'pump-service.log');
    const fd = openSync(logPath, 'a');
    const child = spawn('bun', ['run', 'dev'], {
      cwd: svcDir,
      detached: true, // своя группа процессов: сервис переживёт перезапуск родителя
      stdio: ['ignore', fd, fd],
      env: process.env,
    });
    child.unref();
    console.log(`[instrumentation] pump-service запущен в фоне (pid ${child.pid}), лог: ${logPath}`);
  } catch (e) {
    console.error('[instrumentation] не удалось запустить pump-service:', (e as Error).message);
  }
}
