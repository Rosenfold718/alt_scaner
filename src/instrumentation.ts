/**
 * Автостарт ядра-сканера (pump-service, порт 3003) вместе с сервером Next.js.
 * Node-специфичная логика живёт в src/lib/pump-service-autostart.ts и импортируется
 * только в Node-рантайме, чтобы не ломать Edge-сборку.
 */

export async function register(): Promise<void> {
  if (process.env.NEXT_RUNTIME !== 'nodejs') return;
  if (process.env.NEXT_PHASE === 'phase-production-build') return;
  const { startPumpService } = await import('./lib/pump-service-autostart');
  await startPumpService();
}
