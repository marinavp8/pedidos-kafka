export function env(name: string, fallback?: string): string {
  const v = process.env[name] ?? fallback;
  if (v === undefined || v === '') throw new Error(`Falta la variable de entorno ${name}`);
  return v;
}

export function envInt(name: string, fallback: number): number {
  return parseInt(env(name, String(fallback)), 10);
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
