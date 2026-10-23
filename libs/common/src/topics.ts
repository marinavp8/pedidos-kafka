export const TOPICS = {
  pedidosEventos: 'pedidos.eventos',
  sagaRespuestas: 'saga.respuestas',
  inventarioComandos: 'inventario.comandos',
  pagosComandos: 'pagos.comandos',
  enviosComandos: 'envios.comandos',
  sagaDlq: 'saga.dlq',
} as const;

export const dlq = (topic: string) => `${topic}.dlq`;

/** outbox.aggregate_type -> topic destino (lo usa el relay). */
export const AGGREGATE_TOPIC: Record<string, string> = {
  pedido: TOPICS.pedidosEventos,
  'saga-respuesta': TOPICS.sagaRespuestas,
  'inventario-cmd': TOPICS.inventarioComandos,
  'pagos-cmd': TOPICS.pagosComandos,
  'envios-cmd': TOPICS.enviosComandos,
  'saga-dlq': TOPICS.sagaDlq,
};
