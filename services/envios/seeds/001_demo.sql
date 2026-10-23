INSERT INTO transportista (id, nombre, capacidad_diaria, paises) VALUES
  ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', 'Correos', 1000, '{ES,PT}'),
  ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', 'DHL',     1000, '{ES,PT,FR,DE}')
ON CONFLICT DO NOTHING;
