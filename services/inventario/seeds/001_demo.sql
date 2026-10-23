INSERT INTO producto_stock (producto_id, nombre, disponible) VALUES
  ('11111111-1111-4111-8111-111111111111', 'Portátil',            100),
  ('22222222-2222-4222-8222-222222222222', 'Ratón',               500),
  ('33333333-3333-4333-8333-333333333333', 'Edición limitada',      2)
ON CONFLICT DO NOTHING;
