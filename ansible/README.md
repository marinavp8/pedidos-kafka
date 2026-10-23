# Despliegue en un VPS con Ansible + Docker

Instala Docker, sincroniza el código, genera los secretos **en el servidor** (`openssl rand`, `.env` modo 0600, solo si no existe), renderiza `compose.yml` y levanta el stack:
PostgreSQL (6 BD + 6 roles) · Kafka KRaft ×3 · 6 microservicios (orquestador ×2) · gateway · Traefik con Let's Encrypt.

## Requisitos
- Control: Ansible ≥ 2.15 y `ansible-galaxy collection install -r requirements.yml`.
- VPS: Ubuntu/Debian con SSH, `sudo` sin contraseña y ~4 GB de RAM (3 brokers × 512 MB + servicios). Con `kafka_brokers: 1` bastan ~2 GB.
- Puertos 80 y 443 libres y abiertos (el reto ACME HTTP-01 usa el 80). Si otro servicio los ocupa (p. ej. `temps.service`), pon `traefik_enabled: false` y publica el gateway tú mismo.

## Uso

```bash
cd ansible
cp inventory/hosts.example.yml inventory/hosts.yml   # editar: ansible_host, public_ip, acme_email
ansible-playbook site.yml                            # primer despliegue y actualizaciones
```

La API queda en `https://api.<public_ip>.sslip.io` (cambia `api_domain` para usar tu dominio). El mismo comando actualiza el código (rsync + `docker compose up -d --build`).

Variables en `group_vars/all.yml` (se pueden sobrescribir por host o con `-e`; los booleanos por CLI con JSON: `-e '{"seed_demo_data": true}'`).

| Variable | Defecto | Notas |
|---|---|---|
| `kafka_brokers` | 3 | 1 o 3. RF = min(brokers, 3); `min.insync.replicas` = 2 si RF = 3 |
| `orquestador_replicas` | 2 | |
| `seed_demo_data` | false | `true` para poder correr `scripts/smoke.mjs` |
| `demo_login` | false | `true` activa `/auth/dev` en la web (cualquiera obtiene token de operador): **solo demos** |
| `traefik_enabled` | true | Si es false, el gateway queda en `127.0.0.1:18080` |
| `app_dir` | `~/pedidos-kafka` | Código en `src/`, `compose.yml` y `.env` aquí |

## Operación

```bash
ssh <vps>
cd ~/pedidos-kafka && sudo docker compose ps
sudo docker compose logs -f orquestador
sudo docker compose exec postgres psql -U postgres -c '\l'
# Secreto JWT para generar tokens de operador (no lo muestres en tickets/chats):
sudo grep JWT_SECRET .env
```

Prueba de humo desde tu máquina con un túnel (requiere `seed_demo_data: true`):
```bash
ssh -L 18080:127.0.0.1:18080 <vps>
JWT_SECRET=<secreto> GATEWAY_URL=http://localhost:18080 node ../scripts/smoke.mjs
```

## Notas
- Los secretos nunca están en el repositorio; `.env` solo existe en el servidor. Para rotarlos, edita el `.env` del host y `docker compose up -d` (las contraseñas de PostgreSQL ya creadas requieren además `ALTER ROLE`).
- Cada cambio de `compose.yml` deja una copia con marca de tiempo (`backup: true`).
- Los volúmenes (`pgdata`, `kafkaN`, `traefik-acme`) sobreviven a `docker compose down`; **no** uses `down -v` en producción.
- Cortafuegos (UFW/grupo de seguridad) no se gestiona: abre 22, 80 y 443; no publiques PostgreSQL ni Kafka.
