#!/usr/bin/env bash
# OPCIONAL. Replica las colas pedidos360.* en los dos nodos (ha-mode: all).
# Sin esta política las colas son clásicas y cada una vive solo en el nodo donde se declaró.
#
# Uso (desde backend-pedidos360, con el clúster arriba):  bash rabbitmq/ha-policy.sh
set -euo pipefail

NODO="${RABBITMQ_CONTAINER:-rabbitmq1}"

# En la EC2 docker suele requerir sudo
dk() { if docker info >/dev/null 2>&1; then docker "$@"; else sudo docker "$@"; fi; }

dk exec "$NODO" rabbitmqctl set_policy ha-pedidos360 "^pedidos360\." \
  '{"ha-mode":"all","ha-sync-mode":"automatic"}' --apply-to queues --priority 0

echo
dk exec "$NODO" rabbitmqctl list_policies
