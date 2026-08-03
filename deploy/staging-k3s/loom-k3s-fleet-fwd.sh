#!/bin/bash
# socat forward for the GB10 fleet's kind->k3s connectivity bridge. Argument is
# "<listen_port>-<target_port>"; forwards bb8-1:<listen> -> the k3s router
# hostPort <target> on ${K3S_INGRESS_IP}. Run per-instance by
# loom-k3s-fleet-fwd@.service.
#
# The GB10 nodes SSH-tunnel their local :18081/:19000/:19100 to bb8-1; these
# forwards route that traffic to the deployment-managed routers instead of the
# legacy kind cluster:
#   18081,18082 -> 30080  worker-router (control-plane)
#   19000       -> 30900  minio-router  (object store)
#   19100       -> 30443  gateway-router (llm-gateway)
# INTERIM: the durable form is nodes dialing the router hostPorts directly
# (#906 / node-agent), retiring this host-side hub.
set -euo pipefail
K3S_INGRESS_IP="${K3S_INGRESS_IP:-192.168.50.103}"
listen="${1%%-*}"; target="${1##*-}"
exec /usr/bin/socat -d "TCP-LISTEN:${listen},reuseaddr,fork" "TCP:${K3S_INGRESS_IP}:${target}"
