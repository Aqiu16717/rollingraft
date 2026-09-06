#!/bin/sh

set -eu

output_dir=$1
openssl_bin=$2

"${openssl_bin}" req -x509 -newkey rsa:2048 -nodes -sha256 -days 3650 \
  -subj "/CN=RollingRaft Node Test CA" \
  -keyout "${output_dir}/node_ca.key" \
  -out "${output_dir}/node_ca.crt" >/dev/null 2>&1

for node_id in 1 2 3; do
  "${openssl_bin}" req -new -newkey rsa:2048 -nodes -sha256 \
    -subj "/CN=rollingraft-node-${node_id}" \
    -keyout "${output_dir}/node${node_id}.key" \
    -out "${output_dir}/node${node_id}.csr" >/dev/null 2>&1
  printf 'subjectAltName=URI:rollingraft-node:%s\n' "${node_id}" \
    >"${output_dir}/node${node_id}.ext"
  "${openssl_bin}" x509 -req -sha256 -days 3650 \
    -in "${output_dir}/node${node_id}.csr" \
    -CA "${output_dir}/node_ca.crt" \
    -CAkey "${output_dir}/node_ca.key" \
    -set_serial "${node_id}" \
    -extfile "${output_dir}/node${node_id}.ext" \
    -out "${output_dir}/node${node_id}.crt" >/dev/null 2>&1
done

"${openssl_bin}" req -x509 -newkey rsa:2048 -nodes -sha256 -days 3650 \
  -subj "/CN=RollingRaft Rogue Test CA" \
  -keyout "${output_dir}/rogue_ca.key" \
  -out "${output_dir}/rogue_ca.crt" >/dev/null 2>&1
"${openssl_bin}" req -new -newkey rsa:2048 -nodes -sha256 \
  -subj "/CN=rollingraft-node-3" \
  -keyout "${output_dir}/rogue_node3.key" \
  -out "${output_dir}/rogue_node3.csr" >/dev/null 2>&1
printf 'subjectAltName=URI:rollingraft-node:3\n' >"${output_dir}/rogue_node3.ext"
"${openssl_bin}" x509 -req -sha256 -days 3650 \
  -in "${output_dir}/rogue_node3.csr" \
  -CA "${output_dir}/rogue_ca.crt" \
  -CAkey "${output_dir}/rogue_ca.key" \
  -set_serial 1 \
  -extfile "${output_dir}/rogue_node3.ext" \
  -out "${output_dir}/rogue_node3.crt" >/dev/null 2>&1
