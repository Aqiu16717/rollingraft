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

"${openssl_bin}" req -x509 -newkey rsa:2048 -nodes -sha256 -days 3650 \
  -subj "/CN=RollingRaft Client Test CA" \
  -keyout "${output_dir}/client_ca.key" \
  -out "${output_dir}/client_ca.crt" >/dev/null 2>&1

client_serial=101
for client_name in reader writer unknown; do
  "${openssl_bin}" req -new -newkey rsa:2048 -nodes -sha256 \
    -subj "/CN=rollingraft-client-${client_name}" \
    -keyout "${output_dir}/${client_name}.key" \
    -out "${output_dir}/${client_name}.csr" >/dev/null 2>&1
  printf 'subjectAltName=URI:rollingraft-client:%s\n' "${client_name}" \
    >"${output_dir}/${client_name}.ext"
  "${openssl_bin}" x509 -req -sha256 -days 3650 \
    -in "${output_dir}/${client_name}.csr" \
    -CA "${output_dir}/client_ca.crt" \
    -CAkey "${output_dir}/client_ca.key" \
    -set_serial "${client_serial}" \
    -extfile "${output_dir}/${client_name}.ext" \
    -out "${output_dir}/${client_name}.crt" >/dev/null 2>&1
  client_serial=$((client_serial + 1))
done

for client_name in mixed duplicate_client invalid_client; do
  "${openssl_bin}" req -new -newkey rsa:2048 -nodes -sha256 \
    -subj "/CN=rollingraft-client-${client_name}" \
    -keyout "${output_dir}/${client_name}.key" \
    -out "${output_dir}/${client_name}.csr" >/dev/null 2>&1
done

printf 'subjectAltName=URI:rollingraft-node:1,URI:rollingraft-client:writer\n' \
  >"${output_dir}/mixed.ext"
printf 'subjectAltName=URI:rollingraft-client:writer,URI:rollingraft-client:writer\n' \
  >"${output_dir}/duplicate_client.ext"
printf 'subjectAltName=URI:rollingraft-client:writer/admin\n' \
  >"${output_dir}/invalid_client.ext"

client_serial=201
for client_name in mixed duplicate_client invalid_client; do
  "${openssl_bin}" x509 -req -sha256 -days 3650 \
    -in "${output_dir}/${client_name}.csr" \
    -CA "${output_dir}/client_ca.crt" \
    -CAkey "${output_dir}/client_ca.key" \
    -set_serial "${client_serial}" \
    -extfile "${output_dir}/${client_name}.ext" \
    -out "${output_dir}/${client_name}.crt" >/dev/null 2>&1
  client_serial=$((client_serial + 1))
done

"${openssl_bin}" req -x509 -newkey rsa:2048 -nodes -sha256 -days 3650 \
  -subj "/CN=RollingRaft Rogue Client Test CA" \
  -keyout "${output_dir}/rogue_client_ca.key" \
  -out "${output_dir}/rogue_client_ca.crt" >/dev/null 2>&1
"${openssl_bin}" req -new -newkey rsa:2048 -nodes -sha256 \
  -subj "/CN=rollingraft-client-rogue" \
  -keyout "${output_dir}/rogue_client.key" \
  -out "${output_dir}/rogue_client.csr" >/dev/null 2>&1
printf 'subjectAltName=URI:rollingraft-client:rogue\n' >"${output_dir}/rogue_client.ext"
"${openssl_bin}" x509 -req -sha256 -days 3650 \
  -in "${output_dir}/rogue_client.csr" \
  -CA "${output_dir}/rogue_client_ca.crt" \
  -CAkey "${output_dir}/rogue_client_ca.key" \
  -set_serial 1 \
  -extfile "${output_dir}/rogue_client.ext" \
  -out "${output_dir}/rogue_client.crt" >/dev/null 2>&1
