#!/usr/bin/env sh
set -eu

leader_port=""
attempt=1

while [ "$attempt" -le 30 ]; do
  for port in 8080 8081 8082; do
    status=$(curl --silent --max-time 1 "http://localhost:${port}/status" || true)
    if printf '%s' "$status" | grep --quiet '"role":"LEADER"'; then
      leader_port="$port"
      break
    fi
  done
  if [ -n "$leader_port" ]; then
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done

if [ -z "$leader_port" ]; then
  echo "No leader was found. Check: docker compose logs"
  exit 1
fi

echo "Leader is listening on localhost:${leader_port}"
curl --silent \
  --header 'Content-Type: application/json' \
  --data '{"tickets":5}' \
  "http://localhost:${leader_port}/tickets"
echo

for port in 8080 8081 8082; do
  curl --silent "http://localhost:${port}/status"
  echo
done
