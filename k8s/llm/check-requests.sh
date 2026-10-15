#!/bin/sh
# The requests check.sh sends, run inside the cluster in a curl pod. Expects the
# AGENTS_KEY and BENCHMARK_KEY environment variables. Exits 1 if any check fails.
set -u
gw=http://llm-gateway.llm.svc.cluster.local:8080/v1/chat/completions
fail=0

# key, model, prompt, max_tokens -> prints the status code; the body is in /tmp/body
call() {
  curl -s -o /tmp/body -w '%{http_code}' "$gw" \
    -H "Authorization: Bearer $1" -H 'Content-Type: application/json' \
    -d "{\"model\":\"$2\",\"max_tokens\":${4:-8},\"temperature\":0,\"messages\":[{\"role\":\"user\",\"content\":\"$3\"}]}"
}
expect() {
  if [ "$3" = "$2" ]; then echo "ok   $1 ($3)"; else echo "FAIL $1: wanted $2, got $3: $(head -c 200 /tmp/body 2>/dev/null)"; fail=1; fi
}

expect "no key" 401 "$(curl -s -o /tmp/body -w '%{http_code}' "$gw" -H 'Content-Type: application/json' \
  -d '{"model":"qwen3-4b","messages":[{"role":"user","content":"hi"}]}')"
expect "wrong key" 401 "$(call not-a-key qwen3-4b hi)"
expect "unknown model" 404 "$(call "$AGENTS_KEY" gpt-4 hi)"
expect "agents may not use qwen3-1.7b" 403 "$(call "$AGENTS_KEY" qwen3-1.7b hi)"
expect "agents on qwen3-4b" 200 "$(call "$AGENTS_KEY" qwen3-4b 'Say OK. /no_think')"
if grep -q '"model":"qwen3-4b"' /tmp/body; then echo "ok   the reply names qwen3-4b"
else echo "FAIL the reply does not name qwen3-4b: $(head -c 200 /tmp/body)"; fail=1; fi

# The gateway times the first token and each token after it only on streamed replies.
expect "a streamed reply" 200 "$(curl -s -o /tmp/body -w '%{http_code}' "$gw" \
  -H "Authorization: Bearer $BENCHMARK_KEY" -H 'Content-Type: application/json' \
  -d '{"model":"qwen3-4b","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"Count to five. /no_think"}]}')"

# The card number is masked before the model sees it, so the model cannot repeat it.
# check.sh also confirms the gateway counted a mask, so a model that simply refused
# to repeat cannot pass this.
expect "a prompt with a card number" 200 \
  "$(call "$BENCHMARK_KEY" qwen3-4b 'Repeat exactly, nothing else: my card is 4111 1111 1111 1111 /no_think' 40)"
if grep -q '4111' /tmp/body; then echo "FAIL the card number reached the model: $(head -c 200 /tmp/body)"; fail=1
else echo "ok   the card number did not reach the model"; fi

# Last, because it uses up the agents' allowance for a minute. The 40 requests go out
# together: one after another, the 30-a-minute bucket would refill between them.
for i in $(seq 40); do
  curl -s -o /dev/null -w '%{http_code}\n' "$gw" -H "Authorization: Bearer $AGENTS_KEY" \
    -H 'Content-Type: application/json' \
    -d '{"model":"qwen3-4b","max_tokens":1,"messages":[{"role":"user","content":"hi"}]}' > "/tmp/code.$i" &
done
wait
limited=$(cat /tmp/code.* | grep -c '^429$' || true)
if [ "$limited" -gt 0 ]; then echo "ok   request limit ($limited of 40 got 429)"
else echo "FAIL request limit: none of 40 requests got 429"; fail=1; fi

exit $fail
