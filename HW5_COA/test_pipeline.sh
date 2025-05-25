#!/usr/bin/env bash
set -euo pipefail

BASE_URL="http://localhost:3000"
RAND=$(date +%s)
LOGIN="user_${RAND}"
EMAIL="${LOGIN}@example.com"
PASSWORD="p@ssw0rd"

echo "=== 1) Register new user…"
REG_RESP=$(curl -s -X POST "$BASE_URL/users/register" \
  -H 'Content-Type: application/json' \
  -d "{\"login\":\"$LOGIN\",\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" \
  || true)
echo "$REG_RESP" | jq .
if jq -e '.detail == "User with this login or email already exists"' <<<"$REG_RESP" &>/dev/null; then
  echo "→ user exists, skipping registration"
fi
echo

echo "=== 2) Login and get JWT…"
TOKEN=$(curl -s -X POST "$BASE_URL/users/login" \
  -H 'Content-Type: application/json' \
  -d "{\"login\":\"$LOGIN\",\"password\":\"$PASSWORD\"}" \
  | jq -r '.token // empty')
if [ -z "$TOKEN" ]; then
  echo "ERROR: did not receive token, aborting"
  exit 1
fi
echo "→ token = $TOKEN"
echo

echo "=== 3) Get profile…"
curl -s -X GET "$BASE_URL/users/profile" \
  -H "Authorization: Bearer $TOKEN" \
  | jq .
echo

echo "=== 4) Create a post…"
POST_JSON=$(curl -s -X POST "$BASE_URL/posts" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"title":"My first post","description":"Hello world","isPrivate":false,"tags":["test","bash"]}')
echo "$POST_JSON" | jq .
POST_ID=$(jq -r '.id // empty' <<<"$POST_JSON")
if [ -z "$POST_ID" ]; then
  echo "ERROR: did not receive post id, aborting"
  exit 1
fi
echo "→ postId = $POST_ID"
echo

echo "=== 5) View the post…"
curl -s -X POST "$BASE_URL/posts/$POST_ID/view" \
  -H "Authorization: Bearer $TOKEN" \
  | jq .
echo

echo "=== 6) Like the post…"
curl -s -X POST "$BASE_URL/posts/$POST_ID/like" \
  -H "Authorization: Bearer $TOKEN" \
  | jq .
echo

echo "=== 7) Comment the post…"
curl -s -X POST "$BASE_URL/posts/$POST_ID/comments" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"content":"Nice post!"}' \
  | jq .
echo

# даём время на асинхронную обработку
sleep 2

echo "=== 8) Stats totals…"
curl -s -X GET "$BASE_URL/stats/posts/$POST_ID/totals" | jq .
echo

echo "=== 9) Stats time series…"
curl -s -X GET "$BASE_URL/stats/posts/$POST_ID/views"    | jq .
curl -s -X GET "$BASE_URL/stats/posts/$POST_ID/likes"    | jq .
curl -s -X GET "$BASE_URL/stats/posts/$POST_ID/comments" | jq .
echo

echo "=== 10) Top posts by views…"
curl -s -X GET "$BASE_URL/stats/top/posts?metric=VIEWS" | jq .
echo

echo "=== 11) Top users by likes…"
curl -s -X GET "$BASE_URL/stats/top/users?metric=LIKES" | jq .
echo
