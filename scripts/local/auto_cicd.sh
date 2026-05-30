#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

VENV_DIR="${VENV_DIR:-$ROOT/.venv-cicd}"
PYTHON_VERSION="${PYTHON_VERSION:-3.11}"
PYTHON_BIN="${PYTHON:-$VENV_DIR/bin/python}"
POLL_SECONDS="${POLL_SECONDS:-1}"
CHECK_LOOP_SECONDS="${CHECK_LOOP_SECONDS:-1}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-900}"

LOG_DIR="${LOG_DIR:-$ROOT/logs/cicd}"
mkdir -p "$LOG_DIR"
TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/auto_cicd_${TS}.log"

exec > >(
  # shellcheck disable=SC2005
  tee -a "$LOG_FILE"
) 2>&1

echo "[auto_cicd] root=$ROOT"
echo "[auto_cicd] log=$LOG_FILE"

flake8_cmd=(
  "$PYTHON_BIN" -m flake8 .
  --exclude=venv,.venv,.venv-cicd,vendor,data,ml/notebooks,node_modules,dashboard/frontend/node_modules
  --max-line-length=120
  --extend-ignore=E203,W503,W605
)

black_exclude='(\.git|\.venv|\.venv-cicd|venv|vendor|data|ml/notebooks|node_modules|dashboard/frontend/node_modules)'

run_checks_once() {
  echo "[auto_cicd] black (format check)"
  "$PYTHON_BIN" -m black --check . --exclude="$black_exclude"

  echo "[auto_cicd] flake8"
  "${flake8_cmd[@]}"

  if [ -d tests ]; then
    echo "[auto_cicd] pytest"
    "$PYTHON_BIN" -m pytest -q
  else
    echo "[auto_cicd] pytest skipped (no ./tests)"
  fi
}

ensure_tooling() {
  echo "[auto_cicd] ensure tooling (uv/venv/black/flake8/pytest)"
  if ! command -v uv >/dev/null 2>&1; then
    echo "[auto_cicd] uv not found; cannot bootstrap tooling"
    return 1
  fi

  if [ ! -x "$PYTHON_BIN" ]; then
    echo "[auto_cicd] create venv: $VENV_DIR (python=$PYTHON_VERSION)"
    uv venv "$VENV_DIR" --python "$PYTHON_VERSION" --seed --clear
  fi

  if [ ! -x "$PYTHON_BIN" ]; then
    echo "[auto_cicd] venv python still missing after uv venv: $PYTHON_BIN"
    return 1
  fi

  uv pip install -q -p "$PYTHON_BIN" --upgrade \
    "black==24.3.0" \
    "flake8==7.3.0" \
    "pytest" \
    "platformdirs" \
    "python-dotenv" \
    "numpy" \
    "pandas" \
    "requests" \
    "psycopg2-binary" \
    "pydantic" \
    "pydantic-settings" \
    "fastapi" \
    "email-validator"
}

auto_fix_once() {
  echo "[auto_cicd] black (auto-fix)"
  "$PYTHON_BIN" -m black . --exclude="$black_exclude"
}

commit_and_push() {
  local msg="${1:-}"

  git add -A
  if git diff --cached --quiet; then
    echo "[auto_cicd] no staged changes; skip commit"
  else
    if [ -z "$msg" ]; then
      msg="chore(ci): auto push $(date -Iseconds)"
    fi
    echo "[auto_cicd] commit: $msg"
    git commit -m "$msg"
  fi

  echo "[auto_cicd] push (origin)"
  git push origin HEAD
}

parse_github_repo() {
  local url
  url="$(git remote get-url origin)"
  # Supports:
  # - git@github.com:OWNER/REPO.git
  # - https://github.com/OWNER/REPO.git
  if [[ "$url" =~ ^git@github\.com:([^/]+)/([^/.]+)(\.git)?$ ]]; then
    echo "${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
    return 0
  fi
  if [[ "$url" =~ ^https://github\.com/([^/]+)/([^/.]+)(\.git)?$ ]]; then
    echo "${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
    return 0
  fi
  return 1
}

poll_github_actions() {
  local sha branch repo api token_header started elapsed run_id status conclusion html_url
  sha="$(git rev-parse HEAD)"
  branch="$(git rev-parse --abbrev-ref HEAD)"
  repo="$(parse_github_repo || true)"

  if [ -z "$repo" ]; then
    echo "[auto_cicd] cannot parse GitHub repo from origin; skip Actions polling"
    return 0
  fi

  api="https://api.github.com/repos/${repo}"
  token_header=""
  if [ -n "${GITHUB_TOKEN:-}" ]; then
    token_header="Authorization: Bearer ${GITHUB_TOKEN}"
  fi

  echo "[auto_cicd] poll GitHub Actions for sha=$sha branch=$branch repo=$repo (interval=${POLL_SECONDS}s, max=${MAX_WAIT_SECONDS}s)"
  started="$(date +%s)"

  while true; do
    elapsed="$(( $(date +%s) - started ))"
    if [ "$elapsed" -gt "$MAX_WAIT_SECONDS" ]; then
      echo "[auto_cicd] timeout waiting for GitHub Actions run (elapsed=${elapsed}s)"
      return 2
    fi

    run_id="$(
      curl -fsSL \
        -H "Accept: application/vnd.github+json" \
        ${token_header:+-H "$token_header"} \
        "${api}/actions/runs?per_page=30&branch=${branch}&event=push" \
      | jq -r --arg sha "$sha" '.workflow_runs[] | select(.head_sha==$sha) | .id' \
      | head -n 1
    )"

    if [ -z "$run_id" ] || [ "$run_id" = "null" ]; then
      sleep "$POLL_SECONDS"
      continue
    fi

    status="$(
      curl -fsSL \
        -H "Accept: application/vnd.github+json" \
        ${token_header:+-H "$token_header"} \
        "${api}/actions/runs/${run_id}" \
      | jq -r '.status'
    )"

    conclusion="$(
      curl -fsSL \
        -H "Accept: application/vnd.github+json" \
        ${token_header:+-H "$token_header"} \
        "${api}/actions/runs/${run_id}" \
      | jq -r '.conclusion'
    )"

    html_url="$(
      curl -fsSL \
        -H "Accept: application/vnd.github+json" \
        ${token_header:+-H "$token_header"} \
        "${api}/actions/runs/${run_id}" \
      | jq -r '.html_url'
    )"

    echo "[auto_cicd] run_id=$run_id status=$status conclusion=$conclusion url=$html_url"
    if [ "$status" = "completed" ]; then
      if [ "$conclusion" = "success" ]; then
        return 0
      fi
      return 3
    fi

    sleep "$POLL_SECONDS"
  done
}

main() {
  echo "[auto_cicd] starting"
  ensure_tooling
  auto_fix_once || true

  until run_checks_once; do
    echo "[auto_cicd] checks failed; sleeping ${CHECK_LOOP_SECONDS}s then re-running"
    sleep "$CHECK_LOOP_SECONDS"
    auto_fix_once || true
  done

  commit_and_push "${1:-}"
  poll_github_actions
}

main "${@-}"
