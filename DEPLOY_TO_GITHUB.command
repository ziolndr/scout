#!/bin/bash
set -euo pipefail

SOURCE_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_SLUG="${SCOUT_GITHUB_REPO:-ziolndr/scout}"
REPO_WEB="${SCOUT_GITHUB_WEB:-https://github.com/$REPO_SLUG}"
BRANCH="${SCOUT_GITHUB_BRANCH:-main}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/scout-github-deploy.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

command -v git >/dev/null || { echo "git is required"; exit 1; }
command -v rsync >/dev/null || { echo "rsync is required"; exit 1; }

# Never let git fall back to terminal username/password prompts.
export GIT_TERMINAL_PROMPT=0

if [[ -n "${SCOUT_GITHUB_REMOTE:-}" ]]; then
  REPO_URL="$SCOUT_GITHUB_REMOTE"
elif command -v gh >/dev/null 2>&1; then
  if ! gh auth status --hostname github.com >/dev/null 2>&1; then
    echo "GitHub authentication is not configured. Opening secure browser authentication..."
    gh auth login --hostname github.com --git-protocol https --web
  fi
  gh auth setup-git --hostname github.com >/dev/null
  REPO_URL="https://github.com/$REPO_SLUG.git"
  echo "authentication: GitHub CLI"
else
  REPO_URL="git@github.com:$REPO_SLUG.git"
  export GIT_SSH_COMMAND="ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new"
  echo "authentication: SSH key"
fi

printf '\nSCOUT GITHUB DEPLOY\n'
printf '────────────────────────────────────────────────────────\n'
printf 'source: %s\n' "$SOURCE_DIR"
printf 'remote: %s\n' "$REPO_URL"
printf 'branch: %s\n\n' "$BRANCH"

REPO="$WORK/repo"
if ! git clone "$REPO_URL" "$REPO"; then
  echo
  echo "GitHub authentication failed without prompting for a username."
  if ! command -v gh >/dev/null 2>&1; then
    echo "Install GitHub CLI with: brew install gh"
    echo "Then run this deployer again; it will open browser authentication."
  else
    echo "Run: gh auth login --hostname github.com --git-protocol https --web"
  fi
  exit 1
fi
cd "$REPO"

if git show-ref --verify --quiet "refs/remotes/origin/$BRANCH"; then
  git checkout -B "$BRANCH" "origin/$BRANCH"
else
  git checkout -B "$BRANCH"
fi

# Preserve the currently deployed branch before replacing its working tree.
if git rev-parse --verify HEAD >/dev/null 2>&1; then
  BACKUP_BRANCH="backup/pre-scout-$STAMP"
  git push origin "HEAD:refs/heads/$BACKUP_BRANCH"
  echo "Remote backup created: $BACKUP_BRANCH"
fi

rsync -a --delete \
  --exclude '.git/' \
  --exclude '.DS_Store' \
  "$SOURCE_DIR/" "$REPO/"

chmod +x ./*.command 2>/dev/null || true

# GitHub serves index.html naturally when Pages is enabled.
cp SCOUT_massive_artist_field.html index.html
cp README_SCOUT_ARTIST_FIELD.md README.md

git add -A
if git diff --cached --quiet; then
  echo "Repository already contains this exact SCOUT build."
else
  git config user.name "${GIT_AUTHOR_NAME:-Joel Trout}"
  git config user.email "${GIT_AUTHOR_EMAIL:-ziolndr@users.noreply.github.com}"
  git commit -m "Deploy massive pre-embedded SCOUT artist field"
fi

git push origin "HEAD:$BRANCH"

echo
echo "SCOUT DEPLOYED"
echo "$REPO_WEB"
if [[ "$REPO_WEB" == https://* ]] && command -v open >/dev/null; then
  open "$REPO_WEB"
fi
