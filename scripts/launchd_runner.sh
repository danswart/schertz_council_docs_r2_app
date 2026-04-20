#!/bin/zsh

PROJECT_ROOT="/Users/D/R Working Directory/schertz_council_docs_r2_app"
LOG_DIR="$PROJECT_ROOT/logs"
STAMP=$(date +"%Y-%m-%d_%H-%M-%S")
LOG_FILE="$LOG_DIR/refresh_and_deploy_$STAMP.log"
RSCRIPT="/usr/local/bin/Rscript"

mkdir -p "$LOG_DIR"

cd "$PROJECT_ROOT" || exit 1

"$RSCRIPT" "$PROJECT_ROOT/refresh_and_deploy_local.R" >> "$LOG_FILE" 2>&1
