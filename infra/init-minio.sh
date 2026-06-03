#!/usr/bin/env bash
# =============================================================================
# infra/init-minio.sh
#
# Standalone MinIO bucket initialisation script.
# Creates the `warehouse` and `checkpoints` buckets required by the CDC
# pipeline (Iceberg catalog and Flink checkpoint storage respectively).
#
# Usage:
#   MINIO_ROOT_USER=minioadmin \
#   MINIO_PASSWORD=minioadmin \
#   MINIO_ENDPOINT=http://minio:9000 \
#   ./infra/init-minio.sh
#
# Environment variables:
#   MINIO_ROOT_USER   MinIO root / access-key username  (required)
#   MINIO_PASSWORD    MinIO root / secret-key password  (required)
#   MINIO_ENDPOINT    MinIO S3 API endpoint             (default: http://minio:9000)
#
# Exit codes:
#   0  — all buckets created (or already existed)
#   1  — missing required environment variable
#   2  — MinIO did not become healthy within the wait timeout
#   3  — mc alias configuration failed
#   4  — one or more bucket operations failed
# =============================================================================

set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
ALIAS="minio-init-local"
BUCKETS=("warehouse" "checkpoints")

# Health-check retry settings
MAX_RETRIES=30
RETRY_INTERVAL=2   # seconds between retries

# ── Helpers ───────────────────────────────────────────────────────────────────

log()  { echo "[init-minio] $*"; }
info() { log "INFO  $*"; }
warn() { log "WARN  $*"; }
err()  { log "ERROR $*" >&2; }

# ── Validate required environment variables ───────────────────────────────────

if [[ -z "${MINIO_ROOT_USER:-}" ]]; then
  err "MINIO_ROOT_USER is not set. Export it before running this script."
  exit 1
fi

if [[ -z "${MINIO_PASSWORD:-}" ]]; then
  err "MINIO_PASSWORD is not set. Export it before running this script."
  exit 1
fi

info "MinIO endpoint : ${MINIO_ENDPOINT}"
info "Buckets to create: ${BUCKETS[*]}"

# ── Wait for MinIO to be healthy ──────────────────────────────────────────────

info "Waiting for MinIO to be healthy at ${MINIO_ENDPOINT} ..."

attempt=0
until curl -sf "${MINIO_ENDPOINT}/minio/health/live" > /dev/null 2>&1; do
  attempt=$(( attempt + 1 ))
  if [[ ${attempt} -ge ${MAX_RETRIES} ]]; then
    err "MinIO did not become healthy after $(( MAX_RETRIES * RETRY_INTERVAL )) seconds."
    err "Check that MinIO is running and that MINIO_ENDPOINT (${MINIO_ENDPOINT}) is correct."
    exit 2
  fi
  warn "MinIO not ready yet (attempt ${attempt}/${MAX_RETRIES}). Retrying in ${RETRY_INTERVAL}s ..."
  sleep "${RETRY_INTERVAL}"
done

info "MinIO is healthy."

# ── Configure mc alias ────────────────────────────────────────────────────────

info "Configuring mc alias '${ALIAS}' -> ${MINIO_ENDPOINT} ..."

if ! mc alias set "${ALIAS}" "${MINIO_ENDPOINT}" \
       "${MINIO_ROOT_USER}" "${MINIO_PASSWORD}" > /dev/null 2>&1; then
  err "Failed to configure mc alias. Verify credentials and endpoint."
  exit 3
fi

info "mc alias configured successfully."

# ── Create buckets ────────────────────────────────────────────────────────────

bucket_errors=0

for bucket in "${BUCKETS[@]}"; do
  info "Creating bucket '${bucket}' ..."

  if mc mb --ignore-existing "${ALIAS}/${bucket}"; then
    info "Bucket '${bucket}' is ready."
  else
    err "Failed to create bucket '${bucket}'."
    bucket_errors=$(( bucket_errors + 1 ))
  fi
done

# ── Optional: set anonymous read policy for Iceberg catalog access ────────────
#
# The Iceberg REST catalog uses path-style S3 access with explicit credentials,
# so public read is NOT required for normal operation. Uncomment the block below
# only if your catalog or query engine needs unauthenticated read access.
#
# for bucket in "${BUCKETS[@]}"; do
#   info "Setting public-read policy on '${bucket}' ..."
#   if mc anonymous set download "${ALIAS}/${bucket}"; then
#     info "Public-read policy set on '${bucket}'."
#   else
#     warn "Could not set public-read policy on '${bucket}' (non-fatal)."
#   fi
# done

# ── Summary ───────────────────────────────────────────────────────────────────

if [[ ${bucket_errors} -gt 0 ]]; then
  err "${bucket_errors} bucket operation(s) failed. Review the output above."
  exit 4
fi

info "All MinIO buckets are ready: ${BUCKETS[*]}"
exit 0
