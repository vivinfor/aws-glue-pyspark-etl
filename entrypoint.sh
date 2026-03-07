#!/usr/bin/env sh
set -eu

ENV_NAME="${ENV:-prd}"
SA_KEY="${SA_KEY_FILE:-}"

echo ">>> Entrypoint (ENV=${ENV_NAME})"

if [ "$ENV_NAME" = "dev" ]; then
  if [ -n "$SA_KEY" ] && [ -f "$SA_KEY" ]; then
    echo "Usando SA key em $SA_KEY"
    export GOOGLE_APPLICATION_CREDENTIALS="$SA_KEY"
  else
    echo "SA key não encontrada (SA_KEY_FILE=$SA_KEY)"
    echo "Se precisar de ADC local: gcloud auth application-default login"
  fi
else
  echo "Cloud/PRD - usando ADC do runtime (Workload Identity/SA do Cloud Run)"
fi

exec "$@"
