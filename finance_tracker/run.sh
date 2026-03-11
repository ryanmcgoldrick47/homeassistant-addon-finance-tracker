#!/usr/bin/with-contenv bashio

# Ensure data directory exists
mkdir -p /config/finance_tracker

# Pull Anthropic API key from add-on options if set
ANTHROPIC_KEY=$(bashio::config 'anthropic_api_key' || true)
if [ -n "$ANTHROPIC_KEY" ]; then
    export ANTHROPIC_API_KEY="$ANTHROPIC_KEY"
fi

export DATABASE_URL="sqlite:////config/finance_tracker/finance.db"

exec uvicorn main:app \
    --host 0.0.0.0 \
    --port 8099 \
    --workers 1 \
    --log-level info
