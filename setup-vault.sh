#!/bin/bash

# Vault Configuration Script
# This script configures HashiCorp Vault for Airflow secret management

set -e

echo "🔐 Vault Configuration for Airflow"
echo "=================================="

# Check if .env exists and source it
if [ ! -f ".env" ]; then
    echo "❌ .env file not found!"
    echo "Please run ./setup.sh first"
    exit 1
fi

# Source environment variables
source .env

# Check if VAULT_ROOT_TOKEN is set
if [ -z "$VAULT_ROOT_TOKEN" ]; then
    echo "❌ VAULT_ROOT_TOKEN not set in .env file"
    echo "Please add VAULT_ROOT_TOKEN=your-token to .env"
    exit 1
fi

# Check if SLACK_TOKEN is set
if [ -z "$SLACK_TOKEN" ]; then
    echo "❌ SLACK_TOKEN not set in .env file"
    echo "Please add SLACK_TOKEN=xoxb-your-token to .env"
    exit 1
fi

echo "✅ Environment variables loaded"

# Check if Vault container is running
if ! docker ps | grep airflow-vault-1 > /dev/null; then
    echo "❌ Vault container is not running"
    echo "Please run ./setup.sh first"
    exit 1
fi

echo "✅ Vault container is running"

# Configure Vault
echo "🔧 Configuring Vault..."

# Login to Vault
echo "🔑 Logging into Vault..."
docker exec airflow-vault-1 vault login "$VAULT_ROOT_TOKEN"

# Enable KV secrets engine
echo "📦 Enabling KV secrets engine..."
docker exec airflow-vault-1 vault secrets enable -path=airflow -version=2 kv

# Add Slack token to Vault
echo "🔒 Adding Slack token to Vault..."
docker exec airflow-vault-1 vault kv put airflow/variables/slack_token value="$SLACK_TOKEN"

# Verify the secret was stored
echo "✅ Verifying secret storage..."
if docker exec airflow-vault-1 vault kv get airflow/variables/slack_token > /dev/null 2>&1; then
    echo "✅ Slack token successfully stored in Vault"
else
    echo "❌ Failed to store Slack token in Vault"
    exit 1
fi

echo ""
echo "🎉 Vault configuration complete!"
echo ""
echo "📝 Vault is now configured with:"
echo "• KV secrets engine enabled at path: airflow"
echo "• Slack token stored at: airflow/variables/slack_token"
echo ""
echo "🔍 You can verify the configuration:"
echo "docker exec -it airflow-vault-1 vault kv get airflow/variables/slack_token"
echo ""
echo "🌐 Vault UI available at: http://localhost:8200"
echo "🔑 Root token: $VAULT_ROOT_TOKEN"
