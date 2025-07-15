#!/bin/bash

# Airflow Setup Script
# This script helps initialize the Airflow environment with Vault integration

set -e

echo "🚀 Airflow with Vault Setup Script"
echo "=================================="

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "❌ .env file not found!"
    echo "📋 Please copy .env.example to .env and configure your values:"
    echo "   cp .env.example .env"
    echo "   # Edit .env with your actual values"
    exit 1
fi

echo "✅ Found .env file"

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

echo "✅ Docker is running"

# Start services
echo "🐳 Starting Docker services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 30

# Check if Vault is ready
echo "🔐 Checking Vault status..."
if docker exec airflow-vault-1 vault status > /dev/null 2>&1; then
    echo "✅ Vault is ready"
else
    echo "⚠️  Vault may still be starting. You can check manually with:"
    echo "   docker exec -it airflow-vault-1 vault status"
fi

# Check if Airflow is ready
echo "🌬️  Checking Airflow status..."
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "✅ Airflow is ready"
else
    echo "⚠️  Airflow may still be starting. Check at: http://localhost:8080"
fi

echo ""
echo "🎉 Setup complete!"
echo ""
echo "📝 Next steps:"
echo "1. Configure Vault secrets:"
echo "   ./setup-vault.sh"
echo ""
echo "2. Access Airflow UI:"
echo "   URL: http://localhost:8080"
echo "   Username: airflow"
echo "   Password: airflow"
echo ""
echo "3. Check service status:"
echo "   docker-compose ps"
echo ""
echo "4. View logs:"
echo "   docker-compose logs -f [service_name]"
