#!/usr/bin/env bash
set -e

echo "=== Installing Databricks AI Dev Kit ==="

# 1. Перевірка наявності Databricks CLI
if ! command -v databricks &> /dev/null; then
    echo "Databricks CLI not found. Installing..."
    curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
fi

# 2. Встановлення пакетів AI Dev Kit та MCP
pip install --upgrade "databricks-sdk" "mcp"

# 3. Клонування/підключення AI Dev Kit
if [ ! -d ".ai-dev-kit" ]; then
    git clone https://github.com/databricks-solutions/ai-dev-kit.git .ai-dev-kit
fi

echo "AI Dev Kit installed successfully."
echo "Configuring MCP server profile for Databricks workspace..."