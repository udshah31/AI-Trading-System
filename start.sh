#!/bin/bash
set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Trading System - Startup Script${NC}"
echo -e "${GREEN}========================================${NC}"

# Check .env file
if [ ! -f .env ]; then
    echo -e "${RED}❌ .env file not found!${NC}"
    echo -e "${YELLOW}Copy .env.template to .env and fill in your keys:${NC}"
    echo "  cp .env.template .env"
    echo "  nano .env  # Edit with your API keys"
    exit 1
fi

# Load environment variables
source .env

# Check required variables
REQUIRED_VARS=("KRAKEN_API_KEY" "KRAKEN_API_SECRET" "GOOGLE_API_KEY")
MISSING=()

for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        MISSING+=("$var")
    fi
done

if [ ${#MISSING[@]} -ne 0 ]; then
    echo -e "${RED}❌ Missing required environment variables:${NC}"
    for var in "${MISSING[@]}"; do
        echo "  - $var"
    done
    echo -e "${YELLOW}Edit .env and add your API keys${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Environment validated${NC}"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker not installed${NC}"
    exit 1
fi

if ! docker-compose version &> /dev/null; then
    echo -e "${RED}❌ Docker Compose not installed${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Docker available${NC}"

# Start infrastructure first
echo -e "\n${GREEN}Starting infrastructure...${NC}"
docker-compose up -d postgres redis prometheus grafana

# Wait for health checks
echo -e "${YELLOW}Waiting for services to be healthy...${NC}"
for i in {1..30}; do
    if docker-compose ps postgres | grep -q "healthy" && \
       docker-compose ps redis | grep -q "healthy"; then
        echo -e "${GREEN}✅ Infrastructure ready${NC}"
        break
    fi
    sleep 2
    echo -n "."
done

# Start application services
echo -e "\n${GREEN}Starting trading agents...${NC}"
docker-compose up -d dashboard data-agent quant-agent risk-agent execution-agent orchestrator llm-analyst

# Optional: Start sniper bot
if [ "$ENABLE_SNIPER" = "true" ]; then
    echo -e "${GREEN}Starting sniper bot...${NC}"
    docker-compose --profile sniper up -d sniper-bot
fi

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}  System Started Successfully!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "📊 Dashboard:    http://localhost:8000"
echo "📈 Prometheus:   http://localhost:9090"
echo "📉 Grafana:      http://localhost:3000 (admin/admin)"
echo ""
echo "View logs:"
echo "  docker-compose logs -f                    # All services"
echo "  docker-compose logs -f quant-agent        # Specific service"
echo ""
echo "Stop system:"
echo "  docker-compose down"
