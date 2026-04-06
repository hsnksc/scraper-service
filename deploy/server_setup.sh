#!/usr/bin/env bash
# deploy/server_setup.sh
# Sunucuda bir kez çalıştırılır. Ubuntu 22.04 / Debian 12 varsayımı.
set -euo pipefail

REPO_DIR="/opt/scraper"
GITHUB_REPO="GITHUB_KULLANICI_ADI/REPO_ADI"   # <-- değiştir

echo "=== 1. Docker kurulumu ==="
if ! command -v docker &>/dev/null; then
  curl -fsSL https://get.docker.com | sh
  systemctl enable docker
  systemctl start docker
fi

echo "=== 2. loca_network oluştur ==="
docker network inspect loca_network &>/dev/null || docker network create loca_network

echo "=== 3. Repo klonla ==="
if [ ! -d "$REPO_DIR/.git" ]; then
  git clone "https://github.com/${GITHUB_REPO}.git" "$REPO_DIR"
fi

echo "=== 4. .env dosyasını oluştur ==="
if [ ! -f "$REPO_DIR/deploy/.env" ]; then
  cp "$REPO_DIR/deploy/.env.example" "$REPO_DIR/deploy/.env"
  echo ""
  echo ">>> deploy/.env dosyasını API anahtarlarınızla doldurun:"
  echo "    nano $REPO_DIR/deploy/.env"
  echo ""
fi

echo "=== 5. İlk build & başlat ==="
cd "$REPO_DIR"
docker compose -f deploy/docker-compose.yml up -d --build

echo ""
echo "Kurulum tamamlandı. Servis: http://$(hostname -I | awk '{print $1}'):8010/health"
