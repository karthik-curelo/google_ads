#!/usr/bin/env bash
# One-time setup for a fresh Debian/Ubuntu Compute Engine VM.
# Run as a user with sudo. Does NOT create the VM itself — run this ON it,
# after `gcloud compute instances create` (or via its --metadata-from-file
# startup-script= flag for full automation).
#
# What this does NOT do, on purpose:
#   - does not generate or fetch secrets — you provide a filled-in .env
#     yourself (copy .env.production.example, fill it in, scp it over, or
#     wire it to GCP Secret Manager separately)
#   - does not open any firewall port — do that with
#       gcloud compute firewall-rules create ...
#     scoped to exactly what needs to reach port 8000 (ideally nothing
#     public; put a load balancer or SSH tunnel in front instead)
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/karthik-curelo/google_ads.git}"
APP_DIR="/opt/marketing-connectors"
APP_USER="connectors"

echo "==> System packages"
sudo apt-get update -y
sudo apt-get install -y python3.11 python3.11-venv git

echo "==> Service account/user (no login shell, no home dir writes needed)"
if ! id -u "$APP_USER" >/dev/null 2>&1; then
  sudo useradd --system --create-home --shell /usr/sbin/nologin "$APP_USER"
fi

echo "==> Clone/update repo"
if [ -d "$APP_DIR/.git" ]; then
  sudo -u "$APP_USER" git -C "$APP_DIR" pull
else
  sudo git clone "$REPO_URL" "$APP_DIR"
  sudo chown -R "$APP_USER:$APP_USER" "$APP_DIR"
fi

echo "==> Virtualenv + dependencies"
sudo -u "$APP_USER" python3.11 -m venv "$APP_DIR/.venv"
sudo -u "$APP_USER" "$APP_DIR/.venv/bin/pip" install --upgrade pip
sudo -u "$APP_USER" "$APP_DIR/.venv/bin/pip" install -e "$APP_DIR[postgres]"

if [ ! -f "$APP_DIR/.env" ]; then
  echo ""
  echo "!! $APP_DIR/.env does not exist yet."
  echo "!! Copy .env.production.example's checklist onto a filled-in .env"
  echo "!! (scp it over, or fetch from your secret store) before starting the service."
  echo ""
else
  echo "==> Running migrations (alembic upgrade head)"
  sudo -u "$APP_USER" bash -c "cd $APP_DIR && ./.venv/bin/alembic upgrade head"
fi

echo "==> Installing systemd unit"
sudo cp "$APP_DIR/deploy/marketing-connectors.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable marketing-connectors

echo ""
echo "Setup done. Once .env is in place and migrations have run:"
echo "  sudo systemctl start marketing-connectors"
echo "  sudo systemctl status marketing-connectors"
echo "  journalctl -u marketing-connectors -f          # tail logs"
echo "  curl http://127.0.0.1:8000/healthz             # confirm it's up"
