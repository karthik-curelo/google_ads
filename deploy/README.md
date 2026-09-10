# Deploying to a Compute Engine VM

Three files, one flow. This is for the "keep our own data reliably syncing"
deployment — internal/single-tenant use, not the multi-tenant SaaS roadmap.

## 1. Create the VM (you run this — not automated here)

```bash
gcloud compute instances create marketing-connectors \
  --project=YOUR_PROJECT \
  --zone=asia-south1-a \
  --machine-type=e2-small \
  --image-family=debian-12 --image-project=debian-cloud \
  --tags=marketing-connectors
```

Pick a zone in the same region as your Postgres instance (`asia-south1` —
matches `34.14.197.242`) to keep DB latency low. `e2-small` (2 vCPU burst,
2GB RAM) is comfortably enough for one API process + in-process scheduler at
this data volume; step up to `e2-medium` only if you see memory pressure.

**Firewall:** don't open port 8000 to the public internet unless you
specifically need external API access. If this only needs to serve you (a
dashboard, an internal tool), reach it via `gcloud compute ssh --tunnel...`
port-forwarding or put it behind an internal load balancer / Cloud IAP
instead of a public firewall rule.

## 2. Run the setup script on the VM

```bash
gcloud compute ssh marketing-connectors --zone=asia-south1-a
# on the VM:
curl -O https://raw.githubusercontent.com/karthik-curelo/google_ads/main/deploy/setup_vm.sh
chmod +x setup_vm.sh
./setup_vm.sh
```

This installs Python 3.11, clones the repo to `/opt/marketing-connectors`
under a dedicated `connectors` system user, creates the venv, installs
dependencies, and registers (but does not yet start) the systemd service.

## 3. Provide `.env`

The setup script deliberately does not create or fetch secrets. Copy
`.env.production.example`'s checklist onto a real, filled-in `.env`, then get
it onto the VM — `scp` it directly, or wire it to GCP Secret Manager and have
something populate `/opt/marketing-connectors/.env` from there before the
service starts. Either way it needs to exist, owned by the `connectors` user,
**before** step 4.

Remember: the two OAuth redirect URIs in that file must be re-registered in
the Google Cloud Console and Meta App dashboard to match the VM's real
public URL before anyone reconnects an integration from this host.

## 4. Migrate + start

```bash
cd /opt/marketing-connectors
sudo -u connectors ./.venv/bin/alembic upgrade head   # already run by setup_vm.sh if .env existed then
sudo systemctl start marketing-connectors
sudo systemctl status marketing-connectors
curl http://127.0.0.1:8000/healthz
```

`Restart=always` in the unit file means a crash or VM reboot brings it back
without intervention. `journalctl -u marketing-connectors -f` tails logs.

## Redeploying a code change

```bash
cd /opt/marketing-connectors
sudo -u connectors git pull
sudo -u connectors ./.venv/bin/pip install -e ".[postgres]"
sudo -u connectors ./.venv/bin/alembic upgrade head
sudo systemctl restart marketing-connectors
```
