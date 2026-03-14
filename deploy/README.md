# EC2 deployment bundle for `trend.freechatee.com`

This folder contains copy-ready deployment assets for the first production rollout on AWS EC2.

Target shape:

- Region: `ap-southeast-1`
- Domain: `trend.freechatee.com`
- OS: `Ubuntu 24.04 LTS x86_64`
- Runtime: `Linux OpenD + Python venv + systemd + nginx + certbot`

## Files

- `config.ec2.yaml`: production-oriented app config template
- `stock-trend.env.example`: environment variables for Telegram
- `opend.env.example`: editable OpenD command template
- `systemd/opend.service`: systemd unit for OpenD
- `systemd/stock-trend.service`: systemd unit for the Python app
- `nginx/trend.freechatee.com.conf`: HTTP config for first boot / certbot
- `nginx/trend.freechatee.com.ssl.conf`: final HTTPS config if you prefer manual SSL wiring
- `scripts/bootstrap_ubuntu.sh`: installs packages and prepares directories
- `scripts/install_app.sh`: clones the repo and builds the Python venv
- `OPERATIONS.md`: post-deploy update, restart, and troubleshooting guide

## Recommended order

1. Launch the EC2 instance and point `trend.freechatee.com` to its Elastic IP.
2. Run `deploy/scripts/bootstrap_ubuntu.sh` on the EC2 instance as `root`.
3. Add the generated deploy key to GitHub, then run `deploy/scripts/install_app.sh`.
4. Copy:
   - `deploy/config.ec2.yaml` -> `/etc/stock-trend/config.yaml`
   - `deploy/stock-trend.env.example` -> `/etc/stock-trend/stock-trend.env`
   - `deploy/opend.env.example` -> `/etc/stock-trend/opend.env`
5. Edit the copied files for your real secrets and OpenD binary path.
6. Start OpenD manually first and confirm `127.0.0.1:11111` is listening.
7. Start the Python app manually and confirm `curl http://127.0.0.1:8088` works.
8. Install the systemd units and enable both services.
9. Copy the nginx config, test with `nginx -t`, then reload nginx.
10. Run `certbot --nginx -d trend.freechatee.com`.

## Copy commands

```bash
sudo cp deploy/config.ec2.yaml /etc/stock-trend/config.yaml
sudo cp deploy/stock-trend.env.example /etc/stock-trend/stock-trend.env
sudo cp deploy/opend.env.example /etc/stock-trend/opend.env
sudo cp deploy/systemd/opend.service /etc/systemd/system/opend.service
sudo cp deploy/systemd/stock-trend.service /etc/systemd/system/stock-trend.service
sudo cp deploy/nginx/trend.freechatee.com.conf /etc/nginx/sites-available/stock-trend
sudo ln -sf /etc/nginx/sites-available/stock-trend /etc/nginx/sites-enabled/stock-trend
```

## First checks

```bash
ss -lntp | grep 11111
curl -I http://127.0.0.1:8088
sudo systemctl status opend
sudo systemctl status stock-trend
sudo nginx -t
curl -I http://trend.freechatee.com
```

## Notes

- Keep `app.web.host` on `127.0.0.1`. nginx is the public entrypoint.
- Do not open ports `8088` or `11111` in the EC2 security group.
- `opend.env.example` intentionally requires editing because the Linux OpenD binary name and flags can vary by package version.
