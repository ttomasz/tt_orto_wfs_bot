[![run main.py](https://github.com/ttomasz/tt_orto_wfs_bot/actions/workflows/actions.yaml/badge.svg?branch=main)](https://github.com/ttomasz/tt_orto_wfs_bot/actions/workflows/actions.yaml)

# tt_orto_wfs_bot
Script that checks OGC WFS for new elements and sends Discord message via webhook

# Running locally
You can it with uv from Astral:
```sh
uv run --with 'python-dotenv' main.py
```

.env example:
```
WEBHOOK_URL=https://discord.com/api/webhooks/123....
```
