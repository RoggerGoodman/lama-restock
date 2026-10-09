# lama-restock

LamApp Restock: a Django web app that automates restocking for supermarkets.
It tracks sales and deliveries per product, computes order quantities and
submits orders to the supplier (Dropzone) over HTTP. It also covers inventory,
losses, margins and recipes.

Production runs at `lamarestock.com`.

## Folder map

```
LamApp/                      Django project (manage.py lives here)
  LamApp/                    Project config: settings, urls, celery beat schedule
  supermarkets/              The single Django app; almost all code lives here
    models.py                Django models (supermarkets, storages, logs, schedules...)
    views/                   Web pages, one file per area (inventory, restock, losses...)
    urls.py                  URL routes
    tasks.py                 Celery background tasks (scheduled and on-demand)
    automation_services.py   Restock pipeline: update stats -> decide -> order
    services.py              Shared business logic used by views and tasks
    scripts/                 Core engine: DB access, order maths, Dropzone client
    management/commands/     manage.py commands (link_products, seed_demo, ...)
    templates/               HTML templates, grouped by area
    static/                  JS, icons, equipment catalog images
    migrations/              Django schema migrations
.github/workflows/deploy.yml Auto-deploy on push to main
requirements.txt             Python dependencies
```

Data lives in two places: Django's own tables, and a PostgreSQL database with
one schema per supermarket (`products`, `product_stats`, `economics`,
`extra_losses`), accessed through `scripts/DatabaseManager.py`.

## Running locally (Windows)

```
python -m venv env
.\env\Scripts\python.exe -m pip install -r requirements.txt
cd LamApp
..\env\Scripts\python.exe manage.py runserver
```

Needs `LamApp/LamApp/settings.py` (not in git) and a `.env.production` file
next to `LamApp/` with `SECRET_KEY`, `DEBUG`, `ALLOWED_HOSTS`, `DATABASE_URL`
and the `PG_*` variables for the product database. Background tasks also need
Redis and a Celery worker.

## Deploying

Pushing to `main` deploys automatically: the server pulls, installs
requirements, runs migrations, collects static files and restarts gunicorn.
Celery is **not** restarted, so after a change to tasks or the order logic,
restart the Celery services on the server by hand.

## Not in git

- `settings.py`: hand-edited per machine
- Migrations from 0006 on: created and kept on the server (`makemigrations` runs there)
- `logs/`: runtime logs, one folder per supermarket
- `_local/`: personal scratch scripts and notes
- `docs/`: the Italian user manual and its screenshots
