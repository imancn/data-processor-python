# Data Processor (Modular ETL Framework)

A lightweight, modular ETL/EL framework for extracting data from HTTP (or other sources), transforming it with Pandas, and loading it into ClickHouse (or other sinks). Pipelines are composable and can be scheduled as cron jobs.

## Architecture

- `src/core` – configuration (`config.py`), logging
- `src/pipelines` – pipeline implementations, `pipeline_factory.py`, `pipeline_registry.py`
- `src/pipelines/tools` – reusable extractors, transformers, loaders
- `migrations/sql` – ClickHouse DDL migrations (optional)
- `scripts/run.py` – pipeline runner (list/run by name)
- `run.sh` – helper for local tasks
- `deploy.sh` – one-shot deploy (rsync + venv + migrate + seed + crons)
 - `docs/architecture/class_diagram.puml` – UML diagram

### UML Diagram

![Architecture UML](https://www.plantuml.com/plantuml/png/ZLVTRjis5BxNKn2vw9JEKTAkxG0Z25eawP8XwO8umz0hWQQE9SuKgP97JhskmAxs0DiRzab6Kf95Mr6Mv4BKET_7yo_IRmrIZNKf8Yg4kY_vPoxpo2ovhAYc9IcLLApGgWI2keP0Kr6sf3dCS2s0Q618PIv201FKc7U8cizhX4kcv8p_0UGeWDnhMlt6Cop8orT7KOGS1P0pYfJSQCN06AN9jx_-9UyqApW2mre3UKPELLaf2H9D5BLk4AQiwaj46hCM8XSyzm92lXUAPYd8LRhW0k9kaLOKY_q6aGF6IrXFI0OBMWlinIaJd2qBLHj8cTyr3fKbIsd5TpH2vQc05OZ-rHcqRryt7bwfFETouw81hAjKDCBOaoV4aO8jxpaMAV8I3DAoYktJq1mcX7Nvc11FtFU6llR9gNW5Waim2IebJ0AVKLE6IXj23UdVWWHU85RddQ9paccW26bXTUc65TVQkbe0pK2R2V6LjKz9I_2Ez3LnYRnmYHpPE4nrl_sd6lIwt-_Hm2n7ceNI9JIXXUz1S7Uxuju5GSjvHjE062zaRxq7uhho3R42ZHPKqggobGinJYtgKeQRXRX78t4jsmRHBRyyaCahhnjFOyEDaSu5f2w8FfHkBb05X3YZI2V4uKUhzsjbGEF_MQuzQik12Qabj-LCxQIPBKTQyAWh4-8eEmufQPGOoK2Zwwm_sZjXOczQsNlVUkkTNK6a7IdUzEUahSz3FGBu82Dioo7zwy2RMyYvmTwTcykRywlB3-VfxVbFbxExsu_RURa-xOYxMIbPxmxo4bB3L0Nx0v2m2uCzTeWek46bEQD1WE5ccmWzsbKoNMXLkczbd2qcv0dOrei1NFjiWDuAnyI3-XgcUOLXTMzhAK6Ju-D6TNBIvQU57kfQ9jMwdGrki5bPAJbk3JL3TB9hg-IvfiYL3CrTTy9hAcdUzsW_IRk8DitAkidGHTIQJ_lzEoby1bOZz9euE2mCKgoxb12oKCADYFaaMXSSbEmT7Rr8XqN_L2lxJFtO9PQojyLu6J8ALMkn7eF-448poiUHhqBaFS1o7FewX9PAOZ6EVHDYrq0VyVN7L4C0xcSZQnWcNGX5qHDtQw-giEXdK0crTZrMW11zYsLUdfYYBQfPIqQ-_VqFMO5S-KNIBPgIWbyRWCmlc5RIEDDXDy-AeI7NmvqLLW1RaZ-9wsNxS7kaLc-NpNKZczjbQwuBoytVd1gRbhvFzzmnT7eHX3be6Ek8pzJ9IOXfMsH4cL4-gdDjCAhqXH_LkbA7IXULL9cDQ0Pw1Tf7qonzH5GZNr26PE-mK2KS7jS6zCcXIyH1rTv36zADLhy3mtwaXCfDrBMHUpbqEKxiQizP_OxSWcZcjk2L8S-Qo-rxeUwZzePwS72ozSMU4cjonJCmhaR8fJNGNHMJP1TR6yZ8V1trLxXa--8stBw5Hk7LADcw40qv6spKtaMIaTl0a5fxDMbwvG7wAD51IN_CHD7ksJoap6ikCjCVxD7W04sI7Kftc9YeFHpShWFQr4-GEnwYAEHx5owImJ3qSSUtK0dARFMkGUTmfm7snxPePsLAk6ouFr_UZO8pof-CTRFqPB2VhIUZtQmDmT7Ww6ie5dTsTXMSbb7xkNJgTYBzLaY584nbSEW-ZtweV8LcXOUsjNBGbLf2MBns6pU2hWptcYCOD_y8HFEyGA8M3zOikj6yf7hjRffJOjRbN8deNEPSmfGCgnWBQgbrbL64R3-weHgv2sbAK7sqVzVNEuYPgZK3PnrrZGLPADrD1SYCE0TRD-SAKPKEkTKO6sSsFaJltQ7qvXMHTGcQCpCXxxIcwtY6slgwJv3c9jhyO2HKy5n2Hb0Hj-MkNVUDl_tbGtJgiarYunEvFut8GUleb3m_V4sUD_zEt3Ws9KUXCAEyaHtrCjU1eV254pQTrafVT53NKRtGYWCtsnP9aiRHjrPJb-8_)


## Concepts

- **Pipelines**: Compose extractor → transformer → loader
- **Just-in-Time ingestion**: jobs can snapshot current data at scope boundaries (hourly/daily/weekly/...)
- **Idempotency**: Prefer `ReplacingMergeTree` (e.g., keyed by business dimension + update timestamp) or upsert loader patterns for “latest” facts
- **Migrations**: DDL managed by simple SQL files and a migration runner

## Environment (.env)

```
CLICKHOUSE_HOST=127.0.0.1
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=data-processor
CLICKHOUSE_PASSWORD=***
CLICKHOUSE_DATABASE=data_warehouse
# Optional API keys/settings for your extractors
# API_KEY=***
# API_BASE_URL=https://api.example.com
```

## Run Locally

```
python3 -m venv .venv
. .venv/bin/activate
pip install -U pip aiohttp clickhouse-driver pandas pytz

# List available jobs
python scripts/run.py list

# Run a pipeline by name
python scripts/run.py run <pipeline_name>
```

## Migrations (ClickHouse)

```
python migrations/migration_manager.py status
python migrations/migration_manager.py run
```

## Deploy

```
./deploy.sh <ssh_user> <ssh_host> [--clean]
```

Deploy script will:
- rsync project → server
- create/upgrade venv and install runtime deps
- ensure `.env` and `CLICKHOUSE_DATABASE`
- run migrations
- run each job once (seed)
- install crons

## Cron Schedules (example)

- latest: every 5 min
- hourly: at :00 every hour
- daily: 00:00
- weekly: Monday 00:00
- monthly: 1st 00:00
- yearly: Jan 1st 00:00

## Notes

- Idempotent “latest” facts are best modeled with `ReplacingMergeTree(<updated_at_column>)` and an upsert loader keyed by stable dimensions.
- Historical/scope tables typically use `MergeTree` with Float64 numerics and `Array(String)` where appropriate.
- Logs: server at `<project>/logs/cron.log`.