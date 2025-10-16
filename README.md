# Databricks Medallion Pipeline — Volume Path (workspace.default)

**Ambiente alvo**
- `catalog`: `workspace`
- `schema`: `default`
- `landing_path`: `/Volumes/workspace/default/medallion_demo_landing` (seus CSVs)

**Como rodar**
1) Abra os notebooks na ordem `01` → `02` → `03` e **Run All** — ou importe o `job.json` em Workflows → Jobs.
2) Se usar o Job, confira os parâmetros padrão abaixo.

**Arquivos principais**
- `01_landing_to_bronze.py`
- `02_bronze_to_silver.py`
- `03_silver_to_gold.py`
- `job.json`
- `project.yaml` (opcional, referência de config)