# Databricks Medallion Pipeline — Databricks + Delta Lake

Projeto de referência para **Arquitetura Medalhão (Landing → Bronze → Silver → Gold)** com Databricks, usando o dataset enviado (annex1–4) e uma 5ª tabela derivada de categorias.

## Avaliação (checklist)
- ✅ **Apresentação funcional (Job & Pipeline)**: job JSON incluído em `devops/job.json`, executa 3 notebooks em sequência.
- ✅ **Organização do repo**: pastas `landing/`, `src/notebooks/`, `src/config/`, `docs/`, `devops/`.
- ✅ **Códigos dos notebooks**: `src/notebooks/01_*`, `02_*`, `03_*`.
- ✅ **README**: este arquivo com passo a passo completo.
- ✅ **Documentação MKDocs**: `mkdocs.yml` e `docs/` com visão geral, dicionário de dados e runbook.

---

## Como usar (passo a passo rápido)

1. **Suba os arquivos do repositório para o Databricks** (ou clone do GitHub):
   - Em *Repos* do Databricks, conecte-se ao seu GitHub e importe este projeto.
   - Alternativamente, faça upload manual dos arquivos da pasta `landing/` para: `dbfs:/FileStore/medallion_demo/landing`.
     > Dica: em um notebook, você pode checar com: `display(dbutils.fs.ls("dbfs:/FileStore/medallion_demo/landing"))`.

2. **Crie o database (schema) e paths**:
   - Os notebooks criam `hive_metastore.medallion_demo` automaticamente se não existir.
   - Ajuste `catalog`/`schema`/paths via widgets do job/notebook se for usar Unity Catalog (ex.: `main.retail`), ou edite `src/config/project.yaml`.

3. **Execute os notebooks na ordem (ou crie um Job)**:
   - `01_landing_to_bronze` → carrega CSVs da `landing/` para tabelas Delta *bronze* com colunas de auditoria.
   - `02_bronze_to_silver` → tipagem, normalização de nomes, e *enrichment* (joins com itens, categorias, custo/wholesale, perda).
   - `03_silver_to_gold` → fatos e agregações (diário por item, diário por categoria, top 10/dia, KPIs).

4. **Crie o Job no Databricks**:
   - Em **Workflows → Jobs → Import job** e importe `devops/job.json`.
   - Selecione um cluster (DBR 13+ recomendado) e rode o job.
   - A execução orquestrada dos 3 notebooks é o **pré‑requisito** da avaliação.

5. **Valide**:
   - Rode consultas como:
     ```sql
     USE hive_metastore.medallion_demo;
     SELECT * FROM gold__kpi_summary;
     SELECT * FROM gold__top10_by_revenue ORDER BY date, rank_rev;
     ```

## Tabelas

**Landing (arquivos brutos)**: `annex1.csv`, `annex2.csv`, `annex3.csv`, `annex4.csv`, **categories.csv** (derivada de annex1).
**Bronze**: `bronze__annex1_items`, `bronze__annex2_sales`, `bronze__annex3_wholesale`, `bronze__annex4_lossrates`, `bronze__categories`.
**Silver**: `silver__items`, `silver__categories`, `silver__wholesale`, `silver__lossrates`, `silver__sales_enriched`.
**Gold**: `gold__fact_daily_item`, `gold__fact_daily_category`, `gold__top10_by_revenue`, `gold__kpi_summary`.

## Observações técnicas
- **Delta Lake** com `OPTIMIZE`/`VACUUM` (retention 7 dias) nos estágios principais.
- Datas/horas: `Date` e `Time` da *sales* são tratadas e convertidas para `date`/`timestamp`.
- Preço de custo estimado usa `wholesale_price_rmb_kg` ajustado por `loss_rate_pct`.
- Flags de desconto: `has_discount` a partir da coluna `Discount (Yes/No)`.

## GitHub Integration (Pesquisa)
- Use **Repos** do Databricks para conectar seu workspace ao GitHub.
- *Flow* recomendado: crie este repositório no GitHub → em Databricks, **Repos → Add repo → Git URL** → `git pull`/`git commit`/`git push` dentro do Databricks.

## Troubleshooting (rápido)
- *File not found?* Confirme que os CSVs estão em `dbfs:/FileStore/medallion_demo/landing` ou ajuste o parâmetro `landing_path` no notebook 01.
- *Permissão (Unity Catalog)?* Use `catalog` e `schema` sob os quais você tem permissão de criar tabelas.
- *Tipos estranhos?* Ajuste casts no notebook 02.