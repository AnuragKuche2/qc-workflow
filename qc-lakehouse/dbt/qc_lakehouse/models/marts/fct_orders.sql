-- Sub-project H: Liquid Clustering on zone_id won the layout benchmark (Task 4's cost
-- report; see perf_lab/). `liquid_clustered_by` is dbt-databricks' own config key for this
-- (confirmed against the installed dbt-databricks==1.12.5's DatabricksConfig -
-- src/dbt/adapters/databricks/impl.py - and used by its relation_configs/liquid_clustering.py
-- and materializations/table.sql macros). This table is `+materialized: table`
-- (dbt_project.yml), which dbt-databricks rebuilds via CREATE OR REPLACE TABLE ... AS SELECT
-- on every `dbt run` - without this config declared here, the next dbt_run job (E1's
-- qc_lakehouse_pipeline) would silently rebuild fct_orders WITHOUT clustering, reverting
-- perf_lab/apply_winning_layout.py's one-time migration with no error or signal. Declaring it
-- here makes dbt itself own and re-apply the layout on every future rebuild.
{{ config(
    liquid_clustered_by = ['zone_id']
) }}

select
    order_id,
    order_ref,
    customer_id,
    restaurant_id,
    zone_id,
    date(placed_at) as date_day,
    placed_at,
    order_status,
    subtotal,
    delivery_fee,
    commission_pct,
    commission_amount,
    order_total,
    refund_amount,
    net_revenue,
    item_count
from {{ ref('int_order_economics') }}
