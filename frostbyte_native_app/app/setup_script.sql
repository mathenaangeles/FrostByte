create application role if not exists FROSTBYTE_APP_ROLE;
create schema if not exists APP;
create or replace streamlit APP.FROSTBYTE_UI
from '/streamlit'
main_file = 'streamlit_app.py';

create or replace table APP.FROSTBYTE_CONFIG (
  CONFIG_NAME string,
  INPUT_TABLE string,
  INPUT_REF string,
  DATE_COL string,
  ORG_COL string,
  METRIC_COL string,
  VALUE_COL string,
  CATEGORY_COL string,
  DIM1_COL string,
  DIM2_COL string,
  DIM3_COL string,
  START_DATE date,
  END_DATE date,
  MISSING_DAY_TOLERANCE number,
  MIN_WEEKS_HISTORY number,
  Z_THRESHOLD float,
  WOW_CHANGE_PCT float,
  FORECAST_WEEKS number,
  CLAMP_NONNEGATIVE boolean,
  ENABLE_CLUSTERING boolean,
  ENABLE_CORTEX boolean,
  MODEL_NAME string,
  CORTEX_FN string,
  MAX_SERIES number,
  MAX_ROWS_SCAN number,
  CREATED_AT timestamp
);

create or replace table APP.FROSTBYTE_RUNS (
  RUN_ID string,
  CONFIG_NAME string,
  STATUS string,
  STATUS_MESSAGE string,
  STARTED_AT timestamp,
  FINISHED_AT timestamp,
  COMPUTED_AT timestamp,
  WINDOW_START date,
  WINDOW_END date,
  LATEST_WEEK_START date,
  SERIES_COUNT number,
  ROWS_SCANNED number,
  INPUT_OBJECT string,
  APP_VERSION string
);

create or replace table APP.FROSTBYTE_DQ_RUN (
  RUN_ID string,
  CONFIG_NAME string,
  COMPUTED_AT timestamp,
  START_DATE date,
  END_DATE date,
  ROW_COUNT number,
  DISTINCT_DAYS number,
  EXPECTED_DAYS number,
  MISSING_DAYS number,
  NULL_DT number,
  NULL_ORG_UNIT number,
  NULL_METRIC_NAME number,
  NULL_CATEGORY number,
  NULL_METRIC_VALUE number,
  NEGATIVE_VALUES number,
  DUP_ROWS number,
  ORG_UNIT_COUNT number,
  METRIC_COUNT number,
  CATEGORY_COUNT number,
  SERIES_COUNT number,
  HEALTH_SCORE float
);

create or replace table APP.FROSTBYTE_DQ_SERIES (
  RUN_ID string,
  CONFIG_NAME string,
  COMPUTED_AT timestamp,
  ORG_UNIT string,
  METRIC_NAME string,
  CATEGORY string,
  DIM1 string,
  DIM2 string,
  DIM3 string,
  ROW_COUNT number,
  DISTINCT_DAYS number,
  EXPECTED_DAYS number,
  MISSING_DAYS number,
  MISSING_PCT float,
  DUP_ROWS number,
  NULL_VALUE number,
  NEGATIVE_VALUES number,
  MIN_VALUE float,
  P01 float,
  MEDIAN float,
  P99 float,
  MAX_VALUE float,
  MEAN_VALUE float,
  STD_VALUE float,
  HEALTH_SCORE float
);

create or replace table APP.FROSTBYTE_SERIES_WEEKLY (
  RUN_ID string,
  CONFIG_NAME string,
  COMPUTED_AT timestamp,
  WEEK_START date,
  ORG_UNIT string,
  METRIC_NAME string,
  CATEGORY string,
  DIM1 string,
  DIM2 string,
  DIM3 string,
  VALUE float
);

create or replace table APP.FROSTBYTE_ANOMALIES_WEEKLY (
  RUN_ID string,
  CONFIG_NAME string,
  COMPUTED_AT timestamp,
  WEEK_START date,
  ORG_UNIT string,
  METRIC_NAME string,
  CATEGORY string,
  DIM1 string,
  DIM2 string,
  DIM3 string,
  VALUE float,
  HIST_MEAN float,
  HIST_STD float,
  Z_SCORE float,
  STATUS string
);

create or replace table APP.FROSTBYTE_FORECAST_4W (
  RUN_ID string,
  CONFIG_NAME string,
  COMPUTED_AT timestamp,
  FORECAST_WEEK_START date,
  ORG_UNIT string,
  METRIC_NAME string,
  CATEGORY string,
  DIM1 string,
  DIM2 string,
  DIM3 string,
  FORECAST_VALUE float,
  METHOD string
);

create or replace table APP.FROSTBYTE_SERIES_FEATURES (
  RUN_ID string,
  CONFIG_NAME string,
  COMPUTED_AT timestamp,
  ORG_UNIT string,
  METRIC_NAME string,
  CATEGORY string,
  DIM1 string,
  DIM2 string,
  DIM3 string,
  LEVEL_MEAN float,
  VOLATILITY float,
  SLOPE float,
  RECENT_WOW_ABS float,
  ANOMALY_RATE float,
  MISSING_PCT float
);

create or replace table APP.FROSTBYTE_SERIES_CLUSTERS (
  RUN_ID string,
  COMPUTED_AT timestamp,
  ORG_UNIT string,
  METRIC_NAME string,
  CATEGORY string,
  DIM1 string,
  DIM2 string,
  DIM3 string,
  CLUSTER_ID number
);

create or replace table APP.FROSTBYTE_ACTIONS (
  RUN_ID string,
  CONFIG_NAME string,
  COMPUTED_AT timestamp,
  ACTION_TYPE string,
  SEVERITY string,
  MESSAGE string
);

create or replace table APP.FROSTBYTE_WEEKLY_BRIEF (
  RUN_ID string,
  CONFIG_NAME string,
  COMPUTED_AT timestamp,
  WEEK_START date,
  BRIEF string
);

create or replace table APP.FROSTBYTE_AI_AUDIT (
  RUN_ID string,
  CONFIG_NAME string,
  COMPUTED_AT timestamp,
  PURPOSE string,
  MODEL_NAME string,
  PROMPT_HASH string,
  RESPONSE string
);

create or replace table APP.FROSTBYTE_DEMO_METRIC_DAILY (
  DT date,
  ORG_UNIT string,
  METRIC_NAME string,
  METRIC_VALUE float,
  CATEGORY string,
  DIM1 string,
  DIM2 string,
  DIM3 string
);

create schema if not exists CONFIG;

create or replace procedure CONFIG.REGISTER_REFERENCE(
  ref_name string,
  operation string,
  ref_or_alias string
)
returns string
language sql
as
$$
begin
  case (operation)
    when 'ADD' then
      select system$set_reference(:ref_name, :ref_or_alias);
    when 'REMOVE' then
      select system$remove_reference(:ref_name, :ref_or_alias);
    when 'CLEAR' then
      select system$remove_all_references(:ref_name);
    else
      return 'UNKNOWN_OPERATION';
  end case;
  return null;
end;
$$;


truncate table APP.FROSTBYTE_DEMO_METRIC_DAILY;

insert into APP.FROSTBYTE_DEMO_METRIC_DAILY
with d as (
  select dateadd(day, seq4(), '2025-07-01'::date) as dt
  from table(generator(rowcount => 180))
),
orgs as (
  select 'ORG_' || lpad(seq4()::string, 3, '0') as org_unit
  from table(generator(rowcount => 20))
),
metrics as (
  select column1::string as metric_name
  from values
    ('spend_usd'), ('energy_kwh'), ('attendance'), ('stockouts'), ('emissions_tco2e')
),
cats as (
  select column1::string as category
  from values ('overall'), ('priority'), ('non_priority'), ('other')
),
dims as (
  select
    column1::string as dim1,
    column2::string as dim2,
    column3::string as dim3
  from values
    ('north','channel_a','sub_1'),
    ('north','channel_b','sub_2'),
    ('south','channel_a','sub_1'),
    ('south','channel_b','sub_2'),
    ('east','channel_a','sub_1'),
    ('west','channel_b','sub_2')
)
select
  d.dt,
  o.org_unit,
  m.metric_name,
  greatest(
    0,
    100
    + 10 * sin(datediff(day, '2025-07-01'::date, d.dt) / 7.0)
    + uniform(-15, 15, random())
    + case when m.metric_name = 'spend_usd' then 30 else 0 end
    + case when c.category = 'priority' then 20 else 0 end
  )::float as metric_value,
  c.category,
  di.dim1,
  di.dim2,
  di.dim3
from d
cross join orgs o
cross join metrics m
cross join cats c
join dims di
  on mod(abs(hash(o.org_unit || m.metric_name || c.category || to_varchar(d.dt))), 6) = mod(abs(hash(di.dim1 || di.dim2 || di.dim3)), 6);

merge into APP.FROSTBYTE_CONFIG t
using (
  select
    'demo'::string as CONFIG_NAME,
    null::string as INPUT_TABLE,
    'INPUT_DATA'::string as INPUT_REF,
    'DT'::string as DATE_COL,
    'ORG_UNIT'::string as ORG_COL,
    'METRIC_NAME'::string as METRIC_COL,
    'METRIC_VALUE'::string as VALUE_COL,
    'CATEGORY'::string as CATEGORY_COL,
    'DIM1'::string as DIM1_COL,
    'DIM2'::string as DIM2_COL,
    'DIM3'::string as DIM3_COL,
    null::date as START_DATE,
    null::date as END_DATE,
    1::number as MISSING_DAY_TOLERANCE,
    8::number as MIN_WEEKS_HISTORY,
    3.0::float as Z_THRESHOLD,
    0.2::float as WOW_CHANGE_PCT,
    4::number as FORECAST_WEEKS,
    true::boolean as CLAMP_NONNEGATIVE,
    true::boolean as ENABLE_CLUSTERING,
    true::boolean as ENABLE_CORTEX,
    'llama3.1-8b'::string as MODEL_NAME,
    'SNOWFLAKE.CORTEX.COMPLETE'::string as CORTEX_FN,
    2500::number as MAX_SERIES,
    5000000::number as MAX_ROWS_SCAN,
    current_timestamp() as CREATED_AT
) s
on t.CONFIG_NAME = s.CONFIG_NAME
when matched then update set
  INPUT_TABLE = s.INPUT_TABLE,
  INPUT_REF = s.INPUT_REF,
  DATE_COL = s.DATE_COL,
  ORG_COL = s.ORG_COL,
  METRIC_COL = s.METRIC_COL,
  VALUE_COL = s.VALUE_COL,
  CATEGORY_COL = s.CATEGORY_COL,
  DIM1_COL = s.DIM1_COL,
  DIM2_COL = s.DIM2_COL,
  DIM3_COL = s.DIM3_COL,
  START_DATE = s.START_DATE,
  END_DATE = s.END_DATE,
  MISSING_DAY_TOLERANCE = s.MISSING_DAY_TOLERANCE,
  MIN_WEEKS_HISTORY = s.MIN_WEEKS_HISTORY,
  Z_THRESHOLD = s.Z_THRESHOLD,
  WOW_CHANGE_PCT = s.WOW_CHANGE_PCT,
  FORECAST_WEEKS = s.FORECAST_WEEKS,
  CLAMP_NONNEGATIVE = s.CLAMP_NONNEGATIVE,
  ENABLE_CLUSTERING = s.ENABLE_CLUSTERING,
  ENABLE_CORTEX = s.ENABLE_CORTEX,
  MODEL_NAME = s.MODEL_NAME,
  CORTEX_FN = s.CORTEX_FN,
  MAX_SERIES = s.MAX_SERIES,
  MAX_ROWS_SCAN = s.MAX_ROWS_SCAN,
  CREATED_AT = s.CREATED_AT
when not matched then insert (
  CONFIG_NAME, INPUT_TABLE, INPUT_REF, DATE_COL, ORG_COL, METRIC_COL, VALUE_COL, CATEGORY_COL,
  DIM1_COL, DIM2_COL, DIM3_COL,
  START_DATE, END_DATE, MISSING_DAY_TOLERANCE, MIN_WEEKS_HISTORY, Z_THRESHOLD, WOW_CHANGE_PCT, FORECAST_WEEKS,
  CLAMP_NONNEGATIVE, ENABLE_CLUSTERING, ENABLE_CORTEX, MODEL_NAME, CORTEX_FN, MAX_SERIES, MAX_ROWS_SCAN, CREATED_AT
) values (
  s.CONFIG_NAME, s.INPUT_TABLE, s.INPUT_REF, s.DATE_COL, s.ORG_COL, s.METRIC_COL, s.VALUE_COL, s.CATEGORY_COL,
  s.DIM1_COL, s.DIM2_COL, s.DIM3_COL,
  s.START_DATE, s.END_DATE, s.MISSING_DAY_TOLERANCE, s.MIN_WEEKS_HISTORY, s.Z_THRESHOLD, s.WOW_CHANGE_PCT, s.FORECAST_WEEKS,
  s.CLAMP_NONNEGATIVE, s.ENABLE_CLUSTERING, s.ENABLE_CORTEX, s.MODEL_NAME, s.CORTEX_FN, s.MAX_SERIES, s.MAX_ROWS_SCAN, s.CREATED_AT
);

create or replace procedure APP.RUN_FROSTBYTE_CLUSTER(RUN_ID string)
returns string
language python
runtime_version = '3.11'
packages = ('snowflake-snowpark-python', 'pandas', 'scikit-learn')
handler = 'main'
as
$$
import pandas as pd
from snowflake.snowpark import Session
from sklearn.cluster import KMeans

def main(session: Session, run_id: str) -> str:
    rid = (run_id or "").replace("'", "''")
    session.sql("delete from APP.FROSTBYTE_SERIES_CLUSTERS where RUN_ID = '{}'".format(rid)).collect()
    df = session.sql(
        "select ORG_UNIT, METRIC_NAME, CATEGORY, DIM1, DIM2, DIM3, LEVEL_MEAN, VOLATILITY, SLOPE, RECENT_WOW_ABS, ANOMALY_RATE, MISSING_PCT "
        "from APP.FROSTBYTE_SERIES_FEATURES where RUN_ID = '{}'".format(rid)
    ).to_pandas()
    if df.empty:
        return "NO_FEATURES"
    feats = df[["LEVEL_MEAN","VOLATILITY","SLOPE","RECENT_WOW_ABS","ANOMALY_RATE","MISSING_PCT"]].fillna(0.0).astype(float).values
    k = min(6, max(2, int((len(df) ** 0.5) // 1)))
    model = KMeans(n_clusters=k, n_init="auto", random_state=42)
    labels = model.fit_predict(feats)
    out = df[["ORG_UNIT","METRIC_NAME","CATEGORY","DIM1","DIM2","DIM3"]].copy()
    out["RUN_ID"] = run_id
    out["CLUSTER_ID"] = labels.astype(int)
    out["COMPUTED_AT"] = pd.Timestamp.utcnow()
    session.write_pandas(out, "APP.FROSTBYTE_SERIES_CLUSTERS", auto_create_table=False, overwrite=False)
    return "CLUSTERED:{}".format(k)
$$;

create or replace procedure APP.RUN_FROSTBYTE(CONFIG string)
returns string
language sql
execute as owner
as
$$
declare
  v_run_id string default uuid_string();
  v_now timestamp default current_timestamp();
  v_status string default 'RUNNING';
  v_status_msg string default null;

  v_input_table string;
  v_input_ref string;
  v_date_col string;
  v_org_col string;
  v_metric_col string;
  v_value_col string;
  v_category_col string;
  v_dim1_col string;
  v_dim2_col string;
  v_dim3_col string;

  v_start date;
  v_end date;

  v_missing_tol number;
  v_min_weeks number;
  v_z float;
  v_wow float;
  v_fweeks number;
  v_clamp boolean;
  v_enable_cluster boolean;
  v_enable_cortex boolean;
  v_model string;
  v_cortex_fn string;
  v_max_series number;
  v_max_rows number;

  v_obj string;

  v_dt_expr string;
  v_org_expr string;
  v_metric_expr string;
  v_value_expr string;
  v_cat_expr string;
  v_dim1_expr string;
  v_dim2_expr string;
  v_dim3_expr string;

  v_expected_days number;
  v_latest_week date;
  v_series_count number;
  v_rows_scanned number;

  v_sql string;
begin
  select
    INPUT_TABLE,
    INPUT_REF,
    coalesce(DATE_COL,'DT'),
    coalesce(ORG_COL,'ORG_UNIT'),
    coalesce(METRIC_COL,'METRIC_NAME'),
    coalesce(VALUE_COL,'METRIC_VALUE'),
    coalesce(CATEGORY_COL,'CATEGORY'),
    DIM1_COL,
    DIM2_COL,
    DIM3_COL,
    START_DATE,
    END_DATE,
    coalesce(MISSING_DAY_TOLERANCE, 1),
    coalesce(MIN_WEEKS_HISTORY, 8),
    coalesce(Z_THRESHOLD, 3.0),
    coalesce(WOW_CHANGE_PCT, 0.2),
    coalesce(FORECAST_WEEKS, 4),
    coalesce(CLAMP_NONNEGATIVE, true),
    coalesce(ENABLE_CLUSTERING, false),
    coalesce(ENABLE_CORTEX, false),
    coalesce(MODEL_NAME, 'llama3.1-8b'),
    coalesce(CORTEX_FN, 'SNOWFLAKE.CORTEX.COMPLETE'),
    coalesce(MAX_SERIES, 5000),
    coalesce(MAX_ROWS_SCAN, 5000000)
  into
    :v_input_table,
    :v_input_ref,
    :v_date_col,
    :v_org_col,
    :v_metric_col,
    :v_value_col,
    :v_category_col,
    :v_dim1_col,
    :v_dim2_col,
    :v_dim3_col,
    :v_start,
    :v_end,
    :v_missing_tol,
    :v_min_weeks,
    :v_z,
    :v_wow,
    :v_fweeks,
    :v_clamp,
    :v_enable_cluster,
    :v_enable_cortex,
    :v_model,
    :v_cortex_fn,
    :v_max_series,
    :v_max_rows
  from APP.FROSTBYTE_CONFIG
  where CONFIG_NAME = :CONFIG;

  if (v_input_table is null and v_input_ref is null) then
    return 'Config not found: ' || CONFIG;
  end if;

  if (v_input_ref is not null and length(trim(v_input_ref)) > 0) then
    v_obj := 'reference(''' || replace(v_input_ref, '''', '''''') || ''')';
  else
    v_obj := v_input_table;
  end if;

  if (v_input_ref is null or length(trim(v_input_ref)) = 0) then
    if (not regexp_like(v_obj, '^[A-Za-z0-9_$.]+$')) then
      return 'Unsafe INPUT_TABLE name (allowed: A-Za-z0-9_$.): ' || v_obj;
    end if;
  end if;

  insert into APP.FROSTBYTE_RUNS (
    RUN_ID, CONFIG_NAME, STATUS, STATUS_MESSAGE, STARTED_AT, FINISHED_AT, COMPUTED_AT,
    WINDOW_START, WINDOW_END, LATEST_WEEK_START, SERIES_COUNT, ROWS_SCANNED, INPUT_OBJECT, APP_VERSION
  ) values (
    :v_run_id, :CONFIG, :v_status, :v_status_msg, :v_now, null, :v_now,
    null, null, null, null, null, :v_obj, '2.0.0'
  );

  v_dt_expr := '"' || replace(v_date_col,'"','""') || '"';
  v_org_expr := '"' || replace(v_org_col,'"','""') || '"';
  v_metric_expr := '"' || replace(v_metric_col,'"','""') || '"';
  v_value_expr := '"' || replace(v_value_col,'"','""') || '"';
  v_cat_expr := '"' || replace(v_category_col,'"','""') || '"';

  v_dim1_expr := case when v_dim1_col is null or length(v_dim1_col)=0 then 'null' else '"' || replace(v_dim1_col,'"','""') || '"' end;
  v_dim2_expr := case when v_dim2_col is null or length(v_dim2_col)=0 then 'null' else '"' || replace(v_dim2_col,'"','""') || '"' end;
  v_dim3_expr := case when v_dim3_col is null or length(v_dim3_col)=0 then 'null' else '"' || replace(v_dim3_col,'"','""') || '"' end;

  if (v_start is null or v_end is null) then
    v_sql := 'select min(try_to_date(' || v_dt_expr || '))::date as MIN_DT, max(try_to_date(' || v_dt_expr || '))::date as MAX_DT from ' || v_obj;
    execute immediate :v_sql;
    select MIN_DT, MAX_DT into :v_start, :v_end
    from table(result_scan(last_query_id()));
  end if;

  if (v_start is null or v_end is null) then
    update APP.FROSTBYTE_RUNS set
      STATUS='FAILED',
      STATUS_MESSAGE='Unable to infer START_DATE/END_DATE from data (null min/max dates)',
      FINISHED_AT=current_timestamp()
    where RUN_ID=:v_run_id;
    return v_run_id || ':FAILED';
  end if;

  if (v_start > v_end) then
    update APP.FROSTBYTE_RUNS set
      STATUS='FAILED',
      STATUS_MESSAGE='START_DATE > END_DATE',
      FINISHED_AT=current_timestamp()
    where RUN_ID=:v_run_id;
    return v_run_id || ':FAILED';
  end if;

  v_expected_days := datediff('day', v_start, v_end) + 1;

  v_sql := 'select max(date_trunc(''week'', try_to_date(' || v_dt_expr || ')))::date as WK from ' || v_obj ||
           ' where try_to_date(' || v_dt_expr || ') between ''' || to_varchar(v_start) || '''::date and ''' || to_varchar(v_end) || '''::date';
  execute immediate :v_sql;
  select WK into :v_latest_week
  from table(result_scan(last_query_id()));

  if (v_latest_week is null) then
    update APP.FROSTBYTE_RUNS set
      STATUS='FAILED',
      STATUS_MESSAGE='Unable to infer latest week (no rows in window)',
      FINISHED_AT=current_timestamp()
    where RUN_ID=:v_run_id;
    return v_run_id || ':FAILED';
  end if;

  v_sql := 'create or replace temporary table APP.FROSTBYTE_BASE as
            select
              try_to_date(' || v_dt_expr || ')::date as DT,
              to_varchar(' || v_org_expr || ') as ORG_UNIT,
              to_varchar(' || v_metric_expr || ') as METRIC_NAME,
              to_varchar(' || v_cat_expr || ') as CATEGORY,
              to_varchar(' || v_dim1_expr || ') as DIM1,
              to_varchar(' || v_dim2_expr || ') as DIM2,
              to_varchar(' || v_dim3_expr || ') as DIM3,
              try_to_double(' || v_value_expr || ') as METRIC_VALUE
            from ' || v_obj || '
            where try_to_date(' || v_dt_expr || ') between ''' || to_varchar(v_start) || '''::date and ''' || to_varchar(v_end) || '''::date';
  execute immediate :v_sql;

  select count(*) into :v_rows_scanned from APP.FROSTBYTE_BASE;

  if (v_rows_scanned > v_max_rows) then
    update APP.FROSTBYTE_RUNS set
      STATUS='FAILED',
      STATUS_MESSAGE='Rows in window exceed MAX_ROWS_SCAN ('||v_rows_scanned||' > '||v_max_rows||')',
      ROWS_SCANNED=:v_rows_scanned,
      WINDOW_START=:v_start,
      WINDOW_END=:v_end,
      LATEST_WEEK_START=:v_latest_week,
      FINISHED_AT=current_timestamp()
    where RUN_ID=:v_run_id;
    return v_run_id || ':FAILED';
  end if;

  create or replace temporary table APP.FROSTBYTE_SERIES_ALL as
  select
    ORG_UNIT, METRIC_NAME, coalesce(CATEGORY,'') as CATEGORY,
    DIM1, DIM2, DIM3,
    count(*) as ROW_COUNT
  from APP.FROSTBYTE_BASE
  group by 1,2,3,4,5,6;

  v_sql := 'create or replace temporary table APP.FROSTBYTE_SERIES_KEEP as
            select *
            from APP.FROSTBYTE_SERIES_ALL
            order by ROW_COUNT desc
            limit ' || to_varchar(v_max_series);
  execute immediate :v_sql;

  select count(*) into :v_series_count from APP.FROSTBYTE_SERIES_KEEP;

  update APP.FROSTBYTE_RUNS set
    WINDOW_START=:v_start,
    WINDOW_END=:v_end,
    LATEST_WEEK_START=:v_latest_week,
    SERIES_COUNT=:v_series_count,
    ROWS_SCANNED=:v_rows_scanned
  where RUN_ID=:v_run_id;

  delete from APP.FROSTBYTE_DQ_RUN where RUN_ID=:v_run_id;
  insert into APP.FROSTBYTE_DQ_RUN
  with kept as (
    select b.*
    from APP.FROSTBYTE_BASE b
    join APP.FROSTBYTE_SERIES_KEEP k
      on b.ORG_UNIT=k.ORG_UNIT and b.METRIC_NAME=k.METRIC_NAME and coalesce(b.CATEGORY,'')=k.CATEGORY
     and coalesce(b.DIM1,'')=coalesce(k.DIM1,'') and coalesce(b.DIM2,'')=coalesce(k.DIM2,'') and coalesce(b.DIM3,'')=coalesce(k.DIM3,'')
  ),
  dups as (
    select sum(cnt-1) as DUP_ROWS
    from (
      select DT, ORG_UNIT, METRIC_NAME, coalesce(CATEGORY,'') as CATEGORY, DIM1, DIM2, DIM3, count(*) as cnt
      from kept
      group by 1,2,3,4,5,6,7
    )
    where cnt > 1
  ),
  agg as (
    select
      count(*) as ROW_COUNT,
      count(distinct DT) as DISTINCT_DAYS,
      sum(case when DT is null then 1 else 0 end) as NULL_DT,
      sum(case when ORG_UNIT is null then 1 else 0 end) as NULL_ORG_UNIT,
      sum(case when METRIC_NAME is null then 1 else 0 end) as NULL_METRIC_NAME,
      sum(case when CATEGORY is null then 1 else 0 end) as NULL_CATEGORY,
      sum(case when METRIC_VALUE is null then 1 else 0 end) as NULL_METRIC_VALUE,
      sum(case when METRIC_VALUE < 0 then 1 else 0 end) as NEGATIVE_VALUES,
      count(distinct ORG_UNIT) as ORG_UNIT_COUNT,
      count(distinct METRIC_NAME) as METRIC_COUNT,
      count(distinct coalesce(CATEGORY,'')) as CATEGORY_COUNT
    from kept
  )
  select
    :v_run_id,
    :CONFIG,
    :v_now,
    :v_start,
    :v_end,
    agg.ROW_COUNT,
    agg.DISTINCT_DAYS,
    :v_expected_days,
    greatest(0, :v_expected_days - agg.DISTINCT_DAYS) as MISSING_DAYS,
    agg.NULL_DT,
    agg.NULL_ORG_UNIT,
    agg.NULL_METRIC_NAME,
    agg.NULL_CATEGORY,
    agg.NULL_METRIC_VALUE,
    agg.NEGATIVE_VALUES,
    coalesce(dups.DUP_ROWS,0) as DUP_ROWS,
    agg.ORG_UNIT_COUNT,
    agg.METRIC_COUNT,
    agg.CATEGORY_COUNT,
    :v_series_count,
    (1.0
      - least(0.40, agg.NULL_METRIC_VALUE::float / nullif(agg.ROW_COUNT,0))
      - least(0.30, coalesce(dups.DUP_ROWS,0)::float / nullif(agg.ROW_COUNT,0))
      - least(0.30, greatest(0, :v_expected_days - agg.DISTINCT_DAYS)::float / nullif(:v_expected_days,0))
    )::float as HEALTH_SCORE
  from agg cross join dups;

  delete from APP.FROSTBYTE_DQ_SERIES where RUN_ID=:v_run_id;
  insert into APP.FROSTBYTE_DQ_SERIES
  with kept as (
    select b.*
    from APP.FROSTBYTE_BASE b
    join APP.FROSTBYTE_SERIES_KEEP k
      on b.ORG_UNIT=k.ORG_UNIT and b.METRIC_NAME=k.METRIC_NAME and coalesce(b.CATEGORY,'')=k.CATEGORY
     and coalesce(b.DIM1,'')=coalesce(k.DIM1,'') and coalesce(b.DIM2,'')=coalesce(k.DIM2,'') and coalesce(b.DIM3,'')=coalesce(k.DIM3,'')
  ),
  g as (
    select
      ORG_UNIT, METRIC_NAME, coalesce(CATEGORY,'') as CATEGORY, DIM1, DIM2, DIM3,
      count(*) as ROW_COUNT,
      count(distinct DT) as DISTINCT_DAYS,
      sum(case when METRIC_VALUE is null then 1 else 0 end) as NULL_VALUE,
      sum(case when METRIC_VALUE < 0 then 1 else 0 end) as NEGATIVE_VALUES,
      min(METRIC_VALUE) as MIN_VALUE,
      approx_percentile(METRIC_VALUE, 0.01) as P01,
      approx_percentile(METRIC_VALUE, 0.50) as MEDIAN,
      approx_percentile(METRIC_VALUE, 0.99) as P99,
      max(METRIC_VALUE) as MAX_VALUE,
      avg(METRIC_VALUE) as MEAN_VALUE,
      stddev_samp(METRIC_VALUE) as STD_VALUE
    from kept
    group by 1,2,3,4,5,6
  ),
  dups as (
    select
      ORG_UNIT, METRIC_NAME, coalesce(CATEGORY,'') as CATEGORY, DIM1, DIM2, DIM3,
      sum(case when cnt > 1 then (cnt - 1) else 0 end) as DUP_ROWS
    from (
      select ORG_UNIT, METRIC_NAME, coalesce(CATEGORY,'') as CATEGORY, DIM1, DIM2, DIM3, DT, count(*) as cnt
      from kept
      group by 1,2,3,4,5,6,7
    )
    group by 1,2,3,4,5,6
  ),
  s as (
    select
      g.*,
      coalesce(dups.DUP_ROWS,0) as DUP_ROWS,
      greatest(0, :v_expected_days - g.DISTINCT_DAYS) as MISSING_DAYS,
      greatest(0, :v_expected_days - g.DISTINCT_DAYS)::float / nullif(:v_expected_days,0) as MISSING_PCT
    from g
    left join dups
      on g.ORG_UNIT=dups.ORG_UNIT and g.METRIC_NAME=dups.METRIC_NAME and g.CATEGORY=dups.CATEGORY
     and coalesce(g.DIM1,'')=coalesce(dups.DIM1,'') and coalesce(g.DIM2,'')=coalesce(dups.DIM2,'') and coalesce(g.DIM3,'')=coalesce(dups.DIM3,'')
  )
  select
    :v_run_id,
    :CONFIG,
    :v_now,
    ORG_UNIT, METRIC_NAME, CATEGORY, DIM1, DIM2, DIM3,
    ROW_COUNT,
    DISTINCT_DAYS,
    :v_expected_days,
    MISSING_DAYS,
    MISSING_PCT,
    DUP_ROWS,
    NULL_VALUE,
    NEGATIVE_VALUES,
    MIN_VALUE,
    P01,
    MEDIAN,
    P99,
    MAX_VALUE,
    MEAN_VALUE,
    STD_VALUE,
    (1.0
      - least(0.50, NULL_VALUE::float / nullif(ROW_COUNT,0))
      - least(0.25, DUP_ROWS::float / nullif(ROW_COUNT,0))
      - least(0.25, MISSING_PCT)
    )::float as HEALTH_SCORE
  from s;

  delete from APP.FROSTBYTE_SERIES_WEEKLY where RUN_ID=:v_run_id;
  insert into APP.FROSTBYTE_SERIES_WEEKLY
  with kept as (
    select b.*
    from APP.FROSTBYTE_BASE b
    join APP.FROSTBYTE_SERIES_KEEP k
      on b.ORG_UNIT=k.ORG_UNIT and b.METRIC_NAME=k.METRIC_NAME and coalesce(b.CATEGORY,'')=k.CATEGORY
     and coalesce(b.DIM1,'')=coalesce(k.DIM1,'') and coalesce(b.DIM2,'')=coalesce(k.DIM2,'') and coalesce(b.DIM3,'')=coalesce(k.DIM3,'')
  )
  select
    :v_run_id,
    :CONFIG,
    :v_now,
    date_trunc('week', DT)::date as WEEK_START,
    ORG_UNIT,
    METRIC_NAME,
    coalesce(CATEGORY,'') as CATEGORY,
    DIM1, DIM2, DIM3,
    sum(METRIC_VALUE)::float as VALUE
  from kept
  where DT between dateadd(week, -1 * greatest(12, :v_min_weeks + 4), :v_latest_week) and :v_end
  group by 1,2,3,4,5,6,7,8,9,10;

  delete from APP.FROSTBYTE_ANOMALIES_WEEKLY where RUN_ID=:v_run_id;

  v_sql := 'insert into APP.FROSTBYTE_ANOMALIES_WEEKLY
            with w as (
              select *
              from APP.FROSTBYTE_SERIES_WEEKLY
              where RUN_ID = ''' || replace(v_run_id,'''','''''') || '''
            ),
            scored as (
              select
                w.*,
                avg(VALUE) over(
                  partition by ORG_UNIT, METRIC_NAME, CATEGORY, DIM1, DIM2, DIM3
                  order by WEEK_START
                  rows between ' || to_varchar(v_min_weeks) || ' preceding and 1 preceding
                ) as HIST_MEAN,
                stddev_samp(VALUE) over(
                  partition by ORG_UNIT, METRIC_NAME, CATEGORY, DIM1, DIM2, DIM3
                  order by WEEK_START
                  rows between ' || to_varchar(v_min_weeks) || ' preceding and 1 preceding
                ) as HIST_STD
              from w
            )
            select
              ''' || replace(v_run_id,'''','''''') || ''' as RUN_ID,
              ''' || replace(CONFIG,'''','''''') || ''' as CONFIG_NAME,
              ''' || to_varchar(v_now) || '''::timestamp as COMPUTED_AT,
              WEEK_START,
              ORG_UNIT,
              METRIC_NAME,
              CATEGORY,
              DIM1, DIM2, DIM3,
              VALUE,
              HIST_MEAN,
              HIST_STD,
              case when HIST_STD is null or HIST_STD=0 then null else (VALUE - HIST_MEAN) / HIST_STD end as Z_SCORE,
              case
                when HIST_STD is null or HIST_STD=0 then ''insufficient_history''
                when abs((VALUE - HIST_MEAN) / HIST_STD) >= ' || to_varchar(v_z) || ' then ''anomaly''
                else ''normal''
              end as STATUS
            from scored
            where WEEK_START = ''' || to_varchar(v_latest_week) || '''::date
              and HIST_MEAN is not null
              and HIST_STD is not null
              and HIST_STD <> 0
              and abs((VALUE - HIST_MEAN) / HIST_STD) >= ' || to_varchar(v_z);
  execute immediate :v_sql;

  delete from APP.FROSTBYTE_FORECAST_4W where RUN_ID=:v_run_id;
  insert into APP.FROSTBYTE_FORECAST_4W
  with w as (
    select *
    from APP.FROSTBYTE_SERIES_WEEKLY
    where RUN_ID=:v_run_id
  ),
  baseline as (
    select
      ORG_UNIT, METRIC_NAME, CATEGORY, DIM1, DIM2, DIM3,
      avg(VALUE)::float as AVG_LAST_8W
    from w
    where WEEK_START between dateadd(week, -8, :v_latest_week) and dateadd(week, -1, :v_latest_week)
    group by 1,2,3,4,5,6
  ),
  seq as (
    select seq4() as I
    from table(generator(rowcount => 104))
  ),
  horizon as (
    select dateadd(week, I + 1, :v_latest_week)::date as FORECAST_WEEK_START
    from seq
    where I < :v_fweeks
  ),
  raw as (
    select
      b.ORG_UNIT, b.METRIC_NAME, b.CATEGORY, b.DIM1, b.DIM2, b.DIM3,
      h.FORECAST_WEEK_START,
      b.AVG_LAST_8W as RAW_PRED
    from baseline b
    cross join horizon h
  )
  select
    :v_run_id,
    :CONFIG,
    :v_now,
    FORECAST_WEEK_START,
    ORG_UNIT,
    METRIC_NAME,
    CATEGORY,
    DIM1, DIM2, DIM3,
    case when :v_clamp and RAW_PRED < 0 then 0 else RAW_PRED end as FORECAST_VALUE,
    'avg_last_8w' as METHOD
  from raw;

  delete from APP.FROSTBYTE_SERIES_FEATURES where RUN_ID=:v_run_id;
  insert into APP.FROSTBYTE_SERIES_FEATURES
  with w as (
    select *
    from APP.FROSTBYTE_SERIES_WEEKLY
    where RUN_ID=:v_run_id
      and WEEK_START between dateadd(week, -1 * :v_min_weeks, :v_latest_week) and :v_latest_week
  ),
  idx as (
    select
      ORG_UNIT, METRIC_NAME, CATEGORY, DIM1, DIM2, DIM3, WEEK_START, VALUE,
      dense_rank() over(partition by ORG_UNIT, METRIC_NAME, CATEGORY, DIM1, DIM2, DIM3 order by WEEK_START) as T
    from w
  ),
  reg as (
    select
      ORG_UNIT, METRIC_NAME, CATEGORY, DIM1, DIM2, DIM3,
      avg(VALUE) as LEVEL_MEAN,
      stddev_samp(VALUE) as STD_VALUE,
      regr_slope(VALUE, T) as SLOPE,
      max(abs(WOW_PCT)) as RECENT_WOW_ABS
    from (
      select
        i.*,
        case
          when lag(VALUE) over(partition by ORG_UNIT, METRIC_NAME, CATEGORY, DIM1, DIM2, DIM3 order by WEEK_START) is null then null
          when lag(VALUE) over(partition by ORG_UNIT, METRIC_NAME, CATEGORY, DIM1, DIM2, DIM3 order by WEEK_START) = 0 then null
          else (VALUE / lag(VALUE) over(partition by ORG_UNIT, METRIC_NAME, CATEGORY, DIM1, DIM2, DIM3 order by WEEK_START)) - 1
        end as WOW_PCT
      from idx i
    )
    group by 1,2,3,4,5,6
  ),
  anr as (
    select
      ORG_UNIT, METRIC_NAME, CATEGORY, DIM1, DIM2, DIM3,
      count(*)::float / nullif(:v_min_weeks, 0) as ANOMALY_RATE
    from APP.FROSTBYTE_ANOMALIES_WEEKLY
    where RUN_ID=:v_run_id
    group by 1,2,3,4,5,6
  ),
  miss as (
    select
      ORG_UNIT, METRIC_NAME, CATEGORY, DIM1, DIM2, DIM3,
      max(MISSING_PCT) as MISSING_PCT
    from APP.FROSTBYTE_DQ_SERIES
    where RUN_ID=:v_run_id
    group by 1,2,3,4,5,6
  )
  select
    :v_run_id,
    :CONFIG,
    :v_now,
    reg.ORG_UNIT,
    reg.METRIC_NAME,
    reg.CATEGORY,
    reg.DIM1, reg.DIM2, reg.DIM3,
    reg.LEVEL_MEAN,
    case when reg.LEVEL_MEAN is null or reg.LEVEL_MEAN=0 then null else (reg.STD_VALUE / reg.LEVEL_MEAN) end as VOLATILITY,
    reg.SLOPE,
    reg.RECENT_WOW_ABS,
    coalesce(anr.ANOMALY_RATE, 0.0) as ANOMALY_RATE,
    coalesce(miss.MISSING_PCT, 0.0) as MISSING_PCT
  from reg
  left join anr
    on reg.ORG_UNIT=anr.ORG_UNIT and reg.METRIC_NAME=anr.METRIC_NAME and reg.CATEGORY=anr.CATEGORY
   and coalesce(reg.DIM1,'')=coalesce(anr.DIM1,'') and coalesce(reg.DIM2,'')=coalesce(anr.DIM2,'') and coalesce(reg.DIM3,'')=coalesce(anr.DIM3,'')
  left join miss
    on reg.ORG_UNIT=miss.ORG_UNIT and reg.METRIC_NAME=miss.METRIC_NAME and reg.CATEGORY=miss.CATEGORY
   and coalesce(reg.DIM1,'')=coalesce(miss.DIM1,'') and coalesce(reg.DIM2,'')=coalesce(miss.DIM2,'') and coalesce(reg.DIM3,'')=coalesce(miss.DIM3,'');

  delete from APP.FROSTBYTE_ACTIONS where RUN_ID=:v_run_id;
  insert into APP.FROSTBYTE_ACTIONS
  with dq as (
    select *
    from APP.FROSTBYTE_DQ_RUN
    where RUN_ID=:v_run_id
  ),
  top_anom as (
    select *
    from APP.FROSTBYTE_ANOMALIES_WEEKLY
    where RUN_ID=:v_run_id
    order by abs(Z_SCORE) desc
    limit 20
  )
  select
    :v_run_id,
    :CONFIG,
    :v_now,
    'DATA_QUALITY' as ACTION_TYPE,
    case
      when dq.HEALTH_SCORE < 0.75 then 'HIGH'
      when dq.HEALTH_SCORE < 0.90 then 'MED'
      else 'LOW'
    end as SEVERITY,
    'Run health_score='||to_varchar(dq.HEALTH_SCORE)||', missing_days='||to_varchar(dq.MISSING_DAYS)||', dup_rows='||to_varchar(dq.DUP_ROWS) as MESSAGE
  from dq
  union all
  select
    :v_run_id,
    :CONFIG,
    :v_now,
    'ANOMALY' as ACTION_TYPE,
    case
      when abs(Z_SCORE) >= (:v_z + 2) then 'HIGH'
      when abs(Z_SCORE) >= (:v_z + 1) then 'MED'
      else 'LOW'
    end as SEVERITY,
    'Investigate: week='||to_varchar(WEEK_START)||' org='||ORG_UNIT||' metric='||METRIC_NAME||' cat='||CATEGORY||
    ' dim1='||coalesce(DIM1,'')||' dim2='||coalesce(DIM2,'')||' dim3='||coalesce(DIM3,'')||
    ' value='||to_varchar(VALUE)||' z='||to_varchar(Z_SCORE) as MESSAGE
  from top_anom;

  if (:v_enable_cluster) then
    call APP.RUN_FROSTBYTE_CLUSTER(:v_run_id);
  end if;

  delete from APP.FROSTBYTE_WEEKLY_BRIEF where RUN_ID=:v_run_id;
  if (:v_enable_cortex) then
    v_sql := '
      insert into APP.FROSTBYTE_WEEKLY_BRIEF
      with dq as (
        select *
        from APP.FROSTBYTE_DQ_RUN
        where RUN_ID = '''||replace(v_run_id,'''','''''')||'''
      ),
      top_anom as (
        select ORG_UNIT, METRIC_NAME, CATEGORY, DIM1, DIM2, DIM3, VALUE, HIST_MEAN, Z_SCORE
        from APP.FROSTBYTE_ANOMALIES_WEEKLY
        where RUN_ID = '''||replace(v_run_id,'''','''''')||'''
        order by abs(Z_SCORE) desc
        limit 10
      ),
      top_actions as (
        select listagg(SEVERITY||'':''||ACTION_TYPE||'':''||MESSAGE, '' | '') within group(order by SEVERITY) as ACTS
        from APP.FROSTBYTE_ACTIONS
        where RUN_ID = '''||replace(v_run_id,'''','''''')||'''
      ),
      prompt as (
        select
          ''You are FrostByte. Write an executive weekly ops brief (<=180 words) with headings and bullets: Data quality, anomalies, forecast confidence, next steps. '' ||
          ''Week start: ''||to_varchar('''||to_varchar(v_latest_week)||'''::date)||''. ''||
          ''DQ: health_score=''||to_varchar(dq.HEALTH_SCORE)||'', rows=''||to_varchar(dq.ROW_COUNT)||'', missing_days=''||to_varchar(dq.MISSING_DAYS)||'', dup_rows=''||to_varchar(dq.DUP_ROWS)||'', negative=''||to_varchar(dq.NEGATIVE_VALUES)||''. ''||
          ''Top anomalies: ''||
          coalesce(
            (select listagg(ORG_UNIT||''|''||METRIC_NAME||''|''||CATEGORY||''|''||coalesce(DIM1,'''')||''|''||coalesce(DIM2,'''')||''|''||coalesce(DIM3,'''')||
                           ''|val=''||to_varchar(VALUE)||''|mean=''||to_varchar(HIST_MEAN)||''|z=''||to_varchar(Z_SCORE), ''; '')
             within group(order by abs(Z_SCORE) desc)
             from top_anom),
            ''none''
          )||''. ''||
          ''Top actions: ''||coalesce(top_actions.ACTS, ''none'')
          as P
        from dq cross join top_actions
      ),
      gen as (
        select '||v_cortex_fn||'('''||replace(v_model,'''','''''')||''', P) as RESP
        from prompt
      )
      select
        '''||replace(v_run_id,'''','''''')||''' as RUN_ID,
        '''||replace(CONFIG,'''','''''')||''' as CONFIG_NAME,
        '''||to_varchar(v_now)||'''::timestamp as COMPUTED_AT,
        '''||to_varchar(v_latest_week)||'''::date as WEEK_START,
        RESP as BRIEF
      from gen';
    execute immediate :v_sql;

    v_sql := '
      insert into APP.FROSTBYTE_AI_AUDIT
      select
        RUN_ID,
        CONFIG_NAME,
        COMPUTED_AT,
        ''weekly_brief'' as PURPOSE,
        '''||replace(v_model,'''','''''')||''' as MODEL_NAME,
        sha2(BRIEF, 256) as PROMPT_HASH,
        BRIEF as RESPONSE
      from APP.FROSTBYTE_WEEKLY_BRIEF
      where RUN_ID = '''||replace(v_run_id,'''','''''')||'''';
    execute immediate :v_sql;
  end if;

  update APP.FROSTBYTE_RUNS
  set STATUS='SUCCESS',
      STATUS_MESSAGE=case when :v_series_count < (select count(*) from APP.FROSTBYTE_SERIES_ALL) then 'CAPPED_SERIES: used top '||:v_series_count else null end,
      FINISHED_AT=current_timestamp(),
      WINDOW_START=:v_start,
      WINDOW_END=:v_end,
      LATEST_WEEK_START=:v_latest_week,
      SERIES_COUNT=:v_series_count,
      ROWS_SCANNED=:v_rows_scanned
  where RUN_ID=:v_run_id;

  return v_run_id;

exception
  when statement_error then
    update APP.FROSTBYTE_RUNS
    set STATUS='FAILED',
        STATUS_MESSAGE='Statement error',
        FINISHED_AT=current_timestamp()
    where RUN_ID=:v_run_id;
    return v_run_id || ':FAILED';
  when other then
    update APP.FROSTBYTE_RUNS
    set STATUS='FAILED',
        STATUS_MESSAGE='Unknown error',
        FINISHED_AT=current_timestamp()
    where RUN_ID=:v_run_id;
    return v_run_id || ':FAILED';
end;
$$;

create or replace secure view APP.VW_RUN_CONTEXT as
select
  r.RUN_ID,
  r.CONFIG_NAME,
  r.STATUS,
  r.STATUS_MESSAGE,
  r.STARTED_AT,
  r.FINISHED_AT,
  r.COMPUTED_AT,
  r.WINDOW_START,
  r.WINDOW_END,
  r.LATEST_WEEK_START,
  r.SERIES_COUNT,
  r.ROWS_SCANNED,
  r.INPUT_OBJECT,
  r.APP_VERSION,
  c.INPUT_TABLE,
  c.INPUT_REF,
  c.DATE_COL,
  c.ORG_COL,
  c.METRIC_COL,
  c.VALUE_COL,
  c.CATEGORY_COL,
  c.DIM1_COL,
  c.DIM2_COL,
  c.DIM3_COL,
  c.START_DATE,
  c.END_DATE,
  c.MISSING_DAY_TOLERANCE,
  c.MIN_WEEKS_HISTORY,
  c.Z_THRESHOLD,
  c.WOW_CHANGE_PCT,
  c.FORECAST_WEEKS,
  c.CLAMP_NONNEGATIVE,
  c.ENABLE_CLUSTERING,
  c.ENABLE_CORTEX,
  c.MODEL_NAME,
  c.CORTEX_FN,
  c.MAX_SERIES,
  c.MAX_ROWS_SCAN,
  c.CREATED_AT as CONFIG_CREATED_AT
from APP.FROSTBYTE_RUNS r
join APP.FROSTBYTE_CONFIG c
  on c.CONFIG_NAME = r.CONFIG_NAME;

create or replace secure view APP.VW_DQ_RUN as
select * from APP.FROSTBYTE_DQ_RUN;

create or replace secure view APP.VW_DQ_SERIES as
select * from APP.FROSTBYTE_DQ_SERIES;

create or replace secure view APP.VW_SERIES_TIMELINE as
select * from APP.FROSTBYTE_SERIES_WEEKLY;

create or replace secure view APP.VW_FORECAST as
select * from APP.FROSTBYTE_FORECAST_4W;

create or replace secure view APP.VW_FEATURES as
select * from APP.FROSTBYTE_SERIES_FEATURES;

create or replace secure view APP.VW_ACTIONS as
select * from APP.FROSTBYTE_ACTIONS;

create or replace secure view APP.VW_WEEKLY_BRIEF as
select * from APP.FROSTBYTE_WEEKLY_BRIEF;

create or replace secure view APP.VW_AI_AUDIT as
select * from APP.FROSTBYTE_AI_AUDIT;

create or replace secure view APP.VW_CLUSTERS as
select
  c.RUN_ID,
  r.CONFIG_NAME,
  c.COMPUTED_AT,
  c.ORG_UNIT,
  c.METRIC_NAME,
  c.CATEGORY,
  c.DIM1,
  c.DIM2,
  c.DIM3,
  c.CLUSTER_ID
from APP.FROSTBYTE_SERIES_CLUSTERS c
join APP.FROSTBYTE_RUNS r
  on r.RUN_ID = c.RUN_ID;

create or replace secure view APP.VW_TOP_ANOMALIES as
with lw as (
  select RUN_ID, LATEST_WEEK_START
  from APP.FROSTBYTE_RUNS
)
select
  a.*
from APP.FROSTBYTE_ANOMALIES_WEEKLY a
join lw
  on lw.RUN_ID = a.RUN_ID
 and lw.LATEST_WEEK_START = a.WEEK_START;

create or replace secure view APP.VW_TOP_MOVERS as
with lw as (
  select
    RUN_ID,
    CONFIG_NAME,
    LATEST_WEEK_START as WEEK_LATEST,
    dateadd(week, -1, LATEST_WEEK_START) as WEEK_PREV
  from APP.FROSTBYTE_RUNS
),
latest as (
  select *
  from APP.FROSTBYTE_SERIES_WEEKLY
),
prev as (
  select *
  from APP.FROSTBYTE_SERIES_WEEKLY
)
select
  l.RUN_ID,
  lw.CONFIG_NAME,
  lw.WEEK_LATEST as WEEK_START,
  l.ORG_UNIT,
  l.METRIC_NAME,
  l.CATEGORY,
  l.DIM1,
  l.DIM2,
  l.DIM3,
  l.VALUE as VALUE_LATEST,
  p.VALUE as VALUE_PREV,
  (l.VALUE - p.VALUE) as WOW_ABS,
  case when p.VALUE is null or p.VALUE = 0 then null else (l.VALUE - p.VALUE) / p.VALUE end as WOW_PCT
from latest l
join lw
  on lw.RUN_ID = l.RUN_ID
 and l.WEEK_START = lw.WEEK_LATEST
left join prev p
  on p.RUN_ID = l.RUN_ID
 and p.WEEK_START = lw.WEEK_PREV
 and p.ORG_UNIT = l.ORG_UNIT
 and p.METRIC_NAME = l.METRIC_NAME
 and p.CATEGORY = l.CATEGORY
 and coalesce(p.DIM1,'') = coalesce(l.DIM1,'')
 and coalesce(p.DIM2,'') = coalesce(l.DIM2,'')
 and coalesce(p.DIM3,'') = coalesce(l.DIM3,'');

create or replace secure view APP.VW_DASHBOARD_KPIS as
with an as (
  select RUN_ID, count(*) as ANOMALY_COUNT
  from APP.FROSTBYTE_ANOMALIES_WEEKLY
  group by RUN_ID
),
fc as (
  select
    RUN_ID,
    count(distinct
      ORG_UNIT || '|' || METRIC_NAME || '|' || CATEGORY || '|' ||
      coalesce(DIM1,'') || '|' || coalesce(DIM2,'') || '|' || coalesce(DIM3,'')
    ) as FORECAST_SERIES_COUNT
  from APP.FROSTBYTE_FORECAST_4W
  group by RUN_ID
)
select
  r.RUN_ID,
  r.CONFIG_NAME,
  r.LATEST_WEEK_START,
  d.HEALTH_SCORE,
  d.MISSING_DAYS,
  d.DUP_ROWS,
  d.NEGATIVE_VALUES,
  d.NULL_METRIC_VALUE,
  d.ROW_COUNT,
  coalesce(an.ANOMALY_COUNT, 0) as ANOMALY_COUNT,
  coalesce(fc.FORECAST_SERIES_COUNT, 0) as FORECAST_SERIES_COUNT
from APP.FROSTBYTE_RUNS r
left join APP.FROSTBYTE_DQ_RUN d
  on d.RUN_ID = r.RUN_ID
left join an
  on an.RUN_ID = r.RUN_ID
left join fc
  on fc.RUN_ID = r.RUN_ID;

create or replace secure view APP.VW_FORECAST_JOINED as
with lw as (
  select RUN_ID, LATEST_WEEK_START
  from APP.FROSTBYTE_RUNS
)
select
  f.RUN_ID,
  f.CONFIG_NAME,
  f.COMPUTED_AT,
  f.FORECAST_WEEK_START,
  f.ORG_UNIT,
  f.METRIC_NAME,
  f.CATEGORY,
  f.DIM1,
  f.DIM2,
  f.DIM3,
  f.FORECAST_VALUE,
  f.METHOD,
  a.VALUE as LAST_ACTUAL_VALUE,
  a.WEEK_START as LAST_ACTUAL_WEEK,
  dq.HEALTH_SCORE as SERIES_HEALTH_SCORE,
  dq.MISSING_PCT,
  feat.VOLATILITY,
  feat.ANOMALY_RATE,
  feat.SLOPE
from APP.FROSTBYTE_FORECAST_4W f
join lw
  on lw.RUN_ID = f.RUN_ID
left join APP.FROSTBYTE_SERIES_WEEKLY a
  on a.RUN_ID = f.RUN_ID
 and a.WEEK_START = lw.LATEST_WEEK_START
 and a.ORG_UNIT = f.ORG_UNIT
 and a.METRIC_NAME = f.METRIC_NAME
 and a.CATEGORY = f.CATEGORY
 and coalesce(a.DIM1,'') = coalesce(f.DIM1,'')
 and coalesce(a.DIM2,'') = coalesce(f.DIM2,'')
 and coalesce(a.DIM3,'') = coalesce(f.DIM3,'')
left join APP.FROSTBYTE_DQ_SERIES dq
  on dq.RUN_ID = f.RUN_ID
 and dq.ORG_UNIT = f.ORG_UNIT
 and dq.METRIC_NAME = f.METRIC_NAME
 and dq.CATEGORY = f.CATEGORY
 and coalesce(dq.DIM1,'') = coalesce(f.DIM1,'')
 and coalesce(dq.DIM2,'') = coalesce(f.DIM2,'')
 and coalesce(dq.DIM3,'') = coalesce(f.DIM3,'')
left join APP.FROSTBYTE_SERIES_FEATURES feat
  on feat.RUN_ID = f.RUN_ID
 and feat.ORG_UNIT = f.ORG_UNIT
 and feat.METRIC_NAME = f.METRIC_NAME
 and feat.CATEGORY = f.CATEGORY
 and coalesce(feat.DIM1,'') = coalesce(f.DIM1,'')
 and coalesce(feat.DIM2,'') = coalesce(f.DIM2,'')
 and coalesce(feat.DIM3,'') = coalesce(f.DIM3,'');
