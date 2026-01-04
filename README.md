# FrostByte## FrostByte Native App

FrostByte turns weekly operational metrics into data quality checks, anomaly insights, and forecasts. Each customer connects their own data via an app reference and runs the analysis inside their account.

## Connect your data (required)
Bind your table or view to the `INPUT_DATA` reference before running the app.

```sql
call <app_name>.CONFIG.REGISTER_REFERENCE(
  'INPUT_DATA',
  'ADD',
  '<db>.<schema>.<table_or_view>'
);
```

Optional: verify the binding.

```sql
select system$get_reference('INPUT_DATA');
```

## Run the app
Open the app UI and click **Run Now**, or run it manually:

```sql
call <app_name>.APP.RUN_FROSTBYTE('demo');
```
