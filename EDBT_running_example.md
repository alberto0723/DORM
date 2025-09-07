# Download data from SDSS

# Create and populate DORM baseline

## Create an DORM catalog without data

```bash
python catalogAction.py --hide_warnings --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_baseline --supersede --create design --paradigm 1NF --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_baseline
```

## Migrate the data

Data downloaded from SDSS and already loaded in the DBMS under schema `relational2`, now needs to be inserted in the tables of DORM under schema `dorm_edbt_baseline`. 
For this, some simple insertions can be used, but due to the high number of attributes the insertions are generated automatically with [a python script](catalog/XML2JSON/domain/SQL2INSERT.py). 

This generates a different file for each table under `catalog/XML2JSON/domain/data`.
Then, the ones corresponding to the tables in the DORM catalog must be executed.
However, since the insertion statements are generated from the extracted data, if some foreign keys are not used, they should be removed before executing the statements.
This is the case for `photoobjall_fieldid` and `specobjall_plateid`.

## Complete the data with discriminants

Specializations in the domain require a discriminant that indicates the name of the subclasse the tuple belongs to.
These can be generated with the following update statements:

```sql
UPDATE dorm_edbt_baseline.photoobjall_table
SET photoobjall_discrG1 =   CASE WHEN photoobjall_mode = 1 AND photoobjall_clean = 1 THEN 'photoobj'
								ELSE 'photoobjcompl' END,
	photoobj_discrG2 =      CASE WHEN photoobjall_mode = 1 AND photoobjall_clean = 1 AND (photoobjall_resolveStatus & 0x01) != 0 THEN 'photoprimary'
								ELSE 'photoprimarycompl' END,
	photoobjall_discrG4 =   CASE WHEN photoobjall_objid IN (SELECT photoobjall_objid FROM dorm_edbt_baseline.photoz_table) THEN 'photoz'
								ELSE 'photozcompl' END;													

UPDATE dorm_edbt_baseline.specobjall_table
SET specobjall_discrG3 = CASE WHEN specobjall_scienceprimary = 1 THEN 'specobj'
												 ELSE 'specobjcompl' END;
```

## Update the statistics

```sql
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = 'dorm_edbt_baseline'
          AND table_type = 'BASE TABLE'
    LOOP
        EXECUTE format('ANALYZE %I.%I;', r.table_schema, r.table_name);
    END LOOP;
END;
$$;
```

## Annotate the catalog as containing data

Finally, we need to annotate in the DORM schema that it contains data, so that migration from this is allowed.
This can be done with the following SQL code:

```sql
DO $$
DECLARE
    metadata JSONB;
BEGIN
    SELECT d.description::JSONB INTO metadata
    FROM pg_namespace n JOIN pg_description d ON d.objoid = n.oid
    WHERE n.nspname = 'dorm_edbt_baseline';

    EXECUTE format('COMMENT ON SCHEMA dorm_edbt_baseline IS %L', metadata || '{"has_data": true}');
END $$
```

## Test query performance

```bash
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_baseline --print_cost --save_cost --query_file files/queries/SDSS_2505
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_baseline --print_cost --save_cost --query_file files/queries/SDSS_2506
```

# Migrate the database to NF2

```bash
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_baseline_NF2 --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_baseline  --src_sch dorm_edbt_baseline --src_kind 1NF
```

## Update the statistics

```sql
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = 'dorm_edbt_baseline_NF2'
          AND table_type = 'BASE TABLE'
    LOOP
        EXECUTE format('ANALYZE %I.%I;', r.table_schema, r.table_name);
    END LOOP;
END;
$$;
```

## Test query performance

```bash
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_baseline_NF2 --print_cost --save_cost --query_file files/queries/SDSS_2505
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_baseline_NF2 --print_cost --save_cost --query_file files/queries/SDSS_2506
```

# Migrate database to an optimized version based on the workload of May 2025

```bash
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505 --supersede --create design --paradigm 1NF --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_opt202505  --src_sch dorm_edbt_baseline --src_kind 1NF
```

## Update the statistics

```sql
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = 'dorm_edbt_opt202505'
          AND table_type = 'BASE TABLE'
    LOOP
        EXECUTE format('ANALYZE %I.%I;', r.table_schema, r.table_name);
    END LOOP;
END;
$$;
```

## Test query performance

```bash
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505 --print_cost --save_cost --query_file files/queries/SDSS_2505
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505 --print_cost --save_cost --query_file files/queries/SDSS_2506
```


# Migrate database to an optimized version based on the workload of May 2025, including a list of PhotoObject IDs inside the SpecObj

```bash
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_nf2_plus --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec NF2/SDSS_simple_opt202505  --src_sch dorm_edbt_baseline --src_kind 1NF
```

## Update the statistics

```sql
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = 'dorm_edbt_opt202505_nf2_plus'
          AND table_type = 'BASE TABLE'
    LOOP
        EXECUTE format('ANALYZE %I.%I;', r.table_schema, r.table_name);
    END LOOP;
END;
$$;
```

## Test query performance

```bash
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_nf2_plus --print_cost --save_cost --query_file files/queries/SDSS_2505
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_nf2_plus --print_cost --save_cost --query_file files/queries/SDSS_2506
```
