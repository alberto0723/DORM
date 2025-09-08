# Download data from SDSS

THIS NEEDS TO BE EXPLAINED

# Create and populate DORM baseline

The purpose of the experiment is to compare the performance of different SDSS database designs.
It will be seen that redesigning, migrating data and rewriting queries is happening automatically behind the scenes.
Thus, all we will compare are the different query execution times.

## Create an DORM catalog without data

First of all, we need to create a DORM catalog corresponding to the baseline database.

```bash
python catalogAction.py --hide_warnings --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_baseline --supersede --create design --paradigm 1NF --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_baseline
```

This will be used to:
<ol type="a">
  <li>Compare the costs of the alternative designs.</li>
  <li>Take is as source to migrate the data to the alternative designs.</li>
</ol>

## Migrate the data

Data downloaded from SDSS and already loaded in the DBMS under schema `relational2`, now needs to be inserted in the tables of DORM under schema `dorm_edbt_baseline`. 
For this, some simple insertions can be used, but due to the high number of attributes the insertions are generated automatically with [a python script](catalog/XML2JSON/domain/SQL2INSERT.py). 

This generates a different file for each table under `catalog/XML2JSON/domain/data`.
Then, the ones corresponding to the tables in the DORM catalog must be executed.
However, since the insertion statements are generated from the extracted data, if some foreign keys are not used, they should be removed before executing the statements.
This is the case for `photoobjall_fieldid` and `specobjall_plateid`.

## Complete the data with discriminants

Specializations in the domain require a discriminant that indicates the name of the subclass the tuple belongs to.
These can be generated with the following update SQL statements:

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

The cost of the queries is obtained from PostgreSQL optimizer, which requires gathering statistics before.
DORM automatically updates them after migrating data, but since data was migrated for the baseline manually, so the updating of statistics needs to be done manually, too.

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

Finally, we need to annotate in the DORM schema that it contains data, so that migration from this is allowed later.
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

Once the database is ready, this first creates a folder to leave the CSV files and then runs the queries of two different workloads on the baseline design.

```bash
mkdir EDBT_running_example_costs
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_baseline --save_cost --cost_file EDBT_running_example_costs/baseline_202505 --query_file files/queries/SDSS_2505
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_baseline --save_cost --cost_file EDBT_running_example_costs/baseline_202506 --query_file files/queries/SDSS_2506
```

# Run the different designs

There is a batch file that successively creates different alternative designs, both in 1NF and NF2, and runs the two workload on them.
The costs of the different queries are left in different CSV files under the same output folder.

```bash
EDBT_running_example.bat
```