echo off
echo Starting time is: %TIME%
mkdir EDBT_running_example_costs

echo ************************************************************************** Baseline design
echo ========================================================================== In NF2
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_baseline_NF2 --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_baseline --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_baseline_NF2 --save_cost --cost_file EDBT_running_example_costs/baseline_NF2_202505 --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_baseline_NF2 --save_cost --cost_file EDBT_running_example_costs/baseline_NF2_202506 --query_file files/queries/SDSS_2506

echo ************************************************************************** Design optimized for the workload of May 2025
echo ========================================================================== In 1NF with an FK for the association
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_withFK --supersede --create design --paradigm 1NF --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_opt202505_withFK --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_withFK --save_cost --cost_file EDBT_running_example_costs/opt202505_withFK_202505 --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_withFK --save_cost --cost_file EDBT_running_example_costs/opt202505_withFK_202506 --query_file files/queries/SDSS_2506

echo ========================================================================== In 1NF with an intermediate table for the association
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_withTable --supersede --create design --paradigm 1NF --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_opt202505_withTable --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_withTable --save_cost --cost_file EDBT_running_example_costs/opt202505_withTable_202505 --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_withTable --save_cost --cost_file EDBT_running_example_costs/opt202505_withTable_202506 --query_file files/queries/SDSS_2506

echo ========================================================================== In NF2 with an FK for the association
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_NF2_withFK --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_opt202505_withFK --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_NF2_withFK --save_cost --cost_file EDBT_running_example_costs/opt202505_NF2_withFK_202505 --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_NF2_withFK --save_cost --cost_file EDBT_running_example_costs/opt202505_NF2_withFK_202506 --query_file files/queries/SDSS_2506

echo ========================================================================== In NF2 with an intermediate table for the association
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_NF2_withTable --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_opt202505_withTable --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_NF2_withTable --save_cost --cost_file EDBT_running_example_costs/opt202505_NF2_withTable_202505 --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_NF2_withTable --save_cost --cost_file EDBT_running_example_costs/opt202505_NF2_withTable_202506 --query_file files/queries/SDSS_2506

echo ========================================================================== In NF2 with a list of FKs for the association
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_NF2_withList --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec NF2/SDSS_simple_opt202505_withList --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_NF2_withList --save_cost --cost_file EDBT_running_example_costs/opt202505_NF2_withList_202505 --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202505_NF2_withList --save_cost --cost_file EDBT_running_example_costs/opt202505_NF2_withList_202506 --query_file files/queries/SDSS_2506

echo ************************************************************************** Design optimized for the workload of June 2025
echo ========================================================================== In 1NF with an FK for the association
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_withFK_minimal --supersede --create design --paradigm 1NF --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_opt202506_withFK_minimal --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_withFK_minimal --save_cost --cost_file EDBT_running_example_costs/opt202506_withFK_202505_minimal --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_withFK_minimal --save_cost --cost_file EDBT_running_example_costs/opt202506_withFK_202506_minimal --query_file files/queries/SDSS_2506

echo ========================================================================== In 1NF with an intermediate table for the association
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_withTable_minimal --supersede --create design --paradigm 1NF --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_opt202506_withTable_minimal --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_withTable_minimal --save_cost --cost_file EDBT_running_example_costs/opt202506_withTable_202505_minimal --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_withTable_minimal --save_cost --cost_file EDBT_running_example_costs/opt202506_withTable_202506_minimal --query_file files/queries/SDSS_2506

echo ========================================================================== In NF2 with an FK for the association
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withFK_minimal --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_opt202506_withFK_minimal --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withFK_minimal --save_cost --cost_file EDBT_running_example_costs/opt202506_NF2_withFK_202505_minimal --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withFK_minimal --save_cost --cost_file EDBT_running_example_costs/opt202506_NF2_withFK_202506_minimal --query_file files/queries/SDSS_2506

echo ========================================================================== In NF2 with an intermediate table for the association
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withTable_minimal --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_opt202506_withTable_minimal --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withTable_minimal --save_cost --cost_file EDBT_running_example_costs/opt202506_NF2_withTable_202505_minimal --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withTable_minimal --save_cost --cost_file EDBT_running_example_costs/opt202506_NF2_withTable_202506_minimal --query_file files/queries/SDSS_2506

echo ========================================================================== In NF2 with a list of FKs for the association
echo -------------------------------------------------------------------------- Database creation
REM Be careful, because this design is taken over a modified JSON design (instead of directly taking the one generated from XML)
REM From the automatically generated one, it is just necessary to remove repeated sets
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withList_minimal --supersede --create design --paradigm NF2_JSON --dsg_fmt JSON --dsg_spec NF2/SDSS_simple_opt202506_withList_minimal_modified --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withList_minimal --save_cost --cost_file EDBT_running_example_costs/opt202506_NF2_withList_202505_minimal --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withList_minimal --save_cost --cost_file EDBT_running_example_costs/opt202506_NF2_withList_202506_minimal --query_file files/queries/SDSS_2506

echo ************************************************************************** Design with middle ground optimization
echo ========================================================================== In 1NF with an FK for the association
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_withFK --supersede --create design --paradigm 1NF --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_opt202506_withFK --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_withFK --save_cost --cost_file EDBT_running_example_costs/opt202506_withFK_202505 --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_withFK --save_cost --cost_file EDBT_running_example_costs/opt202506_withFK_202506 --query_file files/queries/SDSS_2506

echo ========================================================================== In 1NF with an intermediate table for the association
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_withTable --supersede --create design --paradigm 1NF --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_opt202506_withTable --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_withTable --save_cost --cost_file EDBT_running_example_costs/opt202506_withTable_202505 --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm 1NF --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_withTable --save_cost --cost_file EDBT_running_example_costs/opt202506_withTable_202506 --query_file files/queries/SDSS_2506

echo ========================================================================== In NF2 with an FK for the association
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withFK --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_opt202506_withFK --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withFK --save_cost --cost_file EDBT_running_example_costs/opt202506_NF2_withFK_202505 --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withFK --save_cost --cost_file EDBT_running_example_costs/opt202506_NF2_withFK_202506 --query_file files/queries/SDSS_2506

echo ========================================================================== In NF2 with an intermediate table for the association
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withTable --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec 1NF/SDSS_simple_opt202506_withTable --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withTable --save_cost --cost_file EDBT_running_example_costs/opt202506_NF2_withTable_202505 --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withTable --save_cost --cost_file EDBT_running_example_costs/opt202506_NF2_withTable_202506 --query_file files/queries/SDSS_2506

echo ========================================================================== In NF2 with a list of FKs for the association
echo -------------------------------------------------------------------------- Database creation
python catalogAction.py --hide_warnings --hide_progress --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withList --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec NF2/SDSS_simple_opt202506_withList --src_sch dorm_edbt_baseline --src_kind 1NF
echo .......................................................................... Get query performance for workload of May 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withList --save_cost --cost_file EDBT_running_example_costs/opt202506_NF2_withList_202505 --query_file files/queries/SDSS_2505
echo .......................................................................... Get query performance for workload of June 2025
python queryExecutor.py --hide_warnings --hide_progress --paradigm NF2_JSON --dbconf_file db_conf_unibo.txt --dbschema dorm_edbt_opt202506_NF2_withList --save_cost --cost_file EDBT_running_example_costs/opt202506_NF2_withList_202506 --query_file files/queries/SDSS_2506

echo Ending time: %TIME%