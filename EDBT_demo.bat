echo off
echo ========================================================================== Creation of source schema with design 1NF/book-authors-topic
echo -------------------------------------------------------------------------- Table creation at baseline schema
python catalogAction.py --hide_progress --dbconf_file DORM-demo.txt --dbschema baseline --check --supersede --create design --paradigm 1NF --dsg_fmt XML --dsg_spec 1NF/book-authors-topic
echo .......................................................................... Insert execution
python insertExecutor.py --hide_progress --dbconf_file DORM-demo.txt --dbschema baseline --paradigm 1NF --insert_file files/inserts/book-authors-topic.json
echo .......................................................................... Query execution
python queryExecutor.py --hide_progress --dbconf_file DORM-demo.txt --dbschema baseline --print_rows --print_counter --show_sql --paradigm 1NF --query_file files/queries/EDBT_demo.json

echo ========================================================================== 1NF/book-authors-topic_test1
echo -------------------------------------------------------------------------- Table creation
python catalogAction.py --hide_progress --dbconf_file DORM-demo.txt --dbschema _1NF_join_materialized --check --supersede --create design --paradigm 1NF --dsg_fmt XML --dsg_spec 1NF/book-authors-topic_test1 --src_sch baseline --src_kind 1NF
echo .......................................................................... Query execution
python queryExecutor.py --hide_progress --dbconf_file DORM-demo.txt --dbschema _1NF_join_materialized --print_rows --print_counter --show_sql --paradigm 1NF --query_file files/queries/EDBT_demo.json

echo ========================================================================== 1NF/book-authors-topic_partitioned
python catalogAction.py --hide_progress --dbconf_file DORM-demo.txt --dbschema _1NF_vertical_partitions --check --supersede --create design --paradigm 1NF --dsg_fmt XML --dsg_spec 1NF/book-authors-topic_partitioned --src_sch baseline --src_kind 1NF
echo .......................................................................... Query execution
python queryExecutor.py --hide_progress --dbconf_file DORM-demo.txt --dbschema _1NF_vertical_partitions --print_rows --print_counter --show_sql --paradigm 1NF --query_file files/queries/EDBT_demo.json

echo ========================================================================== 1NF/book-authors-topic stored in NF2_JSON
echo -------------------------------------------------------------------------- Table creation
python catalogAction.py --hide_progress --dbconf_file DORM-demo.txt --dbschema NF2_baseline --check --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec 1NF/book-authors-topic --src_sch baseline --src_kind 1NF
echo .......................................................................... Query execution
python queryExecutor.py --hide_progress --dbconf_file DORM-demo.txt --dbschema NF2_baseline --print_rows --print_counter --show_sql --paradigm NF2_JSON --query_file files/queries/EDBT_demo.json

echo ========================================================================== NF2/book-authors-topic_test1
python catalogAction.py --hide_progress --dbconf_file DORM-demo.txt --dbschema NF2_list_of_topic_ids --check --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec NF2/book-authors-topic_test1 --src_sch baseline --src_kind 1NF
echo .......................................................................... Query execution
python queryExecutor.py --hide_progress --dbconf_file DORM-demo.txt --dbschema NF2_list_of_topic_ids --print_rows --print_counter --show_sql --paradigm NF2_JSON --query_file files/queries/EDBT_demo.json

echo ========================================================================== NF2/book-authors-topic_test2
python catalogAction.py --hide_progress --dbconf_file DORM-demo.txt --dbschema NF2_list_of_topics --check --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec NF2/book-authors-topic_test2 --src_sch baseline --src_kind 1NF
echo .......................................................................... Query execution
python queryExecutor.py --hide_progress --dbconf_file DORM-demo.txt --dbschema NF2_list_of_topics --print_rows --print_counter --show_sql --paradigm NF2_JSON --query_file files/queries/EDBT_demo.json

echo ========================================================================== NF2/book-authors-topic_test3
python catalogAction.py --hide_progress --dbconf_file DORM-demo.txt --dbschema NF2_single_document_per_book --check --supersede --create design --paradigm NF2_JSON --dsg_fmt XML --dsg_spec NF2/book-authors-topic_test3 --src_sch baseline --src_kind 1NF
echo .......................................................................... Query execution
python queryExecutor.py --hide_progress --dbconf_file DORM-demo.txt --dbschema NF2_single_document_per_book --print_rows --print_counter --show_sql --paradigm NF2_JSON --query_file files/queries/EDBT_demo.json