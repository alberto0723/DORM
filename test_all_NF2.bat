echo off
echo ========================================================================== 1NF/artist-record-track
echo -------------------------------------------------------------------------- Table creation
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec 1NF/artist-record-track
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/artist-record-track.json
echo ========================================================================== 1NF/artist-record-track_test1
echo -------------------------------------------------------------------------- Table creation
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec 1NF/artist-record-track_test1
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/artist-record-track.json
echo ========================================================================== 1NF/book-authors_test2
echo -------------------------------------------------------------------------- Table creation at source schema
python catalogAction.py --dbconf_file db_conf.txt --dbschema source --check --supersede --create design --paradigm NF2_JSON --dsg_spec 1NF/book-authors_test2
echo .......................................................................... Insert execution
python insertionExecutor.py --dbconf_file db_conf.txt --dbschema source --paradigm NF2_JSON --insert_file files/inserts/book-authors.json
echo ========================================================================== 1NF/book-authors
echo -------------------------------------------------------------------------- Table creation
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec 1NF/book-authors --src_sch source --src_kind NF2_JSON
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/book-authors.json
echo ========================================================================== 1NF/book-authors_test1
echo -------------------------------------------------------------------------- Table creation
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec 1NF/book-authors_test1 --src_sch source --src_kind NF2_JSON
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/book-authors.json
echo ========================================================================== 1NF/book-authors_test2
echo -------------------------------------------------------------------------- Table creation
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec 1NF/book-authors_test2 --src_sch source --src_kind NF2_JSON
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/book-authors.json
echo ========================================================================== 1NF/book-authors_test3
echo -------------------------------------------------------------------------- Table creation
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec 1NF/book-authors_test3 --src_sch source --src_kind NF2_JSON
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/book-authors.json
echo ========================================================================== NF2/book-authors_test1
echo -------------------------------------------------------------------------- Table creation
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec NF2/book-authors_test1 --src_sch source --src_kind NF2_JSON
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/book-authors.json
echo ========================================================================== NF2/book-authors_test2
echo -------------------------------------------------------------------------- Table creation
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec NF2/book-authors_test2 --src_sch source --src_kind NF2_JSON
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/book-authors.json
echo ========================================================================== NF2/book-authors_test3
echo -------------------------------------------------------------------------- Table creation
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec NF2/book-authors_test3 --src_sch source --src_kind NF2_JSON
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/book-authors.json
echo ========================================================================== 1NF/students-workers_AllInSuperclassTable
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec 1NF/students-workers_AllInSuperclassTable
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/students-workers.json
echo ========================================================================== 1NF/students-workers_OneClassOneTable
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec 1NF/students-workers_OneClassOneTable
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/students-workers.json
echo ========================================================================== 1NF/students-workers_OneTablePerSubclass
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec 1NF/students-workers_OneTablePerSubclass
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/students-workers.json
echo ========================================================================== 1NF/students-workers_OneTableForTheIntersection
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec 1NF/students-workers_OneTableForTheIntersection
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/students-workers.json
echo ========================================================================== 1NF/book-authors-topic
echo -------------------------------------------------------------------------- Table creation
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec 1NF/book-authors-topic --src_sch source2 --src_kind 1NF
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/book-authors-topic.json
echo ========================================================================== 1NF/book-authors-topic_test1
echo -------------------------------------------------------------------------- Table creation
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec 1NF/book-authors-topic_test1 --src_sch source2 --src_kind 1NF
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/book-authors-topic.json
echo ========================================================================== 1NF/book-authors-topic_partitioned
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec 1NF/book-authors-topic_partitioned --src_sch source2 --src_kind 1NF
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/book-authors-topic.json
echo ========================================================================== NF2/book-authors-topic_test1
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec NF2/book-authors-topic_test1 --src_sch source2 --src_kind 1NF
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/book-authors-topic.json
echo ========================================================================== NF2/book-authors-topic_test2
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec NF2/book-authors-topic_test2 --src_sch source2 --src_kind 1NF
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/book-authors-topic.json
echo ========================================================================== NF2/book-authors-topic_test3
python catalogAction.py --dbconf_file db_conf.txt --check --supersede --create design --paradigm NF2_JSON --dsg_spec NF2/book-authors-topic_test3 --src_sch source2 --src_kind 1NF
echo .......................................................................... Query execution
python queryExecutor.py --dbconf_file db_conf.txt --paradigm NF2_JSON --query_file files/queries/book-authors-topic.json