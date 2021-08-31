.\sqlite3.exe < test_pre.txt

java -Dfile.encoding="UTF-8" -jar etl/out/artifacts/etl_jar/etl.jar etl/flow_test.properties

.\sqlite3.exe < test_post.txt
