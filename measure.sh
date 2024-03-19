
#rm -rf tpch_3/
#mkdir -p tpch_3/

rm -rf imdb/
mkdir -p imdb/

#echo "run TPCH-3 with query split"
#./measure TPCH 1
#echo "copy to tpch_3_query_split"
#mv log.txt tpch_3/tpch_3_query_split_log.txt
#mv result.txt tpch_3/tpch_3_query_split_result.txt

#echo "run TPCH-3 with vanilla postgres"
#./measure TPCH 0
#echo "copy to tpch_3_vanilla"
#mv log.txt tpch_3/tpch_3_vanilla_log.txt
#mv result.txt tpch_3/tpch_3_vanilla_result.txt

echo "run IMDB with vanilla postgres"
./measure IMDB Postgres
echo "copy to imdb_vanilla"
mv log.txt imdb/imdb_vanilla_log.txt
mv result.txt imdb/imdb_vanilla_result.txt

echo "run IMDB with query split"
./measure IMDB relationshipcenter
echo "copy to imdb"
mv log.txt imdb/imdb_query_split_log.txt
mv result.txt imdb/imdb_query_split_result.txt

echo "run IMDB with optimal"
./measure IMDB Optimal
echo "copy to imdb"
mv log.txt imdb/imdb_optimal_log.txt
mv result.txt imdb/imdb_optimal_result.txt
