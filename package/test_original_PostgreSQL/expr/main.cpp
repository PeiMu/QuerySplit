// g++ main.cpp -O3 -std=c++17 -lpq -L/home/pei/Project/project_bins/lib/ -o main

#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <iostream>
#include <string.h>
#include <ctime>
//#include <windows.h>

#define JOB 91
#define DSB 37
#define SSB 13
#define TPCH 22

#define ITERATION_TIME 11

#include <chrono>
#include <fstream>
#include <vector>
#include <cmath>

typedef struct timespec timespec;
timespec diff(timespec start, timespec end)
{
    timespec temp;
    if ((end.tv_nsec-start.tv_nsec)<0) {
        temp.tv_sec = end.tv_sec-start.tv_sec-1;
        temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
    } else {
        temp.tv_sec = end.tv_sec-start.tv_sec;
        temp.tv_nsec = end.tv_nsec-start.tv_nsec;
    }
    return temp;
}

std::string printTimeSpec(timespec t, const char* prefix) {
    std::string str = prefix + std::to_string(t.tv_sec) + "." + std::to_string(t.tv_nsec) + " s";
    return str;
    //    printf("%s: %d.%09d\n", prefix, (int)t.tv_sec, (int)t.tv_nsec);
}

std::chrono::high_resolution_clock::time_point chrono_tic() {
    return std::chrono::high_resolution_clock::now();
}

long chrono_toc(std::chrono::high_resolution_clock::time_point* start_time, const char* prefix) {
    auto current_time = std::chrono::high_resolution_clock::now();
    auto time_diff = std::chrono::duration_cast<std::chrono::microseconds>(current_time - *start_time).count();
    std::string str = prefix + std::to_string(time_diff) + " us";
//    std::cout << str << std::endl;
    *start_time = current_time;
    return time_diff/1000;
}

double calculateGeometricMean(const std::vector<double>& numbers) {
    double product = 1.0;
    for (double num : numbers) {
        product *= num;
    }
    return std::pow(product, 1.0 / numbers.size());
}

double calculateDeviation(const std::vector<double>& numbers, double geometricMean) {
    double sum = 0.0;
    for (double num : numbers) {
        sum += std::pow(num - geometricMean, 2);
    }
    return std::sqrt(sum / numbers.size());
}

int main()
{
	char file[100] = "./log.txt";
	FILE* f_log = fopen(file, "w+");
	fclose(f_log);
	for (int i = 1; i <= JOB; i++)
	{
		char fpath[100];
		//sprintf(fpath, "D:\\Database\\x64\\experiment\\benchmark\\DSB\\nonSPJ\\query_1.sql");
		sprintf(fpath, "/home/pei/Project/QuerySplit/package/query/%d.sql", i);
        std::cout << i << " " << fpath << "   ";
		FILE* f_train = fopen(fpath, "r");
		char line[4096] = "";
		int num = 0;
		while (fgets(line, 4096, f_train) != NULL) {
            num++;
            f_log = fopen(file, "a");
            std::vector<double> time_vec;
            for (int iter = 0; iter < ITERATION_TIME; iter++) {
                PGconn *conn = PQsetdbLogin("localhost", "5432", NULL, NULL, "imdb", "imdb", "");
                if (PQstatus(conn) == CONNECTION_BAD) {
                    std::cout << PQerrorMessage(conn) << std::endl;
                    fprintf(f_log, "%s\n", PQerrorMessage(conn));
                    PQfinish(conn);
                    fclose(f_log);
                    fclose(f_train);
                    return 0;
                }
//                PQexec(conn, "set enable_nestloop = false;");
                // QuerySplit paper config
                PQexec(conn, "set max_parallel_workers = '0';");
                PQexec(conn, "set max_parallel_workers_per_gather = '0';");
                PQexec(conn, "set shared_buffers = '512MB';");
                PQexec(conn, "set temp_buffers = '2047MB';");
                PQexec(conn, "set work_mem = '2047MB';");
                PQexec(conn, "set effective_cache_size = '4 GB';");
                PQexec(conn, "set statement_timeout = '1000s';");
                PQexec(conn, "set default_statistics_target = 100;");
                //PQexec(conn, "set statement_timeout = \'30s\';");
                //char cmd[2048];
                //sprintf(cmd, "explain(analyze) %s", line);
//			clock_t start, end;
//			start = clock();
                auto timer = chrono_tic();
                PGresult *result = PQexec(conn, line);
                double time_diff = chrono_toc(&timer, "time consumption");
                if (iter != 0)
                    time_vec.emplace_back(time_diff);
                //PGresult* result = PQexec(conn, cmd);
                //std::cout << "Ok!" << std::endl;
//			end = clock();
                switch (PQresultStatus(result)) {
                    case PGRES_EMPTY_QUERY: {
                        std::cout << "An unexpected empty query;" << std::endl;
                        fprintf(f_log, "An unexpected empty query;\n");
                        break;
                    }
                    case PGRES_COMMAND_OK: {
//				double endtime = (double)(end - start) / (CLOCKS_PER_SEC/1000);
//				std::cout << endtime << std::endl;
//				fprintf(f_log, "%d\n", endtime);
                        std::cout << time_diff << std::endl;
                        break;
                    }
                    case PGRES_TUPLES_OK: {
//				double endtime = (double)(end - start) / (CLOCKS_PER_SEC/1000);
//				std::cout << endtime << std::endl;
//				fprintf(f_log, "%d\n", endtime);
                        std::cout << time_diff << std::endl;
                        if (0 == iter) {
                            int tuple_num = PQntuples(result);
                            int field_num = PQnfields(result);
                            for (int tuple = 0; tuple < tuple_num; tuple++) {
                                for (int field = 0; field < field_num; field++) {
//                                    fprintf(f_result, "%s, ", PQgetvalue(result, tuple, field));
                                    std::cout << PQgetvalue(result, tuple, field) << std::endl;
                                }
//                                fprintf(f_result, "\n");
                            }
                        }
                        break;
                    }
                    case PGRES_COPY_OUT: {
//				double endtime = (double)(end - start) / (CLOCKS_PER_SEC/1000);
//				std::cout << endtime << std::endl;
//				fprintf(f_log, "%d\n", endtime);
                        std::cout << time_diff << std::endl;
                        break;
                    }
                    case PGRES_COPY_IN: {
//				double endtime = (double)(end - start) / (CLOCKS_PER_SEC/1000);
//				std::cout << endtime << std::endl;
//				fprintf(f_log, "%d\n", endtime);
                        std::cout << time_diff << std::endl;
                        break;
                    }
                    case PGRES_BAD_RESPONSE: {
                        std::cout << PQresultErrorMessage(result);
                        fprintf(f_log, "%s\n", PQresultErrorMessage(result));
                        break;
                    }
                    case PGRES_NONFATAL_ERROR: {
                        std::cout << PQresultErrorMessage(result);
                        fprintf(f_log, "%s\n", PQresultErrorMessage(result));
                        fclose(f_train);
                        break;
                    }
                    case PGRES_FATAL_ERROR: {
                        std::cout << PQresultErrorMessage(result);
                        fprintf(f_log, "%s\n", PQresultErrorMessage(result));
                        break;
                    }
                    case PGRES_COPY_BOTH: {
//				double endtime = (double)(end - start) / (CLOCKS_PER_SEC/1000);
//				std::cout << endtime << std::endl;
//				fprintf(f_log, "%d\n", endtime);
                        std::cout << time_diff << std::endl;
                        break;
                    }
                    case PGRES_SINGLE_TUPLE: {
//				double endtime = (double)(end - start) / (CLOCKS_PER_SEC/1000);
//				std::cout << endtime << std::endl;
//				fprintf(f_log, "%d\n", endtime);
                        std::cout << time_diff << std::endl;
                        break;
                    }
                    default: {
//				double endtime = (double)(end - start) / (CLOCKS_PER_SEC/1000);
//				std::cout << endtime << std::endl;
                        std::cout << time_diff << std::endl;
                    }
                }
                /*
                int nfield = PQnfields(result);
                for (int x = 0; x < PQntuples(result); x++)
                {
                    for (int y = 0; y < nfield; y++)
                    {
                        fprintf(f_log, "%s", PQgetvalue(result, x, y));
                    }
                    fprintf(f_log, "\n");
                }
                */
                PQclear(result);
                PQfinish(conn);
            }
            double geomean_time = calculateGeometricMean(time_vec);
            double deviation = calculateDeviation(time_vec, geomean_time);
            fprintf(f_log, "\nquery %d geomean time is %f ms with deviation = %f\n\n", i, geomean_time, deviation);
            fclose(f_log);
        }
		//else
		//{
		//	std::cout << "Read file failed" << std::endl;
		//	fclose(f_train);
		//}
		fclose(f_train);
		
	}
	return 0;
}