#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <string.h>
#include <ctime>
#include <vector>
#include <cmath>
#include <filesystem>
#include <iomanip>

#define N 33
#define ITERATION_TIME 10

#define ENABLE_QUERY_SPLIT false

enum Benchmark {
	TPCH,
	IMDB,
};

Benchmark benchmark = IMDB;

/***************************************
 * Timer functions of the test framework
 ***************************************/

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

timespec sum(timespec t1, timespec t2) {
	timespec temp;
	if (t1.tv_nsec + t2.tv_nsec >= 1000000000) {
		temp.tv_sec = t1.tv_sec + t2.tv_sec + 1;
		temp.tv_nsec = t1.tv_nsec + t2.tv_nsec - 1000000000;
	} else {
		temp.tv_sec = t1.tv_sec + t2.tv_sec;
		temp.tv_nsec = t1.tv_nsec + t2.tv_nsec;
	}
	return temp;
}

double printTimeSpec(timespec t, const char* prefix) {
	double time = t.tv_sec*1000.0 + t.tv_nsec/1000.0;
	printf("%s: %f ms\n", prefix, time);
	return time;
}

timespec tic( )
{
	timespec start_time;
	clock_gettime(CLOCK_REALTIME, &start_time);
	return start_time;
}

double toc( timespec* start_time, const char* prefix )
{
	timespec current_time;
	clock_gettime(CLOCK_REALTIME, &current_time);
	double time = printTimeSpec( diff( *start_time, current_time ), prefix );
	*start_time = current_time;
	return time;
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

using recursive_directory_iterator = std::filesystem::recursive_directory_iterator;


int main()
{
	char file[100] = "log.txt";
	FILE* f_log = fopen(file, "w+");
	FILE* f_result = fopen("result.txt", "w+");
	fclose(f_log);

	std::string dir_path;
	switch (benchmark) {
		case TPCH:
			dir_path = "/home/pei/Project/benchmarks/tpch-postgre/dbgen/out/pure_queries/";
			break;
		case IMDB:
			dir_path = "/home/pei/Project/benchmarks/imdb_job_postgres/out/pure_queries/";
			break;
		default:
			std::cerr << "No such benchmark!" << std::endl;
			return 1;
	}

	for (const auto& dir_entry : recursive_directory_iterator(dir_path))
	{
		std::string file_path = dir_entry.path().filename().string();
		f_log = fopen(file, "a");

		if (!dir_entry.is_regular_file()) {
			break;
		}

		std::ifstream input_file(dir_path+file_path, std::ios::binary);
		// Check if the file is opened successfully
		if (!input_file.is_open()) {
			std::cerr << "Error opening the file." << std::endl;
			break;
		}
		// Read the file line by line into a stringstream
		std::stringstream buffer;
		std::string line;
		while (std::getline(input_file, line)) {
			// omit comments
			if (line.find("--") == 0)
				continue;
			buffer << line << '\t';
		}
		// Close the input file
		input_file.close();
		// Extract the content of the stringstream as a single string
		std::string query_str = buffer.str();

		PGresult* result;
		double time;
		std::vector<double> time_vec;
		for (int iter = 0; iter < ITERATION_TIME; iter++) {
			PGconn* conn;
			switch (benchmark) {
				case TPCH:
					conn = PQsetdbLogin("localhost", "5432", NULL, NULL, "pei", "pei", "");
					break;
				case IMDB:
					conn = PQsetdbLogin("localhost", "5432", NULL, NULL, "imdb", "imdb", "");
					break;
				default:
					std::cerr << "No such benchmark!" << std::endl;
					return 1;
			}
			if (PQstatus(conn) == CONNECTION_BAD)
			{
				std::cout << PQerrorMessage(conn) << std::endl;
				fprintf(f_log, "%s\n", PQerrorMessage(conn));
				PQfinish(conn);
				fclose(f_log);
				return 0;
			}

			if (PQstatus(conn) == CONNECTION_BAD)
			{
				std::cout << PQerrorMessage(conn) << std::endl;
				fprintf(f_log, "%s\n", PQerrorMessage(conn));
				PQfinish(conn);
				fclose(f_log);
				return 0;
			}
			if (ENABLE_QUERY_SPLIT) {
				PQexec(conn, "switch to c_r;");
				PQexec(conn, "switch to relationshipcenter;");
			}


			timespec timer = tic();
			result = PQexec(conn, query_str.c_str());
			time = toc(&timer, (std::to_string(iter) + ": time consumption").c_str());
			time_vec.emplace_back(time);

			auto status = PQresultStatus(result);
			switch (status)
			{
				case PGRES_EMPTY_QUERY: {
					std::cout << "An unexpected empty query;" << std::endl;
					fprintf(f_log, "An unexpected empty query;\n");
					break;
				}
				case PGRES_COMMAND_OK: {
					fprintf(f_log, "%s: PGRES_COMMAND_OK time is %f ms\n", file_path.c_str(), time);
					break;
				}
				case PGRES_TUPLES_OK: {
					fprintf(f_log, "%s: PGRES_TUPLES_OK time is %f ms\n", file_path.c_str(), time);
					int tuple_num = PQntuples(result);
					int field_num = PQnfields(result);
					for (int tuple = 0; tuple < tuple_num; tuple++) {
						for (int field = 0; field < field_num; field++) {
							fprintf(f_result, "%s, ", PQgetvalue(result, tuple, field));
						}
						fprintf(f_result, "\n");
					}
					break;
				}
				case PGRES_COPY_OUT: {
					fprintf(f_log, "%s: PGRES_COPY_OUT time is %f ms\n", file_path.c_str(), time);
					break;
				}
				case PGRES_COPY_IN: {
					fprintf(f_log, "%s: PGRES_COPY_IN time is %f ms\n", file_path.c_str(), time);
					break;
				}
				case PGRES_COPY_BOTH: {
					fprintf(f_log, "%s: PGRES_COPY_BOTH time is %f ms\n", file_path.c_str(), time);
					break;
				}
				case PGRES_SINGLE_TUPLE: {
					fprintf(f_log, "%s: PGRES_SINGLE_TUPLE time is %f ms\n", file_path.c_str(), time);
					int tuple_num = PQntuples(result);
					int field_num = PQnfields(result);
					for (int tuple = 0; tuple < tuple_num; tuple++) {
						for (int field = 0; field < field_num; field++) {
							fprintf(f_result, "%s, ", PQgetvalue(result, tuple, field));
						}
						fprintf(f_result, "\n");
					}
					break;
				}
				case PGRES_BAD_RESPONSE:
				case PGRES_NONFATAL_ERROR:
				case PGRES_FATAL_ERROR:
				default: {
					std::cout << PQresultErrorMessage(result);
					fprintf(f_log, "%s\n", PQresultErrorMessage(result));
					break;
				}
			}
			PQclear(result);
			PQfinish(conn);
		}
		double geomean_time = calculateGeometricMean(time_vec);
		double deviation = calculateDeviation(time_vec, geomean_time);
		fprintf(f_log, "\nquery %s geomean time is %f ms with deviation = %f\n\n", file_path.c_str(), geomean_time, deviation);

		fclose(f_log);
	}
	return 0;
}