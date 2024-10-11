#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <iostream>
#include <string.h>
#include <ctime>
#include <windows.h>

#define N 9

int main()
{
	char file[100] = ".\\log.txt";
	FILE* f_log = fopen(file, "w+");
	fclose(f_log);
	for (int i = 1; i <= N; i++)
	{
		f_log = fopen(file, "a");
		PGconn* conn = PQsetdbLogin("127.0.0.1", "5432", NULL, NULL, "imdb", "Administrator", "");
		if (PQstatus(conn) == CONNECTION_BAD)
		{
			std::cout << PQerrorMessage(conn) << std::endl;
			fprintf(f_log, "%s\n", PQerrorMessage(conn));
			PQfinish(conn);
			fclose(f_log);
			return 0;
		}
		char fpath[60];
		sprintf(fpath, "D:\\Database\\x64\\query split experiment\\benchmark\\JOB\\query\\%d.sql", i);
		std::cout << i << " " << fpath << "   ";
		FILE* f_train = fopen(fpath, "r");
		char line[2048] = "";
		if (fgets(line, 2048, f_train) != NULL)
		{
			if (PQstatus(conn) == CONNECTION_BAD)
			{
				std::cout << PQerrorMessage(conn) << std::endl;
				fprintf(f_log, "%s\n", PQerrorMessage(conn));
				PQfinish(conn);
				fclose(f_log);
				fclose(f_train);
				return 0;
			}
			clock_t start, end;
			//PQexec(conn, "set statement_timeout = 30000;");
			start = clock();
			PGresult* result = PQexec(conn, line);
			end = clock();
			switch (PQresultStatus(result))
			{
				case PGRES_EMPTY_QUERY: {
					std::cout << "An unexpected empty query;" << std::endl;
					fprintf(f_log, "An unexpected empty query;\n");
					break;
				}
				case PGRES_COMMAND_OK: {
					int endtime = (int)(end - start);
					std::cout << endtime << std::endl;
					fprintf(f_log, "%d\n", endtime);
					fclose(f_train);
					break;
				}
				case PGRES_TUPLES_OK: {
					int endtime = (int)(end - start);
					std::cout << endtime << std::endl;
					fprintf(f_log, "%d\n", endtime);
					fclose(f_train);
					break;
				}
				case PGRES_COPY_OUT: {
					int endtime = (int)(end - start);
					std::cout << endtime << std::endl;
					fprintf(f_log, "%d\n", endtime);
					fclose(f_train);
					break;
				}
				case PGRES_COPY_IN: {
					int endtime = (int)(end - start);
					std::cout << endtime << std::endl;
					fprintf(f_log, "%d\n", endtime);
					fclose(f_train);
					break;
				}
				case PGRES_BAD_RESPONSE: {
					std::cout << PQresultErrorMessage(result);
					fprintf(f_log, "%s\n", PQresultErrorMessage(result));
					fclose(f_train);
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
					fclose(f_train);
					break;
				}
				case PGRES_COPY_BOTH: {
					int endtime = (int)(end - start);
					std::cout << endtime << std::endl;
					fprintf(f_log, "%d\n", endtime);
					fclose(f_train);
					break;
				}
				case PGRES_SINGLE_TUPLE: {
					int endtime = (int)(end - start);
					std::cout << endtime << std::endl;
					fprintf(f_log, "%d\n", endtime);
					fclose(f_train);
					break;
				}
			}
			PQclear(result);
			PQfinish(conn);
		}
		else
		{
			std::cout << "Read file failed" << std::endl;
			fclose(f_train);
		}
		fclose(f_log);
	}
	return 0;
}