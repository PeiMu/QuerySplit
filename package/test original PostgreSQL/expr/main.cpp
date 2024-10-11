#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <iostream>
#include <string.h>
#include <ctime>
#include <windows.h>

#define JOB 91
#define DSB 37
#define SSB 13
#define TPCH 22

int main()
{
	char file[100] = ".\\log.txt";
	FILE* f_log = fopen(file, "w+");
	fclose(f_log);
	for (int i = 1; i <= JOB; i++)
	{
		char fpath[100];
		//sprintf(fpath, "D:\\Database\\x64\\experiment\\benchmark\\DSB\\nonSPJ\\query_1.sql");
		sprintf(fpath, "D:\\Database\\x64\\experiment\\benchmark\\JOB\\query\\%d.sql", i);
		FILE* f_train = fopen(fpath, "r");
		char line[4096] = "";
		int num = 0;
		while (fgets(line, 4096, f_train) != NULL)
		{
			num++;
			f_log = fopen(file, "a");
			PGconn* conn = PQsetdbLogin("127.0.0.1", "5432", NULL, NULL, "imdb_noindex", "Administrator", "");
			if (PQstatus(conn) == CONNECTION_BAD)
			{
				std::cout << PQerrorMessage(conn) << std::endl;
				fprintf(f_log, "%s\n", PQerrorMessage(conn));
				PQfinish(conn);
				fclose(f_log);
				fclose(f_train);
				return 0;
			}
			PQexec(conn, "set enable_nestloop = false;");
			//PQexec(conn, "set statement_timeout = \'30s\';");
			//char cmd[2048];
			//sprintf(cmd, "explain(analyze) %s", line);
			clock_t start, end;
			start = clock();
			PGresult* result = PQexec(conn, line);
			//PGresult* result = PQexec(conn, cmd);
			//std::cout << "Ok!" << std::endl;
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
				break;
			}
			case PGRES_TUPLES_OK: {
				int endtime = (int)(end - start);
				std::cout << endtime << std::endl;
				fprintf(f_log, "%d\n", endtime);
				break;
			}
			case PGRES_COPY_OUT: {
				int endtime = (int)(end - start);
				std::cout << endtime << std::endl;
				fprintf(f_log, "%d\n", endtime);
				break;
			}
			case PGRES_COPY_IN: {
				int endtime = (int)(end - start);
				std::cout << endtime << std::endl;
				fprintf(f_log, "%d\n", endtime);
				
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
				int endtime = (int)(end - start);
				std::cout << endtime << std::endl;
				fprintf(f_log, "%d\n", endtime);
				break;
			}
			case PGRES_SINGLE_TUPLE: {
				int endtime = (int)(end - start);
				std::cout << endtime << std::endl;
				fprintf(f_log, "%d\n", endtime);
				break;
			}
			default: {
				int endtime = (int)(end - start);
				std::cout << endtime << std::endl;
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