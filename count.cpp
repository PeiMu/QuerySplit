#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <ctime>
//#include <windows.h>
#include <string>
#include <vector>
#include <bitset>
#include <libpq-fe.h>

#define JOB 91
#define DSB 94
#define SSB 13
#define TPCH 101

typedef struct rel
{
	int relid;
	double rows;
}rel;

int find(int x, int* father, int length)
{
	int a;
	a = x;
	while (x != father[x])
	{
		x = father[x];
	}
	while (a != father[a])
	{
		int z = a;
		a = father[a];
		father[z] = x;
	}
	return x;
}

void Union(int a, int b, int* father, int length)
{
	int fx = find(a, father, length);
	int fy = find(b, father, length);
	if (fx != fy)
	{
		father[fx] = fy;
	}
}

// index is a member of bms
bool isMember(int bms, int index)
{
	if (bms < index)
		return false;
	else
	{
		int i = 0;
		while (index > 0)
		{
			if (index % 2 == 1)
			{
				int temp = bms >> i;
				if (temp % 2 == 0)
					return false;
			}
			index = index >> 1;
			i++;
		}
	}
	return true;
}

std::string generateSubquery(std::vector<std::string> From, std::vector<int> Whereindex, std::vector<std::string> Where, int bms)
{
	std::string res("SELECT count(*) FROM ");
	std::vector<std::string> local_from;
	std::vector<int> local_fromidx;
	std::vector<std::string> local_where;
	std::vector<int> local_whereidx;
	int temp = bms;
	int a = 0;
	while (temp != 0)
	{
		int flag = temp % 2;
		if (flag == 1)
		{
			local_from.push_back(From[a]);
			local_fromidx.push_back((1 << a));
		}
		temp = temp >> 1;
		a++;
	}
	for (int i = 0; i < Whereindex.size(); i++)
	{
		if (isMember(bms, Whereindex[i]))
		{
			local_where.push_back(Where[i]);
			local_whereidx.push_back(Whereindex[i]);
		}
	}
	if (local_from.size() > 1)
	{
		int m, n, i, k = 0;
		int father[25];
		for (i = 0; i < local_from.size(); i++)
		{
			father[i] = i;
		}
		for (int i = 0; i < local_fromidx.size() - 1; i++)
		{
			for (int j = i + 1; j < local_fromidx.size(); j++)
			{
				for (int z = 0; z < local_whereidx.size(); z++)
				{
					if (local_fromidx[i] + local_fromidx[j] == local_whereidx[z])
					{
						Union(i, j, father, local_from.size());
					}
				}
			}
		}
		for (i = 0; i < local_from.size(); i++)
		{
			if (father[i] == i)
			{
				k++;
			}
		}
		if (k != 1)
			return "";
	}
	for (int i = 0; i < local_from.size(); i++)
	{
		res.append(local_from[i]);
		if (i < local_from.size() - 1)
			res.append(", ");
	}
	for (int i = 0; i < local_where.size(); i++)
	{
		if (i == 0)
			res.append(" WHERE ");
		res.append(local_where[i]);
		if (i < local_where.size() - 1)
			res.append(" AND ");
	}
	res.append(";\n");
	return res;
}

int main()
{
	for (int i = 1; i <= 22; i++)
	{
		char fpath[100];
		sprintf(fpath, "/home/pei/Project/QuerySplit/tpch/%d.sql", i);
		// read file into one line?
		std::ifstream input_file(fpath);

		// Check if the file is opened successfully
		if (!input_file.is_open()) {
			std::cerr << "Error opening the file." << std::endl;
			return 1;
		}

		// Read the file line by line into a stringstream
		std::stringstream buffer;
		std::string line;
		while (std::getline(input_file, line)) {
			buffer << line << ' ';
		}

		// Close the input file
		input_file.close();

		// Extract the content of the stringstream as a single string
		std::string query_str = buffer.str();

		char query_sql[32];
		sprintf(query_sql, "../query%d.txt", i);
		std::vector<rel> records;
		std::vector<std::string> From;
		std::vector<std::string> Where;
		std::vector<int> Whereindex;
		size_t pos = query_str.find("from ");
		if (std::string::npos == pos) {
			std::cout << "not found where!" << std::endl;
			continue;
		}
		std::string from_str(query_str);
		from_str = from_str.substr(pos + 5);
		pos = from_str.find("where ");
		if (std::string::npos == pos) {
			std::cout << "not found where!" << std::endl;
			continue;
		}
		from_str = from_str.substr(0, pos);
		size_t offset = 0;
		std::string temp;
		//process from clause
		while ((pos = from_str.find(", ", offset)) != std::string::npos)
		{
			temp = from_str.substr(offset, pos - offset);
			if (temp.length() > 0)
			{
				From.push_back(temp);
			}
			offset = pos + 2;
		}
		temp = from_str.substr(offset);
		temp = temp.substr(0, temp.length() - 1);
		if (temp.length() > 0)
		{
			From.push_back(temp);
		}
		int length = From.size();
		//process where clause
		std::string where_str(query_str);
		pos = where_str.find("where ");
		where_str = where_str.substr(pos + 6);
		offset = 0;
		//process where clause
		while ((pos = where_str.find(" and ((", offset)) != std::string::npos)
		{
			size_t pos_head = pos;
			offset = pos + 5;
			pos = where_str.find("))", offset);
			temp = where_str.substr(offset, pos + 2 - offset);
			if (temp.length() > 0)
			{
				Where.push_back(temp);
			}
			where_str.erase(pos_head, pos - pos_head + 2);
			offset = 0;
		}
		offset = 0;
		while ((pos = where_str.find(" and ", offset)) != std::string::npos)
		{
			temp = where_str.substr(offset, pos - offset);
			if (temp.length() > 0)
			{
				Where.push_back(temp);
			}
			offset = pos + 5;
		}
		temp = where_str.substr(offset);
		temp = temp.substr(0, temp.length() - 1);
		if (temp.length() > 0)
		{
			Where.push_back(temp);
		}
		//create where index
		for (int a = 0; a < Where.size(); a++)
		{
			int index = 0;
			std::string str = Where[a];
			for (int b = 0; b < From.size(); b++)
			{
				std::string str1(From[b]);
				size_t temp_pos = str1.find(" as");
				str1 = str1.substr(temp_pos + 4, str1.size()- temp_pos - 4);
				str1.append(".");
				if (str.find(str1) != std::string::npos)
				{
					index = index + (1 << b);
				}
			}
			Whereindex.push_back(index);
		}
		//enurmerate subqueries for each from
		std::vector<std::vector<int>> join_rel_level(length);
		for (int a = 0; a < length; a++)
		{
			int bms = 1 << a;
			bool flag;
			if (join_rel_level[0].size() > 0)
			{
				flag = false;
				for(int j = 0; j < join_rel_level[0].size(); j++)
				{
					int prev = join_rel_level[0][j];
					if (prev == bms)
					{
						flag = true;
						break;
					}
				}
				if (flag)
				{
					continue;
				}
			}
			join_rel_level[0].push_back(bms);
			std::string subquery = generateSubquery(From, Whereindex, Where, bms);
			PGconn* conn = PQsetdbLogin("localhost", "5432", NULL, NULL, "pei", "pei", "");
			if (PQstatus(conn) == CONNECTION_BAD)
			{
				std::cout << PQerrorMessage(conn) << std::endl;
				PQfinish(conn);
				return 0;
			}
			PGresult* result = PQexec(conn, subquery.c_str());
			std::string res;
			switch (PQresultStatus(result))
			{
				case PGRES_EMPTY_QUERY: {
					std::cout << "query " << i << ", An unexpected empty query;" << std::endl;
					break;
				}
				case PGRES_BAD_RESPONSE: {
					std::cout << "query " << i << std::endl;
					std::cout << PQresultErrorMessage(result);
					break;
				}
				case PGRES_NONFATAL_ERROR: {
					std::cout << "query " << i << std::endl;
					std::cout << PQresultErrorMessage(result);
					break;
				}
				case PGRES_FATAL_ERROR: {
					std::cout << "query " << i << std::endl;
					std::cout << PQresultErrorMessage(result);
					break;
				}
				case PGRES_COMMAND_OK: {
					res = PQgetvalue(result, 0, 0);
					break;
				}
				case PGRES_TUPLES_OK: {
					res = PQgetvalue(result, 0, 0);
					break;
				}
				default: {
					std::cout << "query " << i << "Unknown error." << std::endl;
					break;
				}
			}
//				std::cout << subquery.c_str() << std::endl;
//				std::cout << PQresultStatus(result) << std::endl;
//				std::string res = PQgetvalue(result, 0, 0);
			rel r = { bms << 1, atof(res.c_str()) };
			FILE* fp = fopen(query_sql, "a");
			fprintf(fp, "%d:%lf\n", r.relid, r.rows);
			fclose(fp);
			PQfinish(conn);
		}


		for (int a = 1; a < length; a++)
		{
			for(int j = 0; j < join_rel_level[a - 1].size(); j++)
			{
				int bms = join_rel_level[a - 1][j];
				for (int k = 0; k < length; k++)
				{
					if (isMember(bms, 1 << k))
					{
						continue;
					}
					int new_bms = (1 << k) + bms;
					if (join_rel_level[a].size() > 0)
					{
						bool flag = false;
						for(int m = 0; m < join_rel_level[a].size(); m++)
						{
							int prev = join_rel_level[a][m];
							if (prev == new_bms)
							{
								flag = true;
								break;
							}
						}
						if (flag)
						{
							continue;
						}
					}
					join_rel_level[a].push_back(new_bms);
					std::string subquery = generateSubquery(From, Whereindex, Where, new_bms);
					if (subquery == "")
						continue;
					PGconn* conn = PQsetdbLogin("localhost", "5432", NULL, NULL, "pei", "pei", "");
					if (PQstatus(conn) == CONNECTION_BAD)
					{
						std::cout << PQerrorMessage(conn) << std::endl;
						PQfinish(conn);
						return 0;
					}
					PQexec(conn, "set statement_timeout = \'60s\';");
					PGresult* result = PQexec(conn, subquery.c_str());
					rel r;
					switch (PQresultStatus(result))
					{
						case PGRES_EMPTY_QUERY: {
							std::cout << "An unexpected empty query;" << std::endl;
							break;
						}
						case PGRES_COMMAND_OK: {
							std::string res = PQgetvalue(result, 0, 0);
							r = { new_bms << 1, atof(res.c_str()) };
							break;
						}
						case PGRES_TUPLES_OK: {
							std::string res = PQgetvalue(result, 0, 0);
							r = { new_bms << 1, atof(res.c_str()) };
							break;
						}
						case PGRES_BAD_RESPONSE: {
							std::cout << PQresultErrorMessage(result);
							break;
						}
						case PGRES_NONFATAL_ERROR: {
							r = { new_bms << 1, 999999999.0 };
							break;
						}
						case PGRES_FATAL_ERROR: {
							r = { new_bms << 1, 999999999.0 };
							break;
						}
						default: {
							std::cout << "Unknown error." << std::endl;
							break;
						}
					}
					FILE* fp = fopen(query_sql, "a");
					fprintf(fp, "%d:%lf\n", r.relid, r.rows);
					fclose(fp);
					PQfinish(conn);
				}
			}
		}

//		fclose(f_train);
	}
	return 0;
}