#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include "sqlite3.h"

#include "Schema.h"
#include "Catalog.h"

using namespace std;

sqlite3 *db;
string tName = NULL;
int no_tables = NULL;

static int callback(void *NotUsed, int argc, char **argv, char **azColName) {
		int i;
		for(i = 0; i<argc; i++) {
		  printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
		}
		printf("\n");
		return 0;
}

Catalog::Catalog(string& _fileName) {
		char *errMessage = 0;
		int rc;
		char file[_fileName.length()];
		strcpy(file, _fileName.c_str());

		//Open connection to our database
		rc = sqlite3_open(file, &db);

		if (rc) {
			fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
		}
		else {
			fprintf(stderr, "Opened database successfully\n");
		}
}

Catalog::~Catalog() {

		sqlite3_close(db);
}

bool Catalog::Save() {

}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {

int i;
_noTuples = 0;
for(i = 0; i < no_tables; i++){
	//First check if table name matches
	if(_table == tName)
		_noTuples++;
	else return false;
}
return true;

}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {



}

bool Catalog::GetDataFile(string& _table, string& _path) {

	//First check if table name matches
	if(_table == tName)
		return true;
	else return false;

}

void Catalog::SetDataFile(string& _table, string& _path) {
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {

int i;
_noDistinct = 0;
	for(i = 0; i < no_tables; i++){
		//First check if table name matches
		if(_table == tName)
			_noDistinct++;
		else return false;
		}
		return true;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
}

void Catalog::GetTables(vector<string>& _tables) {
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {

	//First check if table name matches
	if(_table == tName)
		return true;
	else return false;

}

bool Catalog::GetSchema(string& _table, Schema& _schema) {

	//First check if table name matches
	if(_table == tName)
		return true;
	else return false;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {

		int rc;
		string sql;
		char *zErrMsg = 0;
		//Schema table = new Schema();

		sql = "INSERT INTO table VALUES('" + _table + "', 0, '" + _table + ".dat')";
		char sql1[sql.length()];
		strcpy(sql1, sql.c_str());

		rc = sqlite3_exec(db, sql1, callback, 0, &zErrMsg);

		if( rc != SQLITE_OK ){
		  fprintf(stderr, "SQL error: %s\n", zErrMsg);
		  sqlite3_free(zErrMsg);
		} else {
		  fprintf(stdout, "Table created successfully\n");
		}

		for (int i = 0; i < _attributes.size(); i++) {
			sql = "INSERT INTO attribute VALUES('" + _attributes[i] + "', '" + _attributeTypes[i] + "', 0)";
			char sql2[sql.length()];
			strcpy(sql2, sql.c_str());

			rc = sqlite3_exec(db, sql2, callback, 0, &zErrMsg);

			if( rc != SQLITE_OK ){
			  fprintf(stderr, "SQL error: %s\n", zErrMsg);
			  sqlite3_free(zErrMsg);
			} else {
			  fprintf(stdout, "Attribute created successfully\n");
			}
		}

		return true;
}

bool Catalog::DropTable(string& _table) {
	return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	return _os;
}
