#include <iostream>
#include <fstream>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <vector>

#include "Schema.h"
#include "Catalog.h"
#include "EfficientMap.cc"
#include "Keyify.cc"

using namespace std;

sqlite3 *db;
EfficientMap<Keyify<string>, Schema> tables;
string tName = NULL;
int no_tables = 0;

static int callback(void *NotUsed, int argc, char **argv, char **azColName) {
		int i;
		for(i = 0; i<argc; i++) {
		  printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
		}
		printf("\n");
		return 0;
}

static int callbackCount(void *count, int argc, char **argv, char **azColName) {
		int *c = count;
		*c = atoi(argv[0]);
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
		return true;
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {

		int rc;
		int count = 0;
		char *zErrMsg = 0;

if(tables.IsThere(_table) == 1){
		rc = sqlite3_exec(db, "SELECT numTuples FROM " + _table, callbackCount, &count, &zErrMsg);
		_noTuples = count;
		return true;
}else return false;
		if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
				return false;
    } else return true;


/*int i;
_noTuples = 0;
for(i = 0; i < no_tables; i++){
	//First check if table name matches
	if(_table == tName)
		_noTuples++;
	else return false;
}
return true;
*/
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {

//UPDATE the numTuples
int rc;
char *zErrMsg = 0;
int i;
cin >> i;
i = _noTuples;

rc = sqlite3_exec(db, "UPDATE " + _table + "SET numTuples = " + _noTuples, callbackCount, &count, &zErrMsg);

if (rc != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
		return false;
} else return true;

}

bool Catalog::GetDataFile(string& _table, string& _path) {

	//First check if table name matches
	if(tables.IsThere(_table) == 1){
		Schema schema = tables.Find(_table);
		//string path = _table + ".dat";
		//path = _path;
		return true;
	} else return false;



/*	if(_table == tName)
		return true;
	else return false;
*/
}
void Catalog::SetDataFile(string& _table, string& _path) {


	//_path = _table + ".dat";

}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {

		int rc;
		int count = 0;
		char *zErrMsg = 0;

if(tables.IsThere(_table) == 1){
		rc = sqlite3_exec(db, "SELECT distinctVal FROM " + _attribute + "WHERE tableName = " + _table, callbackCount, &count, &zErrMsg);
		_noDistinct = count;
		return true;
}else return false;
		if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
				return false;
    } else return true;

/*int i;
_noDistinct = 0;
	for(i = 0; i < no_tables; i++){
		//First check if table name matches
		if(_table == tName)
			_noDistinct++;
		else return false;
		}
		return true;
		*/
	}

void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
}

void Catalog::GetTables(vector<string>& _tables) {

		_tables.clear();
		int i = 0;
		tables.MoveToStart();
		while (!tables.AtEnd()){
			_tables.push_back(tables.CurrentKey());
			tables.Advance();
		}

}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {

		Keyify<string> key(_table);
		_attributes.clear();
		//First check if table name matches
		if(tables.IsThere(key) == 1){
			Schema schema = tables.Find(key);
			vector<Attribute> atts = schema.GetAtts();
			for (int i = 0; i < atts.size(); i++) {
				_attributes.push_back(atts[i].name);
			}
			return true;
		}
		else return false;

}

bool Catalog::GetSchema(string& _table, Schema& _schema) {

		Keyify<string> key(_table);
		//First check if table name matches
		if(tables.IsThere(key) == 1){
			_schema = tables.Find(key);
			return true;
		}
		else return false;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {

		int rc;
		string sql;
		vector<unsigned int> distincts;
		Keyify<string> key(_table);
		char *zErrMsg = 0;

		for (int i = 0; i < _attributes.size(); i++) {
			distincts.push_back(0);
		}
		Schema *table = new Schema(_attributes, _attributeTypes, distincts);
		if (tables.IsThere(key) != 1){
			tables.Insert(key, *table);
		}

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
			sql = "INSERT INTO attribute VALUES('" + _attributes[i] + "', '" + _attributeTypes[i] + "', 0, '" + _table + ")";
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

	int i;
	string deletedTable;
	Schema deletedData;
	string sql;
	Keyify<string> key(_table);

	Schema schema = tables.Find(key);
	vector<Attribute> atts = schema.GetAtts();
	for(i = 0; i < atts.size(); i++)
		//sql = "DELETE FROM attribute WHERE name = '" + atts[i] + "'";


	if(tables.IsThere(key) == 1){
		Schema schema = tables.Find(key);
	//sql  = "DELETE FROM table WHERE name = '" + _table + "'";
	//tables.Remove(_table, deletedTable, deletedData);
}
return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	return _os;
}
