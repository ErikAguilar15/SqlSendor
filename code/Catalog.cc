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
EfficientMap<Keyify<string>, Schema> insertedTables;
EfficientMap<Keyify<string>, Schema> deletedTables;
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
		int *c;
		*c = atoi(argv[0]);
		return 0;
}

Catalog::Catalog(string& _fileName) {
		char *errMessage = 0;
		int rc;
		string sql;
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

		char **table_results;
		char **errMessage1 = 0;
		int row, col;
		sqlite3_get_table(db, "SELECT name FROM table", &table_results, &row, &col, errMessage1);

		for (int i = 0; i < row; i++) {
			int attRow, attCol;
			char **att_results;
			char **type_results;
			char **distinct_results;
			vector<string> attributes, types;
			vector<unsigned int> distincts;

			string tableName = string(table_results[i]);

			sql = "SELECT name FROM attribute WHERE tableName = '" + tableName + "'";
			char sql1[sql.length()];
			strcpy(sql1, sql.c_str());
			sqlite3_get_table(db, sql1, &att_results, &attRow, &attCol, errMessage1);
			for (int j = 0; j < attRow; j++) {
				attributes.push_back(string(att_results[j]));
			}

			sql = "SELECT type FROM attribute WHERE tableName = '" + tableName + "'";
			char sql2[sql.length()];
			strcpy(sql2, sql.c_str());
			sqlite3_get_table(db, sql2, &type_results, &attRow, &attCol, errMessage1);
			for (int j = 0; j < attRow; j++) {
				types.push_back(string(type_results[j]));
			}

			sql = "SELECT distinctVal FROM attribute WHERE tableName = '" + tableName + "'";
			char sql3[sql.length()];
			strcpy(sql3, sql.c_str());
			sqlite3_get_table(db, sql3, &distinct_results, &attRow, &attCol, errMessage1);
			for (int j = 0; j < attRow; j++) {
				distincts.push_back(stoi(distinct_results[j]));
			}

			Keyify<string> key(tableName);
			Schema *tempTable = new Schema(attributes, types, distincts);
			tables.Insert(key, *tempTable);
		}

}

Catalog::~Catalog() {

		sqlite3_close(db);

}

bool Catalog::Save() {

		int rc;
		string sql;
		char *zErrMsg = 0;

		//Saving inserted Tables
		insertedTables.MoveToStart();
		while(!insertedTables.AtEnd()){
			string tempStr = insertedTables.CurrentKey();
			Schema tempSchema = insertedTables.CurrentData();
			vector<Attribute> atts = tempSchema.GetAtts();
			vector<string> attributes, attributeTypes;
			vector<unsigned int> distincts;

			if (tables.IsThere(insertedTables.CurrentKey()) != 1){
				sql = "INSERT INTO table VALUES('" + tempStr + "', 0, '" + tempStr + ".dat')";
				char sql1[sql.length()];
				strcpy(sql1, sql.c_str());

				rc = sqlite3_exec(db, sql1, callback, 0, &zErrMsg);

				if( rc != SQLITE_OK ){
				  fprintf(stderr, "SQL error: %s\n", zErrMsg);
				  sqlite3_free(zErrMsg);
				} else {
				  fprintf(stdout, "Table created successfully\n");
				}

				for (int i = 0; i < atts.size(); i++) {
					attributes.push_back(atts[i].name);
					if (atts[i].type == Integer) {
						attributeTypes.push_back("INTEGER");
					}
					else if (atts[i].type == Float) {
						attributeTypes.push_back("FLOAT");
					}
					else attributeTypes.push_back("STRING");
					distincts.push_back(atts[i].noDistinct);
				}

				for (int i = 0; i < attributes.size(); i++) {
					sql = "INSERT INTO attribute VALUES('" + attributes[i] + "', '" + attributeTypes[i] + "', " + "'" + to_string(distincts[i]) + "', '" + tempStr + "')";
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
			}
			else {
				printf("Table is already present in database, therefore can't be saved.");
			}
		}
		tables.SuckUp(insertedTables);

		//Saving removed Tables
		deletedTables.MoveToStart();
		while(!deletedTables.AtEnd()) {
			Schema deletedData;
			string tempStr = deletedTables.CurrentKey();
			Schema tempSchema = deletedTables.CurrentData();
			Keyify<string> deletedTable(tempStr);
			vector<Attribute> atts = tempSchema.GetAtts();
			for(int i = 0; i < atts.size(); i++)
				sql = "DELETE FROM attribute WHERE name = '" + atts[i].name + "'";


			if(tables.IsThere(deletedTables.CurrentKey()) == 1){
				Schema schema = tables.Find(deletedTables.CurrentKey());
				sql  = "DELETE FROM table WHERE name = '" + tempStr + "'";
				tables.Remove(deletedTables.CurrentKey(), deletedTable, deletedData);
				//deletedTables.Insert(deletedTable, deletedData);
			}
		}

		return true;

}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {

		int rc;
		int count = 0;
		char *zErrMsg = 0;
		Keyify<string> key(_table);

		string sql = "SELECT numTuples FROM " + _table;
		char sql1[sql.length()];
		strcpy(sql1, sql.c_str());

		if(tables.IsThere(key) == 1){
				rc = sqlite3_exec(db, sql1, callbackCount, &count, &zErrMsg);
				_noTuples = count;
				return true;
		}else return false;
				if (rc != SQLITE_OK) {
		        fprintf(stderr, "SQL error: %s\n", zErrMsg);
		        sqlite3_free(zErrMsg);
		    }


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

		string sql = "UPDATE " + _table + "SET numTuples = " + to_string(_noTuples);
		char sql1[sql.length()];
		strcpy(sql1, sql.c_str());

		rc = sqlite3_exec(db, sql1, callbackCount, 0, &zErrMsg);

		if (rc != SQLITE_OK) {
				fprintf(stderr, "SQL error: %s\n", zErrMsg);
				sqlite3_free(zErrMsg);
		}

}

bool Catalog::GetDataFile(string& _table, string& _path) {

	Keyify<string> key(_table);
	//First check if table name matches
	if(tables.IsThere(key) == 1){
		Schema schema = tables.Find(key);
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
		Keyify<string> key(_table);

		string sql = "SELECT distinctVal FROM " + _attribute + "WHERE tableName = " + _table;
		char sql1[sql.length()];
		strcpy(sql1, sql.c_str());

		if(tables.IsThere(key) == 1){
				rc = sqlite3_exec(db, sql1 , callbackCount, 0, &zErrMsg);
				_noDistinct = count;
				return true;
		}else return false;
				if (rc != SQLITE_OK) {
		        fprintf(stderr, "SQL error: %s\n", zErrMsg);
		        sqlite3_free(zErrMsg);
		    }

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

		//UPDATE the numDistinct
		int rc;
		char *zErrMsg = 0;
		int i;
		cin >> i;
		i = _noDistinct;

		string sql = "UPDATE " + _attribute + "SET distinctVal = " + to_string(_noDistinct);
		char sql1[sql.length()];
		strcpy(sql1, sql.c_str());

		rc = sqlite3_exec(db, sql1, callbackCount, 0, &zErrMsg);

		if (rc != SQLITE_OK) {
				fprintf(stderr, "SQL error: %s\n", zErrMsg);
				sqlite3_free(zErrMsg);
		}

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
		if (insertedTables.IsThere(key) != 1){
			insertedTables.Insert(key, *table);
		}
		else {
			printf("Table is already present.");
		}

		return true;
}

bool Catalog::DropTable(string& _table) {

		int i;
		Keyify<string> key(_table);
		Schema schema = tables.Find(key);

		if(tables.IsThere(key) == 1){
			deletedTables.Insert(key, schema);
		}
		return true;
		}

ostream& operator<<(ostream& _os, Catalog& _c) {
	return _os;
}
