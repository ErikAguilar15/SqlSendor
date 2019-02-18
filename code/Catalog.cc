#include <iostream>
#include <fstream>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <cstdlib>
#include <cstdio>
#include <vector>

#include "Schema.h"
#include "Catalog.h"
#include "EfficientMap.cc"
#include "Keyify.cc"

using namespace std;

using Records = vector<string>;

sqlite3 *db;
EfficientMap<Keyify<string>, Schema> tables;
EfficientMap<Keyify<string>, Keyify<unsigned int> > tupleMap;
EfficientMap<Keyify<string>, Schema> insertedTables;
EfficientMap<Keyify<string>, Schema> deletedTables;
//string tName = NULL;
//int no_tables = 0;

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

static int callbackT(void *p_data, int num_fields, char **p_fields, char **p_col_names)
{
		Records* records = static_cast<Records*>(p_data);
		for (int i = 0; i < num_fields; i++) {
			records->push_back(p_fields[i]);
		}
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

		Records records;
		char **errMessage1 = 0;
		sqlite3_exec(db, "SELECT name FROM tables", callbackT, &records, errMessage1);

		for (int i = 0; i < records.size(); i++) {
			int attRow, attCol;
			Records att_results;
			Records type_results;
			Records distinct_results;
			vector<string> attributes, types;
			vector<unsigned int> distincts;

			string tableName = records[i];
			cout << tableName << " ";

			sql = "SELECT name FROM attribute WHERE tableName = '" + tableName + "'";
			char sql1[sql.length()];
			strcpy(sql1, sql.c_str());
			sqlite3_exec(db, sql1, callbackT, &att_results, errMessage1);
			for (int j = 0; j < attRow; j++) {
				cout << att_results[j] << " ";
				attributes.push_back(att_results[j]);
			}

			sql = "SELECT type FROM attribute WHERE tableName = '" + tableName + "'";
			char sql2[sql.length()];
			strcpy(sql2, sql.c_str());
			sqlite3_exec(db, sql2, callbackT, &type_results, errMessage1);
			for (int j = 0; j < attRow; j++) {
				cout << type_results[j] << " ";
				types.push_back(type_results[j]);
			}

			sql = "SELECT distinctVal FROM attribute WHERE tableName = '" + tableName + "'";
			char sql3[sql.length()];
			strcpy(sql3, sql.c_str());
			sqlite3_exec(db, sql3, callbackT, &distinct_results, errMessage1);
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

		rc = sqlite3_exec(db, "DELETE FROM tables WHERE name = ''", callback, 0, &zErrMsg);

		//Saving inserted Tables
		insertedTables.MoveToStart();
		while(!insertedTables.AtEnd()){
			string tempStr = insertedTables.CurrentKey();
			Schema tempSchema = insertedTables.CurrentData();
			vector<Attribute> atts = tempSchema.GetAtts();
			vector<string> attributes, attributeTypes;
			vector<unsigned int> distincts;

			if (tables.IsThere(insertedTables.CurrentKey()) != 1){
				sql = "INSERT INTO tables VALUES('" + tempStr + "', 0, '" + tempStr + ".dat')";
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
				printf("Table: %s is already present in database, therefore can't be saved.\n", tempStr.c_str());
			}
			insertedTables.Advance();
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
			for(int i = 0; i < atts.size(); i++) {
				sql = "DELETE FROM attribute WHERE name = '" + atts[i].name + "'";
				char sql1[sql.length()];
				strcpy(sql1, sql.c_str());
				rc = sqlite3_exec(db, sql1, callback, 0, &zErrMsg);

				if( rc != SQLITE_OK ){
					fprintf(stderr, "SQL error: %s\n", zErrMsg);
					sqlite3_free(zErrMsg);
				} else {
					fprintf(stdout, "Attribute deleted successfully\n");
				}
			}

			if(tables.IsThere(deletedTables.CurrentKey()) == 1){
				Schema schema = tables.Find(deletedTables.CurrentKey());
				sql  = "DELETE FROM tables WHERE name = '" + tempStr + "'";
				char sql1[sql.length()];
				strcpy(sql1, sql.c_str());
				rc = sqlite3_exec(db, sql1, callback, 0, &zErrMsg);
				tables.Remove(deletedTables.CurrentKey(), deletedTable, deletedData);
				//deletedTables.Insert(deletedTable, deletedData);

				if( rc != SQLITE_OK ){
					fprintf(stderr, "SQL error: %s\n", zErrMsg);
					sqlite3_free(zErrMsg);
				} else {
					fprintf(stdout, "Table deleted successfully\n");
				}
			}
			deletedTables.Advance();
		}
		deletedTables.Clear();

		//Set number of TUPLES
		tupleMap.MoveToStart();
		for (int i = 0; i < tupleMap.Length(); i++) {

			string tempStr = deletedTables.CurrentKey();
			string nt = to_string(tupleMap.CurrentData());
			sql  = "UPDATE tables SET numTuples = " + nt + " WHERE name = '" + tempStr + "'";
			char sql1[sql.length()];
			strcpy(sql1, sql.c_str());
			rc = sqlite3_exec(db, sql1, callback, 0, &zErrMsg);

			tupleMap.Advance();
		}

		return true;

}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {

		int rc;
		char **table_results;
		char **errMessage1 = 0;
		int row, col;
		char *zErrMsg = 0;
		Keyify<string> key(_table);

		printf("GET NUM TUPLES\n");
		string sql = "SELECT numTuples FROM tables WHERE name = '" + _table + "'";
		char sql1[sql.length()];
		strcpy(sql1, sql.c_str());

		if(tables.IsThere(key) == 1){
				sqlite3_get_table(db, sql1, &table_results, &row, &col, errMessage1);
				_noTuples = atoi(table_results[0]);
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
		Keyify<string> key(_table);
		Keyify<string> removedKey(_table);
		Keyify<unsigned int> data(_noTuples);
		Keyify<unsigned int> removedData(_noTuples);

		if(tables.IsThere(key)) {
			tupleMap.Remove(key, removedKey, removedData);
			tupleMap.Insert(key, data);
			printf("SET NUM TUPLES\n");
			string sql = "UPDATE tables SET numTuples = " + to_string(_noTuples) + " WHERE name = '" + _table + "'";
			char sql1[sql.length()];
			strcpy(sql1, sql.c_str());

			rc = sqlite3_exec(db, sql1, callbackCount, 0, &zErrMsg);

			if (rc != SQLITE_OK) {
			fprintf(stderr, "SQL error: %s\n", zErrMsg);
			sqlite3_free(zErrMsg);
			}
		}
		else if (insertedTables.IsThere(key)) {
			tupleMap.Remove(key, removedKey, removedData);
			tupleMap.Insert(key, data);
		}

}

bool Catalog::GetDataFile(string& _table, string& _path) {

	Keyify<string> key(_table);
	char **table_results;
	char **errMessage1 = 0;
	int row, col;

	printf("GET DATA FILE\n");
	//First check if table name matches
	if(tables.IsThere(key) == 1){
		string sql = "SELECT fileLoc FROM tables WHERE name = '" + _table + "'";
		char sql1[sql.length()];
		strcpy(sql1, sql.c_str());
		sqlite3_get_table(db, sql1, &table_results, &row, &col, errMessage1);
		_path = table_results[0];
		//path = _path;
		return true;
	} else return false;



/*	if(_table == tName)
		return true;
	else return false;
*/
}
void Catalog::SetDataFile(string& _table, string& _path) {

	Keyify<string> key(_table);
	int rc;
	char *zErrMsg = 0;

	printf("SET DATA FILE\n");
	string sql = "UPDATE tables SET fileLoc = '" + _path + "' WHERE name = '" + _table + "'";
	char sql1[sql.length()];
	strcpy(sql1, sql.c_str());

	rc = sqlite3_exec(db, sql1, callback, 0, &zErrMsg);

	if( rc != SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	} else {
		fprintf(stdout, "Updated data file\n");
	}

}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {

		int rc;
		int count = 0;
		char **table_results;
		char *zErrMsg = 0;
		char **errMessage1 = 0;
		int row, col;
		Keyify<string> key(_table);

		printf("GET NUM DISTINCT\n");
		if (tables.IsThere(key)) {
			Schema tempSchem = tables.Find(key);
			vector<Attribute> atts = tempSchem.GetAtts();
			for (int i = 0; i < atts.size(); i++) {
				if (atts[i].name == _attribute) {
					_noDistinct = atts[i].noDistinct;
				}
			}
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
		Keyify<string> key(_table);

		if (tables.IsThere(key)) {
			Schema table = tables.Find(key);
			vector<Attribute> atts = table.GetAtts();
			vector<string> attributes, types;
			vector<unsigned int> distincts;
			for (int j = 0; j < atts.size(); j++) {
				if (atts[j].name == _attribute) {
					atts[j].noDistinct = _noDistinct;
				}
				attributes.push_back(atts[j].name);
				if (atts[i].type == Integer) {
					types.push_back("INTEGER");
				}
				else if (atts[i].type == Float) {
					types.push_back("FLOAT");
				}
				else types.push_back("STRING");
				distincts.push_back(atts[j].noDistinct);
			}

			Schema temp(attributes, types, distincts);
			tables.Find(key).Swap(temp);

			printf("SET NUM DISCTINCT\n");
			string sql = "UPDATE attribute SET distinctVal = " + to_string(_noDistinct) + " WHERE tableName = '" + _table + "' AND name = '" + _attribute + "'";
			char sql1[sql.length()];
			strcpy(sql1, sql.c_str());

			rc = sqlite3_exec(db, sql1, callbackCount, 0, &zErrMsg);

			if (rc != SQLITE_OK) {
					fprintf(stderr, "SQL error: %s\n", zErrMsg);
					sqlite3_free(zErrMsg);
			}
		}
		else if (insertedTables.IsThere(key)) {
			Schema table = insertedTables.Find(key);
			vector<Attribute> atts = table.GetAtts();
			vector<string> attributes, types;
			vector<unsigned int> distincts;
			for (int j = 0; j < atts.size(); j++) {
				if (atts[j].name == _attribute) {
					atts[j].noDistinct = _noDistinct;
				}
				attributes.push_back(atts[j].name);
				if (atts[i].type == Integer) {
					types.push_back("INTEGER");
				}
				else if (atts[i].type == Float) {
					types.push_back("FLOAT");
				}
				else types.push_back("STRING");
				distincts.push_back(atts[j].noDistinct);
			}

			Schema temp(attributes, types, distincts);
			insertedTables.Find(key).Swap(temp);
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
		Keyify<unsigned int> dist(0);
		char *zErrMsg = 0;

		//Check to make sure attributes are unique
		/*vector<string> atts(_attributes);
		sort(atts.begin(), atts.end());
		auto it = unique( atts.begin(), atts.end() );
		if (it == atts.end()) {
			printf("Duplicate attributes.");
			return false;
		}*/

		for (int i = 0; i < _attributes.size(); i++) {
			distincts.push_back(0);
		}
		Schema *table = new Schema(_attributes, _attributeTypes, distincts);
		if (tables.IsThere(key) != 1){
			insertedTables.Insert(key, *table);
			tupleMap.Insert(key, dist);
			printf("Created Table.\n");
			Save();
			return true;
		}
		else {
			printf("Table is already present.\n");
			return false;
		}

}

bool Catalog::DropTable(string& _table) {

		int i;
		Keyify<string> key(_table);
		Schema schema = tables.Find(key);

		if(tables.IsThere(key) == 1){
			deletedTables.Insert(key, schema);
			Save();
			return true;
		}
		else return false;

}

ostream& operator<<(ostream& _os, Catalog& _c) {
	return _os;
}
