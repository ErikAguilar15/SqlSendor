#include <iostream>
#include "sqlite3.h"

#include "Schema.h"
#include "Catalog.h"

using namespace std;

global sqlite3 *db;

Catalog::Catalog(string& _fileName) {
		char *errMessage = 0;
		int rc;

		//Open connection to our database
		rc = sqlite3_open("catalog.sqlite", &db);

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

	//First check if table name matches
	if(_table == tName)
		return true;
	else return false;
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

		//First check if table name matches
		if(_table == tName)
			return true;
		else return false;
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


	return true;
}

bool Catalog::DropTable(string& _table) {
	return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	return _os;
}
