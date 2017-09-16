#include <iostream>
#include <algorithm>
#include "Schema.h"
#include "Catalog.h"
#include <stdio.h>
#include <sstream>
#include "EfficientMap.h"
#include <unordered_map>
#include <pthread.h>
#include "DBFile.h"

using namespace std;

int NUM_THREADS  = 1;
struct Dist {
	unsigned int dist = 0;
	bool noUpdate = true;
};
bool needUpdateDist = true;
sqlite3 * db;
unordered_map<string, unsigned int> *mapTuples;
unordered_map<string, Dist> *mapDist;
unordered_map<string, string> *mapPath;
unordered_map<string, pair<vector<string>, vector<string>>> *mapCreateTable;
unordered_map<string, vector<string>> *cacheVs;
unordered_map<string, Schema> *cacheSc;
unordered_map<string, short> *cacheTableExist;
unordered_map<string, bool> *check;

void split(const string& s, char delim,vector<string>& v) {
    auto i = 0;
    auto pos = s.find(delim);
    while (pos != string::npos) {
      v.push_back(s.substr(i, pos-i));
      i = ++pos;
      pos = s.find(delim, pos);

      if (pos == string::npos)
         v.push_back(s.substr(i, s.length()));
    }
}

static int callback(void *NotUsed, int argc, char **argv, char **azColName){
   return 0;
}

unsigned int fast_atoi( const char * str )
{
    int val = 0;
    while( *str ) {
        val = val*10 + (*str++ - '0');
    }
    return val;
}

Catalog::Catalog(string& _fileName, bool noNeed) {
	   mapTuples = new unordered_map<string, unsigned int>();
	   mapDist = new unordered_map<string, Dist>();
	   mapPath = new unordered_map<string, string>();
	   mapCreateTable = new unordered_map<string, pair<vector<string>, vector<string>>>();
	   cacheVs = new unordered_map<string, vector<string>>();
	   cacheSc = new unordered_map<string, Schema>();
	   cacheTableExist = new unordered_map<string, short>();
	   check = new unordered_map<string, bool>();
	   int  rc;
	   /* Open database */
	   rc = sqlite3_open(_fileName.c_str(), &db);
	   if( rc ){
	      fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
	   }
}

Catalog::Catalog(string& _fileName) {
	   mapTuples = new unordered_map<string, unsigned int>();
	   mapDist = new unordered_map<string, Dist>();
	   mapPath = new unordered_map<string, string>();
	   mapCreateTable = new unordered_map<string, pair<vector<string>, vector<string>>>();
	   cacheVs = new unordered_map<string, vector<string>>();
	   cacheSc = new unordered_map<string, Schema>();
	   cacheTableExist = new unordered_map<string, short>();
	   check = new unordered_map<string, bool>();
	   char *zErrMsg;
	   int  rc;
	   char *sql;

	   /* Open database */
	   rc = sqlite3_open(_fileName.c_str(), &db);
	   if( rc ){
	      fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
	   }

	   sqlite3_exec(db, "PRAGMA synchronous=OFF", NULL, NULL, &zErrMsg);
	   sqlite3_exec(db, "PRAGMA count_changes=OFF", NULL, NULL, &zErrMsg);
	   sqlite3_exec(db, "PRAGMA journal_mode=MEMORY", NULL, NULL, &zErrMsg);
	   sqlite3_exec(db, "PRAGMA temp_store=MEMORY", NULL, NULL, &zErrMsg);
	   sqlite3_exec(db, "PRAGMA locking_mode=exclusive", NULL, NULL, &zErrMsg);
	   //sqlite3_exec(db, "PRAGMA PAGE_SIZE=65535", NULL, NULL, &zErrMsg);
	   sqlite3_exec(db, "PRAGMA CACHE_SIZE=10000", NULL, NULL, &zErrMsg);


	   sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &zErrMsg);
	   /* Create SQL statement */
	   char * drop = "DROP TABLE IF EXISTS  _cse277_tables;";
	   rc = sqlite3_exec(db, drop, callback, 0, &zErrMsg);
	   if( rc != SQLITE_OK ){
	   fprintf(stderr, "SQL error: %s\n", zErrMsg);
	      //sqlite3_free(zErrMsg);
	   }
	   drop = "DROP TABLE IF EXISTS  _cse277_attributes;";
	   	   rc = sqlite3_exec(db, drop, callback, 0, &zErrMsg);
	   	   if( rc != SQLITE_OK ){
	   	   fprintf(stderr, "SQL error: %s\n", zErrMsg);
	   	      //sqlite3_free(zErrMsg);
	   }
	   char * sql_tables = "CREATE TABLE IF NOT EXISTS _cse277_tables ("  \
	         "ID INTEGER PRIMARY KEY AUTOINCREMENT     NOT NULL," \
	         "NAME           TEXT    NOT NULL," \
	         "NOTUPLES       LONG    NULL," \
	         "PATH           TEXT    NULL ," \
	         "DEL            SHORT   NOT NULL DEFAULT 0 " \
	         ");";
	   //cout<<sql_tables<<endl;
	   char * sql_attributes = "CREATE TABLE IF NOT EXISTS _cse277_attributes ("  \
	         "ID INTEGER PRIMARY KEY AUTOINCREMENT    NOT NULL," \
	         "NAME           TEXT    NOT NULL," \
	         "TYPE  	     TEXT     NOT NULL," \
	         "DIST 			 LONG    NULL," \
	         "TABLE_NAME 	     TEXT    NOT NULL," \
	         "DEL            SHORT   NOT NULL DEFAULT 0 " \
	         ");";

	   /* Execute SQL statement */

	   rc = sqlite3_exec(db, sql_tables, callback, 0, &zErrMsg);
	   if( rc != SQLITE_OK ){
	   fprintf(stderr, "SQL error: %s\n", zErrMsg);
	   fprintf(stderr, "SQL error: %s\n", sql_tables);
	      //sqlite3_free(zErrMsg);
	   }
	   rc = sqlite3_exec(db, sql_attributes, callback, 0, &zErrMsg);
	   if( rc != SQLITE_OK ){
	   fprintf(stderr, "SQL error: %s\n", zErrMsg);
	   fprintf(stderr, "SQL error: %s\n", sql_attributes);
	      //sqlite3_free(zErrMsg);
	   }
	   //sqlite3_exec(db, "COMMIT TRANSACTION", NULL, NULL, &zErrMsg);

	  		std::ostringstream oss;
	   ////		oss << "SELECT NAME, NOTUPLES, PATH FROM _cse277_tables WHERE DEL = 0 ORDER BY NAME;";
	   ////		rc = sqlite3_exec(db, oss.str().c_str(), callback_Initial, this, &zErrMsg);
	   ////		if( rc != SQLITE_OK ) {
	   ////			fprintf(stderr, "SQL error: %s\n", zErrMsg);
	   ////			fprintf(stderr, "SQL error: %s\n", oss.str().c_str());
	   ////		}
	   	   oss << "CREATE INDEX name_idx ON _cse277_tables (NAME,DEL);";
	   	   rc = sqlite3_exec(db, sql_attributes, callback, 0, &zErrMsg);
	   	   if( rc != SQLITE_OK ){
	   	   fprintf(stderr, "SQL error: %s\n", zErrMsg);
	   	   fprintf(stderr, "SQL error: %s\n", sql_attributes);
	   	      //sqlite3_free(zErrMsg);
	   	   }
	   	   oss << "CREATE INDEX tabelnamedel_idx ON _cse277_attributes (TABLE_NAME,NAME,DEL);";
	   	   rc = sqlite3_exec(db, sql_attributes, callback, 0, &zErrMsg);
	   	   if( rc != SQLITE_OK ){
	   	   fprintf(stderr, "SQL error: %s\n", zErrMsg);
	   	   fprintf(stderr, "SQL error: %s\n", sql_attributes);
	   	      //sqlite3_free(zErrMsg);
	   	   }
	   	   oss << "CREATE INDEX tabledel_idx ON _cse277_attributes (TABLE_NAME,DEL);";
	   	   rc = sqlite3_exec(db, sql_attributes, callback, 0, &zErrMsg);
	   	   if( rc != SQLITE_OK ){
	   	   fprintf(stderr, "SQL error: %s\n", zErrMsg);
	   	   fprintf(stderr, "SQL error: %s\n", sql_attributes);
	   	      //sqlite3_free(zErrMsg);
	   	   }
	   	   sqlite3_exec(db, "COMMIT TRANSACTION", NULL, NULL, &zErrMsg);
}


Catalog::~Catalog() {
	Save();
	sqlite3_close(db);
}



void *SaveWithThreads(void * eachArg) {
	char *zErrMsg;
	sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &zErrMsg);
	//GetTables(tables);
	bool needDistWrite = false;
	std::ostringstream oss;
	unordered_map<string, pair<vector<string>, vector<string>>> *each = (unordered_map<string, pair<vector<string>, vector<string>>>*)eachArg;
	for(auto i = each->begin(); i != each->end(); ++i) {
		//fprintf(stderr, "line 163\n");
		auto tmp = check->find(i->first);
		//fprintf(stderr, "tb name : %s \n", i->first.c_str());
		if(tmp == check->end()) {
			//createtable
			char *zErrMsg = 0;
			std::ostringstream oss;
			oss << "INSERT INTO _cse277_tables (NAME, NOTUPLES, PATH) " << "VALUES (\""
					<<i->first << "\","<< (*mapTuples)[i->first] << ",\""<< (*mapPath)[i->first]<<"\"); ";
			//sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &zErrMsg);
			int rc = sqlite3_exec(db, oss.str().c_str(), callback, 0, &zErrMsg);
			if( rc != SQLITE_OK ) {
				fprintf(stderr, "SQL error: %s\n", zErrMsg);
				fprintf(stderr, "SQL error: %s\n", oss.str().c_str());
			}
			for(int j = 0; j < i->second.first.size(); ++j) {
				//fprintf(stderr, "line 177\n");
				oss.clear();
				oss.str("");
				oss << "INSERT INTO _cse277_attributes (NAME, TYPE, DIST, TABLE_NAME) "  << "VALUES (\""
						<< i->second.first[j] << "\",\"" << i->second.second[j] <<  "\","  << (*mapDist)[i->first + "@" + i->second.first[j]].dist << ",\"" << i->first << "\"); ";
				int rc = sqlite3_exec(db, oss.str().c_str(), callback, 0, &zErrMsg);
				if( rc != SQLITE_OK ) {
					fprintf(stderr, "SQL error: %s\n", zErrMsg);
					fprintf(stderr, "SQL error: %s\n", oss.str().c_str());
				}
				(*mapDist)[i->first + "@" + i->second.first[j]].noUpdate = true;
			}
			(*check)[i->first] = true;
		} else {
			//fprintf(stderr, "line 192\n");
			int noTuples = (*mapTuples)[i->first];
			string path = (*mapPath)[i->first];
			oss << "UPDATE _cse277_tables set NOTUPLES = "  << noTuples << ", PATH = \""<<path<< "\" WHERE NAME = \""
					<<i->first << "\" ;";
			int rc = sqlite3_exec(db, oss.str().c_str(), callback, 0, &zErrMsg);
			if( rc != SQLITE_OK ){
				fprintf(stderr, "SQL error: %s\n", zErrMsg);
				fprintf(stderr, "SQL error: %s\n", oss.str().c_str());
			}
			oss.clear();
			oss.str("");
			//fprintf(stderr, "line 208\n");
			if(needUpdateDist == true) {
				for(int index = 0; index < i->second.first.size(); ++index) {
					auto tmp = mapDist->find(i->first + "@" + i->second.first[index]);
					if(tmp != mapDist->end()) {
						if(tmp->second.noUpdate == true) continue;
						oss << "UPDATE _cse277_attributes set DIST = "  << tmp->second.dist << " WHERE TABLE_NAME = \""
						<< i->first << "\" " << "AND NAME = \"" << i->second.first[index] << "\";";
						int rc = sqlite3_exec(db, oss.str().c_str(), callback, 0, &zErrMsg);
						if( rc != SQLITE_OK ){
							fprintf(stderr, "SQL error: %s\n", zErrMsg);
							fprintf(stderr, "SQL error: %s\n", oss.str().c_str());
						}
						oss.clear();
						oss.str("");
						//fprintf(stderr, "line 221, size : %d\n", mapDist->size());
					}
				}
			}
//			for ( auto itt = mapDist->begin(); itt != mapDist->end(); ++itt ) {
//				vector<string> tmp;
//				split(itt->first, '@', tmp);
//				if(tmp[0] == i->first) {
//
//				}
//			}
		}
	}

	//fprintf(stderr, "line 224\n");
	sqlite3_exec(db, "COMMIT TRANSACTION", NULL, NULL, &zErrMsg);
}

static int hashstring(string s) {
	int sum = 0;
	for(int i = 0; i < s.length(); ++i) {
		sum += s[i];
	}
	return sum;
}
bool Catalog::Save() {

	  //fprintf(stderr, "mapCreateTable->size() %d\n", mapCreateTable->size());
      if(mapCreateTable->size() == 1) NUM_THREADS = 1;
//	  else if(mapCreateTable->size() <= 10) NUM_THREADS = 2;
//	  else NUM_THREADS = 3;
	  int size =  mapCreateTable->size() / NUM_THREADS;
	  int count = 0;
	  int index = 0;
	  pthread_t threads[NUM_THREADS];
	  pthread_attr_t attr;
	  void *status;
	  pthread_attr_init(&attr);
	  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	  unordered_map<string, pair<vector<string>, vector<string>>> tasks[NUM_THREADS];

	  for(auto i = mapCreateTable->begin(); i != mapCreateTable->end(); ++i) {
		  //if(count == 0) tasks[index] = new unordered_map<string, pair<vector<string>, vector<string>>>();
//		  if(count == size) {
//			  ++index;
//			  count = 0;
//		  }
		  tasks[hashstring(i->first) % NUM_THREADS].insert({i->first, i->second});
		  //fprintf(stderr, "mapCreateTable->size() %d  %d\n",index, tasks[index].size());
	  }
	  for(int i=0; i < NUM_THREADS; i++ ) {
	    //cout << "main() : creating thread, " << i << endl;

	    int rc = pthread_create(&threads[i], &attr, &SaveWithThreads, (void *)(&tasks[i]));

	    if (rc){
		   cout << "Error:unable to create thread," << rc << endl;
		   fprintf(stderr, "line 238\n");
		   //exit(-1);
	    }
	  }
	  pthread_attr_destroy(&attr);

	  for(int i=0; i < NUM_THREADS; i++ ){
	     int rc = pthread_join(threads[i], &status);

	     if (rc){
	        cout << "Error:unable to join," << rc << endl;
	        fprintf(stderr, "line 249\n");
	        //exit(-1);
	     }

	     //cout << "Main: completed thread id :" << i ;
	     //cout << "  exiting with status :" << status << endl;
	  }

	  //cout << "Main: program exiting." << endl;
	  //pthread_exit(NULL);
	  needUpdateDist = false;
	  return true;
}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	auto tmp = mapTuples->find(_table);
	if(tmp != mapTuples->end()) {
		_noTuples = tmp->second;
		return true;
	}
	return false;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
	(*mapTuples)[_table] = _noTuples;
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	auto tmp = mapPath->find(_table);
	if(tmp != mapPath->end()) {
		_path = tmp->second;
		return true;
	}
	return false;

}

void Catalog::SetDataFile(string& _table, string& _path) {
	(*mapPath)[_table] = _path;
}

int _noDistinct_Global = 0;
static int callback_GetNoDistinct(void *NotUsed, int argc, char **argv, char **azColName){
	_noDistinct_Global = fast_atoi(argv[0]);
   return 0;
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	auto tmp = mapDist->find(_table + "@" + _attribute);
	if(tmp != mapDist->end()) {
		_noDistinct = tmp->second.dist;
		return true;
	}
	/*
	char *zErrMsg = 0;
	std::ostringstream oss;
	oss << "SELECT DIST FROM _cse277_attributes WHERE TABLE_NAME = \"" << _table << "\" AND NAME = \"" << _attribute
			<<"\" AND DEL = 0 LIMIT 1;";
	int rc = sqlite3_exec(db, oss.str().c_str(), callback_GetNoDistinct, 0, &zErrMsg);
	_noDistinct =_noDistinct_Global;
	(*mapDist)[_table + "@" + _attribute] = _noDistinct_Global;
	if( rc != SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		fprintf(stderr, "SQL error: %s\n", oss.str().c_str());
		return false;
	}*/
	return false;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	needUpdateDist = true;
	Dist dist;
	dist.dist = _noDistinct;
	dist.noUpdate = false;
	(*mapDist)[_table + "@" + _attribute] = dist;
}

vector<string> _tables_Global;
static int callback_GetTables(void *NotUsed, int argc, char **argv, char **azColName){
//	_tables_Global.clear();
	for(int i = 0; i < argc; ++i) {
		_tables_Global.push_back(argv[i]);
	}
    return 0;
}

void Catalog::GetTables(vector<string>& _tables) {
	char *zErrMsg = 0;
//	auto tmp = cacheVs->find("GetTables");
//	if(tmp != cacheVs->end()) {
//		_tables = tmp->second;
//		return;
//	}
	std::ostringstream oss;
	oss << "SELECT NAME FROM _cse277_tables;";
	int rc = sqlite3_exec(db, oss.str().c_str(), callback_GetTables, 0, &zErrMsg);
	_tables =_tables_Global;
//	(*cacheVs)["GetTables"] = _tables_Global;
	if( rc != SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		fprintf(stderr, "SQL error: %s\n", oss.str().c_str());
		return;
	}
}

vector<string> _attributes_Global;
static int callback_GetAttributes(void *NotUsed, int argc, char **argv, char **azColName) {
	for(int i = 0; i < argc; ++i) {
		_attributes_Global.push_back(argv[i]);
	}
    return 0;
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	auto tmp = cacheVs->find(_table);
	if(tmp != cacheVs->end()) {
		_attributes = tmp->second;
		return true;
	}
	char *zErrMsg = 0;
	std::ostringstream oss;
	oss << "SELECT NAME FROM _cse277_attributes WHERE TABLE_NAME = \"" << _table << "\" AND DEL = 0 ORDER BY NAME;";
	int rc = sqlite3_exec(db, oss.str().c_str(), callback_GetAttributes, 0, &zErrMsg);
	_attributes =_attributes_Global;
	(*cacheVs)[_table] = _attributes_Global;
	if( rc != SQLITE_OK ) {
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		fprintf(stderr, "SQL error: %s\n", oss.str().c_str());
		return false;
	}
	_attributes_Global.clear();
	return true;
}

string _tablename_attributes_Global;
static int callback_GetTableByAttribute(void *NotUsed, int argc, char **argv, char **azColName) {
	for(int i = 0; i < argc; ++i) {
		_tablename_attributes_Global = argv[i];
	}
    return 0;
}
void Catalog::GetTableByAttribute(string& _table, string& _attributes) {
	auto tmp = mapPath->find(_attributes);
	if(tmp != mapPath->end()) {
		_table = tmp->second;
		return;
	}
	char *zErrMsg = 0;
	std::ostringstream oss;
	oss << "SELECT TABLE_NAME FROM _cse277_attributes WHERE NAME = \"" << _attributes << "\" AND DEL = 0 LIMIT 1;";
	int rc = sqlite3_exec(db, oss.str().c_str(), callback_GetTableByAttribute, 0, &zErrMsg);
	_table =_tablename_attributes_Global;
	(*mapPath)[_attributes] = _tablename_attributes_Global;
	if( rc != SQLITE_OK ) {
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		fprintf(stderr, "SQL error: %s\n", oss.str().c_str());
	}
}

vector<string> _schema_attributes_Global;
vector<string> _schema_attributeTypes_Global;
vector<unsigned int> _schema_distincts_Global;
static int callback_GetSchema(void *NotUsed, int argc, char **argv, char **azColName) {
	for(int i = 0; i < argc; ++i) {
		if(i == 0) _schema_attributes_Global.push_back(argv[i]);
		if(i == 1) _schema_attributeTypes_Global.push_back(argv[i]);
		if(i == 2) _schema_distincts_Global.push_back(fast_atoi(argv[i]));
	}
	return 0;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
//	auto tmp = cacheSc->find(_table);
//	if(tmp != cacheSc->end()) {
//		_schema = tmp->second;
//		return true;
//	}
	char *zErrMsg = 0;
	std::ostringstream oss;
	oss << "SELECT NAME, TYPE, DIST FROM _cse277_attributes WHERE TABLE_NAME = \"" << _table
			<<"\" AND DEL = 0 ;";
//	cout<< "SELECT NAME, TYPE, DIST FROM _cse277_attributes WHERE TABLE_NAME = \"" << _table
//			<<"\" AND DEL = 0 ;";
	int rc = sqlite3_exec(db, oss.str().c_str(), callback_GetSchema, 0, &zErrMsg);
//	cout<<"_schema_attributes_Global:"<<_schema_attributes_Global.size()<<endl;
//	cout<<"_schema_attributeTypes_Global:"<<_schema_attributeTypes_Global.size()<<endl;
//	cout<<"_schema_distincts_Global:"<<_schema_distincts_Global.size()<<endl;
	_schema = Schema(_schema_attributes_Global, _schema_attributeTypes_Global, _schema_distincts_Global);
//	(*cacheSc)[_table] = _schema;
	if( rc != SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		fprintf(stderr, "SQL error: %s\n", oss.str().c_str());
		return false;
	}
	_schema_attributes_Global.clear();
	_schema_attributeTypes_Global.clear();
	_schema_distincts_Global.clear();
	return true;
}
bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {
//	cacheVs->clear();
//	cacheSc->clear();
	auto tmp = mapCreateTable->find(_table);
	if(tmp == mapCreateTable->end()) {
		(*mapCreateTable)[_table] = make_pair(_attributes, _attributeTypes);
		(*cacheTableExist)[_table] = 0;
		(*cacheVs)[_table] = _attributes;
		DBFile dbFile;
		char* path = const_cast<char*>(_table.c_str());
//		dbFile.Create(path, FileType::Heap);
//		cout<<"path:"<<_table<<endl;
		return true;
	}
	return false;
}

bool isExist = false;
static int callback_DropTable(void *NotUsed, int argc, char **argv, char **azColName){
//	if(argc > 1) isExist = true;
//	cout<<argc<<" "<<endl;
//	for(int i = 0; i < argc; ++i) {
//		cout<<argv[i]<<" ";
//	}
//	cout<<endl;
	if(argc > 0) isExist = true;
	return 0;
}

bool Catalog::DropTable(string& _table) {
//	//todo clear cache
//	auto tmp = mapTuples->find(_table);
//	if(tmp == mapTuples->end()) {
//		return false;
//	}

	char *zErrMsg = 0;
	std::ostringstream oss;
//	oss << "SELECT NAME FROM _cse277_tables " << " WHERE NAME = \""
//			<<_table << "\" AND DEL = 0 ;";
//	int rc = sqlite3_exec(db, oss.str().c_str(), callback_DropTable, 0, &zErrMsg);
	auto tmp = cacheTableExist->find(_table);
	if(tmp == cacheTableExist->end()) return false;
	sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &zErrMsg);
	isExist = false;
	oss.clear();
	oss.str("");
	oss << "UPDATE _cse277_tables set DEL = "  << "1 " << " WHERE NAME = \""
			<<_table << "\" ;";
    int rc = sqlite3_exec(db, oss.str().c_str(), callback, 0, &zErrMsg);
	if( rc != SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		return false;
	}
	oss.clear();
	oss.str("");
	oss << "UPDATE _cse277_attributes set DEL = "  << "1 " << " WHERE TABLE_NAME = \""
			<<_table << "\" ;";
	rc = sqlite3_exec(db, oss.str().c_str(), callback, 0, &zErrMsg);
	sqlite3_exec(db, "COMMIT TRANSACTION", NULL, NULL, &zErrMsg);
	if( rc != SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		fprintf(stderr, "SQL error: %s\n", oss.str().c_str());
		return false;
	}
	return true;
}


static int callback_operator(void *NotUsed, int argc, char **argv, char **azColName) {
	for(int i = 0; i < argc; ++i) {
		cout<<"\t"<<argv[i];
	}
	cout<<endl;
	return 0;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	vector<string> _tables;
	_c.GetTables(_tables);
	std::ostringstream oss;
	char *zErrMsg = 0;
	unsigned int tuples = 0;
	string path;
	sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &zErrMsg);
	for(int i = 0; i < _tables.size(); ++i) {
		//bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples)
		_c.GetNoTuples(_tables[i], tuples);
		_c.GetDataFile(_tables[i], path);
		cout<<_tables[i]<<"\t"<<tuples<<"\t"<<path<<endl;
		oss.clear();
		oss.str("");
		oss << "SELECT NAME, TYPE, DIST FROM _cse277_attributes WHERE TABLE_NAME = \""
				<< _tables[i] << "\" AND DEL = 0 ORDER BY NAME;";
		int rc = sqlite3_exec(db, oss.str().c_str(), callback_operator, 0, &zErrMsg);
		if( rc != SQLITE_OK ){
			fprintf(stderr, "SQL error: %s\n", zErrMsg);
			fprintf(stderr, "SQL error: %s\n", oss.str().c_str());
		}
	}
	sqlite3_exec(db, "COMMIT TRANSACTION", NULL, NULL, &zErrMsg);
	return _os;
}
