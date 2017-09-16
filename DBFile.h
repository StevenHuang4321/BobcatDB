#ifndef DBFILE_H
#define DBFILE_H

#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Catalog.h"
#include <deque>

using namespace std;


class DBFile {
private:


public:
	File file;
	string fileName;
	//string catalogFile = "catalog.sqlite";
	TwoWayList<Record> tmpList;
	Catalog * catalog;
	string table;
	deque<Record*> * q = new deque<Record*>();
	DBFile ();
	virtual ~DBFile ();
	DBFile(const DBFile& _copyMe);
	DBFile& operator=(const DBFile& _copyMe);
	string GetFileName();
	void SetFileName(string fileName);

	int Create (char* fpath, FileType file_type);
	int Open (char* fpath);
	int Close ();

	void Load (Schema& _schema, char* textFile);

	void MoveFirst ();
	void AppendRecord (Record& _addMe);
	int GetNext (Record& _fetchMe);
	int GetTop (Record& _fetchMe);
	void Move ();

	int indexOfRec = 0;
	int indexOfPage = 0;
	int count = 0;
};

#endif //DBFILE_H
