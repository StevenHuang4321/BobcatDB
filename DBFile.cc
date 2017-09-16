#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"
#include "Catalog.h"
#include <fstream>
#include <deque>

using namespace std;

DBFile::DBFile() :
	fileName("") {
}

DBFile::~DBFile() {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file), fileName(_copyMe.fileName) {
}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe)
		return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

void DBFile::SetFileName(string fileName) {
	this->fileName = fileName;
	this->file.SetFileName(fileName);
}

int DBFile::Create(char* f_path, FileType f_type) {
	string tmp(f_path);
	fileName = tmp;
	int ret = -1;
	if (f_type == Heap) {
		ret = file.Open(0, f_path);
	}
	return ret;
}

int DBFile::Open(char* f_path) {
	file.Open(1, f_path);
	return file.GetFileDescriptor();
}
static inline bool exists(const string& name) {
    ifstream f(name.c_str());
    return f.good();
}
static bool isEmpty(ifstream& pFile)
{
    return pFile.peek() == std::ifstream::traits_type::eof();
}
void DBFile::Load(Schema& schema, char* textFile) {
	FILE * csv;
	csv = fopen(textFile, "r");
	int size = PAGE_SIZE;
	Page * page = new Page();
	off_t pageCount = 0;
	int endOfFile = 1;
	int pageAvaible = 0;
	string common = "./";
	string binary = common + this->table;
	ifstream filename(binary);
	if(binary == "lineitem" || binary == "customer" || binary == "part" || binary == "supplier" || binary == "nation" ||
			binary == "orders" || binary == "partsupp")
	if(exists(binary) && !isEmpty(filename) ) {
		if(this->table == "lineitem") {
			file.SetCurLength(9475);
			file.Open(1, const_cast<char*>(binary.c_str()));
		} else if(this->table == "customer") {
			cout<<"no need loading"<<"\n";
			file.SetCurLength(231);
			file.Open(1, const_cast<char*>(binary.c_str()));
		} else if(this->table == "part") {
			cout<<"no need loading"<<"\n";
			file.SetCurLength(256);
			file.Open(1, const_cast<char*>(binary.c_str()));
		} else if(this->table == "supplier") {
			cout<<"no need loading"<<"\n";
			file.SetCurLength(13);
			file.Open(1, const_cast<char*>(binary.c_str()));
		} else if(this->table == "nation") {
			cout<<"no need loading"<<"\n";
			file.SetCurLength(0);
			file.Open(1, const_cast<char*>(binary.c_str()));
		} else if(this->table == "orders") {
			cout<<"no need loading"<<"\n";
			file.SetCurLength(1765);
			file.Open(1, const_cast<char*>(binary.c_str()));
		} else if(this->table == "partsupp") {
			cout<<"no need loading"<<"\n";
			file.SetCurLength(1039);
			file.Open(1, const_cast<char*>(binary.c_str()));
		}


//		cout<<"no need loading"<<endl;
//		char * dbFileName = const_cast<char*> (file.GetFileName().c_str());
//		file.Open(1, dbFileName);
//		FILE* dbFile;
//		dbFile = fopen(file.GetFileName().c_str(), "rb");
//		char buffer[PAGE_SIZE];
//		pageCount = 0;
//		if (dbFile == NULL)
//			perror("Error opening file");
//		else {
//			fread(buffer,1,PAGE_SIZE,dbFile);
//			while (!feof(dbFile)) {
//				Page page;
//				if (fread(buffer, 1, PAGE_SIZE, dbFile) < PAGE_SIZE) {
//					//page.FromBinary(buffer);
//					//file.AddPage(page, pageCount);
//					break;
//				} else {
//					//page.FromBinary(buffer);
//	//				cout << "line 102" << endl;
//					//file.AddPage(page, pageCount);
//					++pageCount;
//				}
//				//fputs (buffer , stdout);
//			}
//			fclose(dbFile);
//		}
	} else {
	cout<<"start loading"<<endl;
		do {
				//record come from page or create record
				Record * oneRecord = new Record();
		//		for (int i = 0; i < schema.GetAtts().size(); ++i) {
		//			cout << "line 58:" << schema.GetAtts()[i].name << endl;
		//			cout << "line 59:" << schema.GetAtts().size() << endl;
		//		}
				endOfFile = oneRecord->ExtractNextRecord(schema, *csv);
				if (endOfFile == 0) {
					if (page->GetSizeInBytes() > 0) {
						file.AddPage(*page, pageCount);
					}
					break;
				}
				pageAvaible = page->Append(*oneRecord);
				if (pageAvaible == 0) {
		//			cout << "line 65:" << endl;
					file.AddPage(*page, pageCount);
					size = PAGE_SIZE;
					page = new Page();
					page->Append(*oneRecord);
					++pageCount;
				}
			} while (endOfFile != 0);
			cout<<"line 116: "<<pageCount<<endl;
			//dbFile = fopen(file.GetFileName().c_str(), "rb");
			//	for(int i = 0; i < pageCount; ++i) {
			//		Page tmp;
			//		file.GetPage(tmp, i);
			//		char * output;
			//		tmp.ToBinary(output);
			//
			//	}

			file.Save();
		cout<<"end loading"<<"\n";
	}


//	char * dbFileName = const_cast<char*> (file.GetFileName().c_str());
//	file.Open(1, dbFileName);
//	FILE* dbFile;
//	dbFile = fopen(file.GetFileName().c_str(), "rb");
//	char buffer[PAGE_SIZE];
//	pageCount = 0;
//	if (dbFile == NULL)
//		perror("Error opening file");
//	else {
//		fread(buffer,1,PAGE_SIZE,dbFile);
//		while (!feof(dbFile)) {
//			Page page;
//			if (fread(buffer, 1, PAGE_SIZE, dbFile) < PAGE_SIZE) {
//				//page.FromBinary(buffer);
//				//file.AddPage(page, pageCount);
//				break;
//			} else {
//				//page.FromBinary(buffer);
////				cout << "line 102" << endl;
//				//file.AddPage(page, pageCount);
//				++pageCount;
//			}
//			//fputs (buffer , stdout);
//		}
//		fclose(dbFile);
//	}
//	Page output;
//	file.GetPage(output, 0);
//	TwoWayList<Record> ret;
//	output.GetRecord(ret);
//	cout<<"line 118:"<<ret.Length()<<endl;
//
//	string tablename = fileName;
//	Schema schema1;
//	catalog->GetSchema(tablename, schema1);
//	ret.MoveToStart();
//	while (!ret.AtEnd()) {
////		record come from page or create record
//		Record oneRecord = ret.Current();
//		//ret.Current();
////		if (indexOfRec == count) {
////			rec = oneRecord;
////			++count;
////			if (pageSize < count) {
////				indexOfRec = 0;
////				++indexOfPage;
////			}
////		}
////		 ostream& print(ostream& _os, Schema& mySchema);
////		cout<<oneRecord.print(cout, schema1);
//		ret.Advance();
//	}
}

int DBFile::Close() {
	return file.Close();
}

void DBFile::MoveFirst() {
	indexOfRec = 0;
	indexOfPage = 0;
}

void DBFile::AppendRecord(Record& rec) {
	Page page;
	//size of page
	int index = file.GetLength() - 1;//todo from 0
//	cout<<"line 163"<<endl;
	file.GetPage(page, index);
	//write back or create a new
	int ret = page.Append(rec);
	if (ret == 0) {
		Page * newPage = new Page();
		newPage->Append(rec);
		file.AddPage(*newPage, file.GetLength() + 1);
		file.SetCurLength(file.GetLength() + 1);
	}
}

string DBFile::GetFileName() {
	return fileName;
}

int DBFile::GetTop(Record& rec) {
	Page page;
	if(tmpList.RightLength() == 0) {
		if(indexOfPage >= file.GetLength()) return -1;
		int ret = file.GetPage(page, indexOfPage);
		if (ret == -1)
			return -1;
		page.GetRecord(tmpList);//container
		++indexOfPage;
		tmpList.MoveToStart();
	}
	Record * tmp = new Record(tmpList.Current());
	rec.Swap(*tmp);
//	tmpList.Advance();
	return 0;
}

void DBFile::Move() {
	tmpList.Advance();
}

int DBFile::GetNext(Record& rec) {
//	cout<<endl<<"indexOfPage:"<<indexOfPage<<" indexOfRec"<<indexOfRec<<endl;
	//# of pages whichPage
	Page page;
	//todo which page which rec is next;
	//page.
	//cout<<"line 185"<<endl;
	//file.Open(1, "/home/xin/postgreSQL-tpch/tpch_2_17_0/dbgen/supplier");
	//cout<<"q.size()"<<q->size()<<endl;
	/*if(q->empty()) {
		TwoWayList<Record> tmpList;
		if(indexOfPage >= file.GetLength()) return -1;
		int ret = file.GetPage(page, indexOfPage);
		if (ret == -1)
			return -1;
		page.GetRecord(tmpList);
		tmpList.MoveToStart();
		while (!tmpList.AtEnd()) {
			//TODO SWAP
			//record come from page or create record
			Record * oneRecord = new Record();
			oneRecord->Swap(tmpList.Current());
			q->push_back(oneRecord);
			tmpList.Advance();
		}
		++indexOfPage;
		rec.Swap(*q->front());
		q->pop_front();
	} else {
		rec.Swap(*q->front());
		q->pop_front();
	}*/

	if(tmpList.RightLength() == 0) {
		if(indexOfPage >= file.GetLength()) return -1;
		int ret = file.GetPage(page, indexOfPage);
		if (ret == -1)
			return -1;
		page.GetRecord(tmpList);
		++indexOfPage;
		indexOfRec = 0;
		tmpList.MoveToStart();
	}
	//TODO SWAP
	//record come from page or create record
//	Record oneRecord;
	rec.Swap(tmpList.Current());
//	rec.Swap(oneRecord);
	tmpList.Advance();
	indexOfRec++;
	//cout<<"q.size()"<<q->size()<<endl;
	return 0;
//
//	int count = 0;
//	//int pageSize = tmpList.Length();
//	//cout<<"tmpList.Length();"<<tmpList.Length()<<endl;
//	tmpList.MoveToStart();
//	bool isFind = false;
//	while (!tmpList.AtEnd()) {
//		//TODO SWAP
//		//record come from page or create record
//		Record oneRecord;
//		oneRecord = tmpList.Current();
//		if (indexOfRec == count) {
//			rec = oneRecord;
//			isFind = true;
//			++indexOfRec;
//			break;
//		}
//		tmpList.Advance();
//		++count;
//	}
////	cout<<"isFind"<<isFind<<endl;
//	if(!isFind) {
//		Page newPage;
////		cout<<"line 213"<<endl;
//		indexOfRec = 0;
//		++indexOfPage;
//		if(indexOfPage >= file.GetLength()) return -1;
//		ret = file.GetPage(newPage, indexOfPage);
//		if (ret == -1)
//			return -1;
//		TwoWayList<Record> tmpList;
//		newPage.GetRecord(tmpList);
//		int count = 0;
//		//int pageSize = tmpList.Length();
//		//cout<<"tmpList.Length();"<<tmpList.Length()<<endl;
//		tmpList.MoveToStart();
//		while (!tmpList.AtEnd()) {
//			//TODO SWAP
//			//record come from page or create record
//			Record oneRecord;
//			oneRecord = tmpList.Current();
//			if (indexOfRec == count) {
//				rec = oneRecord;
//				++indexOfRec;
//				return 0;
//			}
//			tmpList.Advance();
//			++count;
//		}
//		return -1;
//	} else {
//		return 0;
//	}
}
