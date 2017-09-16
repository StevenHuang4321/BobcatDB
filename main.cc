#include <iostream>
#include <string>

#include "Catalog.h"
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
//#include "RelOp.h"
extern "C"{
#include "QueryParser.h"
}
#include "ParseTree.h"
using namespace std;


// these data structures hold the result of the parsing
extern struct FuncOperator* finalFunction; // the aggregate function
extern struct TableList* tables; // the list of tables in the query
extern struct AndList* predicate; // the predicate in WHERE
extern struct NameList* groupingAtts; // grouping attributes
extern struct NameList* attsToSelect; // the attributes in SELECT
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query

extern struct AttributesList* attsList; // the list of attributes in the create table statement
extern struct NameList* indexList;// for index name
extern struct NameList* indexAtts;// for index attributes
extern struct NameList* loadFiles;// for load data

extern "C" int yyparse();
extern "C" int readInputForLexer( char *buffer, int *numBytesRead, int maxBytesToRead );
extern "C" int yylex_destroy();

static int globalReadOffset = 0;
// Text to read:
static const char *globalInputText = "3+4";

void resuse () {
	finalFunction = NULL;
	tables = NULL;
	predicate = NULL;
	groupingAtts = NULL;
	attsToSelect = NULL;
	attsList = NULL;
	indexList = NULL;
	indexAtts = NULL;
	loadFiles = NULL;
}

int readInputForLexer( char *buffer, int *numBytesRead, int maxBytesToRead ) {
    int numBytesToRead = maxBytesToRead;
    int bytesRemaining = strlen(globalInputText)-globalReadOffset;
    int i;
    if ( numBytesToRead > bytesRemaining ) { numBytesToRead = bytesRemaining; }
    for ( i = 0; i < numBytesToRead; i++ ) {
        buffer[i] = globalInputText[globalReadOffset+i];
    }
    *numBytesRead = numBytesToRead;
    globalReadOffset += numBytesToRead;
    return 0;
}

int main () {
	// this is the catalog
	string dbFile = "catalog.sqlite";
	Catalog catalog(dbFile);

	//customer
	vector<string> atts;
	vector<string> attTypes;
	atts.push_back("c_custkey");
	attTypes.push_back("INTEGER");

	atts.push_back("c_name");
	attTypes.push_back("STRING");

	atts.push_back("c_address");
	attTypes.push_back("STRING");

	atts.push_back("c_nationkey");
	attTypes.push_back("INTEGER");

	atts.push_back("c_phone");
	attTypes.push_back("STRING");

	atts.push_back("c_acctbal");
	attTypes.push_back("FLOAT");

	atts.push_back("c_mktsegment");
	attTypes.push_back("STRING");

	atts.push_back("c_comment");
	attTypes.push_back("STRING");

	string tableName = "customer";
	catalog.CreateTable(tableName, atts, attTypes);
	unsigned int total = 150000;
	catalog.SetNoTuples(tableName, total);
	unsigned int c_custkey = 150000;
	string attr = "c_custkey";
	catalog.SetNoDistinct(tableName, attr, c_custkey);
	attr = "c_name";
	catalog.SetNoDistinct(tableName, attr, c_custkey);
	attr = "c_address";
	catalog.SetNoDistinct(tableName, attr, c_custkey);
	unsigned int c_nationkey = 25;
	attr = "c_nationkey";
	catalog.SetNoDistinct(tableName, attr, c_nationkey);
	unsigned int c_phone = 150000;
	attr = "c_phone";
	catalog.SetNoDistinct(tableName, attr, c_phone);
	unsigned int c_acctbal = 140187;
	attr = "c_acctbal";
	catalog.SetNoDistinct(tableName, attr, c_acctbal);
	unsigned int c_mktsegment = 5;
	attr = "c_mktsegment";
	catalog.SetNoDistinct(tableName, attr, c_mktsegment);
	unsigned int c_comment = 149968;
	attr = "c_comment";
	catalog.SetNoDistinct(tableName, attr, c_comment);

	atts.clear();
	attTypes.clear();
	atts.push_back("l_orderkey");
	attTypes.push_back("INTEGER");

	atts.push_back("l_partkey");
	attTypes.push_back("INTEGER");

	atts.push_back("l_suppkey");
	attTypes.push_back("INTEGER");

	atts.push_back("l_linenumber");
	attTypes.push_back("INTEGER");

	atts.push_back("l_quantity");
	attTypes.push_back("INTEGER");

	atts.push_back("l_extendedprice");
	attTypes.push_back("FLOAT");

	atts.push_back("l_discount");
	attTypes.push_back("FLOAT");

	atts.push_back("l_tax");
	attTypes.push_back("FLOAT");

	atts.push_back("l_returnflag");
	attTypes.push_back("STRING");

	atts.push_back("l_linestatus");
	attTypes.push_back("STRING");

	atts.push_back("l_shipdate");
	attTypes.push_back("STRING");

	atts.push_back("l_commitdate");
	attTypes.push_back("STRING");

	atts.push_back("l_receiptdate");
	attTypes.push_back("STRING");

	atts.push_back("l_shipinstruct");
	attTypes.push_back("STRING");

	atts.push_back("l_shipmode");
	attTypes.push_back("STRING");

	atts.push_back("l_comment");
	attTypes.push_back("STRING");
	tableName = "lineitem";
	catalog.CreateTable(tableName, atts, attTypes);
	unsigned int lineitem = 6001215;
	catalog.SetNoTuples(tableName, lineitem);
	unsigned int l_orderkey = 1500000;
	attr = "l_orderkey";
	catalog.SetNoDistinct(tableName, attr, l_orderkey);
	unsigned int l_partkey = 200000;
	attr = "l_partkey";
	catalog.SetNoDistinct(tableName, attr, l_partkey);
	unsigned int l_suppkey = 10000;
	attr = "l_suppkey";
	catalog.SetNoDistinct(tableName, attr, l_suppkey);
	unsigned int l_linenumber = 7;
	attr = "l_linenumber";
	catalog.SetNoDistinct(tableName, attr, l_linenumber);
	unsigned int l_quantity = 50;
	attr = "l_quantity";
	catalog.SetNoDistinct(tableName, attr, l_quantity);
	unsigned int l_extendedprice = 933900;
	attr = "l_extendedprice";
	catalog.SetNoDistinct(tableName, attr, l_extendedprice);
	unsigned int l_discount = 11;
	attr ="l_discount";
	catalog.SetNoDistinct(tableName, attr, l_discount);
	unsigned int l_tax = 9;
	attr = "l_tax";
	catalog.SetNoDistinct(tableName, attr, l_tax);
	unsigned int l_returnflag = 3;
	attr = "l_returnflag";
	catalog.SetNoDistinct(tableName, attr, l_returnflag);
	unsigned int l_linestatus = 2;
	attr = "l_linestatus";
	catalog.SetNoDistinct(tableName, attr, l_linestatus);
	unsigned int l_shipdate = 2526;
	attr = "l_shipdate";
	catalog.SetNoDistinct(tableName, attr, l_shipdate);
	unsigned int l_commitdate = 2466;
	attr = "l_commitdate";
	catalog.SetNoDistinct(tableName, attr, l_commitdate);
	unsigned int l_receiptdate = 2554;
	attr = "l_receiptdate";
	catalog.SetNoDistinct(tableName, attr, l_receiptdate);
	unsigned int l_comment = 4580667;
	attr = "l_comment";
	catalog.SetNoDistinct(tableName, attr, l_comment);
	unsigned int l_shipmode = 7;
	attr = "l_shipmode";
	catalog.SetNoDistinct(tableName, attr, l_shipmode);
	unsigned int l_shipinstruct = 4;
	attr = "l_shipinstruct";
	catalog.SetNoDistinct(tableName, attr, l_shipinstruct);

	atts.clear();
	attTypes.clear();
	atts.push_back("n_nationkey");
	attTypes.push_back("INTEGER");
	atts.push_back("n_name");
	attTypes.push_back("STRING");
	atts.push_back("n_regionkey");
	attTypes.push_back("INTEGER");
	atts.push_back("n_comment");
	attTypes.push_back("STRING");
	tableName = "nation";
	catalog.CreateTable(tableName, atts, attTypes);
	unsigned int NATION = 25;
	catalog.SetNoTuples(tableName, NATION);
	unsigned int n_nationkey = 25;
	attr = "n_nationkey";
	catalog.SetNoDistinct(tableName, attr, n_nationkey);
	unsigned int n_name = 25;
	attr = "n_name";
	catalog.SetNoDistinct(tableName, attr, n_name);
	unsigned int n_regionkey = 5;
	attr = "n_regionkey";
	catalog.SetNoDistinct(tableName, attr, n_regionkey);
	unsigned int n_comment = 25;
	attr = "n_comment";
	catalog.SetNoDistinct(tableName, attr, n_comment);

	atts.clear();
	attTypes.clear();
	atts.push_back("o_orderkey");
	attTypes.push_back("INTEGER");

	atts.push_back("o_custkey");
	attTypes.push_back("INTEGER");

	atts.push_back("o_orderstatus");
	attTypes.push_back("STRING");

	atts.push_back("o_totalprice");
	attTypes.push_back("FLOAT");

	atts.push_back("o_orderdate");
	attTypes.push_back("STRING");

	atts.push_back("o_orderpriority");
	attTypes.push_back("STRING");

	atts.push_back("o_clerk");
	attTypes.push_back("STRING");

	atts.push_back("o_shippriority");
	attTypes.push_back("INTEGER");

	atts.push_back("o_comment");
	attTypes.push_back("STRING");

	tableName = "orders";
	catalog.CreateTable(tableName, atts, attTypes);
	unsigned int ORDERS = 1500000;
	catalog.SetNoTuples(tableName, ORDERS);
	unsigned int o_orderkey = 1500000;
	attr = "o_orderkey";
	catalog.SetNoDistinct(tableName, attr, o_orderkey);
	unsigned int o_custkey = 99996;
	attr = "o_custkey";
	catalog.SetNoDistinct(tableName, attr, o_custkey);
	unsigned int o_orderstatus = 3;
	attr = "o_orderstatus";
	catalog.SetNoDistinct(tableName, attr, o_orderstatus);
	unsigned int o_totalprice = 1464566;
	attr = "o_totalprice";
	catalog.SetNoDistinct(tableName, attr, o_totalprice);
	unsigned int o_orderdate = 2406;
	attr = "o_orderdate";
	catalog.SetNoDistinct(tableName, attr, o_orderdate);
	unsigned int o_orderpriority = 5;
	attr = "o_orderpriority";
	catalog.SetNoDistinct(tableName, attr, o_orderpriority);
	unsigned int o_clerk = 1000;
	attr = "o_clerk";
	catalog.SetNoDistinct(tableName, attr, o_clerk);
	unsigned int o_shippriority = 1;
	attr = "o_shippriority";
	catalog.SetNoDistinct(tableName, attr, o_shippriority);
	unsigned int o_comment = 1482071;
	attr = "o_comment";
	catalog.SetNoDistinct(tableName, attr, o_comment);

	atts.clear();
	attTypes.clear();
	atts.push_back("p_partkey");
	attTypes.push_back("INTEGER");

	atts.push_back("p_name");
	attTypes.push_back("STRING");

	atts.push_back("p_mfgr");
	attTypes.push_back("STRING");

	atts.push_back("p_brand");
	attTypes.push_back("STRING");

	atts.push_back("p_type");
	attTypes.push_back("STRING");

	atts.push_back("p_size");
	attTypes.push_back("INTEGER");

	atts.push_back("p_container");
	attTypes.push_back("STRING");

	atts.push_back("p_retailprice");
	attTypes.push_back("FLOAT");

	atts.push_back("p_comment");
	attTypes.push_back("STRING");
	tableName = "part";
	catalog.CreateTable(tableName, atts, attTypes);
	unsigned int PART = 200000;
	catalog.SetNoTuples(tableName, PART);
	unsigned int p_partkey = 200000;
	attr = "p_partkey";
	catalog.SetNoDistinct(tableName, attr, p_partkey);
	unsigned int p_name = 199996;
	attr = "p_name";
	catalog.SetNoDistinct(tableName, attr, p_name);
	unsigned int p_mfgr = 5;
	attr = "p_mfgr";
	catalog.SetNoDistinct(tableName, attr, p_mfgr);
	unsigned int p_brand = 25;
	attr = "p_brand";
	catalog.SetNoDistinct(tableName, attr, p_brand);
	unsigned int p_type = 150;
	attr = "p_type";
	catalog.SetNoDistinct(tableName, attr, p_type);
	unsigned int p_size = 50;
	attr = "p_size";
	catalog.SetNoDistinct(tableName, attr, p_size);
	unsigned int p_container = 40;
	attr = "p_container";
	catalog.SetNoDistinct(tableName, attr, p_container);
	unsigned int p_retailprice = 20899;
	attr = "p_retailprice";
	catalog.SetNoDistinct(tableName, attr, p_retailprice);
	unsigned int p_comment = 131753;
	attr = "p_comment";
	catalog.SetNoDistinct(tableName, attr, p_comment);

	atts.clear();
	attTypes.clear();
	atts.push_back("ps_partkey");
	attTypes.push_back("INTEGER");
	atts.push_back("ps_suppkey");
	attTypes.push_back("INTEGER");
	atts.push_back("ps_availqty");
	attTypes.push_back("INTEGER");
	atts.push_back("ps_supplycost");
	attTypes.push_back("FLOAT");
	atts.push_back("ps_comment");
	attTypes.push_back("STRING");
	tableName = "partsupp";
	catalog.CreateTable(tableName, atts, attTypes);
	total = 800000;
	catalog.SetNoTuples(tableName, total);
	unsigned int ps_partkey = 200000;
	attr = "ps_partkey";
	catalog.SetNoDistinct(tableName, attr, ps_partkey);
	unsigned int ps_suppkey = 10000;
	attr = "ps_suppkey";
	catalog.SetNoDistinct(tableName, attr, ps_suppkey);
	unsigned int ps_availqty = 9999;
	attr = "ps_availqty";
	catalog.SetNoDistinct(tableName, attr, ps_availqty);
	unsigned int ps_supplycost = 99865;
	attr = "ps_supplycost";
	catalog.SetNoDistinct(tableName, attr, ps_supplycost);
	unsigned int ps_comment = 799124;
	attr = "ps_comment";
	catalog.SetNoDistinct(tableName, attr, ps_comment);


	atts.clear();
	attTypes.clear();
	atts.push_back("r_regionkey");
	attTypes.push_back("INTEGER");

	atts.push_back("r_name");
	attTypes.push_back("STRING");

	atts.push_back("r_comment");
	attTypes.push_back("STRING");
	tableName = "region";
	catalog.CreateTable(tableName, atts, attTypes);
	total = 5;
	catalog.SetNoTuples(tableName, total);
	unsigned int r_regionkey = 5;
	attr = "r_regionkey";
	catalog.SetNoDistinct(tableName, attr, r_regionkey);
	unsigned int r_name = 5;
	attr = "r_name";
	catalog.SetNoDistinct(tableName, attr, r_name);
	unsigned int r_comment = 5;
	attr = "r_comment";
	catalog.SetNoDistinct(tableName, attr, r_comment);

	atts.clear();
	attTypes.clear();
	atts.push_back("s_suppkey");
	attTypes.push_back("INTEGER");

	atts.push_back("s_name");
	attTypes.push_back("STRING");

	atts.push_back("s_address");
	attTypes.push_back("STRING");

	atts.push_back("s_nationkey");
	attTypes.push_back("INTEGER");

	atts.push_back("s_phone");
	attTypes.push_back("STRING");

	atts.push_back("s_acctbal");
	attTypes.push_back("FLOAT");

	atts.push_back("s_comment");
	attTypes.push_back("STRING");

	tableName = "supplier";
	catalog.CreateTable(tableName, atts, attTypes);
	total = 10000;
	catalog.SetNoTuples(tableName, total);
	unsigned int s_suppkey = 10000;
	attr = "s_suppkey";
	catalog.SetNoDistinct(tableName, attr, s_suppkey);
	unsigned int s_name = 10000;
	attr = "s_name";
	catalog.SetNoDistinct(tableName, attr, s_name);
	unsigned int s_address = 10000;
	attr = "s_address";
	catalog.SetNoDistinct(tableName, attr, s_address);
	unsigned int s_nationkey = 25;
	attr = "s_nationkey";
	catalog.SetNoDistinct(tableName, attr, s_nationkey);
	unsigned int s_phone = 10000;
	attr = "s_phone";
	catalog.SetNoDistinct(tableName, attr, s_phone);
	unsigned int s_acctbal = 9955;
	attr = "s_acctbal";
	catalog.SetNoDistinct(tableName, attr, s_acctbal);
	unsigned int s_comment = 10000;
	attr = "s_comment";
	catalog.SetNoDistinct(tableName, attr, s_comment);
	catalog.Save();


	// this is the query optimizer
	// it is not invoked directly but rather passed to the query compiler

	QueryOptimizer optimizer(catalog);

	// this is the query compiler
	// it includes the catalog and the query optimizer
	QueryCompiler compiler(catalog, optimizer);


//	Schema schemaTmp;
//	string tb = "part";
//	catalog.GetSchema(tb, schemaTmp);
//	DBFile dbFileTmp;
//	dbFileTmp.SetFileName(tb);
//	Scan * sc = new Scan(schemaTmp, dbFileTmp);
////	sc->createIndex(tb, 0, Integer);
//	sc->searchRange(tb, 100,100);
//	return 0;

	string query;
	do {
	    cout << ">" << endl;
	    getline(cin, query);
	    if(query.empty()) continue;
	    globalInputText = query.c_str();
	    cout<<"SQL :"<<query<<endl;
//	    query = "schema";
	    if(query == "schema") {
			//print all schema
			{
				vector<string> tables;
				catalog.GetTables(tables);
				cout<<"tables:"<<tables.size()<<endl;
				for(string onetable : tables) {
					Schema schema;
					catalog.GetSchema(onetable, schema);
					cout<<onetable<<endl;
					for(Attribute one : schema.GetAtts()) {
						string tmpType;
						if(one.type == 1) {
							tmpType = "FLOAT";
						} else if(one.type == 2) {
							tmpType = "INTEGER";
						} else if(one.type == 3) {
							tmpType = "NAME";
						} else {
							tmpType = "STRING";
						}
						cout<<one.name<<" "<<tmpType<<endl;
					}
				}
			}
			cout<<"done"<<endl;
			continue;
		}
	    // the query parser is accessed directly through yyparse
		// this populates the extern data structures
		int parse = -1;
		globalReadOffset = 0;
		if (yyparse () == 0) {
			cout << "OK!" << "\n";
			parse = 0;
		}
		else {
			cout << "Error: Query is not correct!" << endl;
			parse = -1;
			yylex_destroy();
			resuse();
			continue;
		}


//
//		if (parse != 0) return -1;

		//TO CREATE TABLE
		if(attsList != NULL) {
//			printf("%s %d %s\n",attsList->name,attsList->type->code,attsList->type->value);
			//create table code
			{
				string tbname(tables->tableName);
				vector<string> attrs;
				vector<string> types;
				while(attsList != NULL) {
					attrs.push_back(attsList->name);
					types.push_back(attsList->type->value);
					attsList = attsList->next;
				}
				Schema schema;
				catalog.GetSchema(tbname, schema);
				if(schema.GetNumAtts() == 0) {
					catalog.CreateTable(tbname, attrs, types);
					catalog.Save();
				}
			}
			cout<<"done"<<endl;
			yylex_destroy();
			resuse();
			continue;
		}


		{
			//TO CREATE INDEX
			if(indexList != NULL) {
				string index(indexList->name);
				string indexAttribute(indexAtts->name);
				Schema schemaTmp;
				string tb(tables->tableName);
				catalog.GetSchema(tb, schemaTmp);
				DBFile dbFileTmp;
				dbFileTmp.SetFileName(tb);
				Scan * sc = new Scan(schemaTmp, dbFileTmp);
				unsigned int size = schemaTmp.GetNumAtts();
				vector<Attribute> vattributes = schemaTmp.GetAtts();
				int col = 0;
				Type type;
				for(int i = 0; i < size; ++i) {
					if(indexAttribute == vattributes[i].name) {
						col = i;
						type = vattributes[i].type;
						break;
					}
				}
				sc->createIndex(tb, col, type);
				cout<<"done"<<endl;
				indexList = NULL;
				yylex_destroy();
				resuse();
				continue;
			}
		}

		{
			//TO LOAD DATA
			if(loadFiles != NULL) {
				DBFile db;
				string tb(tables->tableName);
				db.file.SetFileName(tables->tableName);
				Schema schema;
				catalog.GetSchema(tb, schema);
				string filename(loadFiles->name);
				loadFiles = loadFiles->next;
				if(loadFiles == NULL) continue;
				string extension(loadFiles->name);
				string tmp = filename + "." + extension;
				db.Load(schema, (char*)tmp.c_str());
				loadFiles = NULL;
				yylex_destroy();
				resuse();
				continue;
			}

		}


		// at this point we have the parse tree in the ParseTree data structures
		// we are ready to invoke the query compiler with the given query
		// the result is the execution tree built from the parse tree and optimized
		QueryExecutionTree queryTree;
		compiler.Compile(tables, attsToSelect, finalFunction, predicate,
			groupingAtts, distinctAtts, queryTree);
//		Schema schemaTmp;
//		string tb = "supplier";
//		catalog.GetSchema(tb, schemaTmp);
//		DBFile dbFileTmp;
//		dbFileTmp.SetFileName(tb);
//		Scan * sc = new Scan(schemaTmp, dbFileTmp);
//		sc->createIndex(tb, 0, Integer);
//		sc->searchRange(tb, 1,1);

		queryTree.catalog = &catalog;
		queryTree.ExecuteQuery();
		yylex_destroy();
		resuse();
	}
	while( !cin.fail() && query!="quit");

	int parse = -1;
	if (yyparse () == 0) {
		cout << "OK!" << "\n";
		parse = 0;
	}
	else {
		cout << "Error: Query is not correct!" << endl;
		parse = -1;
	}

	yylex_destroy();

	if (parse != 0) return -1;


//	QueryExecutionTree queryTree;
//	compiler.Compile(tables, attsToSelect, finalFunction, predicate,
//		groupingAtts, distinctAtts, queryTree);
//	Schema schemaTmp;
//	string tb = "supplier";
//	catalog.GetSchema(tb, schemaTmp);
//	DBFile dbFileTmp;
//	dbFileTmp.SetFileName(tb);
//	Scan * sc = new Scan(schemaTmp, dbFileTmp);
//	sc->createIndex(tb, 0, Integer);
//	sc->searchRange(tb, 1,1);
//	queryTree.catalog = &catalog;
//	queryTree.ExecuteQuery();

	return 0;
}


