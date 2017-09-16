#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"
#include <unordered_map>
#include <algorithm>

using namespace std;
unordered_map<string, Scan*> scansMapping;
unordered_map<string, Select*> selectsMapping;
static void split(const string& s, char delim,vector<string>& v) {
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

QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {
	bool noJoin = false;

	// create a SCAN operator for each table in the query

	TableList* tableListCopy = _tables;
	while(_tables != NULL) {
		string tableName(_tables->tableName);
		Schema schemaTmp;
		catalog->GetSchema(tableName, schemaTmp);
		string dbPath = "";
		catalog->GetDataFile(tableName, dbPath);
		DBFile dbfile;
//		string prefix = "/home/xin/postgreSQL-tpch/tpch_2_17_0/dbgen/";
//		prefix += tableName;
		dbfile.SetFileName(tableName);
		Scan* scanTmp = new Scan(schemaTmp, dbfile);
		vector<string> tablesName;
		tablesName.push_back(tableName);
		scanTmp->tablesName = tablesName;
		scansMapping[tableName] = scanTmp;
		_tables = _tables->next;
	}
	// push-down selections: create a SELECT operator wherever necessary

	auto it = _predicate;
	while(it != NULL) {
		string tmp(it->left->left->value);
		string tmp1(it->left->right->value);

		//todo !=equal
		if(!(it->left->left->code == NAME && it->left->right->code == NAME)) {

			string attLeft(it->left->left->value);//= ansLeft[1];
			//string attRight(it->left->right->value);// = ansRight[1];
			string leftTableName;

			catalog->GetTableByAttribute(leftTableName, attLeft);
			//string rightTableName;
			//catalog->GetTableByAttribute(rightTableName, attRight);

			unsigned int tuplesTmp = 0;
			catalog->GetNoTuples(leftTableName, tuplesTmp);
			unsigned int distTuplesTmp = 1;
			catalog->GetNoDistinct(leftTableName, attLeft, distTuplesTmp);
			if(it->left->code == EQUALS) {
				if(distTuplesTmp == 0) distTuplesTmp = 1;
				tuplesTmp = tuplesTmp/distTuplesTmp;
			} else {
				tuplesTmp = tuplesTmp/3 + 1;
			}
			catalog->SetNoTuples(leftTableName, tuplesTmp);
			catalog->Save();
			//exit(0);
//			distTuplesTmp = distTuplesTmp/3 + 1;
//			catalog->SetNoDistinct(leftTableName, attLeft, distTuplesTmp);
//			tuplesTmp = 0;
//			catalog->GetNoTuples(rightTableName, tuplesTmp);
//			tuplesTmp = tuplesTmp/3 + 1;
//			catalog->SetNoTuples(rightTableName, tuplesTmp);
//			distTuplesTmp = 0;
//			catalog->GetNoDistinct(rightTableName, attRight, distTuplesTmp);
//			distTuplesTmp = distTuplesTmp/3 + 1;
//			catalog->SetNoDistinct(rightTableName, attRight, distTuplesTmp);
			Schema schemaTmp;
			string tableName = "";
			if(it->left->left->code == NAME) {
				tableName = leftTableName;
				string tableNameTmp(it->left->left->value);
				catalog->GetSchema(tableName, schemaTmp);
			}

//			else if(it->left->right->code == NAME) {
////				tableName = rightTableName;
//				string tableNameTmp(it->left->right->value);
//				catalog->GetSchema(tableNameTmp, schemaTmp);
//			}

//			Schema schemaTmp;
//			string tableName = "";
//			if(it->left->left->code == NAME) {
//				string attrNameTmp(it->left->left->value);
//				catalog->GetTableByAttribute(tableName, attrNameTmp);
//				catalog->GetSchema(tableName, schemaTmp);
//			} else if(it->left->right->code == NAME) {
//				string attrNameTmp(it->left->right->value);
//				catalog->GetTableByAttribute(tableName, attrNameTmp);
//				catalog->GetSchema(tableName, schemaTmp);
//			}
			//todo fill the parameters
			CNF cnfTmp;
			Record recordTmp;
			RelationalOp *rTmp;
			auto it = scansMapping.find(tableName);
			if(it != scansMapping.end())
				rTmp = (RelationalOp*)it->second;
			_tables = tableListCopy;
			AndList * _predicateCopy = _predicate;
			//cout<<"GetNumAtts:" << schemaTmp.GetNumAtts()<<endl;
			cnfTmp.ExtractCNF(*_predicateCopy, schemaTmp, recordTmp);
			Select*  selectTmp = new Select(schemaTmp, cnfTmp, recordTmp, rTmp);
			vector<string> tablesName;
			tablesName.push_back(tableName);
			selectTmp->andList = _predicateCopy;
			selectTmp->table = tableName;
			selectTmp->tablesName = tablesName;
//			selectTmp->predicate = _predicate;
			selectsMapping[tableName] = selectTmp;
		}
//			else if((it->left->left->code == NAME && it->left->right->code == NAME)) {
//
//			string attLeft(it->left->left->value);//= ansLeft[1];
//			//string attRight(it->left->right->value);// = ansRight[1];
//			string leftTableName;
//
//			catalog->GetTableByAttribute(leftTableName, attLeft);
//
//			string attrRight(it->left->right->value);//= ansLeft[1];
//			string rightTableName;
//			catalog->GetTableByAttribute(rightTableName, attrRight);
//
//			unsigned int tuplesTmp = 0;
//			catalog->GetNoTuples(leftTableName, tuplesTmp);
//			unsigned int distTuplesTmp = 1;
//			catalog->GetNoDistinct(leftTableName, attLeft, distTuplesTmp);
//			if(it->left->code == EQUALS) {
//				tuplesTmp = tuplesTmp/distTuplesTmp;
//			} else {
//				tuplesTmp = tuplesTmp/3 + 1;
//			}
//			catalog->SetNoTuples(leftTableName, tuplesTmp);
//			catalog->Save();
//			//exit(0);
////			distTuplesTmp = distTuplesTmp/3 + 1;
////			catalog->SetNoDistinct(leftTableName, attLeft, distTuplesTmp);
////			tuplesTmp = 0;
////			catalog->GetNoTuples(rightTableName, tuplesTmp);
////			tuplesTmp = tuplesTmp/3 + 1;
////			catalog->SetNoTuples(rightTableName, tuplesTmp);
////			distTuplesTmp = 0;
////			catalog->GetNoDistinct(rightTableName, attRight, distTuplesTmp);
////			distTuplesTmp = distTuplesTmp/3 + 1;
////			catalog->SetNoDistinct(rightTableName, attRight, distTuplesTmp);
//			Schema schemaTmpLeft;
//			string tableName = "";
//			if(it->left->left->code == NAME) {
//				tableName = leftTableName;
//				string tableNameTmp(it->left->left->value);
//				catalog->GetSchema(tableName, schemaTmpLeft);
//			}
//			Schema schemaTmpRight;
//			string tableNameRight = "";
//			if(it->left->left->code == NAME) {
//				tableNameRight = rightTableName;
//				string tableNameTmp(it->left->left->value);
//				catalog->GetSchema(tableNameRight, schemaTmpRight);
//			}
//			//schemaTmp.Append(schemaTmpRight);
//
////			else if(it->left->right->code == NAME) {
//////				tableName = rightTableName;
////				string tableNameTmp(it->left->right->value);
////				catalog->GetSchema(tableNameTmp, schemaTmp);
////			}
//
////			Schema schemaTmp;
////			string tableName = "";
////			if(it->left->left->code == NAME) {
////				string attrNameTmp(it->left->left->value);
////				catalog->GetTableByAttribute(tableName, attrNameTmp);
////				catalog->GetSchema(tableName, schemaTmp);
////			} else if(it->left->right->code == NAME) {
////				string attrNameTmp(it->left->right->value);
////				catalog->GetTableByAttribute(tableName, attrNameTmp);
////				catalog->GetSchema(tableName, schemaTmp);
////			}
//			//todo fill the parameters
//			CNF cnfTmp;
//			Record recordTmp;
//			RelationalOp *rTmp;
//			auto it = scansMapping.find(tableName);
//			if(it != scansMapping.end())
//				rTmp = (RelationalOp*)it->second;
//			_tables = tableListCopy;
//			AndList * _predicateCopy = _predicate;
//			//cout<<"GetNumAtts:" << schemaTmp.GetNumAtts()<<endl;
//			Schema schemaTmp;
//			schemaTmp.Append(schemaTmpLeft);
//			schemaTmp.Append(schemaTmpRight);
////			cnfTmp.ExtractCNF(*_predicateCopy, schemaTmp, recordTmp);
//			cnfTmp.ExtractCNF(*_predicateCopy, schemaTmpLeft, schemaTmpRight);//in join predicate should be put this in the selection rather than join opertator?
//			Select*  selectTmp = new Select(schemaTmp, cnfTmp, recordTmp, rTmp);
//			vector<string> tablesName;
//			tablesName.push_back(tableName);
//			selectTmp->tablesName = tablesName;
////			selectTmp->predicate_predicate;
//			selectsMapping[tableName] = selectTmp;
//		} else {
//			string attLeft(it->left->left->value);//= ansLeft[1];
//			//string attRight(it->left->right->value);// = ansRight[1];
//			string leftTableName;
//
//			catalog->GetTableByAttribute(leftTableName, attLeft);
//			//string rightTableName;
//			//catalog->GetTableByAttribute(rightTableName, attRight);
//
//			unsigned int tuplesTmp = 0;
//			catalog->GetNoTuples(leftTableName, tuplesTmp);
//			unsigned int distTuplesTmp = 0;
//			catalog->GetNoDistinct(leftTableName, attLeft, distTuplesTmp);
//			if(it->left->code == EQUALS) {
//				tuplesTmp = tuplesTmp/distTuplesTmp;
//			} else {
//				tuplesTmp = tuplesTmp/3 + 1;
//			}
//			catalog->SetNoTuples(leftTableName, tuplesTmp);
//			catalog->Save();
//			//exit(0);
////			distTuplesTmp = distTuplesTmp/3 + 1;
////			catalog->SetNoDistinct(leftTableName, attLeft, distTuplesTmp);
////			tuplesTmp = 0;
////			catalog->GetNoTuples(rightTableName, tuplesTmp);
////			tuplesTmp = tuplesTmp/3 + 1;
////			catalog->SetNoTuples(rightTableName, tuplesTmp);
////			distTuplesTmp = 0;
////			catalog->GetNoDistinct(rightTableName, attRight, distTuplesTmp);
////			distTuplesTmp = distTuplesTmp/3 + 1;
////			catalog->SetNoDistinct(rightTableName, attRight, distTuplesTmp);
//			Schema schemaTmp;
//			string tableName = "";
//			if(it->left->left->code == NAME) {
//				tableName = leftTableName;
//				string tableNameTmp(it->left->left->value);
//				catalog->GetSchema(tableName, schemaTmp);
//			}
//
////			else if(it->left->right->code == NAME) {
//////				tableName = rightTableName;
////				string tableNameTmp(it->left->right->value);
////				catalog->GetSchema(tableNameTmp, schemaTmp);
////			}
//
////			Schema schemaTmp;
////			string tableName = "";
////			if(it->left->left->code == NAME) {
////				string attrNameTmp(it->left->left->value);
////				catalog->GetTableByAttribute(tableName, attrNameTmp);
////				catalog->GetSchema(tableName, schemaTmp);
////			} else if(it->left->right->code == NAME) {
////				string attrNameTmp(it->left->right->value);
////				catalog->GetTableByAttribute(tableName, attrNameTmp);
////				catalog->GetSchema(tableName, schemaTmp);
////			}
//			//todo fill the parameters
//			CNF cnfTmp;
//			Record recordTmp;
//			RelationalOp *rTmp;
//			auto it = scansMapping.find(tableName);
//			if(it != scansMapping.end())
//				rTmp = (RelationalOp*)it->second;
//			_tables = tableListCopy;
//			AndList * _predicateCopy = _predicate;
//			//cout<<"GetNumAtts:" << schemaTmp.GetNumAtts()<<endl;
//			cnfTmp.ExtractCNF(*_predicateCopy, schemaTmp, recordTmp);
//			Select*  selectTmp = new Select(schemaTmp, cnfTmp, recordTmp, rTmp);
//			vector<string> tablesName;
//			tablesName.push_back(tableName);
//			selectTmp->tablesName = tablesName;
////			selectTmp->predicate = _predicate;
//			selectsMapping[tableName] = selectTmp;
//		}
		it = it->rightAnd;
	}
	Schema lastSchema;
	// call the optimizer to compute the join order
	OptimizationTree* root;
	_tables = tableListCopy;

	optimizer->selectsMappingPointer = selectsMapping;
	optimizer->scansMapping = scansMapping;

	optimizer->Optimize(_tables, _predicate, root);

	// create join operators based on the optimal order computed by the optimizer
	Join * joinTmp = optimizer->joinTree;
	RelationalOp * last = joinTmp;
	if(joinTmp != NULL) {
		joinTmp->rootOfOptimizedTree = joinTmp;
		_queryTree.SetRoot(*joinTmp);
		lastSchema = joinTmp->schemaOut;
	} else {
		if(selectsMapping.size() >= 1) {
			last = selectsMapping.begin()->second;
			_queryTree.SetRoot(*last);
			lastSchema.Swap(selectsMapping.begin()->second->schema);
		}
		else if(scansMapping.size() >= 1) {
			lastSchema.Swap(scansMapping.begin()->second->schema);
			last = scansMapping.begin()->second;
			_queryTree.SetRoot(*last);
		}
	}

	// create the remaining operators based on the query
	//todo lastSchema

	bool didSum = false;
	Schema sumOut;
	Sum * sum;
	if((_finalFunction != NULL) && (_groupingAtts == NULL)) {
		Function * function = new Function();
		function->GrowFromParseTree(_finalFunction, lastSchema);
		sum = new Sum(lastSchema, sumOut, *function, _finalFunction, last);
		sum->rootOfOptimizedTree = joinTmp;
		didSum = true;
		lastSchema.Swap(sumOut);
	}
	// connect everything in the query execution tree and return

	RelationalOp * producerLast;
	if((_finalFunction != NULL) && (_groupingAtts == NULL)) {
		producerLast = sum;
		_queryTree.SetRoot(*sum);
		didSum = true;
	} else {
		producerLast = last;
		_queryTree.SetRoot(*last);
	}

	while(_groupingAtts != NULL) {//todo only support one group by attribute
				Schema schemaTmpOut;
				Schema schemaTmp;
				schemaTmp.Swap(lastSchema);
				//schemaTmp.Swap(projectRet);
				string tableNameTmp(_groupingAtts->name);
		//		catalog->GetSchema(tableNameTmp, schemaTmp);
				vector<int> gattrs;
				for(int i = 0; i < schemaTmp.GetNumAtts(); ++i) {
					string gn(_groupingAtts->name);
					if(gn == schemaTmp.GetAtts()[i].name) {
						gattrs.push_back(i);
					}
				}
				int * keep = (int*)malloc(gattrs.size() * sizeof(int));
				for(int i = 0; i < gattrs.size(); ++i) {
					keep[i] = gattrs[i];
				}
				OrderMaker orderMaker(schemaTmp, keep, gattrs.size());
				Function * function = new Function();
				function->GrowFromParseTree(_finalFunction, schemaTmp);
				GroupBy * groupBy = new GroupBy(schemaTmp, schemaTmpOut, orderMaker, *function, _finalFunction, producerLast);
				groupBy->rootOfOptimizedTree = joinTmp;
				vector<string> tables;
				tables.push_back(tableNameTmp);
				groupBy->tablesName = tables;
				last = groupBy;
				producerLast = groupBy;
				_groupingAtts  = _groupingAtts->next;
				lastSchema.Swap(schemaTmpOut);
				if(_finalFunction != NULL) {
					didSum = true;
				}
		}
			_queryTree.SetRoot(*last);

	vector<Attribute> attrsName = lastSchema.GetAtts();
	//cout<<"attrsName:"<<attrsName.size()<<endl;
	vector<int> toKeep;
	if(didSum == true) {
		toKeep.push_back(0);
	}
	int size = attrsName.size();
	while(_attsToSelect != NULL) {
		for(int i = 0; i < size; ++i) {
			string attrName(_attsToSelect->name);
//			cout<<"line 216:"<<attrsName[i].name<<" "<<attrName<<endl;
			if(attrsName[i].name == attrName) {
				toKeep.push_back(i);
			}
		}
		_attsToSelect = _attsToSelect->next;
	}
	sort(toKeep.begin(), toKeep.end());
	int * keep = (int*)malloc(toKeep.size() * sizeof(int));
	for(int i = 0; i < toKeep.size(); ++i) {
		keep[i] = toKeep[i];
	}
	Schema projectRet;
	Project * project = new Project(lastSchema, projectRet, lastSchema.GetNumAtts(), toKeep.size(),
			keep, producerLast);
	string outFile = "";
	project->rootOfOptimizedTree = joinTmp;
	project->vect = toKeep;
	project->GetSchemaOut(projectRet);

	producerLast = project;
	if(_distinctAtts == 1) {
	DuplicateRemoval *distinct = new DuplicateRemoval(projectRet, project);
	distinct->rootOfOptimizedTree = joinTmp;
	// free the memory occupied by the parse tree since it is not necessary anymore
	_queryTree.SetRoot(*distinct);
	producerLast = distinct;
	}



	WriteOut * writeOut = new WriteOut(projectRet, outFile , producerLast);
	writeOut->rootOfOptimizedTree = joinTmp;
	_queryTree.schema = projectRet;

	_queryTree.SetRoot(*writeOut);
	_queryTree.vect = toKeep;
}


