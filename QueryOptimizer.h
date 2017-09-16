#ifndef _QUERY_OPTIMIZER_H
#define _QUERY_OPTIMIZER_H

#include "Schema.h"
#include "Catalog.h"
#include "ParseTree.h"
#include "RelOp.h"

#include <string>
#include <vector>
#include <unordered_map>

using namespace std;


// data structure used by the optimizer to compute join ordering
struct OptimizationTree {
	// list of tables joined up to this node
	vector<string> tables;
	// number of tuples in each of the tables (after selection predicates)
	vector<int> tuples;
	// number of tuples at this node
	int noTuples;

	// connections to children and parent
	OptimizationTree* parent;
	OptimizationTree* leftChild;
	OptimizationTree* rightChild;
};

class QueryOptimizer {
private:
	Catalog* catalog;

public:
	vector<string> orderResult;

	QueryOptimizer(Catalog& _catalog);
	virtual ~QueryOptimizer();

	void Optimize(TableList* _tables, AndList* _predicate, OptimizationTree* _root);

	void partition(int n, vector<string> tables);

	OptimizationTree* buildTree(string str, OptimizationTree* prev);

	RelationalOp * buildTree(string str);

	Join * joinTree;

	unordered_map<string, Select*> selectsMappingPointer;
	unordered_map<string, Scan*> scansMapping;

};

#endif // _QUERY_OPTIMIZER_H
