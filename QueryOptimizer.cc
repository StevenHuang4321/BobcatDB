#include <string>
#include <vector>
#include <iostream>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"
#include <unordered_map>
#include <climits>
#include <algorithm>
#include <iomanip>

using namespace std;


class QNode {
public:
	unsigned long long size = 0L;
	unsigned long long cost = 0L;
	vector<string>* v;
	QNode(unsigned long long size, unsigned long long cost, vector<string>* v) {
		this->size = size;
		this->cost = cost;
		this->v = v;
	}
};

unordered_map<int, QNode*> dp;
unordered_map<string, int> keyMapping;
//multimap<string, string> joinPair;
unordered_map<int, pair<string, string>> tablesMappingAtts;
AndList* parseTreeGlobal;

static void printPostOrder(OptimizationTree* p, int indent=0)
{
    if(p != NULL) {
        if(p->rightChild) {
        	printPostOrder(p->rightChild, indent+4);
        }
        if (indent) {
            cout << setw(indent) << ' ';
        }
        if (p->rightChild) cout<<" /\n" << setw(indent) << ' ';
        cout<<"(";
        for(int i = 0; i < p->tables.size(); ++i) {
        	cout<< p->tables[i] <<" ";
        }
        cout<< ")\n ";
        if(p->leftChild) {
            cout << setw(indent) << ' ' <<" \\\n";
            printPostOrder(p->leftChild, indent+4);
        }
    }
}


static string trim(const string& str)
{
    size_t first = str.find_first_not_of(' ');
    if (string::npos == first)
    {
        return str;
    }
    size_t last = str.find_last_not_of(' ');
    return str.substr(first, (last - first + 1));
}

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

QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
}

QueryOptimizer::~QueryOptimizer() {
}

//int translate(string table, ...) {
//	int ret = 0;
//    va_list args;
//    va_start(args, table);
//    ret = ret | keyMapping[table];
//    va_end(args);
//    return ret;
//}
int translate(vector<string> arg) {
	int ret = 0;
	for(int i = 0; i < arg.size(); ++i)
		ret = ret | keyMapping[arg[i]];
    return ret;
}

int translate(string arg) {
	return keyMapping[arg];
}





//void QueryOptimizer::partition(int n, vector<string> tables) {
//	cout<<"**"<<tables.size()<<endl;
//	for(int i = 0; i < tables.size(); ++i) cout<<tables[i]<<"\t";
//	cout<<endl;
//	if(tables.empty() || tables.size() == 1) return;
//	if(dp[translate(tables)] != NULL) return;
//	long size = 0L;
//	long cost = 0L;
//	//todo large value
//	long minCost = LONG_MAX;
//	vector<string>* order = new vector<string>();
//	do {
//		for(int j = 1; j < n; ++j) {
//			vector<string> left;
//			for(int l = 1; l < j; ++l) {
//				left.push_back(tables[l]);
//			}
//			if(dp[translate(left)] == NULL) partition(left.size(), left);
//
//			vector<string> right;
//			for(int r = j + 1; r < n; ++r) {
//				right.push_back(tables[r]);
//			}
//
//			if(dp[translate(right)] == NULL)  partition(right.size(), right);
//			if(!left.empty() && !right.empty())
//				cost = dp[translate(left)]->cost + dp[translate(right)]->cost;
//			else if(!left.empty()) cost = dp[translate(left)]->cost;
//			else if(!right.empty()) cost = dp[translate(right)]->cost;
//			if(j != 1 && !left.empty()) cost += dp[translate(left)]->size;
//			if(j != n - 1 && !right.empty()) cost += dp[translate(right)]->size;
//			cout<<"cost:"<<cost<<" minicost :"<<minCost<<" ULONG_MAX:"<<ULONG_MAX<<endl;
//			if(cost < minCost) {
//				cout<<"n:"<<n<<" size left:" << left.size()<<" right:"<<right.size()<<endl;
//				long selectity = LONG_MAX;
//				for(int li = 0; li < left.size(); ++li) {
//					for(int ri = 0; ri < right.size(); ++ri) {
//						if(tablesMappingAtts.count(translate(left[li]) | translate(right[ri])) != 0) {
//							auto ii = tablesMappingAtts.find(translate(left[li]) | translate(right[ri]));
//							for( ; ii != tablesMappingAtts.end(); ++ii) {
//								unsigned int dist1 = 0, dist2 = 0;
//								string leftli = left[li];
//								string secondfirst = ii->second.first;
//								catalog->GetNoDistinct(leftli,secondfirst, dist1);
//								string rightri = right[ri];
//								string secondsecond = ii->second.second;
//								catalog->GetNoDistinct(rightri, secondsecond, dist2);
//								selectity = max(dist1, dist2);
//								catalog->GetNoDistinct(rightri, secondfirst, dist1);
//								catalog->GetNoDistinct(leftli, secondsecond, dist2);
//								dist1 = max((int)dist1, (int)dist2);
//								selectity = max((int)selectity, (int)dist1);
//							}
//						}
//					}
//				}
//
//				size = dp[translate(left)]->size * dp[translate(right)]->size / selectity;
//				//todo record each pair of join.
//				order->insert(order->end(), dp[translate(left)]->v->begin(),dp[translate(left)]->v->end());
//				order->insert(order->end(), dp[translate(right)]->v->begin(),dp[translate(right)]->v->end());
//				minCost = cost;
//				//dp[translate(tables)] = new Node(size, minCost, order);
//			}
//		}
//		dp[translate(tables)] = new Node(size, minCost, order);
//		for(int i = 0; i < tables.size(); ++i) cout<<tables[i]<<"\t";
//		cout<<endl;
//	} while (next_permutation(tables.begin(), tables.end()));
//	//dp[translate(tables)] = new Node(size, minCost, order);
//}

void QueryOptimizer::partition(int n, vector<string> tables) {

	if(tables.size() <= 1) return;
	if(dp[translate(tables)] != NULL) return;
	unsigned long long size = 0L;
	unsigned long long cost = 0L;
	//table size == 2 just join.
	if(tables.size() == 2) {
		//todo cache
		long selectity = 1;
		cost = dp[translate(tables[0])]->cost + dp[translate(tables[1])]->cost;
		//cout<<"tablesMappingAtts.count(translate(tables[0]) | translate(tables[1])):"<<tablesMappingAtts.count(translate(tables[0]) | translate(tables[1]))<<endl;
		if(tablesMappingAtts.count(translate(tables[0]) | translate(tables[1])) != 0) {
			auto ii = tablesMappingAtts.find(translate(tables[0]) | translate(tables[1]));
			if( ii != tablesMappingAtts.end()) {
				unsigned int dist1 = 0, dist2 = 0;
				string leftli;// = tables[0];
				string secondfirst = ii->second.first;
				catalog->GetTableByAttribute(leftli, secondfirst);
				//cout<<"leftli:"<<leftli<<" at:"<<secondfirst<<endl;
				catalog->GetNoDistinct(leftli,secondfirst, dist1);
				string rightri;// = tables[1];
				string secondsecond = ii->second.second;
				catalog->GetTableByAttribute(rightri, secondsecond);
				//cout<<"rightri:"<<rightri<<" at:"<<secondsecond<<endl;
				catalog->GetNoDistinct(rightri, secondsecond, dist2);
				dist1 = max(dist1, dist2);
//				catalog->GetNoDistinct(rightri, secondfirst, dist1);
//				catalog->GetNoDistinct(leftli, secondsecond, dist2);
//				dist1 = max((int)dist1, (int)dist2);
				selectity = max((int)selectity, (int)dist1);
				//cout<<"dist1:"<<dist1<<" dist2:"<<dist2<<" selectity: "<<selectity<<endl;
				//cout<<"************************************"<<endl;
			}
		}
		//cout<<"line 218:"<<tables[0]<<" "<<tables[1]<<endl;
		//cout<<"line 219:"<<translate(tables[0])<<" translate : "<<translate(tables[1])<<endl;

		size = dp[translate(tables[0])]->size * dp[translate(tables[1])]->size / selectity;
		//cout<< dp[translate(tables[0])]->size << " "<<dp[translate(tables[1])]->size<<" "<<
			//	selectity<<endl;
		cost += dp[translate(tables[0])]->size;
		cost += dp[translate(tables[1])]->size;
		//cout<<"joins "<<"(" + tables[0] + " join " + tables[1] + ") size:"<< size<<endl;
		//vec2.push_back(tables[1]);
		vector<string> * ret = new vector<string>();
		ret->push_back(tables[0] + " join " + tables[1]);
		dp[translate(tables)] = new QNode(size, cost, ret);
		return;
	}
	long minCost = LONG_MAX;
	do {

		size = 0L;
		cost = 0L;
		vector<string>* order = new vector<string>();
		for(int j = 1; j < n - 1; ++j) {
			//todo large value

			vector<string> left;
			for(int l = 0; l <= j; ++l) {
				left.push_back(tables[l]);
			}
			if(dp[translate(left)] == NULL) partition(left.size(), left);

			vector<string> right;
			for(int r = j + 1; r < n; ++r) {
				right.push_back(tables[r]);
			}

			if(dp[translate(right)] == NULL)  partition(right.size(), right);
			cost = dp[translate(left)]->cost + dp[translate(right)]->cost;
			//if(j != 1 && !left.empty() && dp[translate(left)] != NULL)
			//if(j != 1)
			cost += dp[translate(left)]->size;
			//if(j != n - 1)
			cost += dp[translate(right)]->size;
			long selectity = 1;
			for(int li = 0; li < left.size(); ++li) {
				for(int ri = 0; ri < right.size(); ++ri) {
					if(tablesMappingAtts.count(translate(left[li]) | translate(right[ri])) != 0) {
						auto ii = tablesMappingAtts.find(translate(left[li]) | translate(right[ri]));
						if(ii != tablesMappingAtts.end()) {
							unsigned int dist1 = 0, dist2 = 0;
							string leftli = left[li];
							string secondfirst = ii->second.first;
							catalog->GetNoDistinct(leftli,secondfirst, dist1);
							string rightri = right[ri];
							string secondsecond = ii->second.second;
							catalog->GetNoDistinct(rightri, secondsecond, dist2);
							dist1 = max(dist1, dist2);
							catalog->GetNoDistinct(rightri, secondfirst, dist1);
							catalog->GetNoDistinct(leftli, secondsecond, dist2);
							dist1 = max((int)dist1, (int)dist2);
							selectity = max((int)selectity, (int)dist1);
						}
					}
				}
			}
			//cout<<"size:"<<size<<endl;
			//if(dp[translate(left)] != NULL && dp[translate(right)] != NULL)
			size = dp[translate(left)]->size * dp[translate(right)]->size / selectity;
			//
//			cout << "cost " << cost << " to join: ";
//			for(auto s : left) cout << s << " ";
//			cout << ".... ";
//			for(auto s : right) cout << s << " ";
//			cout << endl;
			if(cost < minCost) {
				//todo record each pair of join.
				string lefts = "";
				for(int i = 0; i < left.size(); ++i) {
					lefts = lefts + " " + left[i];
				}
				string rights = "";
				for(int i = 0; i < right.size(); ++i) {
					rights = rights + " " + right[i];
				}
				lefts = lefts + " join " + rights;

				if(dp[translate(left)] != NULL)
					order->insert(order->end(), dp[translate(left)]->v->begin(), dp[translate(left)]->v->end());
				if(dp[translate(right)] != NULL)
					order->insert(order->end(), dp[translate(right)]->v->begin(), dp[translate(right)]->v->end());
				order->push_back(lefts);
				minCost = cost;
				dp[translate(tables)] = new QNode(size, minCost, order);
			}
			//dp[translate(tables)] = new Node(size, minCost, order);
//			else {
//				if(dp[translate(tables)] == NULL || (dp[translate(tables)] != NULL && cost < dp[translate(tables)]->cost) ) {
//					string lefts = "";
//					order->insert(order->end(), dp[translate(left)]->v->begin(), dp[translate(left)]->v->end());
//					order->insert(order->end(), dp[translate(right)]->v->begin(), dp[translate(right)]->v->end());
//					for(int i = 0; i < left.size(); ++i) {
//						if(lefts == "" ) lefts = left[i];
//						else lefts = lefts + " " + left[i];
//					}
//					string rights = "";
//					for(int i = 0; i < right.size(); ++i) {
//						if(rights == "") rights = right[i];
//						else rights = rights + " " + right[i];
//					}
////					lefts = lefts + " " + rights;
//					lefts = lefts + " join " + rights;
//					order->push_back(lefts);
//					dp[translate(tables)] = new Node(size, cost, order);
//				}
//			}
		}
//		if(translate(tables) == 7) {
//			cout<<"7:"<<size<<" "<<minCost<<" "<<order->size()<<endl;
//		}

//		if(translate(tables) == 7) {
//			if(dp[translate(tables)] == NULL) cout<<"7 is null"<<endl;
//			else cout<<"7 is not null"<<endl;
//		}
//		for(int i = 0; i < tables.size(); ++i) cout<<tables[i]<<"\t";
//		cout<<endl;
	} while (next_permutation(tables.begin(), tables.end()));
}


OptimizationTree * QueryOptimizer::buildTree(string str, OptimizationTree * prev) {
	str = trim(str);
	if(str == "") return NULL;
	string tmp = "";
	vector<string> tables;
	split(str, ' ', tables);
	if(str != "" && tables.size() == 0)
		tables.push_back(str);
	OptimizationTree * rootTmp = new OptimizationTree();
	rootTmp->parent = prev;
	vector<string> tableLeft;
	vector<string> tableRight;
	bool left = true;
	vector<string> total;
//	if(tables.size() == 1) total.push_back(tables[0]);
	for(int i = 0; i < tables.size(); ++i) {
		if(tables[i] == "") continue;
		if(tables[i] == "join") {
			left = false;
			continue;
		}
		if(left) {
			tableLeft.push_back(tables[i]);
		} else {
			tableRight.push_back(tables[i]);
		}
		total.push_back(tables[i]);
	}

	rootTmp->tables = total;

	vector<int> noTuples;
	for(int i = 0; i < tables.size(); ++i) {
		unsigned int tmp = 0;
		string nameTmp;
		nameTmp = tables[i];
		catalog->GetNoTuples(nameTmp, tmp);
		noTuples.push_back(tmp);
	}
	rootTmp->tuples = noTuples;
	rootTmp->noTuples = dp[translate(tables)]->size;
	if(tables.size() == 1) return rootTmp;

	string leftstr;

	//when it is one node.
	if((*dp[translate(tableLeft)]->v).size() == 0 ) {
		rootTmp->leftChild = NULL;
	} else {
		leftstr = (*dp[translate(tableLeft)]->v)[dp[translate(tableLeft)]->v->size() - 1];
		rootTmp->leftChild = buildTree(leftstr, rootTmp);
	}
	string rightstr;
	if((*dp[translate(tableRight)]->v).size() == 0 ) {
		rootTmp->rightChild = NULL;
	} else {
		rightstr = (*dp[translate(tableRight)]->v)[dp[translate(tableRight)]->v->size() - 1];
		rootTmp->rightChild = buildTree(rightstr, rootTmp);
	}
	return rootTmp;
}

RelationalOp * QueryOptimizer::buildTree(string str) {
	str = trim(str);
	if(str == "") return NULL;
	string tmp = "";
	vector<string> tables;
	split(str, ' ', tables);
	if(str != "" && tables.size() == 0)
		tables.push_back(str);
	if(tables.size() == 1) {
		RelationalOp * leftJoin = NULL;
				RelationalOp * rightJoin = NULL;
				//todo use prev pointer to check if use leftJoin or rightJoin.
				auto it = selectsMappingPointer.find(tables[0]);
				if(it != selectsMappingPointer.end()) {
					leftJoin = (RelationalOp*)it->second;
					return (Select*)leftJoin;
				} else {
//					for(auto it = selectsMappingPointer.begin(); it != selectsMappingPointer.end(); ++it) {
//						cout<<it->first<<" "<<endl;
//					}
//					cout<<endl;
					auto it = scansMapping.find(tables[0]);
					if(it != scansMapping.end())
					leftJoin = (RelationalOp*)it->second;
					else cout<<"no found";
					return (Scan*)leftJoin;
				}
	}

	//if(tables.size() == 2) cout<<"line 462"<<endl;
	vector<string> tableLeft;
	vector<string> tableRight;
	bool left = true;
	vector<string> total;
	Schema schemaLeft;
	Schema schemaRight;
	Schema schemaOut;

	for(int i = 0; i < tables.size(); ++i) {
		if(tables[i] == "") continue;
		if(tables[i] == "join") {
			left = false;
			continue;
		}
		Schema schemaTmp;
		if(left) {
			tableLeft.push_back(tables[i]);
			catalog->GetSchema(tables[i], schemaTmp);
			schemaLeft.Append(schemaTmp);
		} else {
			tableRight.push_back(tables[i]);
			catalog->GetSchema(tables[i], schemaTmp);
			schemaRight.Append(schemaTmp);
		}
		total.push_back(tables[i]);
	}



	schemaLeft.SetTablesName(tableLeft);
	schemaRight.SetTablesName(tableRight);

	CNF cnfTmp;

	int retValue = cnfTmp.ExtractCNF(*parseTreeGlobal, schemaLeft, schemaRight);

	if(total.size() == 2) {
		RelationalOp * leftJoin = NULL;
		RelationalOp * rightJoin = NULL;
		//todo use prev pointer to check if use leftJoin or rightJoin.
		auto it = selectsMappingPointer.find(total[0]);
		if(it != selectsMappingPointer.end()) {
			leftJoin = (RelationalOp*)it->second;
//			return (Select*)leftJoin;
		} else {
			auto it = scansMapping.find(total[0]);
			if(it != scansMapping.end())
			leftJoin = (RelationalOp*)it->second;
			else cout<<"no found";
//			return (Scan*)leftJoin;
		}

		it = selectsMappingPointer.find(total[1]);
		if(it != selectsMappingPointer.end()) {
			rightJoin = (RelationalOp*)it->second;
//			return (Select*)rightJoin;
		} else {
//			for(auto it = selectsMappingPointer.begin(); it != selectsMappingPointer.end(); ++it) {
//				cout<<it->first<<" "<<endl;
//			}
			auto it = scansMapping.find(total[1]);
			if(it != scansMapping.end())
			rightJoin = (RelationalOp*)it->second;
			else cout<<"no found";
//			return (Scan*)rightJoin;
		}
		cnfTmp.ExtractCNF(*parseTreeGlobal, schemaLeft, schemaRight);
		return new Join(schemaLeft, schemaRight, schemaOut, cnfTmp, leftJoin, rightJoin);
	}

	RelationalOp * leftJoin = NULL;
	RelationalOp * rightJoin = NULL;
	Join * rootTmp = new Join(schemaLeft, schemaRight, schemaOut, cnfTmp, leftJoin, rightJoin);
	string leftstr;
	//when it is one node.
	if((*dp[translate(tableLeft)]->v).size() == 0 ) {
		leftJoin = NULL;
	} else {
		leftstr = (*dp[translate(tableLeft)]->v)[dp[translate(tableLeft)]->v->size() - 1];
		leftJoin = buildTree(leftstr);
	}
	rootTmp->SetLeft(leftJoin);
	string rightstr;
	if((*dp[translate(tableRight)]->v).size() == 0 ) {
		rightJoin = NULL;
	} else {
		rightstr = (*dp[translate(tableRight)]->v)[dp[translate(tableRight)]->v->size() - 1];
		rightJoin = buildTree(rightstr);
	}
	rootTmp->SetRight(rightJoin);
	rootTmp->tablesName = total;
	Schema retSchema;
	/*rootTmp->schemaOut.Swap(retSchema);
	if(Join * tmpJoin = dynamic_cast<Join *>(leftJoin)) {
		rootTmp->schemaOut.Append(tmpJoin->schemaOut);
	} else if(Select * tmpJoin = dynamic_cast<Select *>(leftJoin)) {
		rootTmp->schemaOut.Append(tmpJoin->schema);
	} else if(Scan * tmpJoin = dynamic_cast<Scan *>(leftJoin)) {
		rootTmp->schemaOut.Append(tmpJoin->schema);
	}
	if(Join * tmpJoin = dynamic_cast<Join *>(rightJoin)) {
		rootTmp->schemaOut.Append(tmpJoin->schemaOut);
	} else if(Select * tmpJoin = dynamic_cast<Select *>(rightJoin)) {
		rootTmp->schemaOut.Append(tmpJoin->schema);
	} else if(Scan * tmpJoin = dynamic_cast<Scan *>(rightJoin)) {
		rootTmp->schemaOut.Append(tmpJoin->schema);
	}
	*/
//	rootTmp->schemaOut.Swap(retSchema);
	return rootTmp;
}


void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate,
	OptimizationTree* _root) {

	parseTreeGlobal = _predicate;
	// compute the optimal join order
	//unordered_map<int, Node*> dp;
	//unordered_map<string, int> keyMapping;

	int count = 0;
		TableList* _tablesCopy = _tables;
		while(_tables != NULL) {
			string tableName(_tables->tableName);
			int key = 1;
			for(int i = 0; i < count; ++i) {
				key = key << 1;
			}
			keyMapping[tableName] = key;
			_tables = _tables->next;
			++count;
		}

	auto it = _predicate;
	while(it != NULL) {
		//if(it->left->code == EQUALS && it->left->left->code == NAME && it->left->right->code == NAME) {
		if(it->left->left->code == NAME && it->left->right->code == NAME) {
//			vector<string> ansLeft;
			//split(it->left->left->value, '.', ansLeft);
			//string tableLeft = ansLeft[0];
			string attLeft(it->left->left->value);//= ansLeft[1];
//			vector<string> ansRight;
			//split(it->left->right->value, '.', ansRight);
//			string tableRight = ansRight[0];
			string attRight(it->left->right->value);// = ansRight[1];
			//joinPair.insert(tableLeft, tableRight);
			//joinPair.insert(tableRight, tableLeft);
			string leftTableName;
			catalog->GetTableByAttribute(leftTableName, attLeft);
			string rightTableName;
			catalog->GetTableByAttribute(rightTableName, attRight);
			tablesMappingAtts[translate(leftTableName) | translate(rightTableName)] =	make_pair(attLeft,attRight);
			//cout<<"translate(leftTableName) | translate(rightTableName):"<<translate(leftTableName)<<" "<<translate(rightTableName)<<endl;
		}
		it = it->rightAnd;
	}
//	if(tablesMappingAtts.empty()) {
////		_root = NULL;
//		return;
//	}

	count = 0;
	_tables = _tablesCopy;
	vector<string> tablesV;
	//initializing dp
	while(_tables != NULL) {

		unsigned int tuples = 0;
		string tableName(_tables->tableName);
//		cout<<"tableName:"<<tableName<<endl;
		catalog->GetNoTuples(tableName, tuples);
		vector<string> *v = new vector<string>();
		//v->push_back(tableName);
		tablesV.push_back(tableName);
		v->push_back(tableName);
		int key = 1;
		for(int i = 0; i < count; ++i) {
			key = key << 1;
		}
		keyMapping[tableName] = key;
		dp[key] = new QNode(tuples, 0, v);
//		cout<<"key:"<<key<<" tuples"<<tuples<<endl;
		_tables = _tables->next;
		++count;
	}


	partition(tablesV.size(), tablesV);
	int key = 0;
	while(_tablesCopy != NULL) {
		key = key | translate(_tablesCopy->tableName);
		_tablesCopy = _tablesCopy->next;
	}

	if(dp[key] == NULL) cout<<"it is null"<<endl;
//	cout<<"size:"<<dp[key]->size<<endl;
//	cout<<"cost:"<<dp[key]->cost<<endl;
//	cout<<"order:"<<dp[key]->v->size()<<endl;;
	vector<string> * tmp =  dp[key]->v;
	vector<string> nodeTables;
	vector<int> bitKey;
	//cout<<"orderResult:";
	for(auto i = tmp->begin(); i != tmp->end(); ++i) {
		//	cout<<(*i)<<"\t";
			orderResult.push_back(*i);
	}
	OptimizationTree* tree = buildTree((*tmp)[(tmp->size() - 1)], NULL);
	if(tablesV.size() == 1)
		joinTree = NULL;
	else joinTree = (Join*)buildTree((*tmp)[(tmp->size() - 1)]);
	_root = tree;
	cout<<"##############print Optimized Tree####################"<<endl;
	printPostOrder(tree);
}



