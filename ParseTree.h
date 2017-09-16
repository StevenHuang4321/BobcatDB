#ifndef _PARSE_TREE_
#define _PARSE_TREE_

// type of operands that can appear in a CNF expression
#define FLOAT 1
#define INTEGER 2
#define NAME 3
#define STRING 4

// type of operators that can appear in a CNF expression
#define LESS_THAN 5
#define GREATER_THAN 6
#define EQUALS 7


// functional expressions
struct FuncOperand {
	// type of the operand
	int code;
	// actual operand
	char* value;
};

struct FuncOperator {
	// type of operator
	int code;
	// operators on the left and on the right
	struct FuncOperator* leftOperator;
	struct FuncOperand* leftOperand;
	struct FuncOperator* right;
};

struct TableList {
	// table name
	char* tableName;
	// next table
	struct TableList* next;
};

struct NameList {
	// name
	char* name;
	// next name in the list
	struct NameList* next;
};



struct LoadDataFile {
	char* file;
	char* table;
};

struct IndexStruct {
	char* index;
	char* table;
	char* attribute;
};


struct Operand {
	// type of the operand
	int code;
	// actual operand
	char* value;
};

struct AttributesList {
	// name
	char* name;
	struct Operand* type;
	// next name in the list
	struct AttributesList* next;
};


struct ComparisonOp {
	// operator type
	int code;
	// operands on the left and on the right
	struct Operand* left;
	struct Operand* right;
};

struct AndList {
	// disjunction to the left of the AND
	struct ComparisonOp* left;
	// AndList to the right of the AND
	struct AndList* rightAnd;
};

#endif
