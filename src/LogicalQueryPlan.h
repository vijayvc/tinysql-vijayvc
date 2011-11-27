#include<iostream>
#include "list.h"
#include "ast.h"

using namespace std;
class Expr;
class Constant;

class Operation
{
protected:
	string OpType;
	Operation(string type):OpType(type) {
	}
public:
	virtual string getOperationType() { return OpType; }
	virtual List<TUPLE*>* Execute()
	{
		return NULL;
	}
};

class JoinOperation:public Operation
{
protected:
	List<EntityName*>* 	table_names;
	Expr* expr;
public:
	JoinOperation(List<EntityName*>* tables, Expr* e):Operation("Join")
	{
		table_names = tables;
		expr = e;
	}
	virtual List<TUPLE*>* Execute()
	{
		Assert(0);
		return NULL;
	}
};

class SelectionOperation:public Operation
{
protected:
	Expr* condition;
	string table_name;
	//Operation* child;
public:
	SelectionOperation(string tn, Expr *cond):Operation("Selection")
	{
		table_name = tn;
		condition = cond;
	}
	virtual List<TUPLE*>* Execute();
};

class DistinctOperation:public Operation
{
protected:
	Operation* child;
public:
	DistinctOperation(Operation * o):Operation("Distinct")
	{
		child = o;
	}
	virtual List<TUPLE*>* Execute();
};

class ProjectionOperation :public Operation
{
protected:
	List<ColumnName*>* projection_list;
	Operation* child;
public:
	ProjectionOperation( List<ColumnName*>* pl,Operation * o)
		:Operation("Project")
	{
		child = o;
		projection_list = pl;
	}
	virtual List<TUPLE*>* Execute();
};

class OrderByOperation : public Operation
{
protected:
	ColumnName * order;
	Operation* child;
public:
	OrderByOperation(ColumnName* OrderBy, Operation *o)
		:Operation("OrderBy")
	{
		order = OrderBy;
		child = o;
	}
	virtual List<TUPLE*>* Execute();
};

/*
class ScanOperation :public Operation
{
protected:
	string table_name;
	Expr* condition;
public:
	ScanOperation(const string tn):Operation("Scan")
	{
		table_name = tn; 
	}
};
*/

class LogicalQueryPlan
{
	Operation* root;
	bool isOptimized;
public:
	LogicalQueryPlan(
		List<EntityName*>* table_names,
		List<ColumnName*>* columns,
		Expr* condition,
		bool distinct,
		ColumnName* orderBy);

	//Operation* Optimize();
	void Optimize();
	List<TUPLE*>* Execute();
};
