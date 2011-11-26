#include <iostream>
#include "list.h"
#include <string.h>
using namespace std;

class Constant;
class EntityName;
class ColumnName;
class Attribute;
class InsertValues;
class SelectStmt;


enum ConstType
{
	eInt,
	eString,
	eNull
};


class Node
{
protected:
public:
		Node() { }
		virtual void Print()
		{
		}
};

#define TUPLE List<Constant*>
class Statement: public Node
{
protected:
public:
	Statement() { }
	virtual List<TUPLE*>* Execute() { 
		return NULL;
	}
	virtual void Print() { }
};

class Expr : public Node 
{
	protected:
	public:
		Expr() {}
};

class Constant: public Expr
{
public:
	Constant() { }
	virtual ConstType GetType() = 0;
	virtual string GetStringValue() { return NULL; }
	virtual int GetIntValue() { return 0; }
};

class IntConstant: public Constant
{
protected:
	int val;
public:
	IntConstant(int v): val(v) {}
	virtual ConstType GetType() { return eInt; }
	virtual int GetIntValue() { return val; }
};

class StringConstant: public Constant
{
protected:
	string val;
public:
	StringConstant(string v): val(v) {}
	virtual ConstType GetType() { return eString; }
	virtual string GetStringValue() { return val; }
};

class NullConstant: public Constant
{
protected:
public:
	NullConstant(){}
	virtual ConstType GetType() { return eNull; }
};

class Operator: public Node
{
protected:
	char tok;
public:
	Operator(char c):tok(c) {}
};

class CompoundExpr: public Expr
{
protected:
	Expr* left;
	Expr* right;
	Operator* op;
public:
	CompoundExpr(Expr* l, Expr* r, Operator* o):left(l),right(r),op(o) { }
};

class ArithmeticExpr: public CompoundExpr
{
protected:
public:
	ArithmeticExpr(Expr* l, Expr* r, Operator* o):CompoundExpr(l,r,o) {}
};

class LogicalExpr: public CompoundExpr
{
protected:
public:
	LogicalExpr(Expr* l, Expr* r, Operator* o):CompoundExpr(l,r,o) {}
};

class RelationalExpr: public CompoundExpr
{
protected:
public:
	RelationalExpr(Expr* l, Expr* r, Operator* o):CompoundExpr(l,r,o) {}
};

class CreateTableStmt: public Statement
{
protected:
	EntityName* table_name;
	List<Attribute*>* attrList;
public:
	CreateTableStmt(EntityName* n, List<Attribute*>* attrs):
		table_name(n), attrList(attrs)
	{}
	virtual List<TUPLE*>* Execute();
};

class DropTableStmt: public Statement
{
protected:
	EntityName* table_name;
public:
	DropTableStmt(EntityName* n):table_name(n)
	{}
	virtual List<TUPLE*>* Execute();
};

class SelectStmt: public Statement
{
protected:
	List<EntityName*>* 	table_names;
	List<ColumnName*>* 	columns;
	bool 				distinct;
	Expr*				condition;
	ColumnName*			orderBy;	
public:
	SelectStmt(List<EntityName*>* tables, List<ColumnName*>* c,
				bool isDistinct, Expr* where, ColumnName* o):
		table_names(tables), columns(c), distinct(isDistinct),
		condition(where), orderBy(o)
	{}
};

class InsertStmt: public Statement
{
protected:
	EntityName* table_name;
	List<EntityName*>* columns;
	InsertValues* values;
public:
	List<TUPLE *> * Execute();
	InsertStmt(EntityName* n, List<EntityName*>* c, 
				InsertValues* v):
		table_name(n),columns(c), values(v)
	{}
};

class DeleteStmt: public Statement
{
protected:
	EntityName* table_name;
	Expr*		condition;
public:
	DeleteStmt(EntityName* n, Expr* c):
		table_name(n), condition(c)
	{
	}
};

class EntityName: public Node
{
protected:
	char* name;
public:
	EntityName(const char* n) { 
		name = strdup(n);
	}
	const char* GetName() { 
		return name;
	}
};

class ColumnName: public Node
{
protected:
	EntityName* table;
	EntityName* column;
public:
	ColumnName(EntityName* t_name, EntityName* f_name)
		:table(t_name),column(f_name) { }
};

class Type
{
protected:
	char* typeName;
public:
	Type(const char* t)
	{
		typeName = strdup(t);
	}
	const char* GetName() { 
		return typeName;
	}
};

class InsertValues
{
protected:
	TUPLE* 	valueList;
	SelectStmt*		select_stmt;
public:
	InsertValues(TUPLE * v)
	{
		valueList = v;
		select_stmt = NULL;
	}
	InsertValues(Statement* s)
	{
		valueList = NULL;
		select_stmt = dynamic_cast<SelectStmt*>(s);
		Assert(select_stmt);
	}
	List<TUPLE*>* GetValueList()
	{
		if(valueList)
		{
			List<TUPLE*>* vl = new List<TUPLE*>();
			vl->Append(valueList);
			return vl;
		}
		else if(select_stmt)
		{
			return select_stmt->Execute();
		}
		else
			Assert(0);
		return NULL;
	}
};

class Attribute: public Node
{
protected:	
	EntityName* name;
	Type*		type;
public:
	Attribute(EntityName* e, Type* t):name(e),type(t) { }
	const char* GetFieldName() {
		return name->GetName();
	}
	const char* GetTypeName() {
		return type->GetName();
	}
};

class ColumnAccess: public Expr
{
protected:
	ColumnName* column;
public:
	ColumnAccess(ColumnName* n):column(n) { }
};
