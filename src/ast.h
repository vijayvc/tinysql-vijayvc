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

#define PRINT_1(indentLevel, arg1) { printf("%*s", indentLevel*2,""); cout << arg1 << endl; }
#define PRINT_2(indentLevel, arg1, arg2) { printf("%*s", indentLevel*2,""); cout << arg1 << arg2 << endl; }

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
	virtual void Print(int indentLevel) = 0;
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
	virtual void Print(int indentLevel) = 0;
};

class Expr : public Node 
{
protected:
public:
	Expr() {}
	virtual void Print(int indentLevel) = 0; 
};

class Constant: public Expr
{
public:
	Constant() { }
	virtual ConstType GetType() = 0;
	virtual void Print(int indentLevel) = 0; 
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
	virtual void Print(int indentLevel) { 
		PRINT_2(indentLevel, "IntConstant: ", val);
	}
	virtual int GetIntValue() { return val; }
};

class StringConstant: public Constant
{
protected:
	string val;
public:
	StringConstant(string v): val(v) {}
	virtual ConstType GetType() { return eString; }
	virtual void Print(int indentLevel) { 
		PRINT_2(indentLevel, "StringConstant: ", val);
	}
	virtual string GetStringValue() { return val; }
};

class NullConstant: public Constant
{
protected:
public:
	NullConstant(){}
	virtual ConstType GetType() { return eNull; }
	virtual void Print(int indentLevel) { 
		PRINT_1(indentLevel, "NullConstant");
	}
};

class Operator: public Node
{
protected:
	char tok;
public:
	Operator(char c):tok(c) {}
	virtual void Print(int indentLevel) { 
		PRINT_2(indentLevel, "Operator: ", tok);
	}
};

class CompoundExpr: public Expr
{
protected:
	Expr* left;
	Expr* right;
	Operator* op;
public:
	CompoundExpr(Expr* l, Expr* r, Operator* o):left(l),right(r),op(o) { }
	virtual void Print(int indentLevel)
	{
		op->Print(indentLevel+1);
		if(left)
			left->Print(indentLevel+1);
		right->Print(indentLevel+1);
	}
};

class ArithmeticExpr: public CompoundExpr
{
protected:
public:
	ArithmeticExpr(Expr* l, Expr* r, Operator* o):CompoundExpr(l,r,o) {}
	virtual void Print(int indentLevel) { 
		PRINT_1(indentLevel, "Arithmetic Expr:");
		CompoundExpr::Print(indentLevel+1);
	}
};

class LogicalExpr: public CompoundExpr
{
protected:
public:
	LogicalExpr(Expr* l, Expr* r, Operator* o):CompoundExpr(l,r,o) {}
	virtual void Print(int indentLevel) { 
		PRINT_1(indentLevel, "Logical Expr:");
		CompoundExpr::Print(indentLevel+1);
	}
};

class RelationalExpr: public CompoundExpr
{
protected:
public:
	RelationalExpr(Expr* l, Expr* r, Operator* o):CompoundExpr(l,r,o) {}
	virtual void Print(int indentLevel) { 
		PRINT_1(indentLevel, "Relational Expr:");
		CompoundExpr::Print(indentLevel+1);
	}
};

class CreateTableStmt: public Statement
{
protected:
	EntityName* table_name;
	List<Attribute*>* attrList;
public:
	CreateTableStmt(EntityName* n, List<Attribute*>* attrs):
		table_name(n), attrList(attrs)
	{
		Assert(attrList);
	}
	virtual void Print(int indentLevel); 
	virtual List<TUPLE*>* Execute();
};

class DropTableStmt: public Statement
{
protected:
	EntityName* table_name;
public:
	DropTableStmt(EntityName* n):table_name(n)
	{}
	virtual void Print(int indentLevel); 
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
	virtual void Print(int indentLevel); 
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
	virtual void Print(int indentLevel); 
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
	virtual void Print(int indentLevel);
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
	virtual void Print(int indentLevel) { 
		PRINT_2(indentLevel, "Name: ", name);
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
	virtual void Print(int indentLevel) { 
		PRINT_1(indentLevel, "ColumnName: ");
		indentLevel += 1;
		if(table)
		{
			PRINT_1(indentLevel, "TableName: ");
			table->Print(indentLevel+1);
		}
		PRINT_1(indentLevel, "FieldName: ");
		column->Print(indentLevel+1);
	}
};

class Type: public Node
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
	virtual void Print(int indentLevel) { 
		PRINT_2(indentLevel, "Type: ", typeName);
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
	virtual void Print(int indentLevel) { 
		PRINT_1(indentLevel, "Values:");
		if(valueList)
		{
			for(int i=0; i<valueList->NumElements(); i++)
				valueList->Nth(i)->Print(indentLevel+1);
		}
		else
		{
			select_stmt->Print(indentLevel+1);
		}
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
	virtual void Print(int indentLevel) { 
		PRINT_1(indentLevel, "Attribute:");
		name->Print(indentLevel+1);
		type->Print(indentLevel+1);
	}
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
	virtual void Print(int indentLevel) { 
			PRINT_1(indentLevel, "ColumnAccess:");
			column->Print(indentLevel+1);
	}
};
