#include "ast.h"
#include "StorageWrapper.h"
#include "LogicalQueryPlan.h"

void CreateTableStmt::Print(int indentLevel)
{
	PRINT_1(indentLevel, "CreateTableStmt:");
	indentLevel += 1;

	PRINT_1(indentLevel, "TableName: ");
	table_name->Print(indentLevel+1);

	PRINT_1(indentLevel, "Attributes: ");
	
	for (int i = 0; i < attrList->NumElements(); i++) 
		attrList->Nth(i)->Print(indentLevel+1);
}

void DropTableStmt::Print(int indentLevel)
{ 
	PRINT_1(indentLevel, "DropTableStmt:");
	indentLevel += 1;

	PRINT_1(indentLevel, "TableName: ");
	table_name->Print(indentLevel+1);
}

void SelectStmt::Print(int indentLevel) 
{ 
	PRINT_1(indentLevel, "SelectStmt:");
	indentLevel += 1;

	PRINT_1(indentLevel, "TableNames:");
	for(int i=0; i<table_names->NumElements(); i++)
		table_names->Nth(i)->Print(indentLevel+1);

	PRINT_1(indentLevel, "Columns:");
	for(int i=0; i<columns->NumElements(); i++)
		columns->Nth(i)->Print(indentLevel+1);

	PRINT_2(indentLevel, "Distinct: ", distinct);

	if(condition)
	{
		PRINT_1(indentLevel, "Condition:");
		condition->Print(indentLevel+1);
	}

	if(orderBy)
	{
		PRINT_1(indentLevel, "OrderBy:");
		orderBy->Print(indentLevel+1);
	}
}

void DeleteStmt::Print(int indentLevel) 
{ 
	PRINT_1(indentLevel, "DeleteStmt:");
	indentLevel += 1;

	PRINT_1(indentLevel, "TableName:");
	table_name->Print(indentLevel+1);

	if(condition)
	{
		PRINT_1(indentLevel, "Condition:");
		condition->Print(indentLevel+1);
	}
}

void InsertStmt::Print(int indentLevel)
 { 
	PRINT_1(indentLevel, "InsertStmt:");
	PRINT_1(indentLevel+1, "TableName:");
	table_name->Print(indentLevel+2);

	PRINT_1(indentLevel+1, "Columns:");
	for(int i=0; i<columns->NumElements(); i++)
		columns->Nth(i)->Print(indentLevel+2);

	values->Print(indentLevel+1);
}

List<TUPLE*>* CreateTableStmt::Execute()
{ 
	cout << "CreateTable Stmt: BEGIN\n";
	vector<enum FIELD_TYPE> field_types;
	vector<string> field_names;
	for (int i = 0; i < attrList->NumElements(); i++)
	{
		field_names.push_back(attrList->Nth(i)->GetFieldName());
		if(!strcmp("string",attrList->Nth(i)->GetTypeName()))
			field_types.push_back(STR20);
		else
			field_types.push_back(INT);
	}
	StorageManagerWrapper::CreateTable(table_name->GetName(), field_names, field_types);
	cout << "\n======END=====\n";
	return NULL;
}

List<TUPLE*>* DeleteStmt::Execute() 
{ 
	cout << "Delete Stmt: BEGIN\n";
	StorageManagerWrapper::ExecuteDeleteTuples(table_name->GetName(), condition);
	//cout << "\nEND\n";
	cout << "\n======END=====\n";

	return NULL;
}

List<TUPLE*>* DropTableStmt::Execute()
{
	cout << "Droptable Stmt: BEGIN\n";
	StorageManagerWrapper::DropTable(table_name->GetName());
	cout << "\n======END=====\n";
	return NULL;
}

List<TUPLE *> * InsertStmt::Execute()
{
	cout << "Insert Stmt: BEGIN\n";
	List<string> column_names;
	List<Constant*> column_values;

	for (int i = 0; i < columns->NumElements(); i++)
		 column_names.Append(columns->Nth(i)->GetName());

	StorageManagerWrapper::InsertTuple(table_name->GetName(),&column_names,values->GetValueList());
	cout << "\n======END=====\n";
	return NULL;
}

List<TUPLE*>* SelectStmt::Execute()
{
	/*
	if (table_names->NumElements() == 1)
	{
		List<string> fields;
		for(int i=0; i < columns->NumElements(); i++)
			fields.Append(columns->Nth(i)->GetName());

		//return StorageManagerWrapper::SimpleSelect(table_names, fields, condition, distinct); 
	}
	*/
	cout << "Select Stmt: BEGIN\n";
	if(table_names->NumElements()== 1)
	{
		return StorageManagerWrapper::ExecuteSingleTableSelect(table_names->Nth(0)->GetName(), columns, condition, distinct, orderBy);
	}
	else
	{
		//LogicalQueryPlan* lqp = new LogicalQueryPlan(table_names, columns, condition, distinct, orderBy);
		//lqp->Optimize();
		//Operation * lqp = Create_LogicalQueryPlan();
		//Optimized_lqp = QueryOptimizer(lqp);
		//ExecuteOptimizedPlan();

		// Optimizations done directly.
		return StorageManagerWrapper::ExecuteMultipleTableSelect(table_names, columns, condition, distinct, orderBy);
    	//StorageManagerWrapper::ExecuteSelect(lqp); 
	}
	cout << "\n======END=====\n";
	return NULL;
}

Constant * ArithmeticExpr::Evaluate(StorageManagerWrapper* sw)
{
	Constant *lvalue = left->Evaluate(sw);
	Constant *rvalue = right->Evaluate(sw);
	int op1 = lvalue->GetIntValue();
	int op2 = rvalue->GetIntValue();
	switch(op->GetOperator())
	{
		case '+':
			return (new IntConstant(op1 + op2));
			break;
		case '-':
			return (new IntConstant(op1 - op2));
			break;
		case '/':
			return (new IntConstant(op1 / op2));
			break;
		case '*':
			return (new IntConstant(op1 * op2));
			break;
	}
	return NULL;
}

Constant* LogicalExpr::Evaluate(StorageManagerWrapper* sw)
{
	Constant *lvalue = NULL;
	if (left)
		lvalue = left->Evaluate(sw);
	Constant *rvalue = right->Evaluate(sw);
	bool op1 = false;
	if (lvalue)
		op1 = lvalue->GetBoolValue();
	bool op2 = rvalue->GetBoolValue();
	switch(op->GetOperator())
	{
		case '&':
			return (new BoolConstant(op1 && op2));
			break;
		case '|':
			return (new BoolConstant(op1 || op2));
			break;
		case '!':
			return (new BoolConstant(! op2));
		default:
			Assert(0);
	}
	return NULL;
}

Constant* RelationalExpr::Evaluate(StorageManagerWrapper* sw)
{
	Constant *lvalue = left->Evaluate(sw);
	Constant *rvalue = right->Evaluate(sw);
	if(lvalue->GetType() == eInt)
	{
		int op1 = lvalue->GetIntValue();
		int op2 = rvalue->GetIntValue();
		switch(op->GetOperator())
		{
			case '=': return new BoolConstant(op1 == op2);
			case '>': return new BoolConstant(op1 > op2);
			case '<': return new BoolConstant(op1 < op2);
			default:  Assert(0);
		}
	}
	else if (lvalue->GetType() == eString)
	{
		string op1 = lvalue->GetStringValue();
		string op2 = rvalue->GetStringValue();
		switch(op->GetOperator())
		{
			case '=': return new BoolConstant(op1 == op2);
			default:  Assert(0);
		}

	}
	return NULL;
}

Constant* ColumnAccess::Evaluate(StorageManagerWrapper* sw)
{
	return sw->GetValue(column->GetTableName(), column->GetColumnName());
}

void RelationalExpr::GetJoinAttributes(vector<ColumnAccess*> &joinAttr)
{
	if (!left) return;
	if (op->GetOperator() != '=') return;

	ColumnAccess* col1 = dynamic_cast<ColumnAccess*>(left); //->GetColumnAccess();
	ColumnAccess* col2 = dynamic_cast<ColumnAccess*>(right); //->GetColumnAccess();
	if (!col1 || !col2) return; 

	if (strcmp(col1->GetTableName(), col2->GetTableName()))
	{
		joinAttr.push_back(col1);
		joinAttr.push_back(col2);
	}
	/*
	else
	{
		joinAttr.push_back(col1);
		joinAttr.push_back(col2);
	}*/
}

