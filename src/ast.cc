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
	vector<enum FIELD_TYPE> field_types;
	vector<string> field_names;
	for (int i = 0; i < attrList->NumElements(); i++)
	{
		field_names.push_back(attrList->Nth(i)->GetFieldName());
		if(!strcmp("STR20",attrList->Nth(i)->GetTypeName()))
			field_types.push_back(STR20);
		else
			field_types.push_back(INT);
	}
	StorageManagerWrapper::CreateTable(table_name->GetName(), field_names, field_types);
	return NULL;
}

List<TUPLE*>* DropTableStmt::Execute()
{
	StorageManagerWrapper::DropTable(table_name->GetName());
	return NULL;
}

List<TUPLE *> * InsertStmt::Execute()
{
	List<string> column_names;
	List<Constant*> column_values;

	for (int i = 0; i < columns->NumElements(); i++)
		 column_names.Append(columns->Nth(i)->GetName());

	StorageManagerWrapper::InsertTuple(table_name->GetName(),&column_names,values->GetValueList());
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
	LogicalQueryPlan* lqp = new LogicalQueryPlan(table_names, columns, condition, distinct, orderBy);

	//lqp->Optimize();
	//Operation * lqp = Create_LogicalQueryPlan();
	//Optimized_lqp = QueryOptimizer(lqp);
	//ExecuteOptimizedPlan();
	return NULL;
}
