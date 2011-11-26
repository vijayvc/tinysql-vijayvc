#include "ast.h"
#include "StorageWrapper.h"

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
