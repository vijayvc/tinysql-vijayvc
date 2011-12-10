#ifndef __STORAGE_WRAPPER__
#define __STORAGE_WRAPPER__

#include "list.h"
#include <cstdlib>
#include <ctime>
#include <string>
#include <iostream>
#include <iterator>
#include "StorageManager/Block.h"
#include "StorageManager/Config.h"
#include "StorageManager/Disk.h"
#include "StorageManager/Field.h"
#include "StorageManager/MainMemory.h"
#include "StorageManager/Relation.h"
#include "StorageManager/Schema.h"
#include "StorageManager/SchemaManager.h"
#include "StorageManager/Tuple.h"

#define TUPLE List<Constant*>
class Constant;
class Expr;
class ColumnName;
class EntityName;

class StorageManagerWrapper
{
protected:
	MainMemory 		mem;
	SchemaManager 	schema_manager;
	Disk 			disk;
	Relation* _CreateTable(string table_name,
			const vector<string> &field_names, 
			const vector <enum FIELD_TYPE> & field_types);
	void _DropTable(string relation_name);
    void _InsertTuple(const string table_name,
			const List<string> *column_name,
			const List<TUPLE*> *column_value);
	
	List<TUPLE*>* _ExecuteSingleTableSelect(const string table_name,
			const List <ColumnName*>* columns,
			Expr* condition,bool distinct,
			ColumnName* orderBy);
	List<TUPLE*>* _ExecuteMultipleTableSelect(List<EntityName*>* table_names,
			const List <ColumnName*>* columns,
			Expr* condition,bool distinct,
			ColumnName* orderBy);
	void OrderByOnePass(vector<Tuple> & tuples,
		const string attr);

	void _print_tuples(const vector<string> &attrName,
			const vector<Tuple> & tuples);
	void DistinctOnePass(vector<Tuple>& tuples,
			const vector<string> &attrName);
	void _print_ATuple(const Tuple & tuple,
			const vector<string> &attrName);
	//		bool  distinct);

	void OrderByTwoPass(const string relationName,
			const string attr, 
			const vector<string> &attrName,
			vector<Tuple> &resultTuples);
	void DistinctTwoPass(const string relationName,
			const vector<string> &attrName,
			vector<Tuple> &distinctTuples);

	void _ExecuteDeleteTuples(string tableName, Expr* condition);
	int OutputBufferIndex()
	{
		//Assert(mem->getMemorySize());
		return mem.getMemorySize()-1;
	}
	StorageManagerWrapper();

	static StorageManagerWrapper* instance;

	// used to evaluate where clause
	static List<string> tableList;
	static List<Tuple*> tupleList;
public:
	static bool Initialize()
	{
		if(!instance)
		{
			instance = new StorageManagerWrapper();
			if(instance)
				return true;
		}
		return false;
	}
	static void CreateTable(string table_name,
			const vector<string> & field_names, 
			const vector <enum FIELD_TYPE> & field_types)
	{
		if(instance)
			instance->_CreateTable(table_name, field_names, field_types);
	}
	static void DropTable(string relation_name)
	{
		if(instance)
			instance->_DropTable(relation_name);
	}
	static void InsertTuple (const string table_name,
			                 const List<string> *column_name,
							 const List<TUPLE*> *column_value)
	{
		if(instance)
			instance->_InsertTuple(table_name,column_name,column_value);
	}


	static List<TUPLE*>* ExecuteSingleTableSelect(const string table_name,
								const List <ColumnName*>* columns,
								Expr* condition,bool distinct,
								ColumnName* orderBy)
	{
		if(instance)
			return instance->_ExecuteSingleTableSelect(table_name,columns, condition,distinct, orderBy);
		return NULL;
	}

	static List<TUPLE*>* ExecuteMultipleTableSelect(List<EntityName*>* table_names,
			const List <ColumnName*>* columns,
			Expr* condition,bool distinct,
			ColumnName* orderBy)
	{
		if(instance)
			return instance->_ExecuteMultipleTableSelect(table_names,columns, condition,distinct, orderBy);
		return NULL;
	}
	static void ExecuteDeleteTuples(string tableName, Expr* condition)
	{
		if(instance)
			instance->_ExecuteDeleteTuples(tableName, condition);
	}
	/*
	static void SimpleSelect (const string table_name,
							  const List<string>* columns,
							  const Expr* conditionTree,
							  bool isDistinct)
	{
		if(instance)
			instance->_SimpleSelect(table_name,columns,conditionTree, isDistinct);
	}
	*/
	Constant* GetValue(const char* table_name, const char* field_name);
	vector<Relation*> *CreateSortedSublists(
			string tableName, string orderBy); //, set<string> requiredFields)

	bool MergeSublists(
		vector<Relation*>* relationPtrs, 
		vector<Relation*>* sublists1, 
		vector<Relation*>* sublists2, 
		string joinAttr,
		Relation* jRelation,
		vector<Tuple> &list1,
		vector<Tuple> &list2);

	void JoinTuples(vector<Tuple> &list1, 
		vector<Tuple> &list2, 
		vector<Tuple> &result,
		Schema schema1,
		Schema schema2,
		string joinAttr,
		Expr* condition);

	bool LoadRelation(Relation* relation, vector<Tuple> &list, bool distinct);
	bool StoreRelation(Relation* relation, vector<Tuple> &list);
	vector<Tuple>* DistinctSecondPass(vector<Relation*> *sublists, 
		vector<string> attrNames,
		vector<Tuple>* merged);
	vector<Tuple>* MergeSortedSublists(vector<Relation*> *sublists, 
		string joinAttr,
		vector<Tuple>* merged);

	List<TUPLE*>* CrossJoin(
		List<EntityName*>* tableNames,
		vector<Relation*> &relationPtrs);

};
#endif //__STOREGE_WRAPPER__
