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

class StorageManagerWrapper
{
protected:
	MainMemory 		mem;
	SchemaManager 	schema_manager;
	Disk 			disk;
	void _CreateTable(string table_name,
			const vector<string> &field_names, 
			const vector <enum FIELD_TYPE> & field_types);
	void _DropTable(string relation_name);
    void _InsertTuple(const string table_name,
			const List<string> *column_name,
			const List<TUPLE*> *column_value);
	int OutputBufferIndex()
	{
		//Assert(mem->getMemorySize());
		return mem.getMemorySize()-1;
	}
	StorageManagerWrapper();

	static StorageManagerWrapper* instance;
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
};

#endif //__STOREGE_WRAPPER__
