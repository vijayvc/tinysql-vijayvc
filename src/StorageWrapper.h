#ifndef __STORAGE_WRAPPER__
#define __STORAGE_WRAPPER__

#include<iterator>
#include<cstdlib>
#include<ctime>
#include<string>
#include<iostream>
#include "StorageManager/Block.h"
#include "StorageManager/Config.h"
#include "StorageManager/Disk.h"
#include "StorageManager/Field.h"
#include "StorageManager/MainMemory.h"
#include "StorageManager/Relation.h"
#include "StorageManager/Schema.h"
#include "StorageManager/SchemaManager.h"
#include "StorageManager/Tuple.h"

class StorageManagerWrapper
{
protected:
	MainMemory 		mem;
	SchemaManager 	schema_manager;
	Disk 			disk;
	void _CreateTable(string table_name,
			const vector<string> & field_names, 
			const vector <enum FIELD_TYPE> & field_types);
	void _DropTable(string relation_name);
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
		instance->_CreateTable(table_name, field_names, field_types);
	}
	static void DropTable(string relation_name)
	{
		instance->_DropTable(relation_name);
	}
};

#endif //__STOREGE_WRAPPER__
