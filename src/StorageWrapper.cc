#include "StorageWrapper.h"
using namespace std;

StorageManagerWrapper* StorageManagerWrapper::instance = NULL;

StorageManagerWrapper::StorageManagerWrapper()
	:schema_manager(&mem, &disk)
{
	disk.resetDiskIOs();
	disk.resetDiskTimer();
}

void StorageManagerWrapper::_CreateTable(
		string table_name,
		const vector<string> &fn, 
		const vector <enum FIELD_TYPE> & ft)
{
	Schema schema(fn, ft);
	// Print the information about the schema
	cout << schema << endl;
	cout << "The schema has " << schema.getNumOfFields() << " fields" << endl;
	cout << "The schema allows " << schema.getTuplesPerBlock() << " tuples per block" << endl;
	cout << "The schema has field names: " << endl;
	vector <string> field_names=schema.getFieldNames();
	copy(field_names.begin(),field_names.end(),ostream_iterator<string,char>(cout," "));
	cout << endl;
	cout << "The schema has field types: " << endl;
	 vector <enum FIELD_TYPE>field_types=schema.getFieldTypes();
	for (int i=0;i<schema.getNumOfFields();i++) {
	cout << (field_types[i]==0?"INT":"STR20") << "\t";
	}
	cout << endl;  
	cout << "The first field is of name " << schema.getFieldName(0) << endl;
	cout << "The second field is of type " << (schema.getFieldType(1)==0?"INT":"STR20") << endl;
	cout << "The field exam is of type " << (schema.getFieldType("exam")==0?"INT":"STR20") << endl;
	cout << "The field homework is at offset " << schema.getFieldOffset("homework") << endl << endl;
	cout << "=====================Relation & SchemaManager=========================" << endl;

	// Create a relation with the created schema through the schema manager
	string relation_name = table_name;
	cout << "Creating table " << relation_name << endl;  
	Relation* relation_ptr=schema_manager.createRelation(relation_name,schema);

	// Print the information about the Relation
	cout << "The table has name " << relation_ptr->getRelationName() << endl;
	cout << "The table has schema:" << endl;
	cout << relation_ptr->getSchema() << endl;
	cout << "The table currently have " << relation_ptr->getNumOfBlocks() << " blocks" << endl;
	cout << "The table currently have " << relation_ptr->getNumOfTuples() << " tuples" << endl << endl;
	return;
}

void StorageManagerWrapper::_DropTable(string relation_name)
{
	cout << "Deleting table " <<relation_name<<endl;
	schema_manager.deleteRelation(relation_name);
	cout << "After deleting a realtion, current schemas and relations: " << endl;
	cout << schema_manager << endl << endl;
	return;
}
