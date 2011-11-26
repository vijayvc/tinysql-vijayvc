#include "StorageWrapper.h"
#include "ast.h"

using namespace std;

StorageManagerWrapper* StorageManagerWrapper::instance = NULL;

void PRINT_TUPLEINFO(Tuple tuple);
// insert single tuple into a relation
void appendTupleToRelation(Relation* relation_ptr, 
							MainMemory& mem, 
							int memory_block_index, 
							Tuple& tuple);

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
	
//====================Tuple=============================
void StorageManagerWrapper::_InsertTuple(string relation_name,
	const List<string> *column_names,
	const List<TUPLE *> *tuple_list)
{
	//cout << "====================Tuple=============================" << endl;
	
	Relation* relation_ptr = schema_manager.getRelation(relation_name);  // Set up the first tuple
	for (int i = 0; i < tuple_list->NumElements(); i++)
	{
		TUPLE* input_tuple = tuple_list->Nth(i);
		Tuple new_tuple = relation_ptr->createTuple(); //The only way to create a tuple is to call "Relation"
		for (int j = 0; j < input_tuple->NumElements(); j++)
		{
			if(input_tuple->Nth(j)->GetType() == eString)
				new_tuple.setField(column_names->Nth(j),input_tuple->Nth(j)->GetStringValue());
			else
				new_tuple.setField(column_names->Nth(j),input_tuple->Nth(j)->GetIntValue());
		}
		PRINT_TUPLEINFO(new_tuple);
		appendTupleToRelation(relation_ptr,mem,OutputBufferIndex(),new_tuple);
	}
}
	
void PRINT_TUPLEINFO(Tuple tuple)
{
	// Print the information about the tuple
	cout << "Created a tuple " << tuple << " through the relation" << endl;
	cout << "The tuple is invalid? " << (tuple.isNull()?"TRUE":"FALSE") << endl;
	Schema tuple_schema = tuple.getSchema();
	cout << "The tuple has schema" << endl;
	cout << tuple_schema << endl;
	cout << "A block can allow at most " << tuple.getTuplesPerBlock() << " such tuples" << endl;
	
	cout << "The tuple has fields: " << endl;
	for (int i=0; i<tuple.getNumOfFields(); i++) {
		if (tuple_schema.getFieldType(i)==INT)
			cout << tuple.getField(i).integer << "\t";
		else
			cout << *(tuple.getField(i).str) << "\t";
	}
	cout << endl;
	
	cout << "The tuple has fields: " << endl;
	cout << *(tuple.getField("f1").str) << "\t";
	cout << tuple.getField("f2").integer << "\t";
	cout << tuple.getField("f3").integer << "\t";
	cout << *(tuple.getField("f4").str) << "\t";
	cout << endl << endl;
	
	
}

// insert single tuple into a relation
void appendTupleToRelation(Relation* relation_ptr, 
							MainMemory& mem, 
							int memory_block_index, 
							Tuple& tuple) 
{
	Block* block_ptr;
	if (relation_ptr->getNumOfBlocks()==0) {
		cout << "The relation is empty" << endl;
		cout << "Get the handle to the memory block " << memory_block_index << " and clear it" << endl;
		block_ptr=mem.getBlock(memory_block_index);
		block_ptr->clear(); //clear the block
		block_ptr->appendTuple(tuple); // append the tuple
		cout << "Write to the first block of the relation" << endl;
		relation_ptr->setBlock(relation_ptr->getNumOfBlocks(),memory_block_index);
	} else {
		cout << "Read the last block of the relation into memory block 5:" << endl;
		relation_ptr->getBlock(relation_ptr->getNumOfBlocks()-1,memory_block_index);
		block_ptr=mem.getBlock(memory_block_index);

		if (block_ptr->isFull()) {
			cout << "(The block is full: Clear the memory block and append the tuple)" << endl;
			block_ptr->clear(); //clear the block
			block_ptr->appendTuple(tuple); // append the tuple
			cout << "Write to a new block at the end of the relation" << endl;
			relation_ptr->setBlock(relation_ptr->getNumOfBlocks(),memory_block_index); //write back to the relation
		} else {
			cout << "(The block is not full: Append it directly)" << endl;
			block_ptr->appendTuple(tuple); // append the tuple
			cout << "Write to the last block of the relation" << endl;
			relation_ptr->setBlock(relation_ptr->getNumOfBlocks()-1,memory_block_index); //write back to the relation
		}
	}  
}
