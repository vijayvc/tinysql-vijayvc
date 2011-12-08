#include "StorageWrapper.h"
#include "ast.h"

using namespace std;

StorageManagerWrapper* StorageManagerWrapper::instance = NULL;
List<string> StorageManagerWrapper::tableList;
List<Tuple*> StorageManagerWrapper::tupleList;

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
	cout <<"create table";
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
	cout << "====================insert Tuple=============================" << endl;
	
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
}

void StorageManagerWrapper::_ExecuteSingleTableSelect(const string relationName,const List <ColumnName*>* columns,Expr* condition,bool distinct,ColumnName* orderBy)
{
	Relation* relationPtr = schema_manager.getRelation(relationName);
	Schema schema = schema_manager.getSchema(relationName);
	cout<<"--------------------SINGLE TABLE SELECT-----------------------"<<endl;
	vector<string> tmpFieldNames = schema.getFieldNames();
	for(unsigned int f = 0; f<tmpFieldNames.size(); f++)
		cout<<tmpFieldNames[f]<<"  ";
	cout<<endl;

	// Set the sequence of tables
	while(tableList.NumElements())
		tableList.RemoveAt(0);
	tableList.Append(relationName);

	Block* block = mem.getBlock(0);
//	int blockNum = relationPtr->getNumOfBlocks();
	int tuplesNum = relationPtr->getNumOfTuples();
	int tuplesPerBlockNum = schema.getTuplesPerBlock();
	cout<<"Currently the the table has" << relationPtr->getNumOfTuples()<< " tuples"<<endl;
	cout<<"Number of tuples per block : "<< schema.getTuplesPerBlock()<<endl;
	if(condition) 
	{
		tuplesNum = tuplesNum/2;
		cout<<"Since there is a condition I assume that the table size reduces by half" << tuplesNum<< " tuples"<<endl;
	}
	int blockNum =	tuplesNum/tuplesPerBlockNum;
	int memSize = mem.getMemorySize();
	if (memSize >= blockNum)
		cout <<"One Pass Algorithm is enough"<<endl;
	else
		cout<<"two Pass algorith"<<endl;
	vector<Tuple> tuples_block;
	vector<Tuple> tuples;
	vector<string> attrName;
	if (strcmp(columns->Nth(0)->GetColumnName(),"*")==0) 
	{ // select * from
		attrName = schema.getFieldNames();
		/*
		for (int i = 0; i != blockNum; i++) 
		{
			block->clear();
			relationPtr->getBlock(i, 0);
			tuples = block->getTuples();
			for (vector<Tuple>::iterator lit=tuples.begin(); lit != tuples.end(); lit++)
			{
				lit->printTuple(cout);
				cout<<endl;
			}
		}*/
	}
	else
	{
		cout<<" single table with projection"<<endl;
		// get attribute names----------------------------------
		string value2;
		for(int i =0;i< columns->NumElements();i++)
			attrName.push_back(columns->Nth(i)->GetColumnName());
	
		// end of get attribute names--------------------------------
	}
	for (int i = 0; i != blockNum; i++) 
	{
		block->clear();
		relationPtr->getBlock(i, 0);
		tuples_block = block->getTuples();
		//List<Tuple*> tupleList;
		for (vector<Tuple>::iterator lit = tuples_block.begin(); lit != tuples_block.end(); lit++) 
		{	
			tupleList.Append(&*lit);
			//_SetCurrenttuples(&tupleList);
			Constant* result = NULL;
			bool print = true;
			if (condition)
				print = condition->Evaluate(this)->GetBoolValue();

			if (print)
			{

				tuples.push_back(*lit);
			
/*				for (vector<string>::iterator iter = attrName.begin(); iter != attrName.end(); iter++) 
				{
					if (schema.getFieldType(*iter) == STR20)
						cout << *(lit->getField(schema.getFieldOffset(*iter)).str) << "\t";
					else if (schema.getFieldType(*iter) == INT)
						cout << lit->getField(schema.getFieldOffset(*iter)).integer << "\t";
				}
				cout << endl; */
			}
			tupleList.RemoveAt(tupleList.NumElements()-1);
		}
	
	}
	if(orderBy)
	{
	//	_orderBySinglePass(tuples,orderBy->GetColumnName());
		_twoPass_OrderBy(relationName,orderBy->GetColumnName(),attrName );
	}
	if(distinct)
	{
		_twoPass_Distinct(relationName,attrName);

//		_distinctSinglePass(tuples,attrName);
	}
	//_print_tuples(attrName,tuples);TEMPERORY
	
}
	
void StorageManagerWrapper::_distinctSinglePass(vector<Tuple>& tuples,const vector<string> &attrName)
{
	Schema schema = tuples[0].getSchema();
	vector<Tuple> distinctTuples;
	distinctTuples.push_back(tuples[0]);
	int size = tuples.size();
	bool duplicate_exists,diff;
	diff = false;
	for(int i =1;i<size;i++)
	{
		duplicate_exists = false;
		//check if it LREADY EXISTS IN DISTICT TUPLES
		for(int j=0;j<distinctTuples.size();j++)
		{
			diff = false;
			for (vector<string>::const_iterator iter = attrName.begin(); iter != attrName.end(); iter++) 
			{
				if (schema.getFieldType(*iter) == STR20)
				{
					if ( *(tuples[i].getField(schema.getFieldOffset(*iter)).str) != *(distinctTuples[j].getField(schema.getFieldOffset(*iter)).str) )
					{
						diff = true;
					}
				}
				else if (schema.getFieldType(*iter) == INT)
				{
					if(tuples[i].getField(*iter).integer != distinctTuples[j].getField(*iter).integer)
					{
						diff = true;
					}
				}
			}
			if(diff == false)
			{
				duplicate_exists = true;
				break;
			}
		}
		if(duplicate_exists == false)
		{
			distinctTuples.push_back(tuples[i]);

		}
		else
		{
			duplicate_exists = false;

		}
	}
	// now clear tuples ansd put distincttuples in tuples
	tuples.clear();
	size = distinctTuples.size();
	for(int k=0;k<size;k++)
	{
		tuples.push_back(distinctTuples[k]);
	}
}
/*
void StorageManagerWrapper::_ExecuteSingleTableSelectTwoPass(const string relationName,const List <ColumnName*>* columns,Expr* condition,bool distinct,ColumnName* orderBy)
{
}
*/
void StorageManagerWrapper::_twoPass_Distinct(const string relationName,const vector<string> &attrName)
{
	Relation* relationPtr = schema_manager.getRelation(relationName);

	int relationSize = relationPtr->getNumOfBlocks();
	//int num = relationSize;

	int memSize = mem.getMemorySize();
	Block* block;
	Schema schema = schema_manager.getSchema(relationName);
	vector<string> tmpFieldNames = schema.getFieldNames();
	for(unsigned int f = 0; f<tmpFieldNames.size(); f++)
		cout<<tmpFieldNames[f]<<"  ";
	cout<<endl;
	cout<<"Creating temporary Relation"<<endl;
	string tempRelationName = "tmpDistinctRelation";
	Relation *tmpRelationPtr= schema_manager.createRelation(tempRelationName,schema);
	//string type = schema.getFieldType(fieldName);
	int segmentNum = relationSize/(memSize - 1);
	vector<pair<int, int> > firstIndicator;
	vector<pair<int, int> >secondIndicator;
	vector<Tuple> tuples_block;
		
	vector<Tuple>tuples;
	for (int i = 0; i < segmentNum; i++) 
	{
		pair<int, int> indicator1 (i * (memSize-1), (i+1) * (memSize-1) - 1);
		firstIndicator.push_back(indicator1);
	}
	if(relationSize%(memSize-1))
	{
		pair<int, int> indicator1 (segmentNum * (memSize-1), relationSize - 1);
		firstIndicator.push_back(indicator1);
	}
	multimap<string, Tuple> my_smap;
   	multimap<int, Tuple> my_imap;
	for (unsigned int i = 0; i < firstIndicator.size(); i++) 
	{
		for (int j = 0; j < memSize; j++) 
		{
			block = mem.getBlock(j);
			block->clear();
		}
		for (int j = firstIndicator[i].first; j <= firstIndicator[i].second; j++) 
		{
			relationPtr->getBlock( j,0);
			block = mem.getBlock(0);
			tuples_block = block->getTuples();
			block->clear();
			if(schema.getFieldType(attrName[0]) == INT){
				for(int i=0;i<tuples_block.size();i++){

					my_imap.insert(pair<int, Tuple>(tuples_block[i].getField(attrName[0]).integer,tuples_block[i]));
				}
				tuples_block.clear();

			}else{
				for(int i=0;i<tuples_block.size();i++){
					my_smap.insert(pair<string,Tuple> (*(tuples_block[i].getField(attrName[0]).str),tuples_block[i]));
				}
				tuples_block.clear();
				
			}

		}

		if(schema.getFieldType(attrName[0]) == INT)
		{
			multimap<int, Tuple>::iterator it;
			for (it=my_imap.begin(); it != my_imap.end(); ++it) {
						//tuples.push_back(it->second);
				appendTupleToRelation(tmpRelationPtr ,mem,OutputBufferIndex(),it->second);
			}
		}
		else
		{
			multimap<string, Tuple>::iterator sit;
			for (sit=my_smap.begin(); sit != my_smap.end(); ++sit) {
				// tuples.push_back(sit->second);
				appendTupleToRelation(tmpRelationPtr ,mem,OutputBufferIndex(),sit->second);
			}
		}
		my_imap.clear();
		my_smap.clear();
	}//end of first pass
	block = mem.getBlock(0);
	int num = tmpRelationPtr->getNumOfBlocks();
	cout<<"tuples count :"<<relationPtr->getNumOfBlocks()<<endl<<tmpRelationPtr->getNumOfBlocks()<<endl;
	tuples.clear();
	for(int i =0;i!=num;i++)
	{
		block->clear();
		tmpRelationPtr->getBlock(i,0);
		tuples_block= block->getTuples();
		for (vector<Tuple>::iterator lit = tuples_block.begin(); lit != tuples_block.end(); lit++) 
			tuples.push_back(*lit);

	}
	cout<<"After first pass we read from the temp tablee"<<endl;
	cout<<"size        :"<<tuples.size()<<endl;
	_print_tuples(attrName,tuples);
//***********
	vector<Tuple> distinctTuples;
	distinctTuples.push_back(tuples[0]);
	int size = tuples.size();
	bool duplicate_exists,diff;
	diff = false;
	for(int i =1;i<size;i++)
	{
		duplicate_exists = false;
		//check if it LREADY EXISTS IN DISTICT TUPLES
		for(int j=0;j<distinctTuples.size();j++)
		{
			diff = false;
			for (vector<string>::const_iterator iter = attrName.begin(); iter != attrName.end(); iter++) 
			{
				if (schema.getFieldType(*iter) == STR20)
				{
					if ( *(tuples[i].getField(schema.getFieldOffset(*iter)).str) != *(distinctTuples[j].getField(schema.getFieldOffset(*iter)).str) )
					{
						diff = true;
					}
				}
				else if (schema.getFieldType(*iter) == INT)
				{
					if(tuples[i].getField(*iter).integer != distinctTuples[j].getField(*iter).integer)
					{
						diff = true;
					}
				}
			}
			if(diff == false)
			{
				duplicate_exists = true;
				break;
			}
		}
		if(duplicate_exists == false)
		{
			distinctTuples.push_back(tuples[i]);

		}
		else
		{
			duplicate_exists = false;

		}
	}

cout<<"************************************************************"<<endl;

	
	_print_tuples(attrName,distinctTuples);
}
	
//dont forget to delete tmp relation
//if no distinct then delete tmp relation else pass tmp relation tp distinct
void StorageManagerWrapper::_twoPass_OrderBy(const string relationName,const string attr,const vector<string> &attrName )
{
	Relation* relationPtr = schema_manager.getRelation(relationName);

	int relationSize = relationPtr->getNumOfBlocks();
	int num = relationSize;
	int memSize = mem.getMemorySize();
	Block* block;
	Schema schema = schema_manager.getSchema(relationName);
	vector<string> tmpFieldNames = schema.getFieldNames();
	for(unsigned int f = 0; f<tmpFieldNames.size(); f++)
		cout<<tmpFieldNames[f]<<"  ";
	cout<<endl;
	cout<<"Creating temporary Relation"<<endl;
	Relation *tmpRelationPtr;
	string tempRelationName = "tmpRelation";
	tmpRelationPtr= schema_manager.createRelation(tempRelationName,schema);
	//string type = schema.getFieldType(fieldName);
	int segmentNum = relationSize/(memSize - 1);
	vector<pair<int, int> > firstIndicator;
	vector<pair<int, int> >secondIndicator;
	vector<Tuple> tuples_block;
		
	vector<Tuple>tuples;
	for (int i = 0; i < segmentNum; i++) 
	{
		pair<int, int> indicator1 (i * (memSize-1), (i+1) * (memSize-1) - 1);
		firstIndicator.push_back(indicator1);
	}
	if(relationSize%(memSize-1))
	{
		pair<int, int> indicator1 (segmentNum * (memSize-1), relationSize - 1);
		firstIndicator.push_back(indicator1);
	}
	multimap<string, Tuple> my_smap;
   	multimap<int, Tuple> my_imap;
	for (unsigned int i = 0; i < firstIndicator.size(); i++) 
	{
		for (int j = 0; j < memSize; j++) 
		{
			block = mem.getBlock(j);
			block->clear();
		}
		for (int j = firstIndicator[i].first; j <= firstIndicator[i].second; j++) 
		{
			relationPtr->getBlock(j,0);
			block = mem.getBlock(0);
			tuples_block = block->getTuples();
			block->clear();
			if(schema.getFieldType(attr) == INT){
				for(int i=0;i<tuples_block.size();i++){

					my_imap.insert(pair<int, Tuple>(tuples_block[i].getField(attr).integer,tuples_block[i]));
				 }
				tuples_block.clear();

			}else{
				for(int i=0;i<tuples_block.size();i++){
					my_smap.insert(pair<string,Tuple> (*(tuples_block[i].getField(attr).str),tuples_block[i]));
				}
				tuples_block.clear();
			}

		}
		if(schema.getFieldType(attrName[0]) == INT)
		{
			multimap<int, Tuple>::iterator it;
			for (it=my_imap.begin(); it != my_imap.end(); ++it) {
						//tuples.push_back(it->second);
				appendTupleToRelation(tmpRelationPtr ,mem,OutputBufferIndex(),it->second);
			}
		}
		else
		{
			multimap<string, Tuple>::iterator sit;
			for (sit=my_smap.begin(); sit != my_smap.end(); ++sit) {
				// tuples.push_back(sit->second);
				appendTupleToRelation(tmpRelationPtr ,mem,OutputBufferIndex(),sit->second);
			}
		}
		my_imap.clear();
		my_smap.clear();

	}//end of first pass
	block = mem.getBlock(0);

	num = tmpRelationPtr->getNumOfBlocks();
	for (int i = 0; i != num; i++) 
	{
		block->clear();
		tmpRelationPtr->getBlock(i, 0);
		tuples_block = block->getTuples();
		for (vector<Tuple>::iterator lit = tuples_block.begin(); lit != tuples_block.end(); lit++) 
			tuples.push_back(*lit);
	}
//****
	//multimap<string, Tuple> my_smap;
     cout<<"Size after two pass1"<<tuples.size()<<endl;
	//multimap<int, Tuple> my_imap;
    my_smap.clear();
	my_imap.clear();
	if(schema.getFieldType(attr) == INT){	

		for(int i=0;i<tuples.size();i++){

			my_imap.insert(pair<int, Tuple>(tuples[i].getField(attr).integer,tuples[i]));
		}
		tuples.clear();
		multimap<int, Tuple>::const_iterator it;
		for (it=my_imap.begin(); it != my_imap.end(); ++it) {
			 tuples.push_back(it->second);
		}

	}else{

		for(int i=0;i<tuples.size();i++){
			my_smap.insert(pair<string,Tuple> (*(tuples[i].getField(attr).str),tuples[i]));
		}
		tuples.clear();
		multimap<string, Tuple>::const_iterator sit;
		for (sit=my_smap.begin(); sit != my_smap.end(); ++sit) {
			 tuples.push_back(sit->second);
		}
	}
     cout<<"Size after two pass"<<tuples.size();
	_print_tuples(attrName, tuples);

//	****
}

void StorageManagerWrapper::_print_tuples(const vector<string> &attrName, const vector<Tuple> & tuples )
{
	Schema schema = tuples[0].getSchema();
	for(vector<Tuple>::const_iterator lit = tuples.begin(); lit != tuples.end();lit++)
	{
		for (vector<string>::const_iterator iter = attrName.begin(); iter != attrName.end(); iter++) 
		{
			if (schema.getFieldType(*iter) == STR20)
				cout << *(lit->getField(schema.getFieldOffset(*iter)).str) << "\t";
			else if (schema.getFieldType(*iter) == INT)
				cout << lit->getField(schema.getFieldOffset(*iter)).integer << "\t";
		}
		cout<<endl;
	}
}

void StorageManagerWrapper::_print_ATuple(const Tuple & tuple,const vector<string> &attrName)
{
	Schema schema = tuple.getSchema();
	for (vector<string>::const_iterator iter = attrName.begin(); iter != attrName.end(); iter++) 
	{
		if (schema.getFieldType(*iter) == STR20)
			cout << *(tuple.getField(schema.getFieldOffset(*iter)).str) << "\t";
		else if (schema.getFieldType(*iter) == INT)
			cout << tuple.getField(schema.getFieldOffset(*iter)).integer << "\t";
	}
	cout<<endl;

}
/*
void StorageManagerWrapper::_print_tuples(const vector<string> &attrName, const vector<Tuple> & tuples,bool distinct )
{
	Schema schema = tuples[0].getSchema();

	Tuple old_tuple = tuples[0];
	_print_ATuple(tuples[0],attrName);
	bool diff = false;
	int size = tuples.size();
	for(int i =1;i<size;i++)
	{
		if (distinct)
		{
			for (vector<string>::const_iterator iter = attrName.begin(); iter != attrName.end(); iter++) 
			{
				if (schema.getFieldType(*iter) == STR20)
				{
					if ( *(tuples[i].getField(schema.getFieldOffset(*iter)).str) != *(old_tuple.getField(schema.getFieldOffset(*iter)).str) )
					{
						diff = true;
					}
				}
				else if (schema.getFieldType(*iter) == INT)
				{
					if(tuples[i].getField(*iter).integer != old_tuple.getField(*iter).integer)
					{
						diff = true;
					}
				}
			}
		
		}
		else
		{
			_print_ATuple(tuples[i],attrName);
		}
//		cout<<endl;
		old_tuple = tuples[i];
		if(distinct && diff)
		{

			_print_ATuple(tuples[i],attrName);
			diff = false;
		}
	}
	
}
*/
void StorageManagerWrapper::_orderBySinglePass(vector<Tuple> & tuples,const string attr)
{
	Schema schema = tuples[0].getSchema();
	multimap<string, Tuple> my_smap;
	multimap<int, Tuple> my_imap;

	if(schema.getFieldType(attr) == INT){	

		for(int i=0;i<tuples.size();i++){

			my_imap.insert(pair<int, Tuple>(tuples[i].getField(attr).integer,tuples[i]));
		}
		tuples.clear();
		multimap<int, Tuple>::const_iterator it;
		for (it=my_imap.begin(); it != my_imap.end(); ++it) {
			 tuples.push_back(it->second);
		}

	}else{

		for(int i=0;i<tuples.size();i++){
			my_smap.insert(pair<string,Tuple> (*(tuples[i].getField(attr).str),tuples[i]));
		}
		tuples.clear();
		multimap<string, Tuple>::const_iterator sit;
		for (sit=my_smap.begin(); sit != my_smap.end(); ++sit) {
			 tuples.push_back(sit->second);
		}
	}

	return;

}

/*
void StorageWrapper::_SetCurrentTuples(List<Tuple*>* tupl, List<string>* tabl)
{
	//static List<Tuple*>* tupleList = NULL;
	//static List<string>* tableList = NULL;
	tupleList = tupl;
	tableList = tabl; 
}
*/

Constant* StorageManagerWrapper::GetValue(const char* table_name, const char* field_name)
{
	Schema schema;
	int index = 0;
	if (table_name)
	{
		schema = schema_manager.getSchema(table_name);
		for (int i = 0; i < tableList.NumElements(); i++)
		{
			if (tableList.Nth(i) == string(table_name))
			{
				index = i;
				break;
			}
		}
	}
	else
		schema = tupleList.Nth(0)->getSchema();

	if (schema.getFieldType(field_name) == STR20)
		return new StringConstant(*(tupleList.Nth(index)->getField(schema.getFieldOffset(field_name)).str));
		 //*(lit->getField(schema.getFieldOffset(*iter)).str) << "\t";
	else if (schema.getFieldType(field_name) == INT)
		return new IntConstant(tupleList.Nth(index)->getField(schema.getFieldOffset(field_name)).integer);
		//t << lit->getField(schema.getFieldOffset(*iter)).integer << "\t";

	return NULL;
}

/*
void StorageWrapper::_SetCurrentRelation(string rn)
{
	static string relation_name = "";
	relation_name
}
*/

/*
void executeSelectStmt(logic *lqp)
{

}
*/
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
