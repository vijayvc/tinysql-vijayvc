#include "StorageWrapper.h"
#include "ast.h"
#include <algorithm>

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


vector<Tuple>* MergeSortedSublists(vector<Relation*> *sublists, 
		string joinAttr,
		MainMemory* mem,
		vector<Tuple>* merged);

void PrintRelation(Relation* r, Block* block);

StorageManagerWrapper::StorageManagerWrapper()
	:schema_manager(&mem, &disk)
{
	disk.resetDiskIOs();
	disk.resetDiskTimer();
}
	
void PrintSchema(Schema &schema)
{
	// Print the information about the schema
	cout << "Schema Information: \n";
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
}

void PrintRelationInfo(Relation &rel)
{
	// Print the information about the Relation
	cout << "Relation Information: \n";
	cout << "Name " << rel.getRelationName() << endl;
	cout << "Schema:" << endl;
	cout << rel.getSchema() << endl;
	cout << "The table currently have " << rel.getNumOfBlocks() << " blocks" << endl;
	cout << "The table currently have " << rel.getNumOfTuples() << " tuples" << endl << endl;
}

void PrintTuple(Tuple tuple)
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

Relation* StorageManagerWrapper::_CreateTable(
	string relation_name,
	const vector<string> &fn, 
	const vector <enum FIELD_TYPE> & ft)
{
	cout <<"Create Table: \"" << relation_name << "\". ";

	Schema schema(fn, ft);
	//PrintSchema(schema);
	for (int i = 0; i < fn.size(); i++)
	{
		cout << fn[i] << ":"; 
		cout << (ft[i]==0?"INT":"STR20") << ", ";
	}

	// Create a relation with the created schema through the schema manager
	Relation* relation_ptr = schema_manager.createRelation(relation_name,schema);
	if (!relation_ptr)
	{
		cout << " -- FAILED";
		return NULL;
	}
	cout << "-- SUCCESS";
	cout << endl;
	//PrintRelationInfo(*relation_ptr);
	return relation_ptr;
}
	
void StorageManagerWrapper::_DropTable(string relation_name)
{
	cout << endl;
	cout << "Drop table: \"" << relation_name << "\": "; 
	bool result = schema_manager.deleteRelation(relation_name);
	if (result)
		cout << " -- SUCCESS";
	else
		cout << " -- FAILED";
	cout << endl;
	return;
}
	
//====================Tuple=============================
void StorageManagerWrapper::_InsertTuple(string relation_name,
	const List<string> *column_names,
	const List<TUPLE *> *tuple_list)
{
	cout << "Insert into \"" << relation_name << "\": \t\t";
	
	Relation* relation_ptr = schema_manager.getRelation(relation_name);  // Set up the first tuple
	if (!relation_ptr)
	{
		cout << "\nError: Relation Doesn't Exists.\n";
		return;
	}
	for (int i = 0; i < tuple_list->NumElements(); i++)
	{
		TUPLE* input_tuple = tuple_list->Nth(i);
		Tuple new_tuple = relation_ptr->createTuple(); //The only way to create a tuple is to call "Relation"
		for (int j = 0; j < input_tuple->NumElements(); j++)
		{
			if(input_tuple->Nth(j)->GetType() == eString)
			{	
				string val = input_tuple->Nth(j)->GetStringValue();
				new_tuple.setField(column_names->Nth(j),val);
				cout << val << ",";
			}
			else
			{
				int val = input_tuple->Nth(j)->GetIntValue();
				new_tuple.setField(column_names->Nth(j),val);
				cout << val << ",";
			}
		}
		//PrintTuple(new_tuple);
		appendTupleToRelation(relation_ptr,mem,OutputBufferIndex(),new_tuple);
	}
	cout << endl;
}

std::set<string>* GetRequiredFields(SchemaManager schema_manager,
		string tableName,
		const List<ColumnName*>* columns,
		Expr* condition,
		ColumnName* orderBy)
{
	std::set<string>* result = new std::set<string>;
	if (columns->NumElements() == 1 
			&& !strcmp(columns->Nth(0)->GetColumnName(),"*"))
	{
		//schema.getFieldNames()
		Schema schema = schema_manager.getSchema(tableName);
		std::vector<string> res = schema.getFieldNames();
		for (int i=0; i<res.size(); i++)
			result->insert(res.at(i));
		return result; 
	}

	for (int i = 0; i < columns->NumElements(); i++)
	{
		ColumnName* cn = columns->Nth(i);
		if (cn && cn->GetTableName() && cn->GetTableName() == tableName)
			result->insert(columns->Nth(i)->GetColumnName());
	}

	if (condition)
		condition->GetFieldsForRelation(tableName, result);

	if(orderBy && orderBy->GetTableName() && orderBy->GetTableName() == tableName)
		result->insert(orderBy->GetColumnName());

	//cout << "Table: " << tableName << endl;
	//PrintSet(result);
	return result;
}

List<TUPLE*>* StorageManagerWrapper::_ExecuteSingleTableSelect(
		const string relationName,
		const List <ColumnName*>* columns,
		Expr* condition,
		bool distinct,
		ColumnName* orderBy)
{
	cout << "Select: Count:1 Relation: \"" << relationName << "\" ";//endl;

	Relation* relationPtr = schema_manager.getRelation(relationName);
	if (! relationPtr )
	{
		cout << "\nERROR: Relation doesn't exists!\n";
		return NULL; 
	}
	Schema schema = schema_manager.getSchema(relationName);

	//PrintSchema(schema);
#if 0
	vector<string> tmpFieldNames = schema.getFieldNames();
	for(unsigned int f = 0; f<tmpFieldNames.size(); f++)
		cout<<tmpFieldNames[f]<<"  ";
	cout<<endl;
#endif 

	cout<<"NumTuples: " << relationPtr->getNumOfTuples() << " ";
	cout<<"NumTuplesPerBlock: "<< schema.getTuplesPerBlock()<<endl;

	int numPasses = 0;
	if (relationPtr->getNumOfBlocks() <= mem.getMemorySize())
		numPasses = 1;
	else
	{
		//std::set<string> *set1 = GetRequiredFields(schema_manager, relationName, columns, condition, orderBy);
		//cout << "NumFieldsRequired: " << set1->size() << endl;
		//return NULL;
		numPasses = 2;
	}

	vector<string> attrNames;
	vector<FIELD_TYPE> attrTypes;
	// select all?
	if (columns->NumElements() == 1 
			&& !strcmp(columns->Nth(0)->GetColumnName(),"*")) 
	{
		attrNames = schema.getFieldNames();
		attrTypes = schema.getFieldTypes();
	}
	else {
		for(int i =0;i< columns->NumElements();i++)
		{
			attrNames.push_back(columns->Nth(i)->GetColumnName());
			attrTypes.push_back(schema.getFieldType(columns->Nth(i)->GetColumnName()));
		}
	}
	Schema rSchema = Schema(attrNames, attrTypes);

	// Global Var: Set the sequence of tables. Used for WHERE
	while(tableList.NumElements())
		tableList.RemoveAt(0);
	tableList.Append(relationName);

	vector<Tuple> resultTuples;
	Block* block = mem.getBlock(0);
	if ((!orderBy && !distinct ) || numPasses == 1)
	{
		// no need to store anywhere. Read, check condition, display output!
		for (int i = 0; i < relationPtr->getNumOfBlocks(); i++) 
		{
			block->clear();
			relationPtr->getBlock(i, 0);
			vector<Tuple> tuples_block = block->getTuples();
			for (vector<Tuple>::iterator lit = tuples_block.begin(); lit != tuples_block.end(); lit++) 
			{	
				tupleList.Append(&*lit);
				bool select = true;
				if (condition)
					select = condition->Evaluate(this)->GetBoolValue();

				if (select)
					resultTuples.push_back(*lit);
				
				tupleList.RemoveAt(tupleList.NumElements()-1);
			}
		}
		if (distinct)
			DistinctOnePass(resultTuples, attrNames);
		if (orderBy)
			OrderByOnePass(resultTuples, orderBy->GetColumnName());
	}
	else
	{
		// NOTE: since we don't know how many passes will be required. Lets try project/select first. 
		// If the tuple count exceeds memory limit, continue from second pass.
		int tuplesPerBlock = FIELDS_PER_BLOCK/attrNames.size();
		int tuplesInMem = mem.getMemorySize() * tuplesPerBlock;

		vector<Tuple> tuples_block;
		vector<Tuple> tuples;

		vector<Relation*> sublists;
		vector<string> sl_names;

		int tupleIndex = 0;
		int slCount = 0;
		char slname[100];
		for (int i = 0; i < relationPtr->getNumOfBlocks(); i++) 
		{
			block->clear();
			relationPtr->getBlock(i, 0);
			vector<Tuple> tuples_block = block->getTuples();
			for (vector<Tuple>::iterator lit = tuples_block.begin(); lit != tuples_block.end(); lit++) 
			{	
				tupleList.Append(&*lit);
				bool select = true;
				if (condition)
					select = condition->Evaluate(this)->GetBoolValue();

				if (select)
				{
					tupleIndex++;
					if (tupleIndex > tuplesInMem)
					{
						cout << "Tuples in SL: " << resultTuples.size() << endl;
						if (distinct)
							DistinctOnePass(resultTuples, attrNames);
						if (orderBy)
							OrderByOnePass(resultTuples, orderBy->GetColumnName());
						sprintf(slname, "%s_sl_%d", relationName.c_str(), slCount++);
						Relation* sl = schema_manager.createRelation(slname, rSchema);
						sublists.push_back(sl);
						sl_names.push_back(slname);
						StoreRelation(sl, resultTuples);
						resultTuples.clear();
						//PrintRelation(sl, mem.getBlock(0));
						tupleIndex = 1;
					}
					resultTuples.push_back(*lit);
				}
				tupleList.RemoveAt(tupleList.NumElements()-1);
			}
		}
		if (resultTuples.size())
		{
			if (distinct)
				DistinctOnePass(resultTuples, attrNames);
			if (orderBy)
				OrderByOnePass(resultTuples, orderBy->GetColumnName());
			sprintf(slname, "%s_sl_%d", relationName.c_str(), slCount++);
			Relation* sl = schema_manager.createRelation(slname, rSchema);
			sublists.push_back(sl);
			sl_names.push_back(slname);
			if (sublists.size() > 1)
			{
				StoreRelation(sl, resultTuples);
				resultTuples.clear();
			}
			//PrintRelation(sl, mem.getBlock(0));
		}
		if (sublists.size() > 1)
		{
			resultTuples.clear();
			if(sublists.size() > 1 && distinct)
				DistinctSecondPass(&sublists, attrNames, &resultTuples);

			if(sublists.size() > 1 && orderBy)
			{
				MergeSortedSublists(&sublists, orderBy->GetColumnName(), &resultTuples);
				//OrderBySecondPass(sublists, attrNames, resultTuples);
				//OrderByOnePass(resultTuples,orderBy->GetColumnName());
			}
			for (int i = 0; i < sl_names.size(); i++)
				schema_manager.deleteRelation(sl_names[i]);
		}
		for (int i = 0; i < sublists.size(); i++)
		{
			//PrintRelation(sublists[i], mem.getBlock(0));
			schema_manager.deleteRelation(sl_names[i]);
		}
		
	}
	_print_tuples(attrNames, resultTuples);
	return NULL;
}

void PrintSet(std::set<string>* inp)
{
	std::set<string>::iterator i = inp->begin();
	for (; i != inp->end(); i++)
		cout << "\t" << *i << endl;
}

bool StorageManagerWrapper::LoadRelation(Relation* relation, vector<Tuple> &list, bool distinct)
{
	int remaining = relation->getNumOfBlocks();
	int canRead = mem.getMemorySize();
	int index = 0;
	while(1)
	{
		if (remaining == 0)
			break;

		for (int i = 0; i < mem.getMemorySize(); i++)
			mem.getBlock(i)->clear();

		int readCount = remaining < canRead ? remaining : canRead-1;
		bool loaded = relation->getBlocks(index, 0, readCount);
		if (!loaded)
			return false;

		index += readCount;
		remaining -= readCount;
		Assert(remaining >= 0);

		for (int i = 0; i < readCount; i++)
		{
			vector<Tuple> temp = mem.getBlock(i)->getTuples();
			for (int j=0; j < temp.size(); j++)
				list.push_back(temp[j]);
		}
	}
	if (distinct && relation->getNumOfBlocks() < mem.getMemorySize())
		DistinctOnePass(list, relation->getSchema().getFieldNames());
	return true;
}
		
bool StorageManagerWrapper::StoreRelation(Relation* relation, vector<Tuple> &list)
{
	Schema schema = relation->getSchema();
	int tuplesPerBlock = FIELDS_PER_BLOCK / schema.getNumOfFields();
	int numBlocksReq = list.size() / tuplesPerBlock;

	int remaining = numBlocksReq;
	int canWrite = mem.getMemorySize();
	int index = 0;
	while(1)
	{
		if (remaining == 0)
			break;

		for (int i = 0; i < mem.getMemorySize(); i++)
			mem.getBlock(i)->clear();

		int writeCount = remaining < canWrite ? remaining : canWrite;
		int tupleIndex = 0;
		for (int i = 0; i < writeCount; i++)
		{
			Block* block_ptr = mem.getBlock(i);
			block_ptr->clear(); //clear the block
			while(! block_ptr->isFull())
			{
				block_ptr->appendTuple(list[tupleIndex++]);
			}
		}
		bool result = relation->setBlocks(index, 0, writeCount); 
		if (!result)
		{
			cout << "Failed to write temporary relation!\n";
			return false;
		}

		index += writeCount;
		remaining -= writeCount;
		Assert(remaining >= 0);
	}
	return true;
}
	
void GetRequiredFieldsSchema(
		SchemaManager  &schema_manager,
		List<EntityName*> *tableNames,
		const List<ColumnName*>* columns,
		//string except, // omit join attribute
		//Schema& resultSchema)
		vector<string>& result_fn,
		vector<FIELD_TYPE>& result_ft)
{
	//vector<string> result_fn;
	//vector<FIELD_TYPE> &result_ft;

	if (columns->NumElements() == 1 
			&& !strcmp(columns->Nth(0)->GetColumnName(),"*")) 
	{
		Relation* rel;
		//attrName = schema.getFieldNames();
		for(int i = 0; i < tableNames->NumElements(); ++i)
		{
			rel = schema_manager.getRelation(tableNames->Nth(i)->GetName());
			vector<string> fn = rel->getSchema().getFieldNames();
			vector<FIELD_TYPE> ft = rel->getSchema().getFieldTypes();
			for(int j = 0; j < fn.size(); ++j)
			{
				string fname = tableNames->Nth(i)->GetName(); fname.append("."); fname.append(fn[j]);
				result_fn.push_back(fname);
				result_ft.push_back(ft[j]);
			}
		}
	}
	else
	{
		Relation* rel;
		for (int i = 0; i < columns->NumElements(); i++)
		{
			ColumnName* cn = columns->Nth(i);
			string fname = cn->GetTableName();
			fname.append(".");
			fname.append(cn->GetColumnName());
			result_fn.push_back(fname);	
			rel = schema_manager.getRelation(cn->GetTableName());
			FIELD_TYPE ftype = rel->getSchema().getFieldType(cn->GetColumnName());
			result_ft.push_back(ftype);
		}
	}
	/*
	else {
		for(int i =0;i< columns->NumElements();i++)
			attrName.push_back(columns->Nth(i)->GetColumnName());
	}
	*/

}

void GetNaturalJoinSchema(
		//SchemaManager schema_manager,
		List<EntityName*> *tableNames,
		vector<Relation*> &relationPtrs,
		//const List<ColumnName*>* columnNames,
		string except, // omit join attribute
		//Schema& resultSchema)
		vector<string>& result_fn,
		vector<FIELD_TYPE>& result_ft)
{
	// now add other fields
	//string tbnames[] = { "_tb1", "_tb2", "_tb3", "_tb4" };
	for(int i = 0; i < relationPtrs.size(); ++i)
	{
		vector<string> fn = relationPtrs[i]->getSchema().getFieldNames();
		vector<FIELD_TYPE> ft = relationPtrs[i]->getSchema().getFieldTypes();
		for(int j = 0; j < fn.size(); ++j)
		{
			if (fn[j] != except)
			{
				string fname = tableNames->Nth(i)->GetName(); fname.append("."); fname.append(fn[j]);
				//result_fn.push_back(fn[j].append(tableNames->Nth(i)->GetName()));
				result_fn.push_back(fname);
				result_ft.push_back(ft[j]);
			}
		}
	}
}

void DeleteSortedSublists(string tableName, vector<Relation*> *sl, SchemaManager &schema_manager)
{
	char slname[100];
	for (int i=0; i < sl->size(); i++)
	{
		sprintf(slname, "%s_sl_%d", tableName.c_str(), i+1);
		schema_manager.deleteRelation(slname);
	}
}

/*
void StorageManagerWrapper::JoinTuples(vector<Tuple> &list1, 
		vector<Tuple> &list2, 
		vector<Tuple> &result,
		Schema schema1,
		Schema schema2,
		string joinAttr,
		Expr* condition)
		*/

List<TUPLE*>* StorageManagerWrapper::_ExecuteMultipleTableSelect(List<EntityName*> *relationNames,const List <ColumnName*>* columns,Expr* condition,bool distinct,ColumnName* orderBy)
{
	cout << endl;
	while(tableList.NumElements())
		tableList.RemoveAt(0);
	for (int i = 0; i < relationNames->NumElements(); i++)
		tableList.Append(relationNames->Nth(i)->GetName());

	vector<Relation*> relationPtrs;
	for (int i = 0; i < relationNames->NumElements(); i++)
	{
		string relName = relationNames->Nth(i)->GetName();

		Relation* rel = schema_manager.getRelation(relName);
		if (!rel)
		{
			cout << "\nERROR: Relation \"" << relName <<  "\" doesn't exist!\n";
			return NULL;
		}
		Assert(rel);
		relationPtrs.push_back(rel);
	}

	// Try to find out how many fields are required from each relation. So that we can do projection when scanning 
	std::set<string> *set1 = GetRequiredFields(schema_manager, relationNames->Nth(0)->GetName(), columns, condition, orderBy);
	std::set<string> *set2 = GetRequiredFields(schema_manager, relationNames->Nth(1)->GetName(), columns, condition, orderBy);

	//cout << "Required fields to project from \"" << relationNames->Nth(0)->GetName() << "\"\n";
	//PrintSet(set1);
	//cout << "Required fields to project from \"" << relationNames->Nth(1)->GetName() << "\"\n";
	//PrintSet(set2);

	//try to create new in-memory table

	int tuplesPerBlock1 = FIELDS_PER_BLOCK/set1->size();
	int tuplesPerBlock2 = FIELDS_PER_BLOCK/set2->size();

	int numBlocksRequired1 = relationPtrs[0]->getNumOfTuples() / tuplesPerBlock1;
	int numBlocksRequired2 = relationPtrs[1]->getNumOfTuples() / tuplesPerBlock2;

	// two table join
	int passes = 2;
	int inMemRelIndex = -1;
	int numBlocksReq = numBlocksRequired1 + numBlocksRequired2;
	if (numBlocksReq <= mem.getMemorySize())
	{
		cout << "One Pass, In-Memory: Both the tables require \"" << numBlocksReq << "blocks\" combined.\n";
		passes = 0; // in memory join
	}
	else if (numBlocksRequired1 < mem.getMemorySize())  // need one block to read other relation
	{
		passes = 1;
		inMemRelIndex = 0;

		cout << "One Pass: \"" << relationNames->Nth(0)->GetName() << "\" requires " << numBlocksRequired1 << " Blocks in main memory." << endl;
	}
	else if (numBlocksRequired2 < mem.getMemorySize())
	{
		passes = 1;
		inMemRelIndex = 1;

		cout << "One pass: \"" << relationNames->Nth(1)->GetName() << "\" requires " << numBlocksRequired2 << " Blocks in main memory." << endl;
	}
	else
		cout << "Two Pass: Can't fit any in MainMemory!\n";

	vector<ColumnAccess*> joinAttrs;
	condition->GetJoinAttributes(joinAttrs);
	string joinAttr = joinAttrs[0]->GetColumnName(); // assume same named col in table1 n table 2

	/*
	for (int i = 0; i < joinAttrs.size() ; i++)
	{
		cout << joinAttrs[i]->GetTableName() << "." << joinAttrs[i]->GetColumnName() << endl;
	}
	*/

	Schema schema1 = relationPtrs[0]->getSchema();
	Schema schema2 = relationPtrs[1]->getSchema();
	int offset1 = schema1.getFieldOffset(joinAttr);
	int offset2 = schema2.getFieldOffset(joinAttr);

	if (offset1 == -1 || offset2 == -1)
	{
		cout << "Error: Invalid Join Attribute\n";
		return NULL;
	}

	FIELD_TYPE joinFieldType = schema1.getFieldType(joinAttr);
	// in the given queries, all joins are on integer fields
	Assert(joinFieldType == INT);

	vector<string> jFieldNames;
	vector<FIELD_TYPE> jFieldTypes;
	jFieldNames.push_back(joinAttr);
	jFieldTypes.push_back(joinFieldType);
	GetNaturalJoinSchema(relationNames, relationPtrs, joinAttr, jFieldNames, jFieldTypes);
	Schema jSchema = Schema(jFieldNames, jFieldTypes);

	string jName = relationNames->Nth(0)->GetName();
	jName.append("_");
	jName.append(relationNames->Nth(1)->GetName());

	//Relation* jRelation = _CreateTable(jName, jFieldNames, jFieldTypes);
	Relation* jRelation = schema_manager.createRelation(jName, jSchema);
	Tuple jTuple = jRelation->createTuple();

	vector<Tuple> list1;
	vector<Tuple> list2;
	if (passes == 0 || passes == 1)
	{
		// full in-memory 
		// read both and join
		LoadRelation(relationPtrs[0], list1, distinct);	
		LoadRelation(relationPtrs[1], list2, distinct);	
		/*
		bool loaded = relationPtrs[0]->getBlocks(0, 0, relationPtrs[0]->getNumOfBlocks());
		for (int i = 0; i < relationPtrs[0]->getNumOfBlocks(); i++)
		{
			vector<Tuple> temp = mem.getBlock(i)->getTuples();
			for (int i=0; i < temp.size(); i++)
				list1.push_back(temp[i]);
		}
		
		loaded = relationPtrs[1]->getBlocks(0, 0, relationPtrs[1]->getNumOfBlocks());
		for (int i = 0; i < relationPtrs[1]->getNumOfBlocks(); i++)
		{
			vector<Tuple> temp = mem.getBlock(i)->getTuples();
			for (int i=0; i < temp.size(); i++)
				list2.push_back(temp[i]);
		}
		*/
		OrderByOnePass(list1, joinAttrs[0]->GetColumnName());
		OrderByOnePass(list2, joinAttrs[1]->GetColumnName());

		//vector<Tuple> result;
		//JoinTuples(list1, list2, result);
		//DistinctOnePass(resultTable);
		//_print_tuples(jFieldNames, resultTable);
		//_DropTable(jName);
		//schema_manager.deleteRelation(jName);
	}
	else if (passes == 2)
	{
		int numSublist1 = mem.getMemorySize()/numBlocksRequired1;
		int numSublist2 = mem.getMemorySize()/numBlocksRequired2;
		// read once, write once
		vector<Relation*> *sortedSublists1 = CreateSortedSublists(relationNames->Nth(0)->GetName(), joinAttr);
		vector<Relation*> *sortedSublists2 = CreateSortedSublists(relationNames->Nth(1)->GetName(), joinAttr);

		// second pass 
		MergeSublists(&relationPtrs, sortedSublists1, sortedSublists2, joinAttr, jRelation, list1, list2);  
		DeleteSortedSublists(relationNames->Nth(0)->GetName(), sortedSublists1, schema_manager);
		DeleteSortedSublists(relationNames->Nth(1)->GetName(), sortedSublists2, schema_manager);
		//vector<string> tempFields1;
		//vector<FIELD_TYPE> tempFieldTypes1;

		/*
		CopyFromSetToVector(*set1, tempFields1);
		Schema modifiedTable1 = CreateSchema(*set1, schema1);
		Schema modifiedTable2 = CreateSchema(*set1, schema1);
		*/
		// create sublists and sort them using orderBySinglePass
		/*
		vector<Tuple> list1;
		vector<Tuple> list2;
		bool loaded = relationPtrs[0]->getBlocks(0, 0, relationPtrs[0]->getNumOfBlocks());
		for (int i = 0; i < relationPtrs[0]->getNumOfBlocks(); i++)
		{
			vector<Tuple> temp = mem.getBlock(i)->getTuples();
			for (int i=0; i < temp.size(); i++)
				list1.push_back(temp[i]);
		}
		
		loaded = relationPtrs[1]->getBlocks(0, 0, relationPtrs[1]->getNumOfBlocks());
		for (int i = 0; i < relationPtrs[1]->getNumOfBlocks(); i++)
		{
			vector<Tuple> temp = mem.getBlock(i)->getTuples();
			for (int i=0; i < temp.size(); i++)
				list2.push_back(temp[i]);
		}

		OrderByOnePass(list1, joinAttr);
		OrderByOnePass(list2, joinAttr);
		*/
	}

	vector<string> rFields;
	vector<FIELD_TYPE> rTypes;
	GetRequiredFieldsSchema(schema_manager, relationNames, columns, rFields, rTypes); 
	Schema rSchema = Schema(rFields, rTypes);

	string rName = jName;
	rName.append("_results");
	Relation* rRelation = schema_manager.createRelation(rName, rSchema); 
	Tuple rTuple = rRelation->createTuple();

	vector<Tuple>::iterator iter1 = list1.begin();
	vector<Tuple>::iterator iter2 = list2.begin();
	/*
	cout << "Table1\n";
	_print_tuples(schema1.getFieldNames(), list1);
	cout << "Table2\n";
	_print_tuples(schema2.getFieldNames(), list2);
	*/
	vector<Tuple> resultTuples;
	while (iter1 != list1.end() && iter2 != list2.end())
	{
		int field1 = iter1->getField(offset1).integer;
		int field2 = iter2->getField(offset2).integer;
		if (field1 <= field2)
		{
			bool add = false;
			if (field1 == field2)
			{
				while(tupleList.NumElements())
					tupleList.RemoveAt(0);
				tupleList.Append(&*iter1);
				tupleList.Append(&*iter2);
				if (condition)
					add = condition->Evaluate(this)->GetBoolValue();
				if (add)
				{
					int index = 0;
					jTuple.setField(index++, field1);
					for (int i = 0; i < iter1->getNumOfFields(); i++)
					{
						if (i != offset1)
						{
							Field field = iter1->getField(i);
							if (schema1.getFieldType(i) == INT)
							{
								jTuple.setField(index++, field.integer);
							}
							else
							{
								jTuple.setField(index++, *field.str);
							}
						}
					}
					for (int i = 0; i < iter2->getNumOfFields(); i++)
					{
						if (i != offset2)
						{
							Field field = iter2->getField(i);
							if (schema2.getFieldType(i) == INT)
								jTuple.setField(index++, field.integer);
							else
								jTuple.setField(index++, *field.str);
						}
					}
					// apply projection
					for (int i = 0; i < rFields.size(); i++)
					{
						string field = rFields[i]; 
						if (field.find(joinAttr) != string::npos)
							rTuple.setField(field, field1);
						else
						{
							//Field f = jTuple.getField(field);
							if (jSchema.getFieldType(field) == INT)
								rTuple.setField(field, jTuple.getField(field).integer);
							else 
								rTuple.setField(field, *(jTuple.getField(field).str));
						}
					}
					//resultTuples.push_back(jTuple);
					resultTuples.push_back(rTuple);
				}
			}
			// increment iter1
			iter1++;
		}
		else
			iter2++;
	}
	if (distinct || orderBy)
	{
		// lets see if we need to store it in disk before applying these operations.
		int pass;
		int tuplesPerBlock = FIELDS_PER_BLOCK / rSchema.getNumOfFields();
		int numBlocksReq = resultTuples.size() / tuplesPerBlock;
		if (numBlocksReq <= mem.getMemorySize())
			pass = 1;
		else
		{
			StoreRelation(rRelation, resultTuples); // have to store before doing other operation! sigh!
			pass = 2;
		}
		// apply distinct first
		//vector<Tuple> unique;
		int prevSize = resultTuples.size();
		if (distinct)
		{
			if (pass == 1)
				DistinctOnePass(resultTuples, rFields);
			else
			{
				resultTuples.clear();
				DistinctTwoPass(rName, rFields, resultTuples);
			}
		}
		if (orderBy)
		{
			string orderByName = orderBy->GetTableName();
			orderByName.append(".");
			orderByName.append(orderBy->GetColumnName());
			bool sort = false;
			for (int i = 0; i < rFields.size(); i++)
				if (rFields[i] == orderByName)
				{
					sort = true;break;
				}
			if(sort)
			{
				pass = 2;
				if (prevSize == resultTuples.size())
				{
					resultTuples.clear();
					OrderByTwoPass(rName, orderByName, rFields, resultTuples);
				}
				else
				{
					int blocksReq = resultTuples.size()/tuplesPerBlock;
					if (blocksReq <= mem.getMemorySize())
						OrderByOnePass(resultTuples, orderByName);
					else
					{
						Assert(0);
						resultTuples.clear();
						OrderByTwoPass(rName, orderByName, rFields, resultTuples);
					}
				}
			}
		}
	}
	_print_tuples(rFields, resultTuples);
	
	schema_manager.deleteRelation(jName);
	schema_manager.deleteRelation(rName);
	return NULL;
}

Tuple GetMinimum(vector<vector<Tuple> >* table, int &index, int offset)
{
	index = 0;
	vector<Tuple> vect = table->at(0);
	//int minValue = table->at(0)[0].getField(offset).integer;
	int minValue = (vect[0].getField(offset)).integer;
	for (int i = 1; i < table->size(); i++)
	{
		vector<Tuple> vect = table->at(i);
		int current = (vect[0].getField(offset)).integer;
		//int current = (table[i][0].getField(offest)).integer;
		if (minValue > current)
		{
			index = i;
			minValue = current;
		}
	}
	return (table->at(index)[0]);
}

vector<Tuple>* StorageManagerWrapper::DistinctSecondPass(vector<Relation*> *sublists, 
		vector<string> attrNames,
		vector<Tuple>* merged)
{
	vector<Tuple>* result = merged; 
	for (int i = 0; i < sublists->size(); i++)
	{
		Relation* sublist = sublists->at(i);
		for (int j = 0; j < sublist->getNumOfBlocks(); j++)
		{
			Block* block = mem.getBlock(j);
			block->clear();
			sublist->getBlock(j, j);
			vector<Tuple> temp = block->getTuples();
			for (int k = 0; k < temp.size(); k++)
				result->push_back(temp[k]);
		}
	}
	DistinctOnePass(*merged, attrNames);
	return result;
}

vector<Tuple>* StorageManagerWrapper::MergeSortedSublists(vector<Relation*> *sublists, 
		string joinAttr,
		vector<Tuple>* merged)
{
	int offset = sublists->at(0)->getSchema().getFieldOffset(joinAttr);
	vector<Tuple>* result = merged; 
	for (int i = 0; i < sublists->size(); i++)
	{
		Relation* sublist = sublists->at(i);
		for (int j = 0; j < sublist->getNumOfBlocks(); j++)
		{
			Block* block = mem.getBlock(j);
			block->clear();
			sublist->getBlock(j, j);
			vector<Tuple> temp = block->getTuples();
			for (int k = 0; k < temp.size(); k++)
				result->push_back(temp[k]);
		}
	}
	OrderByOnePass(*merged, joinAttr);
	return result;
}

// one tuple joins with the sorted sublists of other table
void JoinTuple(Tuple tuple, vector<vector<Tuple> > *table, 
		vector<int> *memMap, vector<int> *nextToRead, 
		int offset1, int offset2, Relation* targetRelation)
{
	int value = tuple.getField(offset1).integer;
	for (int i = 0; i < table->size(); i++)
	{
		vector<Tuple> currentVect = table->at(i);
		int currentVal = currentVect[0].getField(offset2).integer;
		int j;
		for (j=0; j < currentVect.size() && value == currentVal; j++)
		{
			//Join(tuple, 
		}
	}
}
			
void deleteTop(vector<vector<Tuple> > *table1, int tableIndex, vector<int> *memMap, vector<int> *nextToRead1){}

bool StorageManagerWrapper::MergeSublists(
		vector<Relation*>* relationPtrs, 
		vector<Relation*>* sublists1, 
		vector<Relation*>* sublists2, 
		string joinAttr,
		Relation* jRelation,
		vector<Tuple> &list1,
		vector<Tuple> &list2)
{
	int blocksInUse = sublists1->size() + sublists2->size();
	/*
	if (blocksInUse > mem.getMemorySize())
	{
		cout << "Number of Sublists exceeded main memory size. Cannot merge in Two Pass!\n";
		return false;
	}
	*/
	int offset1 = relationPtrs->at(0)->getSchema().getFieldOffset(joinAttr);
	int offset2 = relationPtrs->at(1)->getSchema().getFieldOffset(joinAttr);
	vector<vector<Tuple> > table1;
	vector<vector<Tuple> > table2;
	int blockIndex = 0;
	vector<int> nextToRead1;
	vector<int> nextToRead2;
	vector<int> memMap1;
	vector<int> memMap2;
	vector<Relation*> sublistss1;

	for (int i = 0; i < sublistss1.size(); i++)
	{
		memMap1.push_back(blockIndex);
		Block* block = mem.getBlock(blockIndex);
		block->clear();

		if (! sublists1->at(i)->getBlock(0, blockIndex++))
			return false;
		table1.push_back(block->getTuples());
		nextToRead1.push_back(1);
	}

	for (int i = 0; i < sublistss1.size(); i++)
	{
		memMap2.push_back(blockIndex);
		Block* block = mem.getBlock(blockIndex);
		block->clear();

		if (! sublists2->at(i)->getBlock(0, blockIndex++))
			return false;
		table2.push_back(block->getTuples());
		nextToRead2.push_back(1);
	}

	while (table1.size() && table2.size() && !blocksInUse)
	{
		int index1, index2;
		Tuple tuple1 = GetMinimum(&table1, index1, offset1);
		Tuple tuple2 = GetMinimum(&table2, index2, offset2);
		int field1 = tuple1.getField(joinAttr).integer;
		int field2 = tuple2.getField(joinAttr).integer;
		if (field1 <= field2)
		{
			if (field1 == field2)
			{
				// try to join tuple1 with all in the second table
				JoinTuple(tuple1, &table2, &memMap2, &nextToRead2, offset1, offset2, jRelation);
			}
			deleteTop(&table1, index1, &memMap1, &nextToRead1);
		}
		else
			deleteTop(&table2, index2, &memMap2, &nextToRead2);
	}
	MergeSortedSublists(sublists1, joinAttr, &list1);
	//OrderByOnePass(list1, joinAttr);
	//vector<Tuple> tab2;
	MergeSortedSublists(sublists2, joinAttr, &list2);
	//OrderByOnePass(list2, joinAttr);
	//for (int i=0; i < table1.size(); i++)
	//{
	//	Tuple tuple1 = Minimum(&table1, index1, offset1);
	//}
	return true;
}

void PrintRelation(Relation* r, Block* block)
{
	//Block* block = mem.getBlock(0);
	Schema tuple_schema = r->getSchema();
	for (int i = 0; i < r->getNumOfBlocks(); i++)
	{
		block->clear();
		r->getBlock(i, 0);
		vector<Tuple> temp = block->getTuples();
		for (int i = 0; i < temp.size(); i++)
		{
			Tuple tuple = temp[i];
			for (int i=0; i<tuple.getNumOfFields(); i++) {
				if (tuple_schema.getFieldType(i)==INT)
					cout << tuple.getField(i).integer << "\t";
				else
					cout << *(tuple.getField(i).str) << "\t";
			}
		}
		cout << endl;
	}
	cout << "=================\n";
}

vector<Relation*> *StorageManagerWrapper::CreateSortedSublists(string tableName, string orderBy) //, set<string> requiredFields)
{
	Relation* relation = schema_manager.getRelation(tableName);
	Schema schema = relation->getSchema();
	int canRead = mem.getMemorySize();
	int remaining = relation->getNumOfBlocks();

	int sublistCount = 1;
	char slname[100];
	int index = 0;
	vector<Relation*> *result = new vector<Relation*>;
	while(1)
	{
		if (remaining == 0)
		{
			cout << endl << sublistCount-1 << " sorted sublist relations created."; 
			break;
		}

		sprintf(slname, "%s_sl_%d", tableName.c_str(), sublistCount++);
		
		int readCount = remaining < canRead? remaining:canRead-1;
		//if (readCount != 1) --readCount;
		//--readCount; // looks like a bug with StorageManager
		for (int i = 0; i < mem.getMemorySize(); i++)
			mem.getBlock(i)->clear();

		bool loaded = relation->getBlocks(index, 0, readCount);  // -1 for indexing stuff
		Assert(loaded);
		index += readCount;
		remaining -= readCount;
		Assert(remaining >= 0);

		vector<Tuple> sublist;
		for (int i = 0; i < readCount; i++)
		{
			vector<Tuple> temp = mem.getBlock(i)->getTuples();
			for (int j=0; j < temp.size(); j++)
				sublist.push_back(temp[j]);
		}
		OrderByOnePass(sublist, orderBy); // in-memory sorting

		int tupleIndex = 0;
		for (int i = 0; i < readCount; i++)
		{
			Block* block_ptr = mem.getBlock(i);
			block_ptr->clear(); //clear the block
			while(! block_ptr->isFull())
			{
				block_ptr->appendTuple(sublist[tupleIndex++]);
			}
		}
		Relation* slRelation = schema_manager.createRelation(slname, schema);
		slRelation->setBlocks(0, 0, readCount); // -1 while indexing
		result->push_back(slRelation);

		//cout << "Printing Relation: " << slname << endl;
		//PrintRelation(slRelation, mem.getBlock(0));
	}
	return result;
}

/*
void StorageManagerWrapper::CrossJoin(const vector<Relation*> relationPtrs,
		bool distinct,ColumnName* orderBy)
{

}
*/
/*
void TwoPassSelect()
{
// Global Var: Set the sequence of tables. Used for WHERE
	while(tableList.NumElements())
		tableList.RemoveAt(0);
	tableList.Append(relationName);

	Block* block = mem.getBlock(0);
	
	vector<Tuple> tuples_block;
	vector<Tuple> tuples;
	if (numPasses == 1)
	{
		cout <<"One Pass Algorithm"<<endl;

	for (int i = 0; i != numBlocks; i++) 
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
			
/\*				for (vector<string>::iterator iter = attrName.begin(); iter != attrName.end(); iter++) 
				{
					if (schema.getFieldType(*iter) == STR20)
						cout << *(lit->getField(schema.getFieldOffset(*iter)).str) << "\t";
					else if (schema.getFieldType(*iter) == INT)
						cout << lit->getField(schema.getFieldOffset(*iter)).integer << "\t";
				}
				cout << endl; *\/
			}
			tupleList.RemoveAt(tupleList.NumElements()-1);
		}
	
	}
	if(orderBy)
	{
	//	OrderByOnePass(tuples,orderBy->GetColumnName());
		OrderByTwoPass(relationName,orderBy->GetColumnName(),attrName );
	}
	if(distinct)
	{
		DistinctTwoPass(relationName,attrName);

//		DistinctOnePass(tuples,attrName);
	}
	//_print_tuples(attrName,tuples);TEMPERORY
	
}

}
*/

void StorageManagerWrapper::DistinctOnePass(vector<Tuple>& tuples,const vector<string> &attrName)
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
void StorageManagerWrapper::DistinctTwoPass(
		const string relationName,
		const vector<string> &attrName, 
		vector<Tuple> &distinctTuples)
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
			vector<Tuple> tempStorage;
			for (it=my_imap.begin(); it != my_imap.end(); ++it) {
				tempStorage.push_back(it->second);
						//tuples.push_back(it->second);
				//appendTupleToRelation(tmpRelationPtr ,mem,OutputBufferIndex(),it->second);
			}
			StoreRelation(tmpRelationPtr, tempStorage);
		}
		else
		{
			multimap<string, Tuple>::iterator sit;
			vector<Tuple> tempStorage;
			for (sit=my_smap.begin(); sit != my_smap.end(); ++sit) {
				tempStorage.push_back(sit->second);
				// tuples.push_back(sit->second);
				//appendTupleToRelation(tmpRelationPtr ,mem,OutputBufferIndex(),sit->second);
			}
			StoreRelation(tmpRelationPtr, tempStorage);
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
	//cout<<"After first pass we read from the temp tablee"<<endl;
	//cout<<"size        :"<<tuples.size()<<endl;
	//_print_tuples(attrName,tuples);
//***********
	//vector<Tuple> distinctTuples;
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
	//cout<<"************************************************************"<<endl;
	//_print_tuples(attrName,distinctTuples);
}
	
//dont forget to delete tmp relation
//if no distinct then delete tmp relation else pass tmp relation tp distinct
void StorageManagerWrapper::OrderByTwoPass(const string relationName,
		const string attr,
		const vector<string> &attrName,
		vector<Tuple> &resultTuples)
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

	string tempRelationName = "tmpRelation";
	Relation *tmpRelationPtr = schema_manager.createRelation(tempRelationName,schema);
	//string type = schema.getFieldType(fieldName);
	int segmentNum = relationSize/(memSize - 1);
	vector<pair<int, int> > firstIndicator;
	vector<pair<int, int> >secondIndicator;
	vector<Tuple> tuples_block;
		
	//vector<Tuple> resultTuples;
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
			vector<Tuple> tempStorage;
			for (it=my_imap.begin(); it != my_imap.end(); ++it) {
				tempStorage.push_back(it->second);
						//resultTuples.push_back(it->second);
				//appendTupleToRelation(tmpRelationPtr ,mem,OutputBufferIndex(),it->second);
			}
			StoreRelation(tmpRelationPtr, tempStorage);
		}
		else
		{
			multimap<string, Tuple>::iterator sit;
			vector<Tuple> tempStorage;
			for (sit=my_smap.begin(); sit != my_smap.end(); ++sit) {
				tempStorage.push_back(sit->second);
				// resultTuples.push_back(sit->second);
				appendTupleToRelation(tmpRelationPtr ,mem,OutputBufferIndex(),sit->second);
			}
			StoreRelation(tmpRelationPtr, tempStorage);
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
			resultTuples.push_back(*lit);
	}
//****
	//multimap<string, Tuple> my_smap;
     cout<<"Size after two pass1"<<resultTuples.size()<<endl;
	//multimap<int, Tuple> my_imap;
    my_smap.clear();
	my_imap.clear();
	if(schema.getFieldType(attr) == INT){	

		for(int i=0;i<resultTuples.size();i++){

			my_imap.insert(pair<int, Tuple>(resultTuples[i].getField(attr).integer,resultTuples[i]));
		}
		resultTuples.clear();
		multimap<int, Tuple>::const_iterator it;
		for (it=my_imap.begin(); it != my_imap.end(); ++it) {
			 resultTuples.push_back(it->second);
		}

	}else{

		for(int i=0;i<resultTuples.size();i++){
			my_smap.insert(pair<string,Tuple> (*(resultTuples[i].getField(attr).str),resultTuples[i]));
		}
		resultTuples.clear();
		multimap<string, Tuple>::const_iterator sit;
		for (sit=my_smap.begin(); sit != my_smap.end(); ++sit) {
			 resultTuples.push_back(sit->second);
		}
	}
	//cout<<"Size after two pass"<<resultTuples.size();
	//_print_tuples(attrName, tuples);
}

void StorageManagerWrapper::_print_tuples(const vector<string> &attrName, const vector<Tuple> & tuples )
{
	cout << endl;
	for (int i = 0; i < attrName.size(); i++)
		cout << attrName[i] << " | ";
	cout << endl;
	for (int i = 0; i < attrName.size(); i++)
		cout <<"======\t";
	cout << endl;
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
void StorageManagerWrapper::OrderByOnePass(vector<Tuple> & tuples,const string attr)
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
		//cout << "The relation is empty" << endl;
		//cout << "Get the handle to the memory block " << memory_block_index << " and clear it" << endl;
		block_ptr=mem.getBlock(memory_block_index);
		block_ptr->clear(); //clear the block
		block_ptr->appendTuple(tuple); // append the tuple
		//cout << "Write to the first block of the relation" << endl;
		relation_ptr->setBlock(relation_ptr->getNumOfBlocks(),memory_block_index);
	} else {
		//cout << "Read the last block of the relation into memory block 5:" << endl;
		relation_ptr->getBlock(relation_ptr->getNumOfBlocks()-1,memory_block_index);
		block_ptr=mem.getBlock(memory_block_index);

		if (block_ptr->isFull()) {
			//cout << "(The block is full: Clear the memory block and append the tuple)" << endl;
			block_ptr->clear(); //clear the block
			block_ptr->appendTuple(tuple); // append the tuple
			//cout << "Write to a new block at the end of the relation" << endl;
			relation_ptr->setBlock(relation_ptr->getNumOfBlocks(),memory_block_index); //write back to the relation
		} else {
			//cout << "(The block is not full: Append it directly)" << endl;
			block_ptr->appendTuple(tuple); // append the tuple
			//cout << "Write to the last block of the relation" << endl;
			relation_ptr->setBlock(relation_ptr->getNumOfBlocks()-1,memory_block_index); //write back to the relation
		}
	}  
}

/*
List<TUPLE*>* StorageManagerWrapper::CrossJoin(
		List<EntityName*> *relationNames,
		const List <ColumnName*>* columns,
		Expr* condition,
		bool distinct,
		ColumnName* orderBy)
{

}
*/
