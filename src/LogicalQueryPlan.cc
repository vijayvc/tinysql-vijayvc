#include "LogicalQueryPlan.h"

LogicalQueryPlan::LogicalQueryPlan(
		List<EntityName*>* table_names,
		List<ColumnName*>* columns,
		Expr* condition,
		bool distinct,
		ColumnName* orderBy)
{
	Operation* lqp_node = NULL;
	bool singleTable = (table_names->NumElements() == 1)?true:false;

	if (singleTable)
	{
		lqp_node = new SelectionOperation(table_names->Nth(0)->GetName(), condition);
		isSelectSingleTable= true;
	}
	else
	{
		lqp_node = new JoinOperation(table_names, condition);
		isSelectSingleTable= false;
	}

	//if(condition)
	//	lqp_node = new SelectOperation(condition, lqp_node) ;

	lqp_node = new ProjectionOperation(columns, lqp_node);

	if(distinct)
		lqp_node = new DistinctOperation(lqp_node);

	if(orderBy)
		lqp_node = new OrderByOperation(orderBy, lqp_node);

	root = lqp_node;
	isOptimized = false;
}

void LogicalQueryPlan::Optimize()
{
	// Optimize joins!!
}

List<TUPLE*>* LogicalQueryPlan::Execute()
{
//	if (!isOptimized)
//		Optimize();
/*
		if (isSelectSingleTable == true)
		{
			p = tree->SearchNode(tree->root, "select-list");
			string relationName = tableList[0];
			Relation* relationPtr = schema_manager->getRelation(relationName);
			Schema schema = schema_manager->getSchema(relationName);

			vector<string> tmpFieldNames = schema.getFieldNames();
			for(unsigned int f = 0; f<tmpFieldNames.size(); f++)
				cout<<tmpFieldNames[f]<<"  ";
			cout<<endl;

			Block* block = mem->getBlock(0);
			int blockNum = relationPtr->getNumOfBlocks();
			vector<Tuple> tuples;
			if (p->firstChild->data == "*") 
			{ // select * from
				for (int i = 0; i != blockNum; i++) 
				{
					block->clear();
					relationPtr->getBlock(i, 0);
					tuples = block->getTuples();
					for (vector<Tuple>::iterator lit=tuples.begin(); lit != tuples.end(); lit++)
						lit->printTuple();
				}
			} 

		
		}

*/
	return root->Execute();
}
/*
Operation *SearchNode(Operation *root, string value)
{
	if (node == NULL)
		return NULL;
	if (node->getOperationType() == value)
		return node;
	TreeNode *p;
	if((p = SearchNode(node->child, value)) != NULL)
		return p;
	return NULL;
}
*/
List<TUPLE*>* OrderByOperation::Execute()
{
	List<TUPLE*>* childResults = child->Execute();
	//sort!	
 	List<TUPLE*>* myresults = childResults;
	return myresults;
}

List<TUPLE*>* DistinctOperation::Execute()
{
	List<TUPLE*>* childResults = child->Execute();
	//duplicate removal!	
 	List<TUPLE*>* myresults = childResults;
	return myresults;
}

List<TUPLE*>* ProjectionOperation::Execute()
{
	List<TUPLE*>* childResults = child->Execute();
	//pick columns!	
 	List<TUPLE*>* myresults = childResults;
	return myresults;
}

List<TUPLE*>* SelectionOperation::Execute()
{
	/*
	List<TUPLE*>* childResults = child->Execute();
	//apply condition!	
 	List<TUPLE*>* myresults = childResults;
	return myresults;
	*/
	return NULL;
}

/*
List<TUPLE*>* ScanOperation::Execute()
{
	List<TUPLE*>* childResults = child->Execute();
	//simple scan!	
 	List<TUPLE*>* myresults = childResults;
	return myresults;
}
*/
