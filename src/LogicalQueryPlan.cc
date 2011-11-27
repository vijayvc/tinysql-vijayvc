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
		lqp_node = new SelectionOperation(table_names->Nth(0)->GetName(), condition);
	else
		lqp_node = new JoinOperation(table_names, condition);

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
	if (!isOptimized)
		Optimize();

	return root->Execute();
}

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
