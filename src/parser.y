/* File: parser.y
 * --------------
 * Yacc input file to generate the parser for the compiler.
 */

%{

/* Just like lex, the text within this first region delimited by %{ and %}
 * is assumed to be C/C++ code and will be copied verbatim to the y.tab.c
 * file ahead of the definitions of the yyparse() function. Add other header
 * file inclusions or C++ variable declarations/prototypes that are needed
 * by your code here.
 */
#include "scanner.h" // for yylex
#include "parser.h"

#define MAX_NAME_LEN 1024
void yyerror(char *msg); // standard error-handling routine
%}

/* The section before the first %% is the Definitions section of the yacc
 * input file. Here is where you declare tokens and types, add precedence
 * and associativity options, and so on.
 */
 
/* yylval 
 * ------
 * Here we define the type of the yylval global variable that is used by
 * the scanner to store attibute information about the token just scanned
 * and thus communicate that information to the parser. 
 */

%union {
	EntityName* entityName;
	ColumnName* columnName;
	Type*	type;
	Statement*	stmt;
	List<Statement*>* stmtList;

	Operator*		opera;
	Constant*		constant;
	Expr*			expr;
	bool			distinct;
	char*			name;
	List<ColumnName*>*	columnList;	
	List<EntityName*>*	entityList;
	InsertValues*		insertValues;
	List<Attribute*>*	attrList;
	List<Constant*>*	valueList;
	int					intConstant;
    char 				identifier[MAX_NAME_LEN]; 
    char *stringConstant;
}

/* Tokens
 * ------
 * Here we tell yacc about all the token types that we are using.
 * Yacc will assign unique numbers to these and export the #define
 * in the generated y.tab.h header file.
 */
%token	T_Int T_String T_Null 
%token	T_Less T_Greater T_Equal T_And T_Or
%token	T_Create T_Drop T_Select T_Delete T_Insert
%token	T_Table T_Distinct T_From T_Where T_Order
%token	T_By T_Values T_Not T_Into T_LineEnd

%token   <identifier> T_Name
%token   <name> T_StringConstant
%token	 <intConstant> T_IntConstant

/* Non-terminal types
 */

%type <entityName>	Entity_Name
%type <columnName>	Column_Name OrderBy_Clause
/*%type <attribute>	Attribute*/
%type <stmt> Statement Stmt Create_Table_Stmt Drop_Table_Stmt Select_Stmt Delete_Stmt Insert_Stmt
%type <stmtList> StatementList
%type <type> Data_Type

%type <distinct>   Opt_Distinct
%type <opera>   Comp_Op
%type <entityList> Table_List Column_List
%type <attrList>   Attribute_List
%type <columnList> Select_List Select_Sublist
%type <expr>	   Search_Condition Boolean Expression Where_Clause 
%type <constant>   Value 
%type <valueList>  Value_List
%type <insertValues> Insert_Tuples

%left ','
%left T_Or
%left T_And
%left T_Equal
%left T_Greater 
%left T_Less
%left '-' '+'
%left '*' '/'
%right T_Not
%left '(' ')'
%left '[' ']'
%left '.'

%%
/* Rules
 * -----
 * All productions and actions should be placed between the start and stop
 * %% markers which delimit the Rules section.
	 
 */
StatementList		: Statement StatementList			{ /*$2->Append($1); $$ = $2; */} 
					| Statement							{ /*$$ = new List<Statement*>; $$->Append($1);*/ }

Statement			: Stmt								{ 
						  									//$1->Print(0); 
						  									$1->Execute(); 
					  									}

Stmt				: Create_Table_Stmt	T_LineEnd		{ $$ = $1; }
					| Drop_Table_Stmt	T_LineEnd		{ $$ = $1; }
					| Select_Stmt		T_LineEnd		{ $$ = $1; }
					| Delete_Stmt		T_LineEnd		{ $$ = $1; }
					| Insert_Stmt		T_LineEnd		{ $$ = $1; };

Entity_Name			: T_Name							{ $$ = new EntityName($1); }
Column_Name			: Entity_Name '.' Entity_Name	 	{ $$ = new ColumnName($1, $3);  }
					| Entity_Name						{ $$ = new ColumnName(NULL, $1); }

Create_Table_Stmt	: T_Create T_Table Entity_Name '(' Attribute_List ')'
													{
														$$ = new CreateTableStmt($3, $5);
													}
Attribute_List		: Entity_Name Data_Type			{ 
						  								$$ = new List<Attribute*>;  
														$$->Append(new Attribute($1, $2));
					  								}
					| Attribute_List ',' Entity_Name Data_Type 	{ ($$=$1)->Append(new Attribute($3, $4)); }

Data_Type			: T_Int					{ $$ = new Type("int"); }
					| T_String				{ $$ = new Type("string"); }

Drop_Table_Stmt		: T_Drop T_Table Entity_Name	{
						  								$$ = new DropTableStmt($3);
					  								}

Select_Stmt			: T_Select Opt_Distinct Select_List T_From Table_List Where_Clause OrderBy_Clause
													{
														$$ = new SelectStmt($5, $3,	$2, $6, $7);
													}
Opt_Distinct		: T_Distinct					{ $$ = true; }
					| 								{ $$ = false; }

Where_Clause		: T_Where Search_Condition		{ $$ = $2; }
					|								{ $$ = NULL; } 

OrderBy_Clause		: T_Order T_By Column_Name		{ $$ =	$3; } 
					| 								{ $$ = NULL; }

Select_List			: '*'							{ 
						  								$$ = new List<ColumnName*>; 
						  								$$->Append(new ColumnName(NULL, new EntityName("*")));
					  								}
					| Select_Sublist				{ $$ = $1; }

Select_Sublist 		: Column_Name					 { $$ = new List<ColumnName*>; $$->Append($1); } 
					| Select_Sublist ',' Column_Name { ($$=$1)->Append($3); } 

Table_List 			: Entity_Name					 { $$ = new List<EntityName*>; $$->Append($1); } 
					| Table_List ',' Entity_Name 	 { ($$=$1)->Append($3); }

Delete_Stmt 		: T_Delete T_From Entity_Name Where_Clause 
													{
														$$ = new DeleteStmt($3, $4); 
													}
Insert_Stmt 		: T_Insert T_Into Entity_Name '(' Column_List ')' Insert_Tuples
													{
														$$ = new InsertStmt($3, $5, $7);
													}

Insert_Tuples 		: T_Values '(' Value_List ')'	{ $$ = new InsertValues($3); } 
					| Select_Stmt					{ $$ = new InsertValues($1); }

Column_List 		: Entity_Name					{ $$ = new List<EntityName*>; $$->Append($1); } 
					| Column_List ',' Entity_Name   { ($$=$1)->Append($3); }

Value 				: T_StringConstant 				{ $$ = new StringConstant($1); }
					| T_IntConstant					{ $$ = new IntConstant($1); } 
					| T_Null						{ $$ = new NullConstant(); }

Value_List 			: Value							{ $$ = new List<Constant*>; $$->Append($1); } 
					| Value_List ',' Value 			{ ($$=$1)->Append($3); }

Search_Condition 	: Boolean	 					{ $$ = $1; }

Expression			: Column_Name							{ $$ = new ColumnAccess($1); }
					| Value									{ $$ = $1; }
					| '(' Expression ')'					{ $$ = $2; }
					| Expression '+' Expression				{ $$ = new ArithmeticExpr($1, $3, new Operator('+')); }
					| Expression '-' Expression				{ $$ = new ArithmeticExpr($1, $3, new Operator('-')); }
					| Expression '*' Expression				{ $$ = new ArithmeticExpr($1, $3, new Operator('*')); }
					| Expression '/' Expression				{ $$ = new ArithmeticExpr($1, $3, new Operator('/')); }

Boolean				: Expression Comp_Op Expression			{ $$ = new RelationalExpr($1, $3, $2); }
					| '[' Boolean ']'						{ $$ = $2; }
					| Boolean T_Or Boolean					{ $$ = new LogicalExpr($1, $3, new Operator('|')); }
					| Boolean T_And Boolean					{ $$ = new LogicalExpr($1, $3, new Operator('&')); }
					| T_Not Boolean							{ $$ = new LogicalExpr(NULL, $2, new Operator('!')); }

Comp_Op				: T_Less								{ $$ = new Operator('<'); }
					| T_Greater								{ $$ = new Operator('>'); }
					| T_Equal								{ $$ = new Operator('='); }

%%

/* The closing %% above marks the end of the Rules section and the beginning
 * of the User Subroutines section. All text from here to the end of the
 * file is copied verbatim to the end of the generated y.tab.c file.
 * This section is where you put definitions of helper functions.
 */

/* Function: InitParser
 * --------------------
 * This function will be called before any calls to yyparse().  It is designed
 * to give you an opportunity to do anything that must be done to initialize
 * the parser (set global variables, configure starting state, etc.). One
 * thing it already does for you is assign the value of the global variable
 * yydebug that controls whether yacc prints debugging information about
 * parser actions (shift/reduce) and contents of state stack during parser.
 * If set to false, no information is printed. Setting it to true will give
 * you a running trail that might be helpful when debugging your parser.
 * Please be sure the variable is set to false when submitting your final
 * version.
 */

void InitParser()
{
   PrintDebug("parser", "Initializing parser");
   yydebug = false;
}
