/* File: parser.y
 * --------------
 * Yacc input file to generate the parser for the compiler.
 *
 * pp2: your job is to write a parser that will construct the parse tree
 *      and if no parse errors were found, print it.  The parser should 
 *      accept the language as described in specification, and as augmented 
 *      in the pp2 handout.
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
//#include "errors.h"

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
 *
 * pp2: You will need to add new fields to this union as you add different 
 *      attributes to your non-terminal symbols.
 */
%union {
	EntityName* entityName;
	ColumnName* columnName;
	//Attribute*	attribute;
	Type*	type;
	Statement*	stmt;
	//CreateTableStmt* createTable;
	//DropTableStmt* 	dropTable;
	//SelectStmt*		selectStmt;
	//DeleteStmt*		deleteStmt;
	//InsertStmt*		insertStmt;
	Operator*		opera;
	Constant*		constant;
	Expr*			expr;
	bool			distinct;
	char*			name;
	//List<EntityName*>*	tableList;
	List<ColumnName*>*	columnList;	
	List<EntityName*>*	entityList;
	InsertValues*		insertValues;
	List<Attribute*>*	attrList;
	List<Constant*>*	valueList;
	int					intConstant;
    char 				identifier[MAX_NAME_LEN]; // +1 for terminating null
    char *stringConstant;
	/*
    int integerConstant;
    bool boolConstant;
    double doubleConstant;
    Decl *decl;
    List<Decl*> *declList;
    VarDecl* varDecl;
    List<VarDecl*> *varDeclList;

    Type* type;
    List<Type*> *typeList;
    NamedType* namedType;
    List<NamedType*> *namedTypeList;

    Expr* expr;
    List<Expr*> *exprList;
    LValue* lvalue;
    Stmt* stmt;
    List<Stmt*> *stmtList;

    CaseStmt*  caseStmt;
    DefaultStmt* defStmt;
    List<CaseStmt*>* caseStmtList;
	*/
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
/*
%token   T_Void T_Bool T_Int T_Double T_String T_Class 
%token   T_LessEqual T_GreaterEqual T_Equal T_NotEqual T_Dims
%token   T_And T_Or T_Null T_Extends T_This T_Interface T_Implements
%token   T_While T_For T_If T_Else T_Return T_Break
%token   T_New T_NewArray T_Print T_ReadInteger T_ReadLine
%token	 T_Increment T_Decrement
%token   T_Switch T_Case T_Default
*/

/*
%token   <identifier> T_Identifier
%token   <stringConstant> T_StringConstant 
%token   <integerConstant> T_IntConstant
%token   <doubleConstant> T_DoubleConstant
%token   <boolConstant> T_BoolConstant
*/
/*%token	 <operator> Comp_Op*/

/* Non-terminal types
 * ------------------
 * In order for yacc to assign/access the correct field of $$, $1, we
 * must to declare which field is appropriate for the non-terminal.
 * As an example, this first type declaration establishes that the DeclList
 * non-terminal uses the field named "declList" in the yylval union. This
 * means that when we are setting $$ for a reduction for DeclList ore reading
 * $n which corresponds to a DeclList nonterminal we are accessing the field
 * of the union named "declList" which is of type List<Decl*>.
 * pp2: You'll need to add many of these of your own.
 */

%type <entityName>	Entity_Name
%type <columnName>	Column_Name OrderBy_Clause
/*%type <attribute>	Attribute*/
%type <stmt> Statement Stmt Create_Table_Stmt Drop_Table_Stmt Select_Stmt Delete_Stmt Insert_Stmt
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

/*
Where_Clause
OrderBy_Clause
Select_List
Select_Sublist
Attribute_List
Value
Value_List
Search_Condition
Expression
Boolean
Comp_Op
*/
/*
%type <decl>      Decl FunctionDecl ClassDecl InterfaceDecl Prototype Field
%type <declList>  DeclPlus FieldStar PrototypeStar

%type <varDecl>	    Variable VariableDecl 
%type <varDeclList> VariableList VariableDeclStar Formals 

%type <expr>	  Expr Call OptExpr Constant PostFix
%type <lvalue>	  LValue
%type <exprList>  ExprList Actuals

%type <type>	  Type 
%type <namedType>  ExtendsIdent
%type <namedTypeList>  ImplementsIdentList ImplementsIdentOpt 

%type <stmt>	  Stmt IfStmt ForStmt WhileStmt PrintStmt ReturnStmt BreakStmt SwitchStmt StmtBlock 
%type <stmtList>  StmtStar StmtPlus

%type <caseStmt>  CaseStmt
%type <caseStmtList> CasePlus
%type <defStmt>   OptDefault
*/

%left ':'
%left ';' '{' '}'
%left ','
%left '='
%left T_Or
%left T_And
%left T_NotEqual
%left T_Equal
%left T_GreaterEqual
%left '>'
%left T_LessEqual
%left '<'
/*%left '-'
  */
%left '-' '+'
%left '*' '/' '%'
%right NEG
%right '!'
%right T_Not
/*
%left PREDEC
%left PREINC
*/
%left POSTFIX
%left '(' ')'
%left '[' ']'
%left '.'

%nonassoc LOWER_THAN_ELSE
%nonassoc T_Else 

%%
/* Rules
 * -----
 * All productions and actions should be placed between the start and stop
 * %% markers which delimit the Rules section.
	 
 */
Statement			: Stmt					{ $1->Print(); };

Stmt				: Create_Table_Stmt		{ $$ = $1; }
					| Drop_Table_Stmt		{ $$ = $1; }
					| Select_Stmt			{ $$ = $1; }
					| Delete_Stmt			{ $$ = $1; }
					| Insert_Stmt			{ $$ = $1; };

Entity_Name			: T_Name				{ $$ = new EntityName($1); }
Column_Name			: Entity_Name '.' Entity_Name	 { $$ = new ColumnName($3, $1); }
					| Entity_Name			{ $$ = new ColumnName($1, NULL); }

Create_Table_Stmt	: T_Create T_Table Entity_Name '(' Attribute_List ')'	
													{
														$$ = new CreateTableStmt($3, $5);
													}
Attribute_List		: Entity_Name Data_Type			{ 
						  								$$ = new List<Attribute*>;  
														Attribute* temp = new Attribute($1, $2); 
														$$->Append(temp);
					  								}
					| Entity_Name Data_Type ',' Attribute_List	{ $4->Append(new Attribute($1, $2)); $$ = $4; }

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
						  								$$->Append(new ColumnName(new EntityName("*"), NULL));
					  								}
					| Select_Sublist				{ $$ = $1; }

Select_Sublist 		: Column_Name					 { $$ = new List<ColumnName*>; $$->Append($1); } 
					| Column_Name ',' Select_Sublist { $3->Append($1); $$ = $3; } 
Table_List 			: Entity_Name					 { $$ = new List<EntityName*>; $$->Append($1); } 
					| Entity_Name ',' Table_List	 { $3->Append($1); $$ = $3; }

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
					| Entity_Name ',' Column_List { $3->Append($1); $$ = $3; }

Value 				: T_StringConstant 				{ $$ = new StringConstant($1); }
					| T_IntConstant					{ $$ = new IntConstant($1); } 
					| T_Null						{ $$ = new NullConstant(); }

Value_List 			: Value							{ $$ = new List<Constant*>; $$->Append($1); } 
					| Value ',' Value_List			{ $3->Append($1); $$ = $3; }

Search_Condition 	: Boolean	 					{ $$ = $1; }

Expression			: Column_Name							{ $$ = new ColumnAccess($1); }
					| T_StringConstant						{ $$ = new StringConstant($1); }
					| T_IntConstant							{ $$ = new IntConstant($1); }
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

Comp_Op				: '<'									{ $$ = new Operator('<'); }
					| '>'									{ $$ = new Operator('>'); }
					| '='									{ $$ = new Operator('='); }

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
