/* File: main.cc
 * -------------
 * This file defines the main() routine for the program and not much else.
 * You should not need to modify this file.
 */
 
#include <string.h>
#include <stdio.h>
#include "utility.h"
//#include "errors.h"
#include "parser.h"
#include "StorageWrapper.h"

void yyerror(char* msg)
{
}

/* Function: main()
 * ----------------
 * Entry point to the entire program.  We parse the command line and turn
 * on any debugging flags requested by the user when invoking the program.
 * InitScanner() is used to set up the scanner.
 * InitParser() is used to set up the parser. The call to yyparse() will
 * attempt to parse a complete program from the input. 
 */
int main(int argc, char *argv[])
{
	StorageManagerWrapper::Initialize();
   // ParseCommandLine(argc, argv);
  	if (argc == 1)
	{
		printf("No input File. \n ./dcc <file_name>\n");
		return 1;
	}

	FILE* inputFile = fopen(argv[1], "r");
	if(!inputFile)
	{
		printf("No such file exists!\n");
		return 1;
	}
	yyrestart(inputFile);
    //FILE *filtered = popen("./dpp", "r"); // start up the preprocessor
    //yyrestart(filtered); // tell lex to read from output of preprocessor

    InitScanner();
    InitParser();
    yyparse();
    //return (ReportError::NumErrors() == 0? 0 : -1);
	StorageManagerWrapper::Finish();
	return 0;
}

