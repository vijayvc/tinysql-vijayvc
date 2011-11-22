/* File: utility.h
 * ---------------
 * This file just includes a few support functions you might find
 * helpful in writing the projects 
 */

#ifndef _H_utility
#define _H_utility

#include <stdlib.h>
#include <stdio.h>


/* Function: Failure()
 * Usage: Failure("Out of memory!");
 * --------------------------------
 * Reports an error and exits the program immediately.  You should not
 * need to call this since you should always try to continue parsing,
 * even after an error is encountered.  Some of the provided code calls
 * this in unrecoverable error situations (cannot allocate memory, etc.)
 * Failure accepts printf-style arguments in the message to be printed.
 */
void Failure(const char *format, ...);



/* Macro: Assert()
 * Usage: Assert(num > 0);
 * ----------------------
 * This macro is designed to assert the truth of a necessary condition.
 * It tests the given expression, and if it evalutes true, nothing happens.
 * If it is false, it calls Failure to print a message and abort.
 * For example:  Assert(ptr != NULL)
 * will print something similar to the following if ptr is NULL:
 *   *** Failure: Assertion failed: hashtable.cc, line 55:
 *       ptr != NULL
 */ 
#define Assert(expr)  \
  ((expr) ? (void)0 : Failure("Assertion failed: %s, line %d:\n    %s", __FILE__, __LINE__, #expr))

#endif
