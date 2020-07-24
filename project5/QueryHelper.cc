#include <iostream>

#include "Errors.h"
#include "QueryHelper.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "QueryPlan.h"
#include "Database.h"

using std::cout;

extern "C" {
  int yyparse(void);
}

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;
extern char* newtable;
extern char* oldtable;
extern char* newfile;
extern char* deoutput;
extern struct AttrList *newattrs;

void QueryHelper::run() {
  char *fileName = "Statistics.txt";
      Statistics s;
  Database d; 
  QueryPlan plan(&s);
  while(true) {
    cout << "SQL> ";
    yyparse();

    s.Read(fileName);

    if (newtable) {
      if (d.createTable()) cout << "New Table has been created with the name " << newtable << std::endl;
      else cout << "Table " << newtable << " already exists." << std::endl;
    } else if (oldtable && newfile) {
      if (d.insertInto()) cout << "Values inserted into " << oldtable << std::endl;
      else cout << "Insert failed." << std::endl;
    } else if (oldtable && !newfile) {
      if (d.dropTable()) cout << oldtable <<"dropped" << std::endl;
      else cout << "Table " << oldtable << " does not exist." << std::endl;
    } else if (deoutput) {
      plan.setOutput(deoutput);
    } else if (attsToSelect || finalFunction) {
      plan.plan();
      plan.print();
      plan.execute();
    }
    clear();
  }
}

void QueryHelper::clear() {
  newattrs = NULL;
  finalFunction = NULL;
  tables = NULL;
  boolean = NULL;
  groupingAtts = NULL;
  attsToSelect = NULL;
  newtable = oldtable = newfile = deoutput = NULL;
  distinctAtts = distinctFunc = 0;
  FATALIF (!remove ("*.tmp"), "remove tmp files failed");
}
