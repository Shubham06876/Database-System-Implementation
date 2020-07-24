#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {
	Record* currentRecord;
	
	off_t pIndex;
	off_t indexLocation;
	int dirty;
	int eof;
	Page* r;
	Page* w;
	File *f;
public:
	DBFile (); 
	~DBFile (); 
	int Create (const char *filepath, fType file_type, void *startup);
	void MoveFirst ();
	void Add (Record &addme);
	void Load (Schema &schema, const char *loadpath);
	int Open (const char *file_path);
	int Close ();

	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
