#ifndef DBFILE_H
#define DBFILE_H

#include <string>

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {HEAP, SORTED, TREE} fType;

class DBFileBase;

class DBFile {
public:
  DBFile();
  ~DBFile();

  int Create (char* fpath, fType ftype, void* startup);
  int Open (char* fpath);
  int Close ();

  void Add (Record& addme);
  void Load (Schema& myschema, char* loadpath);

  void MoveFirst();
  int GetNext (Record& fetchme);
  int GetNext (Record& fetchme, CNF& cnf, Record& literal);

private:
  DBFileBase* db;
  
  void createFile(fType ftype);

  DBFile(const DBFile&);
  DBFile& operator=(const DBFile&);
};


class DBFileBase {
  friend class DBFile;
protected:
  DBFileBase(): mode(READ) {}
  virtual ~DBFileBase() {};

  virtual int Create (char* fpath, void* startup);
  virtual int Open (char* fpath);
  virtual int Close() = 0;

  virtual void Add (Record& addme) = 0;
  virtual void Load (Schema& myschema, char* loadpath);

  // this function does not deal with spanned records
  virtual void MoveFirst () = 0;
  virtual int GetNext (Record& fetchme);
  virtual int GetNext (Record& fetchme, CNF& cnf, Record& literal) = 0;

  /** Extracts the table name from the file path, the string between the last '/' and '.' */
  static std::string getTableName(const char* fpath) {
    std::string path(fpath);
    size_t start = path.find_last_of('/'),
           end   = path.find_last_of('.');
    return path.substr(start+1, end-start-1);
  }

  off_t curPageIdx;
  Page curPage;
  File theFile;

protected:
  enum Mode { READ, WRITE } mode;

  virtual void startWrite() = 0;
  virtual void startRead() = 0;

private:
  DBFileBase(const DBFileBase&);
  DBFileBase& operator=(const DBFileBase&);
};

#endif
