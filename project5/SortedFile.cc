#include <fstream>
#include <stdio.h>    // rename

#include "Errors.h"
#include "SortedFile.h"
#include "HeapFile.h"

#define safeDelete(p) {  \
  delete p; p = NULL; }

using std::ifstream;
using std::ofstream;
using std::string;

extern char* catalog_path;
extern char* dbfile_dir; 
extern char* tpch_dir;




/*


int SortedFile::Open (char *f_path) {


  
  char *fName = new char[20];
  isDirty=0;
  sprintf(fName, "%s.meta", f_path);


  nameOfFile = (char *)malloc(sizeof(f_path)+1);
  strcpy(nameOfFile,f_path);

  ifstream ifs(fName,ios::binary);

  ifs.seekg(sizeof(nameOfFile)-1);
  
  if(sorted_info==NULL){
    sorted_info = new SortInfo;
    sorted_info->myOrder = new OrderMaker();
  }


  ifs.read((char*)sorted_info->myOrder, sizeof(*(sorted_info->myOrder))); 

  ifs.read((char*)&(sorted_info->runLength), sizeof(sorted_info->runLength));

  ifs.close();

  m = R;

  // open the file for which file path has been provided
  file->Open(1, f_path);  
  //set index to 0 i.e. start
  indexOfRecord = 0;
  eofile = 0;
  pIndex = 1;
  
}





*/
int SortedFile::binarySearch(Record& fetchme, OrderMaker& queryorder, Record& literal, OrderMaker& cnforder, ComparisonEngine& cmp) {
  
  if (!GetNext(fetchme)) return 0;
  int result = cmp.Compare(&fetchme, &queryorder, &literal, &cnforder);
  if (result > 0) return 0;
  else if (result == 0) return 1;

  
  off_t low=curPageIdx, high=theFile.lastIndex(), mid=(low+high)/2;
  for (; low<mid; mid=(low+high)/2) {
    theFile.GetPage(&curPage, mid);
    FATALIF(!GetNext(fetchme), "empty page found");
    result = cmp.Compare(&fetchme, &queryorder, &literal, &cnforder);
    if (result<0) low = mid;
    else if (result>0) high = mid-1;
    else high = mid;  
  }

  theFile.GetPage(&curPage, low);
  do {   
    if (!GetNext(fetchme)) return 0;
    result = cmp.Compare(&fetchme, &queryorder, &literal, &cnforder);
  } while (result<0);
  return result==0;
}


/*

void SortedFile::MoveFirst () { 

  
  pIndex = 1;
  isDirty = 0;
  indexOfRecord = 0;

  if(m!=R){
    m = R;
    MergeFrompipeOut();
    file->GetPage(readBufferPage,pIndex);
    //get first page from record to readBufferPage
    readBufferPage->GetFirst(currentRecord);

  }
  else{
    if(file->GetLength()==0){
      
    }
    else{
      file->GetPage(readBufferPage,pIndex);
  
      int result = readBufferPage->GetFirst(currentRecord);
    }
  }
  queryFlag = true;
}




*/


int SortedFile::Open (char* fpath) {
  table = getTableName((tpath=fpath).c_str());
  int ftype;
  ifstream ifs(metafName());
  FATALIF(!ifs, "Meta file missing.");
  ifs >> ftype >> myOrder >> runLength;
  ifs.close();
  return DBFileBase::Open(fpath);
}



int SortedFile::GetNext (Record& fetchme, CNF& cnf, Record& literal) {
  OrderMaker queryorder, cnforder;
  OrderMaker::queryOrderMaker(myOrder, cnf, queryorder, cnforder);
  ComparisonEngine cmp;
  if (!binarySearch(fetchme, queryorder, literal, cnforder, cmp)) return 0; 
  do {
    if (cmp.Compare(&fetchme, &queryorder, &literal, &cnforder)) return 0; 
    if (cmp.Compare(&fetchme, &literal, &cnf)) return 1;   
  } while(GetNext(fetchme));
  return 0;  
}

void SortedFile::Add (Record& addme) {
  startWrite();
  in->Insert(&addme);
}

void SortedFile::Load (Schema& myschema, char* loadpath) {
  startWrite();
  DBFileBase::Load(myschema, loadpath);
}


// int DBFile::GetNext(Record &fetchme)
// {
//     if (!end)
//     {
//         fetchme.Copy(current);
//         if (page.GetFirst(current) == 0)
//         {
//             if (++roff >= file.GetLength() - 1)
//                 end = true;
//             else
//             {
//                 file.GetPage(&page, roff);
//                 page.GetFirst(current);
//             }
//         }
//         return 1;
//     }
//     return 0;
// }

void SortedFile::MoveFirst () {
  startRead();
  theFile.GetPage(&curPage, curPageIdx=0);
}

int SortedFile::GetNext (Record& fetchme) {
  return DBFileBase::GetNext(fetchme);
}




int SortedFile::Create (char* fpath, void* startup) {
  table = getTableName((tpath=fpath).c_str());
  typedef struct { OrderMaker* o; int l; } *pOrder;
  pOrder po = (pOrder)startup;
  myOrder = *(po -> o);
  runLength = po -> l;
  return DBFileBase::Create(fpath, startup);
}

void SortedFile::merge() {
  in->ShutDown();
  Record fromFile, fromPipe;
  bool fileNotEmpty = !theFile.empty(), pipeNotEmpty = out->Remove(&fromPipe);

  HeapFile tmp;
  tmp.Create(const_cast<char*>(tmpfName()), NULL);  
  ComparisonEngine ce;

  if (fileNotEmpty) {
    theFile.GetPage(&curPage, curPageIdx=0);           
    fileNotEmpty = GetNext(fromFile);
  }

  while (fileNotEmpty || pipeNotEmpty)
    if (!fileNotEmpty || (pipeNotEmpty && ce.Compare(&fromFile, &fromPipe, &myOrder) > 0)) {
      tmp.Add(fromPipe);
      pipeNotEmpty = out->Remove(&fromPipe);
    } else if (!pipeNotEmpty
               || (fileNotEmpty && ce.Compare(&fromFile, &fromPipe, &myOrder) <= 0)) {
      tmp.Add(fromFile);
      fileNotEmpty = GetNext(fromFile);
    } else FATAL("Two-way merge failed.");

  // write back
  tmp.Close();
  FATALIF(rename(tmpfName(), tpath.c_str()), "Merge write failed.");  
  deleteQ();
}

// void DBFile::Load(Schema &f_schema, char *loadpath)
// {
//     cout << "Loading all the records from tbl file..." << endl;
//     FILE *tblFile = fopen(loadpath, "r");
//     Record record;
//     while (record.SuckNextRecord(&f_schema, tblFile) != 0)
//     {
//         Add(record);
//     }
//     fclose(tblFile);
//     if (pDirty)
//     {
//         file.AddPage(&page, poff++);
//         page.EmptyItOut();
//         end = true;
//         pDirty = false;
//     }
//     cout << "All records loaded." << endl;
// }




const char* SortedFile::metafName() const {
  std::string p(dbfile_dir);
  return (p+table+".meta").c_str();
}



int SortedFile::Close() {
  ofstream ofs(metafName());  
  ofs << "1\n" << myOrder << '\n' << runLength << std::endl;
  ofs.close();
  if(mode==WRITE) merge(); 
  return theFile.Close();
}