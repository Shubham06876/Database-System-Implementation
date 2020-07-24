#include <iostream> 

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "Errors.h"

#include "HeapFile.h"

HeapFile::HeapFile() {}


int HeapFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
  ComparisonEngine comp;
  while(GetNext(fetchme))
    if(comp.Compare(&fetchme, &literal, &cnf)) return 1;  
  return 0; 
}

/*



void HeapFile::Load (Schema &f_schema, char *loadpath) {
  //open file from loadpath
  FILE *input_file = fopen(loadpath, "r");
  if (!input_file){
    //#ifdef verbose
    cerr << "Not able to open input file " << loadpath << endl;
    //#endif
    exit(1);
  }

  //if empty file then page number starts with 0 else current file length - 1
  off_t page_no = db_file.GetLength();
  if (page_no != 0) page_no--;

  //if program is in reading mode and there are records in page then empty out the page and set mode to writing mode
  if (!writingMode && curr_page.NumRecords() != 0){
    curr_page.EmptyItOut();
  }
  writingMode = true;

  Record temp;
  while (temp.SuckNextRecord(&f_schema, input_file)){
    //write current record to page
    int isAppended = curr_page.Append(&temp);

    if (!isAppended){
      //page is full, write page to file on disk
      db_file.AddPage(&curr_page, page_no++);
      curr_page.EmptyItOut();

      //write current record to page
      curr_page.Append(&temp);
    }
    num_rec++;
  }
  //write last page to file on disk
  db_file.AddPage(&curr_page, page_no);
  curr_page.EmptyItOut();

  fclose(input_file);
}


*/


void HeapFile::Add (Record& addme) {
  startWrite();
  if(!curPage.Append(&addme)) {
    theFile.addPage(&curPage);   
    curPage.EmptyItOut();
    curPage.Append(&addme);
  }
}

/*

int HeapFile::GetNext (Record &fetchme) {
  //if there is anything that is not written to file yet, write it
  if (writingMode && curr_page.NumRecords() != 0){
    //if empty file then page number starts with 0 else current file length - 1
    off_t page_no = db_file.GetLength();
    if (page_no != 0) page_no--;

    db_file.AddPage(&curr_page, page_no);
    curr_page.EmptyItOut();

    //fetch current page from disk for reading
    //db_file.GetPage(&curr_page, curr_page_index);
  }
  writingMode = false;

  if (!writingMode && curr_page.NumRecords() == 0){
    //if reached at the end of current page then fetch next page if current page is not last page
    if (curr_page_index < db_file.GetLength()-1){
      db_file.GetPage(&curr_page, curr_page_index);
      curr_page_index++;
    }
    else{
      return 0;
    }
  }
  
  return curr_page.GetFirst(&fetchme);
}

*/

void HeapFile::MoveFirst() {
  startRead();
  theFile.GetPage(&curPage, curPageIdx=0);
}

int HeapFile::Close () {
  if (mode == WRITE && !curPage.empty()) theFile.addPage(&curPage);
  return theFile.Close();
}
