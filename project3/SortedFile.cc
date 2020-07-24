#include "SortedFile.h"
#include "DBFile.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <fstream>
#include <string>
#include <iostream>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>


SortedFile::SortedFile () {
  GenericDBFile::file_type = sorted;
  
  big_Que = NULL;
  pipe_input = NULL;
  ifQueryFound = false;
  ifSearched = false;
  ifQueryExists = false;
  pipe_output = NULL;
  whetherQueryCreated = false;
  
}

SortedFile::~SortedFile () {
    delete pipe_input;
    delete pipe_output;
    delete big_Que;
}



void SortedFile::MoveFirst () {
  if(writingMode == true){
    MergeDifferential();
  }

  writingMode = false;
  curr_page_index = 0;

  if(0 != db_file.GetLength()){
    db_file.GetPage(&curr_page, curr_page_index);
  }else{
    curr_page.EmptyItOut();
  }
  
  curr_page_index++;
}


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


void SortedFile::Load (Schema &f_schema, char *loadpath) {
  writingMode = true;
  
  if (NULL == big_Que) {
      pipe_input = new Pipe(pipe_buffer_size);
      pipe_output = new Pipe(pipe_buffer_size);
      big_Que = new BigQ(*pipe_input, *pipe_output, sortorder, runlen);
  }

  //open file from loadpath
  FILE *input_file = fopen(loadpath, "r");
  if (!input_file){
    cerr << "Not able to open input file " << loadpath << endl;
    exit(1);
  }

  Record temp;
  while (temp.SuckNextRecord (&f_schema, input_file)){ 
      pipe_input->Insert(&temp);
  }

  fclose(input_file);
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


int SortedFile::Create (char *f_path, fType f_type, void *startup){
  string meta_file_name;
  meta_file_name.append(f_path);
  meta_file_name.append(".meta");
  ofstream meta_file; 
  meta_file.open(meta_file_name.c_str());
  if (!meta_file) {
    cerr << "Not able to create meta file"<<endl;
    return 0;
  }
  meta_file << f_type << endl;
  SortInfo sortinfo = *((SortInfo *) startup);
  runlen = sortinfo.runLength;
  meta_file << runlen << endl;
  sortorder = *((OrderMaker *) sortinfo.myOrder);
  meta_file << sortorder; 
  meta_file.close();

  cout<<"Run length: "<<runlen<<endl;
  cout<<"Sort Order: "<<endl;
  sortorder.Print();

  filepath.append(f_path);
  db_file.Open(0, f_path);
  writingMode = true;

  cout<<"Created sorted db file at "<<f_path<<endl;
  return 1;
}




void SortedFile::Add (Record &rec) {
  cout<<"Add record in sorted dbfile"<<endl;
  if (writingMode) {
      pipe_input->Insert(&rec);
  }else { 
      writingMode = true;
      if (NULL == big_Que) {
        cout<<"creating pipes and bigq"<<endl;
        pipe_input = new Pipe(pipe_buffer_size);
        pipe_output = new Pipe(pipe_buffer_size);
        big_Que = new BigQ(*pipe_input, *pipe_output, sortorder, runlen);
      }
      pipe_input->Insert(&rec);
    }
    cout<<"record added in sorted dbfile<<endl";
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

int SortedFile::GetNext (Record &fetchme) {
  if(writingMode){
    MergeDifferential();
    MoveFirst();
  }

  writingMode = false;

  if (curr_page.NumRecords() == 0){
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

int SortedFile::Open (char *f_path)
{
  int f_type;
  string meta_file_name;
  meta_file_name.append(f_path);
  meta_file_name.append(".meta");
  ifstream meta_file; 
  meta_file.open(meta_file_name.c_str());
  if (!meta_file) {
    cerr << "Not able to open meta file"<<endl;
    return 0;
  }
  meta_file >> f_type;
  meta_file >> runlen;
  meta_file >> sortorder;
  meta_file.close();

  filepath.append(f_path);
  db_file.Open(1, f_path);
  writingMode = false;
  MoveFirst();

  lseek (db_file.GetMyFilDes(), sizeof (off_t), SEEK_SET);
  read (db_file.GetMyFilDes(), &num_rec, sizeof (off_t));

  return 1;
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

int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

  ComparisonEngine comp;
  while (GetNext(fetchme)){
    if (comp.Compare(&fetchme, &literal, &cnf)){
      return 1;
    }
  }

  return 0;
}

int SortedFile::Close () {
  MergeDifferential();
  cout<<"merge differential complete"<<endl;
  lseek (db_file.GetMyFilDes(), sizeof (off_t), SEEK_SET);
  write (db_file.GetMyFilDes(), &num_rec, sizeof (off_t));
  cout<<"num_rec written to file"<<endl;
  
  int file_len = db_file.Close();
  cout<<"file closed"<<endl;
  return 1;
}


int SortedFile::BinarySearch(Record &fetchme, Record &literal, OrderMaker &query, off_t startInd, off_t endInd){
  ComparisonEngine comp;
  Page temp_page;
  cout<<"startInd: "<<startInd<<" endInd: "<<endInd<<endl;

  off_t mid = 0;
  int comp_rc = 0;
  while(startInd <= endInd){
    mid  = (startInd + endInd)/2;
    cout<<"mid: "<<mid<<endl;

    if(mid == curr_page_index && curr_page.NumRecords() != 0){
      temp_page = curr_page;
    }else{
      cout<<"loading page "<<mid<<endl;
      db_file.GetPage(&temp_page, mid);
    }

    if(temp_page.GetFirst(&fetchme)){
      comp_rc = comp.Compare(&fetchme, &literal, &query);
      cout<<"comp_rc: "<<comp_rc<<endl;

      if(comp_rc < 0){
        startInd = mid + 1;
      }else if (comp_rc > 0){
        endInd = mid - 1;
      }else{
        if(--mid > 0){
          pair<off_t, int> res = BacktraceFile(fetchme, literal, query, mid, temp_page);
          curr_page_index = res.first;
          curr_page_index++;
          if(res.second){
            db_file.GetPage(&curr_page, res.first);
          }else{
            curr_page = temp_page;
          } 
        }
        return 1;
      }
    }
  }

  if(comp_rc < 0){
    cout<<"loading page "<<mid<<endl;
    db_file.GetPage(&temp_page, mid);
  }else if (comp_rc > 0 && mid > 0){
    mid = mid - 1;
    cout<<"loading page "<<mid<<endl;
    db_file.GetPage(&temp_page, mid);
  }
  
  while(temp_page.GetFirst(&fetchme)){
    int rc = comp.Compare(&fetchme, &literal, &query); 
    cout<<"comparing records: "<<rc<<endl;
    if(!rc){
      curr_page = temp_page;
      curr_page_index = mid;
      curr_page_index++;
      return 1;
    }
  }

  return 0;
}



/*

Record* SortedFile::GetMatchPage(Record &literal) {
  
  if(queryFlag == true) {
    int pageMatch = bsearch(pIndex, file->GetLength()-2, orderOfQuery, literal);
    if(pageMatch == -1) {
      //no match found
      return NULL;
    }
    if(pageMatch != pIndex) {
      readBufferPage->EmptyItOut();
      file->GetPage(readBufferPage, pageMatch);
      pIndex = pageMatch+1;
    }
    queryFlag = false;
  }

  Record *nextRecd = new Record;
  ComparisonEngine compEngine;

  while(readBufferPage->GetFirst(nextRecd)) {
    if(compEngine.Compare(nextRecd, &literal, orderOfQuery) == 0) {
      //match found
      return nextRecd;
    }
  }
  
  if(pIndex < file->GetLength()-2) {
    //first match may exist on next page
    pIndex++;
    file->GetPage(readBufferPage, pIndex);
    while(readBufferPage->GetFirst(nextRecd)) {
      if(compEngine.Compare(nextRecd, &literal, orderOfQuery) == 0) {
        //find the first one
        return nextRecd;
      }
    }
  } else {
    //No match found till max element of file
    return NULL;
  }
  return NULL;
    

}

int SortedFile::bsearch(int low, int high, OrderMaker *queryOrderMaker, Record &literal) {
  
  cout<<"serach OM "<<endl;
  queryOrderMaker->Print();
  cout<<endl<<"file om"<<endl;
  sorted_info->myOrder->Print();

  
  if(high == low) return low;
  if(high < low) return -1;
  
  ComparisonEngine *comp_Engine;
  Record *tempRecord = new Record;
  Page *tempPage = new Page;
  
  int mid = (int) (high+low)/2;
  file->GetPage(tempPage, mid);
  
  int tempRes;

  Schema nu("catalog","lineitem");



  tempPage->GetFirst(tempRecord);

  tempRecord->Print(&nu);

    tempRes = comp_Engine->Compare(tempRecord,sorted_info->myOrder, &literal,queryOrderMaker );
  

  delete tempPage;
  delete tempRecord;


  if( tempRes == -1) {
    if(low==mid)
      return mid;
    return bsearch(low, mid-1, queryOrderMaker, literal);
  }
  else if(tempRes == 0) {
    return mid;
  }
  else
    return bsearch(mid+1, high,queryOrderMaker, literal);
}





*/






pair<off_t, int> SortedFile::BacktraceFile(Record &fetchme, Record &literal, OrderMaker &query, off_t pageno, Page temp_page){
  Record temp_rec;
  ComparisonEngine comp;
  int is_full = 1;

  while(pageno > 0){
    db_file.GetPage(&temp_page, pageno);
    //         fetchme.Copy(current);
    if(temp_page.GetFirst(&temp_rec)){
      if(!comp.Compare(&temp_rec, &literal, &query)){ 
        fetchme = temp_rec;
        pageno--;
      }else{
        while(temp_page.GetFirst(&temp_rec)){
          if(!comp.Compare(&temp_rec, &literal, &query)){
           fetchme = temp_rec;
           is_full = 0;
           break;
          }
        }
        pageno++;
      }
    }
  }

  return make_pair(pageno, is_full) ;
} 



void SortedFile::MergeDifferential(void) {
  writingMode = false;
  
  if(NULL != big_Que){
    //add records from the file to in_pipe
    cout<<"Number of pages in sorted file before merge: "<<db_file.GetLength()<<endl;
    cout<<"Number of records in sorted file before merge: "<<num_rec<<endl;
    Record temp;
    if(db_file.GetLength() != 0){
      MoveFirst();
      while(GetNext(temp)){
        pipe_input->Insert(&temp);
      }
    }
    pipe_input->ShutDown();

    cout<<"Closing sorted db file"<<endl;
    int file_len = db_file.Close();
    cout<<"Number of pages in sorted file before merge: "<<file_len<<endl;
    cout << "Re-opening file in create mode: " << filepath << endl;
    db_file.Open(0, const_cast<char *>(filepath.c_str()));
    curr_page_index = 0;
    num_rec = 0;
    //         fetchme.Copy(current);
    cout<<"Current page index in new sorted file before merge: "<<curr_page_index<<endl;
    cout<<"Number of pages in new sorted file before merge: "<<db_file.GetLength()<<endl;

    curr_page.EmptyItOut();
    while(pipe_output->Remove(&temp)){
      if(!curr_page.Append(&temp)){
        db_file.AddPage(&curr_page, curr_page_index++);
        curr_page.EmptyItOut();

        curr_page.Append(&temp);
      }
      num_rec++;
    }
    db_file.AddPage(&curr_page, curr_page_index);
    curr_page.EmptyItOut();
    cout<<"Current page index in new sorted file after merge: "<<curr_page_index<<endl;
    cout<<"Number of pages in new sorted file after merge: "<<db_file.GetLength()<<endl;
    cout<<"Number of records in new sorted file after merge: "<<num_rec<<endl;

    delete pipe_input;
    delete pipe_output;
    delete big_Que;
    cout<<"deleted pointers"<<endl;

    pipe_input = NULL;
    pipe_output = NULL;
    big_Que = NULL;
    cout<<"pointers set to null"<<endl;
  }

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