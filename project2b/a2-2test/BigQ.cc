#include "BigQ.h"
#include <unistd.h>


File file;
char *path = "temporary.bin";

std::vector<int> indexOfPages;
std::vector<RecordTracker *> sortRec;
std::vector<Page *> pagesOfRuns;
OrderMaker *order;
int atRun = 0;



RecordTracker::RecordTracker() {
	r = new Record();
	runCount = 0;
	numberOfPage = 0;
	
}

//This method is used to compare 2 records. Returns true if arg1 is greater else false.
bool compareHeap(const RecordTracker *left, const RecordTracker *right)
{

  if(right==NULL || left==NULL)
     return true;

  ComparisonEngine cEngine;

  Record *sRecord = right->r;
  Record *fRecord = left->r;
  

  if (cEngine.Compare(fRecord, sRecord, order) < 0) {
    return false;
  } 
  return true;
}



//Used to compare two Records. Returns true if the Record passed in argument1 is 
//less than the Record passed in argument 2 else returns false.

bool compareRecord(const RecordTracker *left, const RecordTracker *right) 
{

  if(right==NULL || left==NULL)
     return false;

  ComparisonEngine cEngine;

  Record *sRecord = right->r;
  Record *fRecord = left->r;
  

  if (cEngine.Compare(fRecord, sRecord, order) < 0) {
    return true;
  }
   return false;

}


//We use compareHeap and implements a vector on each run and merges the records using the vector
void mergingProcessOnPages(thread_p *tparameters)
{


  Record *varRecord;
  RecordTracker *nextRecord;
  Page* pageTe = NULL;
  RecordTracker *currentRecord;
  

  sortRec.clear();
  file.Open(1, path);

  	int i = 0;
	int currentRun = 0;
    int outgoingPage = 0;
	int sizeOfPageVec = indexOfPages.size();
	int no = 0;
	int varP = 0;
	
	if(sizeOfPageVec == 1){
		
		pageTe = new Page();
		while(file.GetPage(pageTe,i)){
			
			varRecord = new Record();
			while(pageTe->GetFirst(varRecord)) {								
				tparameters->outP->Insert(varRecord);
				varRecord = new Record();	
				
			}
			pageTe->EmptyItOut();
			i++;
		}
			
	
	}
 else
 {
	while(i < sizeOfPageVec){
		
	pageTe = new Page();
    file.GetPage(pageTe,indexOfPages[i]);
    pagesOfRuns.push_back(pageTe);


    varRecord = new Record();
    pagesOfRuns[i]->GetFirst(varRecord);
    

    currentRecord = new RecordTracker();
    currentRecord->r = varRecord;
    currentRecord->runCount = i;
    currentRecord->numberOfPage = indexOfPages[i];

    sortRec.push_back(currentRecord);
		
		
		i++;
	}
 
 
 
  while(!sortRec.empty())
  {
	no++;

	std::make_heap(sortRec.begin(),sortRec.end(),compareHeap);
    

    currentRecord = new RecordTracker();
    currentRecord = sortRec.front(); 
    currentRun = currentRecord->runCount;
    outgoingPage = currentRecord->numberOfPage;

    std::pop_heap(sortRec.begin(),sortRec.end());
    sortRec.pop_back();


    tparameters->outP->Insert(currentRecord->r);

    nextRecord = new RecordTracker();
    nextRecord->runCount = currentRun;
    nextRecord->numberOfPage = outgoingPage;


    if(pagesOfRuns[currentRun]->GetFirst(nextRecord->r)) {
      sortRec.push_back(nextRecord);
      
    }
    else {
		
		if((currentRun+1) == (indexOfPages.size()))
		{
			varP = 1;
		}

      if(outgoingPage+2 < file.GetLength()){ 

        if((outgoingPage+1)< indexOfPages[currentRun+1] || varP==1) {
          
          pageTe = new Page();
          file.GetPage(pageTe,outgoingPage+1);
          pagesOfRuns[currentRun] = pageTe;
        
          if(pagesOfRuns[currentRun]->GetFirst(nextRecord->r)) {
            nextRecord->numberOfPage = outgoingPage+1;
            sortRec.push_back(nextRecord);
          
          }
          
        }
      } 
      
      varP = 0;

    }
  }
}
  

  file.Close();
   tparameters->outP->ShutDown();

}


// int DBFile::Create(char *f_path, fType f_type, void *startup)
// {
//     if (f_path == NULL)
//         return 0;
//     cout << "Creating new binary heap file..." << endl;
//     file.Open(0, (char *)f_path);
//     this->filePath = (char *)f_path;
//     poff = 0;
//     roff = 0;
//     pDirty = false;
//     end = true;
//     return 1;
// }

// /*
//     Loads all the records from table files to respective binary database files
// */
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







void pagesToFile() 
{

	
	Page pageTe;
	Record *tempRec = NULL;


  int numberOfPage = 0;

  numberOfPage = file.GetLength();
  
  
  if(numberOfPage>0){
	  
	  numberOfPage = numberOfPage-1;
  }

  indexOfPages.push_back(numberOfPage);
  int i = 0;
  int size = sortRec.size();
  

	while(i < size) {
	
	
	 tempRec = sortRec[i]->r;


    if (!pageTe.Append(tempRec)) {


      file.AddPage(&pageTe, numberOfPage);


      pageTe.EmptyItOut();
      numberOfPage++;
	  pageTe.Append(tempRec);
    }
	
	i++;
	
}

  file.AddPage(&pageTe, numberOfPage);
 
  

}




//This is the main logic for sorting of the process. It contains pipeFilled which checks if data is present 
//in the pipeline. It uses compareRecord to compare the records. This function also takes pages to 
//temporary.bin for a short period and then these pages are taken to be sorted

void *eSorting(void *tparameters){
	
	
thread_p *td = (thread_p *) tparameters;

	int pagesCount = 0;
	bool pipeFilled = 1;
	
	Record fetchRecord;
	RecordTracker *tRec;
	Page pageTe;
	int no = 0;




	file.Open(0, path);
	//until pipe is filled, loop
	while (pipeFilled) {
		while (pagesCount < td->runLength) {

			if (td->inP->Remove(&fetchRecord)) {
				no++;

				tRec = new (std::nothrow) RecordTracker;
				tRec->r = new Record;

				tRec->r->Copy(&fetchRecord);
				tRec->runCount = atRun;


				sortRec.push_back(tRec);




				if (0 == pageTe.Append(&fetchRecord)) {

					pagesCount++;
					pageTe.EmptyItOut();
					pageTe.Append(&fetchRecord);

				}
			} else {
				//If pipe is empty, exit from loop
				pipeFilled = 0;
				
				break;
			}
		}


		atRun++;
		
		std::sort(sortRec.begin(), sortRec.end(), compareRecord);

		pagesToFile();


		
		pagesCount = 0;
	
		
		sortRec.clear();
	
	}
	 

	file.Close();

	mergingProcessOnPages(td);
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



BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {



thread_p *thrd = new (std::nothrow) thread_p;



	thrd->inP = &in;
	thrd->outP = &out;
	thrd->sortOrder = &sortorder;
	thrd->runLength = runlen;
	order = &sortorder;

	sortorder.Print();
	

	pthread_t bThread;
	pthread_create(&bThread, NULL, eSorting, (void *) thrd);
}


	

BigQ::~BigQ () {
}

