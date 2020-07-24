#include "BigQ.h"

//Constructor for BigQ class
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    inputPipe = &in;
    outputPipe = &out;
    this->sortOrder = &sortorder;
    lengthOfRuns = runlen;
    totalPages = 1;
    pthread_create(&worker, NULL, StartMainThread, (void *)this);
}

//function to write runs to file
bool BigQ ::fileWriter() {
    int pageCounter = 1;
    Page* tempPage = new Page();
    int firstPageOffset = totalPages;
    int recordListSize = listOfRecords.size();

    //run until the sizeOfJoin of the records list
    for (int index = 0; index < recordListSize; index++) {
        Record* tempRecord = listOfRecords[index];
        if ((tempPage->Append (tempRecord)) == 0) {
            pageCounter++;
            fileForRuns.AddPage (tempPage, totalPages);
            totalPages++;

            tempPage->EmptyItOut ();
            tempPage->Append (tempRecord);
        }
        delete tempRecord;
		
    }
	
    fileForRuns.AddPage(tempPage, totalPages);
    totalPages++;

    tempPage->EmptyItOut();
    listOfRecords.clear();
    delete tempPage;

    UpdateQueue(recordListSize, firstPageOffset);
    return true;
	
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

//Function to add runs to priority Queue
void BigQ :: UpdateQueue (int sizeOfRuns, int pageOffset) {

    SortInterface* tempRun = new SortInterface(sizeOfRuns, pageOffset, &fileForRuns, sortOrder);
    priorityQueueForRuns.push(tempRun);
}

//Function to sort the list of records
void BigQ :: ListOfRecordsSorter() {
	
    Page* tempPage = new Page();
    Record* record = new Record();

    int pageIndex = 0;
    int recordIndex = 0;
    //int runIndex = 0;

	
    srand (time(NULL));
    fileNamePointer = new char[100];
    sprintf (fileNamePointer, "%d.txt", (long int)worker);
    fileForRuns.Open (0, fileNamePointer);

    //Until input pipe has records
    while (inputPipe->Remove(record)) {
		
        Record* tempRecord = new Record ();
		tempRecord->Copy (record);
        
		recordIndex++;

		//for pages of run length
        if (tempPage->Append (record) == 0) {
            pageIndex++;
            if (pageIndex == lengthOfRuns) {
                sort (listOfRecords.begin (), listOfRecords.end (), RecordsComparisonHelper (sortOrder));
                fileWriter();
                pageIndex = 0;
            }
			
            tempPage->EmptyItOut ();
            tempPage->Append (record);
        }
        listOfRecords.push_back (tempRecord);
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
    
    // for the last run
    if(listOfRecords.size () > 0) {
        sort (listOfRecords.begin (), listOfRecords.end (), RecordsComparisonHelper (sortOrder));
        fileWriter();
        tempPage->EmptyItOut ();
    }
    delete record;
    delete tempPage;
}

//Function to merge the sorted runs, record by record
void BigQ :: MergeSortedRuns () {
    
    SortInterface* tempRun = new SortInterface (&fileForRuns, sortOrder);
    Page page;

    //until priority is not empty
    while (!priorityQueueForRuns.empty ()) {
		
        Record* tempRecord = new Record ();
        tempRun = priorityQueueForRuns.top ();
        priorityQueueForRuns.pop ();
        tempRecord->Copy (tempRun->initialRecord);
        outputPipe->Insert (tempRecord);
		
        if (tempRun->fetchInitialRecord() > 0) {
            priorityQueueForRuns.push(tempRun);
		}
        delete tempRecord;
    }

    fileForRuns.Close();
    remove(fileNamePointer);
    outputPipe->ShutDown();
    delete tempRun;
}

