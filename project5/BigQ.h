#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "SortInterface.h"
#include "ComparisonHelper.h"

using namespace std;


class BigQ {

private:
	int totalPages;
    Pipe *inputPipe;
    Pipe *outputPipe;
    pthread_t worker;
    OrderMaker *sortOrder;
    char *fileNamePointer;
    priority_queue<SortInterface*, vector<SortInterface*>, RunsComparisonHelper> priorityQueueForRuns;
	
	bool fileWriter();
    void UpdateQueue (int sizeOfRuns, int pageOffset);

public:
	File fileForRuns;
    vector<Record*> listOfRecords;
    int lengthOfRuns;
	
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ () {};
	void ListOfRecordsSorter();
    void MergeSortedRuns ();
	
    static void *StartMainThread (void *start) {
        BigQ *bigQ = (BigQ *)start;
        bigQ->ListOfRecordsSorter();
        bigQ->MergeSortedRuns();
    }
};

#endif
