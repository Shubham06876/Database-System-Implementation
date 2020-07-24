#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "algorithm"
#include "vector"
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "exception"
#include "Defs.h"



using namespace std;




class RecordTracker {

public : 
	Record *r;

	int runCount;
	int numberOfPage;

	RecordTracker();
	~RecordTracker();

};


class BigQ {

public:


	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();


};	

struct ThreadData {


Pipe *inP;
Pipe *outP;
OrderMaker *sortOrder;
int runLength;


};

typedef struct ThreadData thread_p;






	bool compareHeap(const RecordTracker *left, const RecordTracker *right);
	bool compareRecord(const RecordTracker *left, const RecordTracker *right) ;


#endif
