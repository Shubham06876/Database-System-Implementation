#include "RelOp.h"
#include "BigQ.h"

#include <vector>
#include <sstream>
#include <string>

void RelationalOp::WaitUntilDone() {
  	pthread_join (thread, NULL);
}

int RelationalOp::create_joinable_thread(pthread_t *thread,
                                         void *(*start_routine) (void *), void *arg) {
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
  int retVal = pthread_create(thread, &attr, start_routine, arg);
  pthread_attr_destroy(&attr);
  return retVal;
}

//SelectFile takes a DBFile and a pipe as input. It also takes a CNF. It then performs a
//scan of the underlying file, and for every tuple accepted by the CNF, it stuffs the tuple
//into the pipe as output. 


SelectFile::~SelectFile(){

	outPipe = NULL;
	inFile = NULL;
	literal = NULL;
	selOp = NULL;
	
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {

	this->outPipe = &outPipe;
	this->inFile = &inFile;
	this->numberOfPgs = DEFAULT_NUM_PAGES;
	this->literal = &literal;
	this->selOp = &selOp;
		

	create_joinable_thread(&thread, SelectFile::HelperRun, (void *) this);
}



int SelectFile::Use_n_Pages (int n) {
	if(n < 0){
		return 0;
	}
	this->numberOfPgs = n;
	return 1;
}

void * SelectFile::HelperRun(void *context) {
	((SelectFile *) context)->StartRun();
}

void SelectFile::StartRun(){

	Record tRec;

	while(inFile->GetNext(tRec, *selOp, *literal)){
		outPipe->Insert(&tRec);
	}

	outPipe->ShutDown();
}

void SelectFile::WaitUntilDone (){
	RelationalOp::WaitUntilDone();
}


//SelectPipe takes two pipes as input: an input pipe and an output pipe. It also takes a
//CNF. It simply applies that CNF to every tuple that comes through the pipe, and every
//tuple that is accepted is stuffed into the output pipe.



SelectPipe::~SelectPipe(){
	outPipe = NULL;
	inPipe = NULL;
	literal = NULL;
	selOp = NULL;
	
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal){
	this->outPipe = &outPipe;
	this->inPipe = &inPipe;
	this->literal = &literal;
	this->numberOfPgs = DEFAULT_NUM_PAGES;
	this->selOp = &selOp;
	

	create_joinable_thread(&thread, SelectPipe::HelperRun, (void *) this);
}

void SelectPipe::StartRun(){
	

	Record tRec;
	ComparisonEngine comparisonEngine;

	while(inPipe->Remove(&tRec)){
		if(comparisonEngine.Compare(&tRec, literal, selOp)){
			outPipe->Insert(&tRec);
		}
	}

	outPipe->ShutDown();
}

void SelectPipe::WaitUntilDone (){
	RelationalOp::WaitUntilDone();
}
	
int SelectPipe::Use_n_Pages (int n){
	if(n < 0){
		return 0;
	}
	this->numberOfPgs = n;
	return 1;
}

void * SelectPipe::HelperRun(void *context){
	((SelectPipe *) context)->StartRun();
}





//Project takes an input pipe and an output pipe as input. It also takes an array of
//integers keepMe as well as the number of attributes for the records coming through the
//input pipe and the number of attributes to keep from those input records. The array of
//integers tells Project which attributes to keep from the input records, and which order
//to put them in. So, for example, say that the array keepMe had the values [3, 5, 7,
//1]. This means that Project should take the third attribute from every input record
//and treat it as the first attribute of those records that it puts into the output pipe.
//Project should take the fifth attribute from every input record and treat it as the
//second attribute of every record that it puts into the output pipe. The seventh input
//attribute becomes the third. And so on.




Project::~Project(){

	outPipe = NULL;
	keepMe = NULL;
	inPipe = NULL;
	
	
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput){

	this->outPipe = &outPipe;
	this->keepMe = keepMe;
	this->inPipe = &inPipe;
	this->numAttsOutput = numAttsOutput;
	this->numberOfPgs = DEFAULT_NUM_PAGES;
	this->numAttsInput = numAttsInput;
	

	create_joinable_thread(&thread, Project::HelperRun, (void *) this);
}

void Project::StartRun(){

	Record tRec;
	ComparisonEngine comparisonEngine;

	while(inPipe->Remove(&tRec)){
		tRec.Project(keepMe, numAttsOutput, numAttsInput);
		outPipe->Insert(&tRec);
	}

	outPipe->ShutDown();	
}

void * Project::HelperRun(void *context){
	((Project *) context)->StartRun();
}

int Project::Use_n_Pages (int n){
	if(n < 0){
		return 0;
	}
	this->numberOfPgs = n;
	return 1;
}

void Project::WaitUntilDone (){
	RelationalOp::WaitUntilDone();
}
	
//Join takes two input pipes, an output pipe, and a CNF, and joins all of the records from
//the two pipes according to that CNF. Join should use a BigQ to store all of the tuples coming
//from the left input pipe, and a second BigQ for the right input pipe, and then perform a
//merge in order to join the two input pipes. You’ll create the OrderMakers for the two
//BigQ’s using the CNF (the function GetSortOrders will be used to create the
//OrderMakers). If you can’t get an appropriate pair of OrderMakers because the CNF
//can’t be implemented using a sort-merge join (due to the fact it does not have an equality
//check) then your Join operation should default to a block-nested loops join.


Join::~Join(){
	outPipe = NULL;
	
	literal = NULL;
	selOp = NULL;
	inPipeL = NULL;
	inPipeR = NULL;
	
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->numberOfPgs = DEFAULT_NUM_PAGES;
	this->literal = &literal;
	

	create_joinable_thread(&thread, Join::HelperRun, (void *) this);
}

void * Join::HelperRun(void *context){
	((Join *) context)->StartRun();
}

void Join::StartRun(){

	OrderMaker leftOrder, rightOrder;
	int isSorted = selOp->GetSortOrders(leftOrder, rightOrder);

	if(!isSorted){
		//nested loop to do a cross product of both input pipes
		BlockNestedJoin();
	}else{
		//do a sort-merge join
		SortMergeJoin(leftOrder, rightOrder);
	}
	
	outPipe->ShutDown();
}

void Join::SortMergeJoin(OrderMaker &leftOrder, OrderMaker rightOrder){

	ComparisonEngine comparisonEngine; 
	Record tRecL, tRecR, tMergeRecord;
	


	Pipe outPipeL(numberOfPgs);
	Pipe outPipeR(numberOfPgs);
	BigQ bigQL(*inPipeL, outPipeL, leftOrder, numberOfPgs);
	BigQ bigQR(*inPipeR, outPipeR, rightOrder, numberOfPgs);

	int isOutPipeLEmpty = 0, isOutPipeREmpty = 0;

	if(!outPipeL.Remove(&tRecL)){
		isOutPipeLEmpty = 1;
	}
	if(!outPipeR.Remove(&tRecR)){
		isOutPipeREmpty = 1;
	}

	const int attL = tRecL.GetNumAtts();
	const int attR = tRecR.GetNumAtts();
	const int sumOfAtts = attL + attR;
	int atrRelevant[sumOfAtts];

	int varCnt=0;
	int i=0;
	while(i<attL){
			atrRelevant[varCnt] = i; 
			varCnt++;
			i++;
	}

	i=0;
	while(i<attR){
		atrRelevant[varCnt] = i; 
		varCnt++;
		i++;
	}

	vector<Record> leftBuffer;
	vector<Record> rightBuffer;
	leftBuffer.reserve(1000);
	rightBuffer.reserve(1000);

	while(!isOutPipeLEmpty && !isOutPipeREmpty){
		if(comparisonEngine.Compare(&tRecL, &leftOrder, &tRecR, &rightOrder) < 0){
			
			//move to next record in left pipe
			if(!outPipeL.Remove(&tRecL)){
				isOutPipeLEmpty = 1;
			}
		}else if(comparisonEngine.Compare(&tRecL, &leftOrder, &tRecR, &rightOrder) > 0){
			
			//move to next record in right pipe
			if(!outPipeR.Remove(&tRecR)){
				isOutPipeREmpty = 1;
			}
		}else{ 
			//records match as per sort order
			
			leftBuffer.push_back(tRecL);
			rightBuffer.push_back(tRecR);

			//fetch all records from left pipe which match current sort order
			Record recL;
			recL.Copy(&tRecL);
			if(outPipeL.Remove(&tRecL)){
				while(!comparisonEngine.Compare(&tRecL, &recL, &leftOrder)){
					leftBuffer.push_back(tRecL);
					if(!outPipeL.Remove(&tRecL)){
						isOutPipeLEmpty = 1;
						break;
					}
				}
			}else{
				isOutPipeLEmpty = 1;
			}

			//fetch all records from right pipe which match current sort order
			Record recR;
			recR.Copy(&tRecR);
			if(outPipeR.Remove(&tRecR)){
				//cout<<"fetching matching records from right pipe"<<endl;
				while(!comparisonEngine.Compare(&tRecR, &recR, &rightOrder)){
					rightBuffer.push_back(tRecR);
					if(!outPipeR.Remove(&tRecR)){
						isOutPipeREmpty = 1;
						break;
					}
				}
			}else{
				isOutPipeREmpty = 1;
			}

			int sizeL = leftBuffer.size();
			int sizeR = rightBuffer.size();

			//join left and right buffer records which satisfy given cnf
			for(int i=0; i<sizeL; i++){
				for(int j=0; j<sizeR; j++) {
					if(comparisonEngine.Compare(&leftBuffer[i], &rightBuffer[j], literal, selOp)){
						
						tMergeRecord.MergeRecords(&leftBuffer[i], &rightBuffer[j], attL, attR, atrRelevant, sumOfAtts, attL);
						outPipe->Insert(&tMergeRecord);
					}
				}
			}

			leftBuffer.clear();
			rightBuffer.clear();
		}
	}
}

int Join::Use_n_Pages (int n){
	if(n < 0){
		return 0;
	}
	this->numberOfPgs = n;
	return 1;
}

void Join::WaitUntilDone (){
	RelationalOp::WaitUntilDone();
}
	



void Join::BlockNestedJoin(){


	Record tRecL, tRecR, tMergeRecord;
	ComparisonEngine comparisonEngine; 

	if(!inPipeL->Remove(&tRecL)){
		return;
	}
	if(!inPipeR->Remove(&tRecR)){
		return;
	}

	const int attL = tRecL.GetNumAtts();
	const int attR = tRecR.GetNumAtts();
	const int sumOfAtts = attL + attR;
	int atrRelevant[sumOfAtts];

	int varCnt=0;
	int i=0;
	while(i<attL){
			atrRelevant[varCnt] = i; 
			varCnt++;
			i++;
	}

	i=0;
	while(i<attR){
		atrRelevant[varCnt] = i; 
		varCnt++;
		i++;
	}

	vector<Record> leftBuffer;
	do{
		leftBuffer.push_back(tRecL);
	}while(inPipeL->Remove(&tRecL));

	int sizeL = leftBuffer.size();

	do{
		for(int i=0; i<sizeL; i++){
			if(comparisonEngine.Compare(&leftBuffer[i], &tRecR, literal, selOp)){
				tMergeRecord.MergeRecords(&leftBuffer[i], &tRecR, attL, attR, atrRelevant, sumOfAtts, attL);
				outPipe->Insert(&tMergeRecord);
			}
		}
	}while(inPipeR->Remove(&tRecR));

	leftBuffer.clear();
}



//DuplicateRemoval takes an input pipe, an output pipe, as well as the schema for the
//tuples coming through the input pipe, and does a duplicate removal. That is, everything that
//comes through the output pipe will be distinct. It will use the BigQ class to do the duplicate
//removal. 

DuplicateRemoval::~DuplicateRemoval(){
	
	mySchema = NULL;
	outPipe = NULL;
	inPipe = NULL;
	
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {

	this->outPipe = &outPipe;
	this->mySchema = &mySchema;
	this->inPipe = &inPipe;
	this->numberOfPgs = DEFAULT_NUM_PAGES;

	create_joinable_thread(&thread, DuplicateRemoval::HelperRun, (void *) this);
}

int DuplicateRemoval::Use_n_Pages (int n){
	if(n < 0){
		return 0;
	}
	this->numberOfPgs = n;
	return 1;
}

void * DuplicateRemoval::HelperRun(void *context){
	((DuplicateRemoval *) context)->StartRun();
}

void DuplicateRemoval::WaitUntilDone (){
	RelationalOp::WaitUntilDone();
}
	

void DuplicateRemoval::StartRun(){

	OrderMaker sortOrder(mySchema);
	Pipe tempOutPipe(numberOfPgs);
	BigQ bigQ(*inPipe, tempOutPipe, sortOrder, numberOfPgs);

	ComparisonEngine comparisonEngine;
	Record prevRec, currRec;
	if(tempOutPipe.Remove(&currRec)){
		prevRec.Copy(&currRec);
		outPipe->Insert(&currRec);

		while(tempOutPipe.Remove(&currRec)){
			if(comparisonEngine.Compare(&prevRec, &currRec, &sortOrder)){
				prevRec.Copy(&currRec);
				outPipe->Insert(&currRec);
			}
		}	
	}

	outPipe->ShutDown();	
}


//Sum computes the SUM SQL aggregate function over the input pipe, and puts a single
//tuple into the output pipe that has the sum.

Sum::~Sum(){
	outPipe = NULL;
	computeMe = NULL;
	inPipe = NULL;
	
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	this->outPipe = &outPipe;
	this->computeMe = &computeMe;
	this->inPipe = &inPipe;
	
	this->numberOfPgs = DEFAULT_NUM_PAGES;

	create_joinable_thread(&thread, Sum::HelperRun, (void *) this);
}

void Sum::WaitUntilDone (){
	RelationalOp::WaitUntilDone();
}


void * Sum::HelperRun(void *context){
	((Sum *) context)->StartRun();
}

int Sum::Use_n_Pages (int n){
	if(n < 0){
		return 0;
	}
	this->numberOfPgs = n;
	return 1;
}

void Sum::StartRun(){

	Record tRec;
	int tIntRes, intRes = 0;
	double tDblRes, dblRes = 0.0;
	Type typeOfRes;

	while(inPipe->Remove(&tRec)){
		
		typeOfRes = computeMe->Apply(tRec, tIntRes, tDblRes);

		if(Int == typeOfRes){
			intRes += tIntRes;
		}else{
			dblRes += tDblRes;
		}
	}

    stringstream rec;
    Attribute attr;
    attr.name = "SUM";
    if (Int == typeOfRes){
        attr.myType = Int;
        rec << intRes;
    }else{
        attr.myType = Double;
       	rec << dblRes;
    }
    rec << "|";
    
    Schema tempSchema("temp_schema", 1, &attr);
    tRec.ComposeRecord(&tempSchema, rec.str().c_str());
    outPipe->Insert(&tRec);

	outPipe->ShutDown();	
}


//GroupBy is a lot like Sum, except that it does grouping, and then puts one sum into the
//output pipe for each group. Every tuple put into the output pipe has a sum as the first
//attribute, followed by the values for each of the grouping attributes as the remainder of the
//attributes. The grouping is specified using an instance of the OrderMaker class that is
//passed in. The sum to compute is given in an instance of the Function class.

GroupBy::~GroupBy(){

	computeMe = NULL;
	outPipe = NULL;
	groupAtts = NULL;
	inPipe = NULL;
	
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {

	this->groupAtts = &groupAtts;
	this->computeMe = &computeMe;
	
	this->outPipe = &outPipe;
	this->inPipe = &inPipe;
	this->numberOfPgs = DEFAULT_NUM_PAGES;

	create_joinable_thread(&thread, GroupBy::HelperRun, (void *) this);
}

void * GroupBy::HelperRun(void *context){
	((GroupBy *) context)->StartRun();
}

void GroupBy::StartRun(){
	
	//sort records from input pipe
	Pipe tempOutPipe(numberOfPgs);
	BigQ bigQ(*inPipe, tempOutPipe, *groupAtts, numberOfPgs);
	Record tRec1, tRec2, resRec;
	ComparisonEngine comparisonEngine;
	

	if(tempOutPipe.Remove(&tRec1)){

		Type typeOfRes;
		double dblRes = 0.0;
		int intRes = 0;
		
		ApplyFunction(tRec1, intRes, dblRes, typeOfRes);

		while(tempOutPipe.Remove(&tRec2)){
			
			if(!comparisonEngine.Compare(&tRec1, &tRec2, groupAtts)){
				ApplyFunction(tRec2, intRes, dblRes, typeOfRes);
			}else{
				PackResultInRecord(typeOfRes, intRes, dblRes, tRec1, resRec);
				
				outPipe->Insert(&resRec); 
				dblRes = 0.0;
				intRes = 0;
				
				ApplyFunction(tRec2, intRes, dblRes, typeOfRes);

				tRec1.Consume(&tRec2);
			}
		}

		PackResultInRecord(typeOfRes, intRes, dblRes, tRec1, resRec);
		outPipe->Insert(&resRec);

	}

	outPipe->ShutDown();	
}
int GroupBy::Use_n_Pages (int n){
	if(n < 0){
		return 0;
	}
	this->numberOfPgs = n;
	return 1;
}

void GroupBy::ApplyFunction(Record &rec, int &intRes, double &dblRes, Type &typeOfRes){
	double tDblRes;
	int tIntRes;
	

	typeOfRes = computeMe->Apply(rec, tIntRes, tDblRes);
	if(Int == typeOfRes){
		intRes += tIntRes;
	}else{
		dblRes += tDblRes;
	}
}

void GroupBy::WaitUntilDone (){
	RelationalOp::WaitUntilDone();
}
	

void GroupBy::PackResultInRecord(Type typeOfRes, int &intRes, double &dblRes, Record &groupRec, Record &resultRec){

	stringstream recordStream;
    Attribute attr;
    attr.name = "SUM";
    if (Int == typeOfRes){
        attr.myType = Int;
        recordStream << intRes;
    }else{
        attr.myType = Double;
       	recordStream << dblRes;
    }
    recordStream << "|";
    
    Record sumRec;
    Schema tempSchema("temp_schema", 1, &attr);
    sumRec.ComposeRecord(&tempSchema, recordStream.str().c_str());

    const int numAtts = groupAtts->GetNumAtts()+1;
	int atrRelevant[numAtts];

	atrRelevant[0] = 0;
	for(int i=1; i<numAtts; i++){
		atrRelevant[i] = (groupAtts->GetWhichAtts())[i-1]; 
	}


	resultRec.MergeRecords(&sumRec, &groupRec, 1, groupRec.GetNumAtts(), atrRelevant, numAtts, 1);
}



//WriteOut accepts an input pipe, a schema, and a FILE*, and uses the schema to
//write text version of the output records to the file.

WriteOut::~WriteOut(){
	outFile = NULL;
	mySchema = NULL;
	inPipe = NULL;
	
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {

	this->mySchema = &mySchema;
	this->numberOfPgs = DEFAULT_NUM_PAGES;
	this->inPipe = &inPipe;
	this->outFile = outFile;
	

	create_joinable_thread(&thread, WriteOut::HelperRun, (void *) this);
}

int WriteOut::Use_n_Pages (int n) {
	if(n < 0){
		return 0;
	}
	this->numberOfPgs = n;
	return 1;
}

void * WriteOut::HelperRun(void *context){
	((WriteOut *) context)->StartRun();
}

void WriteOut::WaitUntilDone (){
	RelationalOp::WaitUntilDone();
}



void WriteOut::StartRun(){

	Record tRec;
	ostringstream os;

	while(inPipe->Remove(&tRec)){
      tRec.Print(mySchema, os);
	}

	fputs(os.str().c_str(), outFile);
}

