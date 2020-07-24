#include <algorithm>
#include <utility>
#include <cstdio>
#include <queue>
#include "BigQ.h"

BigQ :: BigQ (Pipe &_in, Pipe &_out, OrderMaker &_sortorder, int _lengthOfRun):
in(_in), out(_out), sortorder(_sortorder), lengthOfRun(_lengthOfRun), numberOfRuns(0), numberOfRecords(0), curr_pageno(0) {
	pthread_create (&worker_thread, NULL, &BigQ::thread_starter, this);
}

void * BigQ :: thread_starter(void *context){
  ((BigQ *)context)->WorkerThread();
}

void * BigQ :: WorkerThread(void) {
	char tempfilename[] = "/tmp/tempfileXXXXXX";
	tempfile.OpenTemp(tempfilename);

	
	// read the data from input pipe and sort
	const size_t max_runsize = lengthOfRun * PAGE_SIZE;
	size_t curr_runsize = 0;

	vector<Record> run;
	run.reserve(lengthOfRun);

	Record temprec;
	while(in.Remove(&temprec)){
		numberOfRecords++;
		size_t recsize = temprec.GetSize();

		if(curr_runsize + recsize > max_runsize){
			SortRun(run);
			pair<off_t, off_t> offset = WriteRunToTempFile(run);
			run_page_offsets.push_back(offset);

			numberOfRuns++;
			run.clear();
			curr_runsize = 0;
		}

		run.push_back(temprec);
		curr_runsize += recsize;
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

	if(curr_runsize > 0){
		SortRun(run);
		pair<off_t, off_t> offset = WriteRunToTempFile(run);
		run_page_offsets.push_back(offset);

		numberOfRuns++;
		run.clear();
		curr_runsize = 0;
	}
    
    // construct priority queue over sorted runs and dump sorted data into the out pipe
 	if(numberOfRuns <= RUN_THRESHOLD){
 		vector<Run> runs;
		runs.reserve(numberOfRuns);
		for(int i=0; i<numberOfRuns; i++){
			runs.emplace_back(Run(i, run_page_offsets[i].first, run_page_offsets[i].second, &tempfile));
		}

		vector<Record> records;
		records.reserve(numberOfRuns);
		for(int i=0; i<numberOfRuns; i++){
			Record temp;
			runs[i].GetNextRecord(&temp);
			records.push_back(temp);
		}

		CompareRecord comp(sortorder);
		for(int i=0; i<numberOfRecords; i++){

			int runid = distance(records.begin(), min_element(records.begin(), records.end(), comp));

			out.Insert(&records[runid]);

			if(!runs[runid].GetNextRecord(&records[runid])){
				records.erase(records.begin() + runid);
				runs.erase(runs.begin() + runid);
			}
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

		
	}else{
		vector<Run> runs;
		runs.reserve(numberOfRuns);
		for(int i=0; i<numberOfRuns; i++){
			runs.emplace_back(Run(i, run_page_offsets[i].first, run_page_offsets[i].second, &tempfile));
		}

		priority_queue<RunRecord, vector<RunRecord>, CompareRunRecord> pq(sortorder);
		for(int i=0; i<numberOfRuns; i++){
			Record temp;
			runs[i].GetNextRecord(&temp);
			pq.push(RunRecord(i, temp));
		}

		for(int i=0; i<numberOfRecords; i++){
			RunRecord rr(pq.top());
			Record r(rr.record);
			int runid = rr.runid;

			out.Insert(&r);
			pq.pop();

			if(runs[runid].GetNextRecord(&r)){
				pq.push(RunRecord(runid, r));
			}
		}
	}

	out.ShutDown ();

 	tempfile.Close();
 	remove(tempfilename);

 	pthread_exit(NULL); 
}

BigQ::~BigQ () {}


/*

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



*/

void BigQ::SortRun(vector<Record> &run){
	CompareRecord comp(sortorder);
	sort(run.begin(), run.end(), comp);
}

pair<off_t, off_t> BigQ::WriteRunToTempFile(vector<Record> &run){
	off_t start = curr_pageno;

	Page page;
	for(Record r: run){
		if(!page.Append(&r)){
			tempfile.AddPage(&page, curr_pageno);
			page.EmptyItOut();
			curr_pageno++;
			
			page.Append(&r);
		}
	}

	tempfile.AddPage(&page, curr_pageno);
	page.EmptyItOut();
	curr_pageno++;

	return make_pair(start, curr_pageno);
}




Run::Run(int _runid, off_t _startpage, off_t _endpage, File *_tempfile): 
runid(_runid), startpage(_startpage), endpage(_endpage), currpage(_startpage), tempfile(_tempfile) {
	tempfile->GetPage(&page, startpage);
}

Run::Run(const Run &run) :
  runid(run.runid), startpage(run.startpage), endpage(run.endpage), currpage(run.currpage), tempfile(run.tempfile){
      tempfile->AddPage(const_cast<Page *>(&run.page), currpage);
      tempfile->GetPage(&page,currpage);
  }

Run & Run::operator= (const Run &run){
      runid = run.runid;
      startpage = run.startpage;
      endpage = run.endpage;
      currpage = run.currpage; 

      run.tempfile->AddPage(const_cast<Page *>(&run.page),currpage);
      tempfile = run.tempfile;
      tempfile->GetPage(&page,currpage);
      return * this;
}



/*


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



*/

Run::~Run(){}

int Run::GetNextRecord(Record *record){
	if(!page.GetFirst(record)){
		if(currpage < endpage-1){
			page.EmptyItOut();
			currpage++;
			tempfile->GetPage(&page, currpage);

			return page.GetFirst(record);
		}else{
			return 0;
		}
	}

	return 1;
}


RunRecord::RunRecord(int _runid, Record _record):runid(_runid), record(_record) {}
RunRecord::~RunRecord(){ 
}




CompareRecord::CompareRecord(OrderMaker _sortorder): sortorder(_sortorder){}
CompareRecord::~CompareRecord(){}

bool CompareRecord::operator()(const Record &r1, const Record &r2){
	return (comp.Compare(const_cast<Record*>(&r1), const_cast<Record*>(&r2), &sortorder) < 0);
}


CompareRunRecord::CompareRunRecord(OrderMaker _sortorder): sortorder(_sortorder){}
CompareRunRecord::~CompareRunRecord(){}

bool CompareRunRecord::operator()(const RunRecord &rr1, const RunRecord &rr2){
	return (comp.Compare(const_cast<Record*>(&rr1.record), const_cast<Record*>(&rr2.record), &sortorder) < 0);
}
