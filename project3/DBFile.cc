#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "HeapFile.h"
#include "SortedFile.h"
#include "Defs.h"
#include <fstream>
#include <string>
#include <iostream>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>

using namespace std;



DBFile::DBFile () {
}

DBFile::~DBFile(){
	delete dbfile;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
	//instantiate dbfile based on file type

		if(f_type==heap){
			cout<<"Creating heap dbfile"<<endl; 
			dbfile = new HeapFile();
		}
		else if(f_type==sorted){
			cout<<"Creating sorted dbfile"<<endl; 
			dbfile = new SortedFile();
		}
		else{
		cout<<"Invalid file type !! Exiting !!"<<endl;
		exit(-1);
		}

	//create a dbfile
	return dbfile->Create(f_path, f_type, startup);
}

int DBFile::Open (char *f_path) {
	//Fetching file type from meta file
	int f_type;
    string tempFileName;
	tempFileName.append(f_path);
	tempFileName.append(".meta");
	ifstream tempFile; 
	tempFile.open(tempFileName.c_str());
	if (!tempFile) {
		cerr << "unable to open meta file"<<endl;
		return 0;
	}
    tempFile >> f_type;
    fType file_type = (fType) f_type;
    tempFile.close();

    if(f_type==heap){
		cout<<"Creating heap dbfile"<<endl; 
		dbfile = new HeapFile();
	}
	else if(f_type==sorted){
		cout<<"Creating sorted dbfile"<<endl; 
		dbfile = new SortedFile();
	}
	else{
	cout<<"Invalid file type !! Exiting !!"<<endl;
	exit(-1);
	}

	return dbfile->Open(f_path);
}

/*


int DBFile::Create (char *f_path, fType f_type, void *getParams) {

	if(f_type== sorted){

		this->file= new SortedFile();

	}
	else if(f_type== heap){

		this->file= new HeapFile();	

	}

	if(file!=NULL){

		file->Create(f_path,f_type,getParams);		

	}	

}




*/
void DBFile::Load (Schema &f_schema, char *loadpath) {
	dbfile->Load(f_schema, loadpath);
}


int DBFile::GetNext (Record &fetchme) {
	return dbfile->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	return dbfile->GetNext(fetchme, cnf, literal);
}

void DBFile::MoveFirst () {
	dbfile->MoveFirst();
}



/*

int DBFile::GetNext (Record &fetchme) {

	return file->GetNext(fetchme);

}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

	return file->GetNext(fetchme,cnf,literal);

}



*/

void DBFile::Add (Record &rec) {
	cout<<"Adding record to dbfile"<<endl;
	dbfile->Add(rec);
}



int DBFile::GetRecNum(){
	return dbfile->GetRecNum();
}

int DBFile::Close () {
	int ret = dbfile->Close();
	return ret;
}
