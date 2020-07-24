#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

//constructor
DBFile::DBFile () {
    f= new File();
    r = new Page();
	w = new Page();
    currentRecord = new Record();
}
DBFile::~DBFile () {
	
}


int DBFile::Open (const char *file_path) {
    if (file_path[0] != '\0' && file_path != NULL) {
    f->Open(1,file_path);
    eof = 0;
    pIndex=1;        
    return 1;

    }
    return 0;   
      
    
}

int DBFile::Create (const char *file_path, fType file_type, void *startup) {

    if (file_path == NULL || file_path[0] == '\0') {
        return 0;
    }
    if (file_type !=heap){
        return 0;
    }

    f->Open(0,file_path);
    dirty=0;
    eof=1;
    pIndex=1;
    indexLocation=1;
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

//Below fnction forces the pointer to first record in the file:
void DBFile::MoveFirst () {
    //fetch first page from file
    f->GetPage(r,1);
    // delete and return the first record from a page; return zero if no records found
	r->GetFirst(currentRecord);
}

//Add new record to end of file
void DBFile::Add (Record &rec) {
	dirty=1;
	if(w->Append(&rec)==0)
	{		
		f->AddPage(w,indexLocation);
		indexLocation++;
		w->EmptyItOut();
		w->Append(&rec);
	}
}

// void DBFile::Add(Record &rec)
// {
//     if (&rec == NULL)
//         return;

//     pDirty = true;
//     if (page.Append(&rec) == 0)
//     {
//         file.AddPage(&page, poff++);
//         page.EmptyItOut();
//         page.Append(&rec);
//     }
// }


void DBFile::Load (Schema &f_schema, const char *loadpath) {
    FILE *file_access = fopen (loadpath,"r");
    if(file_access!=NULL){
        Record *v_record=new Record();
    while(v_record->SuckNextRecord(&f_schema,file_access)!=0)
        Add(*v_record);
    delete v_record;    
    fclose(file_access);
    }
    
}





int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine *cEngine = new ComparisonEngine(); 
	int res = 1;
	int cResult = 0;
	while(cResult==0&&res!=0){
		res = GetNext(fetchme);
        if(res==0)
		    return 0;
		cResult = cEngine->Compare(&fetchme,&literal,&cnf);
	}
	if(cResult==1)
		return 1;
    else
        return 0;
}

// fetch next record from file and return to user
int DBFile::GetNext (Record &fetchme) {
    //if end of file return 0
    if(!eof){
        fetchme.Copy(currentRecord);
        if(r->GetFirst(currentRecord)==0){
            pIndex++;
            if(pIndex<f->GetLength()-1){
                
                f->GetPage(r,pIndex);
                r->GetFirst(currentRecord);
                return 1;
               
            }
            else{
                eof = 1;
            }
        }
        return 1;
        }
    else{
       return 0;
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


int DBFile::Close () {
    if(dirty==1){
        f->AddPage(w, indexLocation++);
        w->EmptyItOut();
  }
    eof = 1;
    
	return f->Close();

}



// int DBFile::Close()
// {
//     cout << "Closing file." << endl;
//     return file.Close();
// }

