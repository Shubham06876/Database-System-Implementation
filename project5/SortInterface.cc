#include "SortInterface.h"

SortInterface :: SortInterface (int runLength1, int pageOffset1, File *file, OrderMaker* order) {

    runsSize = runLength1;
    pageIndex = pageOffset1;
    initialRecord = new Record ();
    fileForRuns = file;
    sortOrder = order;
    fileForRuns->GetPage (&bufferPage, pageIndex);
    fetchInitialRecord();

}



int SortInterface :: fetchInitialRecord () {
    Record* record = new Record();
    if(runsSize <= 0)
        return 0;

    if (bufferPage.GetFirst(record) == 0) {
        pageIndex++;
        fileForRuns->GetPage(&bufferPage, pageIndex);
        bufferPage.GetFirst(record);
    }
    runsSize--;
    initialRecord->Consume(record);
    return 1;
}

SortInterface :: SortInterface (File *file, OrderMaker *order) {
    fileForRuns = file;
    sortOrder = order;
    initialRecord = NULL;
}

SortInterface :: ~SortInterface () {
    delete initialRecord;
}
