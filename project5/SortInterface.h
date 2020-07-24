
#ifndef SORTINTERFACE_H
#define SORTINTERFACE_H


#include "File.h"

class SortInterface {

private:
    Page bufferPage;
public:
    int pageIndex;
    int runsSize;
    Record* initialRecord;
    OrderMaker* sortOrder;
    File *fileForRuns;

    SortInterface (int runLength1, int pageOffset1, File *file, OrderMaker *order);
    SortInterface (File *file, OrderMaker *order);
    ~SortInterface();
    int fetchInitialRecord();
};


#endif
