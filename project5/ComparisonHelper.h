

#ifndef P2_2WORKINGVERSION_ComparisonHelper_H
#define P2_2WORKINGVERSION_ComparisonHelper_H

#include "Comparison.h"
#include "SortInterface.h"

class RecordsComparisonHelper {

private:
    OrderMaker *sortOrder;
public:
    RecordsComparisonHelper (OrderMaker *givenOrder);
    bool operator() (Record* leftRecord, Record* rightRecord);
};

class RunsComparisonHelper {
public:
    bool operator() (SortInterface* left, SortInterface* right);
};


#endif 
