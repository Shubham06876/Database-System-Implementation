#include "ComparisonHelper.h"

// This function initializes the sortOrder with the given Order
RecordsComparisonHelper :: RecordsComparisonHelper (OrderMaker *givenOrder) {
    sortOrder = givenOrder;
}

//returns true is the records are in the correct order as per the given sort order
bool RecordsComparisonHelper::operator() (Record* leftRecord, Record* rightRecord) {

    ComparisonEngine comparisonEngine;
    return comparisonEngine.Compare(leftRecord, rightRecord, sortOrder) < 0;
}

//returns true is the records in the run are in the correct order as per the given sort order
bool RunsComparisonHelper :: operator() (SortInterface* left, SortInterface* right) {

    ComparisonEngine comparisonEngine;
    return comparisonEngine.Compare(left->initialRecord, right->initialRecord, left->sortOrder) >= 0;
}
