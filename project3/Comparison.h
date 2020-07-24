#ifndef COMPARISON_H
#define COMPARISON_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <iostream>


// This stores an individual comparison that is part of a CNF
class Comparison {

	friend class ComparisonEngine;
	friend class CNF;

	Target operand1;
	int whichAtt1;
	Target operand2;
	int whichAtt2;

	Type attType;

	CompOperator op;

public:

	Comparison();

	//copy constructor
	Comparison(const Comparison &copyMe);

	// print to the screen
	void Print ();

	// getter methods
    int GetAtt1();
    int GetAtt2();
    Target GetOperand1();
    Target GetOperand2();
    CompOperator GetOp();
};


class Schema;
class CNF;

// This structure encapsulates a sort order for records
class OrderMaker {

	friend class ComparisonEngine;
	friend class CNF;

	int numAtts;

	int whichAtts[MAX_ANDS];
	Type whichTypes[MAX_ANDS];

public:
	
	// creates an empty OrdermMaker
	OrderMaker();

	// creates an empty OrdermMaker
	// create an OrderMaker that can be used to sort records
	// based upon ALL of their attributes
	OrderMaker(Schema *schema);

	//creates a new ordermaker which matches sortorder with cnf in some way
	OrderMaker(OrderMaker *sortorder, CNF *cnf);

	// writes out an ordermaker
    friend std::ostream& operator<<(std::ostream&, const OrderMaker&);
    // reads into an ordermaker
    friend std::istream& operator>>(std::istream&, OrderMaker&);

	// print to the screen
	void Print ();

	// getter methods
    int GetNumAtts();
    int* GetWhichAtts();
    Type* GetWhichTypes();

    // setter methods
    void SetNumAtts(int num);
    void SetWhichAtts(int whichAtts[]);
    void SetWhichTypes(Type whichTypes[]);
};

class Record;

// This structure stores a CNF expression that is to be evaluated
// during query execution

class CNF {

	friend class ComparisonEngine;

	Comparison orList[MAX_ANDS][MAX_ORS];
	
	int orLens[MAX_ANDS];
	int numAnds;

public:

	// this returns an instance of the OrderMaker class that
	// allows the CNF to be implemented using a sort-based
	// algorithm such as a sort-merge join.  Returns a 0 if and
	// only if it is impossible to determine an acceptable ordering
	// for the given comparison
	int GetSortOrders (OrderMaker &left, OrderMaker &right);

	// print the comparison structure to the screen
	void Print ();

        // this takes a parse tree for a CNF and converts it into a 2-D
        // matrix storing the same CNF expression.  This function is applicable
        // specifically to the case where there are two relations involved
        void GrowFromParseTree (struct AndList *parseTree, Schema *leftSchema, 
		Schema *rightSchema, Record &literal);

        // version of the same function, except that it is used in the case of
        // a relational selection over a single relation so only one schema is used
        void GrowFromParseTree (struct AndList *parseTree, Schema *mySchema, 
		Record &literal);

		 // getter methods
        int GetNumAnds();
        int* GetOrLens();
        Comparison* GetOrList(int i);
};

#endif
