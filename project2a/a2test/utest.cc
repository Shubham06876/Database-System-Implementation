#include "BigQ.h"
#include "test.h"
#include <gtest/gtest.h>


TEST(BigQ, recordComparator1){
	//Both records null
    ASSERT_FALSE(compareRecord(NULL,NULL));
}
TEST(BigQ, recordComparator3){
	//left record null
    const RecordTracker *right;
    ASSERT_FALSE(compareRecord(NULL,right));
}
TEST(BigQ, recordComparator2){
	//right record null
    const RecordTracker *left;
    ASSERT_FALSE(compareRecord(left,NULL));
}


TEST(BigQ, heapComparator3){
	//right and left entries null
    ASSERT_TRUE(compareHeap(NULL,NULL));
}
TEST(BigQ, heapcomparator1){
	//left entry null
    const RecordTracker *right;
    ASSERT_TRUE(compareHeap(NULL,right));
}
TEST(BigQ, heapComparator2){
	//right entry null
    const RecordTracker *left;
    ASSERT_TRUE(compareHeap(left,NULL));
}



int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
