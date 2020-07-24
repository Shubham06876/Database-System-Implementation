
#include "Statistics.h"
#include <gtest/gtest.h>


TEST(Statistics, TestFailureAddRel) {
    Statistics stat;
    ASSERT_EQ(stat.AddRel(NULL,0),false);
}

TEST(Statistics, TestSuccessAddRel) {
    Statistics stat;
    ASSERT_EQ(stat.AddRel("customer",0),true);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}