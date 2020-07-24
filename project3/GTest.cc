#include "../project3/DBFile.h"
#include "../project3/BigQ.h"
#include "../project3/test.h"
#include "../project3/Pipe.h"
#include <gtest/gtest.h>
#include "../project3/RelOp.h"

Pipe in(100);
Pipe out(100);



TEST(SelectFile, Run) {
    SelectFile file;
    file.Use_n_Pages(-1);
    ASSERT_FALSE(file.Use_n_Pages(-1) == 1);
}

TEST(Project, Run) {
    Project project;
    project.Use_n_Pages(-1);
    ASSERT_FALSE(project.Use_n_Pages(-1) == 1);
}

TEST(Sum, Run) {
    Sum sum;
    sum.Use_n_Pages(-1);
    ASSERT_FALSE(sum.Use_n_Pages(-1) == 1);
}

TEST(Join, Run) {
    Join join;
    join.Use_n_Pages(-1);
    ASSERT_FALSE(join.Use_n_Pages(-1) == 1);
}




int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}