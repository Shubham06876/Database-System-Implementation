#include "gtest/gtest.h"
#include "Database.h"

Database *database = new Database();

TEST(Database, Test1){
    
    database->createTable();
    FILE* f = fopen("test_tbl.bin", "r");
    ASSERT_TRUE(f != NULL);
}

TEST(Database, Test2){
    
    database->dropTable();
    FILE* f = fopen("test_tbl.bin", "r");
    ASSERT_FALSE(f != NULL);
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}