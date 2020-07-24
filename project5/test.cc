#include <iostream>

#include "QueryHelper.h"

using namespace std;

char* catalog_path = "catalog";
char* dbfile_dir = "";
char* tpch_dir = "../DATA/1G";

int main (int argc, char* argv[]) {
  QueryHelper it;
  it.run();
  return 0;
}
