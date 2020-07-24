#ifndef _DATABASE_H_
#define _DATABASE_H_

#include <algorithm>

class Database {
public:
  bool createTable();
  bool insertInto();
  bool dropTable();
  void setOutput();

private:
  bool exists(const char* relName);

  static inline std::string &ltrim(std::string &s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
    return s;
  }

  
  static inline std::string &rtrim(std::string &s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
    return s;
  }

  
  static inline std::string &trim(std::string &s) {
    return ltrim(rtrim(s));
  }
};


#endif
