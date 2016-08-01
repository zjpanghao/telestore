#ifndef TELEDBSERVER
#define TELEDBSERVER
#include <string>
#include "dbpool.h"
struct Url_data {
unsigned int count;
std::string time_stamp;
};
class TeleDBServer {
 public:
  TeleDBServer(DBPool *pool);
  ~TeleDBServer();
  int CheckAndUpdate(const char *url_key, Url_data *url_data);
  
 private:
  int InsertOrUpdateTeleCount(const char *url_key, Url_data *url_data); 
   Connection_T  conn;
};
#endif
