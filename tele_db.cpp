#include "tele_db.h"
#include <string>
#include <glog/logging.h>
TeleDBServer::TeleDBServer(DBPool *pool) : conn(NULL) {
 if (pool) {
	 conn = pool->GetConnection();
	 if (!conn) {
		LOG(ERROR) << "NULL connection";
		} 
	}
}

TeleDBServer::~TeleDBServer() {
 if (conn)
	Connection_close(conn);
	
}

int TeleDBServer::InsertOrUpdateTeleCount(const char *url_key, Url_data *url_data) {
	int nums = url_data->count;
	std::string url_stamp = url_data->time_stamp;
	std::string sql = "insert into url_count(url_key, count, lastupdate, url_stamp)  values(?, ?, now(), ?) on duplicate key update count = count + ? , lastupdate = now(), url_stamp = ?";
	TRY {
	PreparedStatement_T p = Connection_prepareStatement(conn, sql.c_str());
	PreparedStatement_setString(p, 1, url_key);
	PreparedStatement_setInt(p, 2, nums);
	PreparedStatement_setString(p, 3, url_stamp.c_str());
	PreparedStatement_setInt(p, 4, nums);
	PreparedStatement_setString(p, 5, url_stamp.c_str());
	PreparedStatement_execute(p);
	} CATCH(SQLException) {
	  LOG(ERROR) << "SQLException", Exception_frame.message;
	  LOG(ERROR) << sql;
	} 
	END_TRY;
	return 0;
}

int TeleDBServer::CheckAndUpdate(const char *url_key, Url_data *url_data) {
	
	return InsertOrUpdateTeleCount(url_key, url_data);
}
