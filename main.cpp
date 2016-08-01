#include <pthread.h>
#include <map>
#include <glog/logging.h>
#include "KafkaWrapper.h"
#include "tele_db.h"
#include <glog/logging.h>


#define URL_FREQ_BUF_NUM  64
#define MAX_URL_BUFFER_LEN 80000
#define MAX_DELAY_SECONDS 3600

class Url_freq_contrl {
 public:
  Url_freq_contrl();
  int AddUrl(const char*url, std::string time_stamp);	
  int Store();	
  static Url_freq_contrl* GetInstance();
 private:
  static Url_freq_contrl *url_freq_cnt;
  std::map<std::string, Url_data*> url_freq_buf[64];
  int read_inx;
  int write_inx;
  int url_count;
  pthread_mutex_t   *url_mux;
  pthread_cond_t not_empty;
  pthread_cond_t not_full;
};

Url_freq_contrl *Url_freq_contrl::url_freq_cnt = NULL;

Url_freq_contrl* Url_freq_contrl::GetInstance() {
	if (url_freq_cnt == NULL)
		url_freq_cnt = new Url_freq_contrl();
	return url_freq_cnt;

}

Url_freq_contrl::Url_freq_contrl() {
	url_mux = new pthread_mutex_t;
	pthread_mutex_init(url_mux, NULL);
	pthread_cond_init(&not_empty, NULL);
	pthread_cond_init(&not_full, NULL);
	write_inx = 0;
	read_inx = 0;
  url_count = 0;
}

static std::string get_time_str(time_t t) {

  struct tm current_time;
  localtime_r(&t, &current_time);
	char buffer[64] = { 0 };
	sprintf(buffer, "%d-%02d-%02d %02d:%02d:%02d",
	        current_time.tm_year + 1900,
	        current_time.tm_mon + 1,
	        current_time.tm_mday,
	        current_time.tm_hour,
	        current_time.tm_min,
	current_time.tm_sec);
	return buffer;
}
int Url_freq_contrl::AddUrl(const char *url, std::string time_stamp) {
	
	if (strlen(url) < 1)
		return -1;
	int count = 0;	
	pthread_mutex_lock(url_mux);
	while ((write_inx + 1) % URL_FREQ_BUF_NUM == read_inx) {
		pthread_cond_wait(&not_full, url_mux);
	}
	std::map<std::string, Url_data*>::iterator it = url_freq_buf[write_inx].find(url);
	if (it == url_freq_buf[write_inx].end()) {
	   Url_data *url_data = new Url_data;
	   url_data->count = 1;
	   url_data->time_stamp = time_stamp;
	   url_freq_buf[write_inx][url] = url_data;
	}  else {
	     it->second->count++;
	}
	++url_count;
	//printf("url_count %d\n", url_count);
	if (url_count >= MAX_URL_BUFFER_LEN) {
		url_count = 0;
		pthread_cond_signal(&not_empty);
		write_inx = (write_inx + 1) % URL_FREQ_BUF_NUM;
		LOG(INFO) << "send not empty signal  winx " <<  write_inx <<"read_inx" << read_inx;
	}

	pthread_mutex_unlock(url_mux);
	return 0;
}

int Url_freq_contrl::Store() {
	std::map<std::string, Url_data*> freq_store;
	pthread_mutex_lock(url_mux);
	while (read_inx == write_inx) {
		LOG(INFO)<< "Now wait for not empty rinx: " << read_inx;
    pthread_cond_wait(&not_empty, url_mux);
		
	}

	freq_store = url_freq_buf[read_inx];
	url_freq_buf[read_inx].clear();
	read_inx = (read_inx + 1) % URL_FREQ_BUF_NUM;
	pthread_cond_signal(&not_full);
	LOG(INFO) << "send notfull " << "write_inx" << write_inx << "read inx"<< read_inx;
	pthread_mutex_unlock(url_mux);

	TeleDBServer server(DBPool::GetInstance());
	std::map<std::string, Url_data*>::iterator it = freq_store.begin();
	int rc = 0;
	while(it != freq_store.end()) {
	  std::string value = it->first;
	  Url_data *url_data = it->second;
	  int nums = url_data->count;
	  
	  int rt = server.CheckAndUpdate(value.c_str(), url_data);
          if (rt < 0) {
	     rc = -1;
  	  }  
	  delete url_data;
	  it++;

	}
	return rc;
}
void * tele_storage_thd(void *arg) {

	while(1) {
		Url_freq_contrl::GetInstance()->Store();		
	}  
}

void test_consume_data(const char* data, const int data_len)
{
	if (data_len == 0)
		return;
	//std::string prefix = "tele:";
	//printf("pull data:%s len:%d\n", data, data_len);
	std::string str = data;
	bool find = 1;
	int count = 0;
        int pos = 0;
        std::string url_stamp;
	int pre_pos = 0; 
	std::string value;
	
	while (count < 3) {
	  pos = str.find("\t", pre_pos);
	  if (pos == -1)
		return;
	  std::string temp = str.substr(pre_pos, pos - pre_pos);
	  switch (count) {
	    case 1:
	      url_stamp = temp.substr(0, temp.length() -3);
	      break;
	    case 2:
	      value = temp;
	    default:
	      break;
	  }
	  pos++;
	  pre_pos = pos;
          count++; 
	}
		
	unsigned long stamp = atoi(url_stamp.c_str());
	if (time(NULL) - stamp > MAX_DELAY_SECONDS) {
		return;	
	}
	url_stamp = get_time_str(stamp);
	if (Url_freq_contrl::GetInstance()->AddUrl(value.c_str(), url_stamp) != 0 ) {
	
	}

}

static void InitGlog(const char *name) {
  google::InitGoogleLogging(name);
	google::SetLogDestination(google::INFO,"log/teleloginfo");
	google::SetLogDestination(google::WARNING,"log/telelogwarn");
	google::SetLogDestination(google::GLOG_ERROR,"log/telelogerror");
	FLAGS_logbufsecs = 10;
}

static int InitDbpool(std::string dbserver) {
	/*init dbpool */
	DBPool *pool = DBPool::GetInstance();
	if (pool->PoolInit(dbserver.c_str(), "tele", "tele", "tele", 5, 1, 10) < 0) {
	    LOG(ERROR) << "error init dbpool" << dbserver;
	    return -1;
	}
	return 0;
}

enum TeleCommand{
	INVALID_COMMAND = 0,
	KAFKA_COMMAND = 1,
	DB_COMMAND = 2,
};

int main(int argc, char*argv[]) {
	wrapper_Info test_info;
	int i;
	TeleCommand cmd = INVALID_COMMAND;
	std::string kafka_server = "localhost";
	std::string db_server = "127.0.0.1";
	for (int i = 0; i < argc; i++) {
		switch (cmd) {
		  case KAFKA_COMMAND:
		  	kafka_server = argv[i];
			  break;
		  case DB_COMMAND:
		  	db_server = argv[i];
			  break;
	}
		cmd = INVALID_COMMAND;
		if (strcmp(argv[i], "-k") == 0) {
			cmd = KAFKA_COMMAND;
		} else if (strcmp(argv[i], "-d") == 0) {
			cmd = DB_COMMAND;
		} 
	}
	InitGlog(argv[0]);
	InitDbpool(db_server);
	Url_freq_contrl::GetInstance();
	pthread_t tid;
	pthread_create(&tid, NULL, tele_storage_thd, NULL);
    printf("%s\n", kafka_server.c_str());
	int rc = consumer_init(0, "kunyan_to_upload_inter_tab_up", kafka_server.c_str(), test_consume_data, &test_info);
	if (rc < 0) {
		LOG(ERROR) << "conumer init failed rc:" <<  rc;
		return -1;
	}


while(true) {
		if (PUSH_DATA_SUCCESS == consumer_pull_data(&test_info)) {
		}
}
	return 0;
}
