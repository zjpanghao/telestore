#ifndef KAFKAWRAPPER_H
#define KAFKAWRAPPER_H

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
* is builtin from within the librdkafka source tree and thus differs. */
#include "rdkafka.h"  /* for Kafka driver */

#define MAX_BUF_LEN 1024*10
#define PUSH_DATA_SUCCESS 0
#define PUSH_DATA_FAILED -1
#define PRODUCER_INIT_FAILED -1
#define PRODUCER_INIT_SUCCESS 0

#define CONSUMER_INIT_FAILED -1
#define CONSUMER_INIT_SUCCESS 0
#define PULL_DATA_SUCCESS 0
#define PULL_DATA_FAILED 1

void msg_delivered(rd_kafka_t *rk,
	void *payload, size_t len,
	int error_code,
	void *opaque, void *msg_opaque);

int msg_consume(rd_kafka_message_t *rkmessage,
	void *opaque);

typedef void(*Msg_Delivered)(rd_kafka_t *rk,
	void *payload, size_t len,
	rd_kafka_resp_err_t error_code,
	void *opaque, void *msg_opaque);
typedef void(*Consume_Data)(const char* data, const int data_len);

void consume_data(const char* data, const int data_len);

typedef struct wrapper_Info{
	int64_t start_offset;
	char brokers[100];
	char topic[100];
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	int partition;
	FILE* kafka_log;
	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	Msg_Delivered func_msg_delivered;
	Consume_Data func_consume_data;
}wrapper_Info;

int producer_init(const int partition, const char* topic, const char* brokers, Msg_Delivered func_msg_delivered, wrapper_Info* producer_info);
int producer_push_data(const char* buf, const int buf_len, const wrapper_Info* producer_info);
void producer_close(wrapper_Info* producer_Info);

int consumer_init(const int partition, const char* topic, const char* brokers, Consume_Data consume_data, wrapper_Info* producer_info);
int consumer_pull_data(const wrapper_Info* consumer_info);
void consumer_close(wrapper_Info* consumer_info);


void test_msg_delivered(rd_kafka_t *rk,
	void *payload, size_t len,
	rd_kafka_resp_err_t error_code,
	void *opaque, void *msg_opaque);
void push_data_to_kafka(char *push_data);

#endif
