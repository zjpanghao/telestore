#include "KafkaWrapper.h"

/**
* Message delivery report callback.
* Called once for each message.
* See rdkafka.h for more information.
*/
void msg_delivered(rd_kafka_t *rk,
	void *payload, size_t len,
	int error_code,
	void *opaque, void *msg_opaque) {

	if (error_code)
		fprintf(stderr, "%% Message delivery failed: %d\n", error_code);
	else
		fprintf(stderr, "%% Message delivered (%zd bytes)\n", len);
}

int msg_consume(rd_kafka_message_t *rkmessage,
	void *opaque) {
	if (rkmessage->err) {
		if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
			// 			fprintf(stderr,
			// 				"%% Consumer reached end of %s [%"PRId32"] "
			// 				"message queue at offset %"PRId64"\n",
			// 				rd_kafka_topic_name(rkmessage->rkt),
			// 				rkmessage->partition, rkmessage->offset);
			return PULL_DATA_SUCCESS;
		}

		// 		fprintf(stderr, "%% Consume error for topic \"%s\" [%"PRId32"] "
		// 			"offset %"PRId64": %s\n",
		// 			rd_kafka_topic_name(rkmessage->rkt),
		// 			rkmessage->partition,
		// 			rkmessage->offset,
		// 			rd_kafka_message_errstr(rkmessage));

		if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
			rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
			return rkmessage->err;
		return PULL_DATA_FAILED;
	}
	consume_data((const char*)rkmessage->payload, (const int)rkmessage->len);

	return PULL_DATA_SUCCESS;
}


/**
* Kafka logger callback (optional)
*/
void logger(const rd_kafka_t *rk, int level,
	const char *fac, const char *buf) {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
		(int)tv.tv_sec, (int)(tv.tv_usec / 1000),
		level, fac, rk ? rd_kafka_name(rk) : NULL, buf);
}

int producer_init(const int partition, const char* topic, const char* brokers, Msg_Delivered func_msg_delivered, wrapper_Info* producer_info)
{
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	rd_kafka_t *rk;
	char errstr[512];

	producer_info->partition = partition;
	strcpy(producer_info->topic, topic);

	if (NULL != func_msg_delivered)
		producer_info->func_msg_delivered = func_msg_delivered;
	else
		return PRODUCER_INIT_FAILED;

	/* Kafka configuration */
	conf = rd_kafka_conf_new();

	/* Set logger */
	rd_kafka_conf_set_log_cb(conf, logger);

	/* Topic configuration */
	topic_conf = rd_kafka_topic_conf_new();

	rd_kafka_conf_set_dr_cb(conf, func_msg_delivered);

	/* Create Kafka handle */
	if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
		errstr, sizeof(errstr)))) {
		fprintf(stderr,
			"%% Failed to create new producer: %s\n",
			errstr);
		return PRODUCER_INIT_FAILED;
	}

	/* Add brokers */
	if (rd_kafka_brokers_add(rk, brokers) == 0) {
		fprintf(stderr, "%% No valid brokers specified\n");
		return PRODUCER_INIT_FAILED;
	}

	/* Create topic */
	producer_info->rkt = rd_kafka_topic_new(rk, topic, topic_conf);
	producer_info->rk = rk;

	return PRODUCER_INIT_SUCCESS;
}

int producer_push_data(const char* buf, const int buf_len, const wrapper_Info* producer_info)
{
	int produce_ret;
	if (NULL == buf)
		return 0;
	if (0 == buf_len || buf_len > MAX_BUF_LEN)
		return -2;
	//printf("118producer->topic:%s, producer->partition:%d\n", producer_info->topic, producer_info->partition);
	/* Send/Produce message. */
	produce_ret = rd_kafka_produce(producer_info->rkt, producer_info->partition,
		RD_KAFKA_MSG_F_COPY,
		/* Payload and length */
		(void*)buf, (size_t)buf_len,
		/* Optional key and its length */
		NULL, 0,
		/* Message opaque, provided in
		* delivery report callback as
		* msg_opaque. */
		NULL);
	//printf("129produce_ret = %d\n", produce_ret);
	if (produce_ret == -1)
	{
		printf("errno %d", errno);
		fprintf(stderr,
			"%% Failed to produce to topic %s "
			"partition %i: %s\n",
			rd_kafka_topic_name(producer_info->rkt), producer_info->partition,
			rd_kafka_err2str(
			rd_kafka_errno2err(errno)));
		printf("%s", stderr);
		/* Poll to handle delivery reports */
		rd_kafka_poll(producer_info->rk, 0);
	
		return PUSH_DATA_FAILED;
	}

	//fprintf(stderr, "136%% Sent %d bytes to topic %s partition %i\n",buf_len, rd_kafka_topic_name(producer_info->rkt), producer_info->partition);
	/* Poll to handle delivery reports */
	rd_kafka_poll(producer_info->rk, 0);
	return PUSH_DATA_SUCCESS;
}

void producer_close(wrapper_Info* producer_info)
{
	/* Destroy topic */
	rd_kafka_topic_destroy(producer_info->rkt);

	/* Destroy the handle */
	rd_kafka_destroy(producer_info->rk);
}

int consumer_init(const int partition, const char* topic, const char* brokers, Consume_Data consume_data, wrapper_Info* producer_info)
{
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	rd_kafka_t *rk;
	char errstr[512];

	producer_info->start_offset = RD_KAFKA_OFFSET_STORED;
	producer_info->partition = partition;

	if (NULL != consume_data)
		producer_info->func_consume_data = consume_data;
	else
		return CONSUMER_INIT_FAILED;

	/* Kafka configuration */
	conf = rd_kafka_conf_new();
	if (NULL == conf)
		return CONSUMER_INIT_FAILED;

	if (RD_KAFKA_CONF_OK != rd_kafka_conf_set(conf, "group.id", "one", errstr, sizeof(errstr)))
		return CONSUMER_INIT_FAILED;

	/* Create Kafka handle */
	if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
		errstr, sizeof(errstr)))) {
		fprintf(stderr,
			"%% Failed to create new consumer: %s\n",
			errstr);
		return CONSUMER_INIT_FAILED;
	}

	rd_kafka_set_log_level(rk, LOG_DEBUG);

	/* Add brokers */
	if (rd_kafka_brokers_add(rk, brokers) == 0) {
		fprintf(stderr, "%% No valid brokers specified\n");
		return CONSUMER_INIT_FAILED;
	}

	/* Topic configuration */
	topic_conf = rd_kafka_topic_conf_new();

	/* Create topic */
	producer_info->rkt = rd_kafka_topic_new(rk, topic, topic_conf);
	producer_info->rk = rk;

	/* Start consuming */
	if (rd_kafka_consume_start(producer_info->rkt, partition, RD_KAFKA_OFFSET_END) == -1){
		fprintf(stderr, "%% Failed to start consuming: %s\n",
			rd_kafka_err2str(rd_kafka_errno2err(errno)));
		return CONSUMER_INIT_FAILED;
	}

	return CONSUMER_INIT_SUCCESS;
}

int consumer_pull_data(const wrapper_Info* consumer_info)
{
	rd_kafka_message_t *rkmessage;
	int consume_result;

	//printf("217consumer_info->rk:%p, rkt:%p, partition=%d\n", consumer_info->rk, consumer_info->rkt, consumer_info->partition);

	//printf("217 begin poll\n");
	/* Poll for errors, etc. */
	rd_kafka_poll(consumer_info->rk, 0);
	//printf("219begin consume\n");
	/* Consume single message.
	* See rdkafka_performance.c for high speed
	* consuming of messages. */
	rkmessage = rd_kafka_consume(consumer_info->rkt, consumer_info->partition, 1000);
	if (!rkmessage) /* timeout */
		return PULL_DATA_FAILED;

	if (rkmessage->err) {
		if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
			// 				fprintf(stderr,
			// 					"%% Consumer reached end of %s [%"PRId32"] "
			// 					"message queue at offset %"PRId64"\n",
			// 					rd_kafka_topic_name(rkmessage->rkt),
			// 					rkmessage->partition, rkmessage->offset);
			consume_result = PULL_DATA_SUCCESS;
		}

		// 			fprintf(stderr, "%% Consume error for topic \"%s\" [%"PRId32"] "
		// 				"offset %"PRId64": %s\n",
		// 				rd_kafka_topic_name(rkmessage->rkt),
		// 				rkmessage->partition,
		// 				rkmessage->offset,
		// 				rd_kafka_message_errstr(rkmessage));

		if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
			rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
			return rkmessage->err;
		consume_result = PULL_DATA_FAILED;
	}
	consumer_info->func_consume_data((const char*)rkmessage->payload, (const int)rkmessage->len);
	/* Return message to rdkafka */
	rd_kafka_message_destroy(rkmessage);

	return consume_result;
}

void consumer_close(wrapper_Info* consumer_info)
{
	/* Stop consuming */
	rd_kafka_consume_stop(consumer_info->rkt, consumer_info->partition);

	while (rd_kafka_outq_len(consumer_info->rk) > 0)
		rd_kafka_poll(consumer_info->rk, 10);

	/* Destroy topic */
	rd_kafka_topic_destroy(consumer_info->rkt);

	/* Destroy handle */
	rd_kafka_destroy(consumer_info->rk);
}

void consume_data(const char* data, const int data_len)
{
	fprintf(stdout, "Message %s, len %d", data, data_len);
}

void test_msg_delivered(rd_kafka_t *rk,
	void *payload, size_t len,
	rd_kafka_resp_err_t error_code,
	void *opaque, void *msg_opaque)
{
	//printf("8test_msg_delivered\n");
}

void push_data_to_kafka(char *push_data)
{
	int partition = 0;
	const char* topic = "test";
	//strcpy(push_data, "helloworld");

	wrapper_Info test_info;
	if (PRODUCER_INIT_SUCCESS == producer_init(partition, topic, "192.168.1.68:9092", &test_msg_delivered, &test_info))
		printf("producer init success!\n");
	else
		printf("producer init failed\n");

	size_t len = strlen(push_data);
	if (push_data[len - 1] == '\n')
		push_data[--len] = '\0';

	if (PUSH_DATA_SUCCESS == producer_push_data(push_data, strlen(push_data), &test_info))
		printf("push data success: %s\n", push_data);
	else
		printf("push data failed %s\n", push_data);

	printf("-----------------------------------------\n");

	producer_close(&test_info);
}
