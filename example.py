from rd_kafka import Config, TopicConfig, Producer, Consumer


def run():

    ## Configuration:

    config = Config()
    config.set("metadata.broker.list", "kafka0:9092")
    config.set("queue.buffering.max.ms", "10")
    config.set("dr_msg_cb", delivery_report_callback) # see further down

    topic_config = TopicConfig()
    # let's drop in a very dumb partitioner...
    topic_config.set("partitioner", lambda key, partitions: partitions[0])


    ## Produce:

    producer = Producer(config)
    producer_topic = producer.open_topic("my_topic", topic_config)
    producer_topic.produce("hello world", key="whatever")
    producer.poll() # optional


    ## Consume:

    consumer = Consumer(config)
    consumer_topic = consumer.open_topic("my_topic", topic_config)

    # for simple reading:
    reader = consumer_topic.open_partition(
                     0, start_offset=consumer_topic.OFFSET_BEGINNING)
    msg = reader.consume() # also available: consume_batch(), consume_callback()
    print "Read: '{}'".format(msg.payload)
    reader.close() # only one reader can have a topic+partition open at a time

    # reading multiple topics+partitions from a merged queue:
    reader = consumer.new_queue()
    reader.add_toppar(consumer_topic, 0, consumer_topic.OFFSET_BEGINNING)
    reader.add_toppar(consumer_topic, 1, consumer_topic.OFFSET_BEGINNING)
    # ... then use it as before.


def delivery_report_callback(msg, **kwargs):
    print "Delivered: '{}'".format(msg.payload)


if __name__ == "__main__":
    run()
