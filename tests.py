from collections import defaultdict
import logging
import random
import time
import unittest

from rd_kafka import *
import rd_kafka.config_handles
from rd_kafka.partition_reader import PartitionReaderException, ConsumerPartition
import example


kafka_docker = "kafka:9092" # TODO make portable (see fig.yml etc)
logging.basicConfig(level=logging.DEBUG)


class ExampleTestCase(unittest.TestCase):
    """ Make sure example.py is still current """
    def test_example_py(self):
        example.run()


class QueueReaderTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = {"metadata.broker.list": kafka_docker}

        # make sure we have some messages:
        p = Producer(cls.config)
        t = p.open_topic("TopicPartitionTestCase")
        for _ in range(1000):
            t.produce(b"boohoohoo")
        p.flush_queues()

    def setUp(self):
        self.consumer = Consumer(self.config)
        self.topic = self.consumer.open_topic("TopicPartitionTestCase")
        self.reader = self.topic.open_partition(0, start_offset=0)

    def test_double_instantiations(self):
        with self.assertRaises(PartitionReaderException):
            second_reader = self.topic.open_partition(0, 0)

        self.reader.close()
        second_reader = self.topic.open_partition(0, 0)
        msg = second_reader.consume() # should succeed without exceptions

        # Now that second_reader has opened the partition again, reader should
        # still refuse to read from it:
        with self.assertRaises(PartitionReaderException):
            msg = self.reader.consume()

    def test_after_close(self):
        """ Test that methods called after close() raise exceptions """
        self.reader.close()
        with self.assertRaises(PartitionReaderException):
            self.reader.add_toppar(self.topic, 0, -1)
        with self.assertRaises(PartitionReaderException):
            msg = self.reader.consume()

        # finally, the add_toppar() above must not reserve toppar forever:
        try:
            # if this raises, the add_toppar above got hold of the toppar:
            cp = ConsumerPartition(self.topic, 0, -1)
        except:
            del self.reader
            # try again, this should work:
            cp = ConsumerPartition(self.topic, 0, -1)

    def test_magic_offsets(self):
        self.reader.close()
        r = self.topic.open_partition(0, self.topic.OFFSET_BEGINNING)
        self.assertIsNotNone(r.consume())

        r.close()
        r = self.topic.open_partition(0, self.topic.OFFSET_END)
        self.assertIsNone(r.consume())

        r.close()
        r = self.topic.open_partition(0, -1)
        offset_e = r.consume().offset
        self.assertIsNone(r.consume())

        r.close()
        r = self.topic.open_partition(0, -10)
        self.assertEqual(offset_e - r.consume().offset, 9)

    def test_multi_topic_reader(self):
        top2 = self.consumer.open_topic("TopicPartitionTestCasePlusPlus")
        # write some stuff:
        stuff = _random_str()
        p = Producer(self.config)
        t = p.open_topic("TopicPartitionTestCasePlusPlus")
        t.produce(stuff, partition=0)

        r = self.consumer.new_queue()
        r.add_toppar(self.topic, 1, -1)
        r.add_toppar(top2, 0, -1)
        # we don't know in which order we'll get messages, but:
        messages = []
        for _ in range(4):
            msg = r.consume()
            # FIXME clearly the following is a symptom of our poor interface,
            # we should probably do sth else than returning None all the time
            if msg is not None:
                messages.append(bytes(msg.payload))
        self.assertIn(stuff, messages)
        self.assertIn("boohoohoo", messages)


class ConfigTestCase(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.config = {"metadata.broker.list": kafka_docker}

    def test_set_dr_msg_cb(self):
        n_msgs = 100
        msg_payload = b"hello world!"
        call_counter = [0]

        def count_callbacks(msg, **kwargs):
            self.assertEqual(bytes(msg.payload), msg_payload)
            call_counter[0] += 1

        self.config["dr_msg_cb"] = count_callbacks
        producer = Producer(self.config)
        t = producer.open_topic("test_set_dr_msg_cb")

        for _ in range(n_msgs):
            t.produce(msg_payload)
        _poll_until_true(lambda: call_counter[0] == n_msgs, producer)

    def test_default_config(self):
        """
        Check that default_config() returns acceptable input for KafkaHandles
        """
        default_config = rd_kafka.config_handles.default_config()
        default_config.update(self.config)
        prod = Producer(default_config)
        cons = Consumer(default_config)


class TopicConfigTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = {"metadata.broker.list": kafka_docker}
        cls.producer = Producer(cls.config)

    def test_set_partitioner_cb(self):
        test_key = "test_key"
        n_msgs = 1000
        call_counter = [0]

        def count_callbacks(key, part_list):
            call_counter[0] += 1
            self.assertEqual(key, test_key)
            # try special return-value None a few times, too:
            return random.choice(part_list) if call_counter[0] % 20 else None

        topic_config = {"partitioner": count_callbacks}
        t = self.producer.open_topic("test_set_partitioner_cb", topic_config)

        for _ in range(n_msgs):
            t.produce(b"partition me", key=test_key)
        _poll_until_true(lambda: call_counter[0] >= n_msgs, self.producer)

    def test_default_topic_config(self):
        """
        Check that default_topic_config() returns acceptable input for Topics
        """
        default_topic_config = rd_kafka.config_handles.default_topic_config()

        producer_topic = self.producer.open_topic("test_default_topic_config",
                                                  default_topic_config)
        cons = Consumer(self.config)
        consumer_topic = cons.open_topic("test_default_topic_config",
                                         default_topic_config)


class MsgOpaquesTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = {
                "metadata.broker.list": kafka_docker,
                "queue.buffering.max.ms": "10"}

        cls.msg_opaques = defaultdict(int)
        def dr_msg_cb(msg, **kwargs):
            cls.msg_opaques[id(msg.opaque)] += 1
        cls.config["dr_msg_cb"] = dr_msg_cb

        cls.producer = Producer(cls.config)
        cls.topic = cls.producer.open_topic("MsgOpaquesTestCase")

    def setUp(self):
        self.msg_opaques.clear()

    def test_msg_opaque_none(self):
        """ Assert we can safely 'dereference' msg_opaque=None """
        self.topic.produce(b"yoyoyo", msg_opaque=None)
        self.producer.poll()
        self.assertEqual(1, self.msg_opaques[id(None)])

    def test_msg_opaque_multiple(self):
        """ Assert we can pass the same object with multiple messages """
        objs = [0, 1, [], None, object(), object(), object()]
        n = 5
        for i in range(n):
            for o in objs:
                self.topic.produce(bytes(i), msg_opaque=o)
        _poll_until_true(
                lambda: sum(self.msg_opaques.values()) == n * len(objs),
                self.producer)
        for o in objs:
            self.assertEqual(n, self.msg_opaques[id(o)])


class ProduceConsumeTestCase(unittest.TestCase):
    """ Seemingly unnecessary round-trip-test; cf bitbucket issue #2 """

    def test_produce_consume(self):
        config = {
                "metadata.broker.list": kafka_docker,
                "queue.buffering.max.ms": "10"}
        testtopic_name = "ProduceConsumeTestCase"

        delivery_reports = {}
        def dr_msg_cb(msg, **kwargs):
            delivery_reports[str(msg.key)] = msg.cdata.err # FIXME ugly need for str-coercion

        config["dr_msg_cb"] = dr_msg_cb
        producer = Producer(config)
        p_topic = producer.open_topic(testtopic_name,
                                      {"request.required.acks": "-1"})

        consumer = Consumer(config)
        reader = consumer.new_queue()
        c_topic = consumer.open_topic(testtopic_name)
        meta = c_topic.metadata()
        for partition_id in meta["topics"][testtopic_name]["partitions"]:
            reader.add_toppar(c_topic, partition_id, c_topic.OFFSET_END)

        del config # this shouldn't break anything; it's just here to double-check
                   # that indeed it really doesn't
        for round_trips in range(10):
            k = _random_str()
            p_topic.produce("whatevah", key=k)
            _poll_until_true(lambda: k in delivery_reports, producer)
            self.assertEqual(delivery_reports[k], 0) # ie no errors

            while True: # ugly pattern for getting a message out :(
                msg = reader.consume()
                if msg is not None: break
            self.assertEqual(str(msg.key), k)


def _poll_until_true(fbool, kafka_handle, timeout=10):
    t0 = time.time()
    while not fbool():
        kafka_handle.poll()
        if (time.time() - t0 > timeout):
            raise Exception("Timed out while polling")


def _random_str():
    return bytes(random.getrandbits(64))


if __name__ == "__main__":
    unittest.main()
