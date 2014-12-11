from collections import defaultdict
import random
import unittest

from rd_kafka import *


kafka_docker = "kafka0:9092" # TODO make portable (see fig.yml etc)


class QueueReaderTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = Config()
        cls.config.set("metadata.broker.list", kafka_docker)

        # make sure we have some messages:
        p = Producer(cls.config)
        t = p.open_topic("TopicPartitionTestCase", TopicConfig())
        for _ in range(1000):
            t.produce(b"boohoohoo")
        # force flushing queues:
        p.poll(100)

    def setUp(self):
        self.consumer = Consumer(self.config)
        self.topic = self.consumer.open_topic(
                            "TopicPartitionTestCase", TopicConfig())
        self.reader = self.topic.open_partition(0, start_offset=0)

    def test_double_instantiations(self):
        with self.assertRaises(PartitionReaderException):
            second_reader = self.topic.open_partition(0, 0)

        self.reader.close()
        second_reader = self.topic.open_partition(0, 0)
        msg = second_reader.consume()
        self.assertEqual(0, msg.offset)
        # Now that second_reader has opened the partition again, reader should
        # still refuse to read from it:
        with self.assertRaises(PartitionReaderException):
            msg = self.reader.consume()

    def test_magic_offsets(self):
        self.reader.close()
        r = self.topic.open_partition(0, self.topic.OFFSET_BEGINNING)
        self.assertIsNotNone(r.consume().offset)

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
        top2 = self.consumer.open_topic(
                       "TopicPartitionTestCasePlusPlus", TopicConfig())
        # write some stuff:
        stuff = _random_str()
        p = Producer(self.config)
        t = p.open_topic("TopicPartitionTestCasePlusPlus", TopicConfig())
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
        self.config = Config()
        self.config.set("metadata.broker.list", kafka_docker)

    def test_set_dr_msg_cb(self):
        n_msgs = 100
        msg_payload = b"hello world!"
        call_counter = [0]

        def count_callbacks(msg, **kwargs):
            self.assertEqual(bytes(msg.payload), msg_payload)
            call_counter[0] += 1

        self.config.set("dr_msg_cb", count_callbacks)
        producer = Producer(self.config)
        tc = TopicConfig()
        t = producer.open_topic("test_set_dr_msg_cb", tc)

        for _ in range(n_msgs):
            t.produce(msg_payload)
        n_polls = 0
        while call_counter[0] != n_msgs and n_polls < 10:
            producer.poll()
            n_polls += 1
        self.assertEqual(call_counter[0], n_msgs)


class TopicConfigTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = Config()
        cls.config.set("metadata.broker.list", kafka_docker)
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

        tc = TopicConfig()
        tc.set("partitioner", count_callbacks)
        t = self.producer.open_topic("test_set_partitioner_cb", tc)

        for _ in range(n_msgs):
            t.produce(b"partition me", key=test_key)
        self.producer.poll(2000)
        # ^^ We only need more time here when count_callbacks() is made to
        # return None sometimes - that makes sense I suppose.
        self.assertGreaterEqual(call_counter[0], n_msgs)


class MsgOpaquesTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = Config()
        cls.config.set("metadata.broker.list", kafka_docker)
        cls.config.set("queue.buffering.max.ms", "10")

        cls.msg_opaques = defaultdict(int)
        def dr_msg_cb(msg, **kwargs):
            cls.msg_opaques[id(msg.opaque)] += 1
        cls.config.set("dr_msg_cb", dr_msg_cb)

        cls.producer = Producer(cls.config)
        cls.topic = cls.producer.open_topic(
                        "MsgOpaquesTestCase", TopicConfig())

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
        while sum(self.msg_opaques.values()) < n * len(objs):
            self.producer.poll()
        for o in objs:
            self.assertEqual(n, self.msg_opaques[id(o)])


def _random_str():
    return bytes(random.getrandbits(64))


if __name__ == "__main__":
    unittest.main()
