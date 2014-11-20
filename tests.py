import random
import unittest

from rd_kafka import *
from rd_kafka.partition_reader import PartitionReaderException


kafka_docker = "kafka0:9092" # TODO make portable (see fig.yml etc)


class PartitionReaderTestCase(unittest.TestCase):
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
        c = Consumer(self.config)
        self.topic = c.open_topic("TopicPartitionTestCase", TopicConfig())
        self.reader = self.topic.open_partition(0, start_offset=0)

    def test_seek(self):
        msg = self.reader.consume()
        self.assertEqual(0, msg.offset)
        self.reader.seek(100)
        msg = self.reader.consume()
        self.assertEqual(100, msg.offset)

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

        r.seek(r.OFFSET_END)
        self.assertIsNone(r.consume())

        r.seek(-1)
        offset_e = r.consume().offset
        self.assertIsNone(r.consume())

        r.seek(-10)
        self.assertEqual(offset_e - r.consume().offset, 9)


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


if __name__ == "__main__":
    unittest.main()

