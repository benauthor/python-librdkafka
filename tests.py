import unittest

from librdkafka import *
from partition_reader import PartitionReaderException


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
        del t
        del p

    def setUp(self):
        c = Consumer(self.config)
        self.topic = c.open_topic("TopicPartitionTestCase", TopicConfig())
        self.reader = self.topic.open_partition(
                          partition=0, start_offset=0, default_timeout_ms=1000)

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
        second_reader = self.topic.open_partition(0, 0, 1000)
        msg = second_reader.consume()
        self.assertEqual(0, msg.offset)
        # Now that second_reader has opened the partition again, reader should
        # still refuse to read from it:
        with self.assertRaises(PartitionReaderException):
            msg = self.reader.consume()


if __name__ == "__main__":
    unittest.main()

