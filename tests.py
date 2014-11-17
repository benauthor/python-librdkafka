import unittest

from librdkafka import *


kafka_docker = "kafka0:9092" # TODO make portable (see fig.yml etc)

class TopicPartitionTestCase(unittest.TestCase):
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

    def test_double_instantiations(self):
        reader = self.topic.open_partition(partition=0, start_offset=0,
                                           default_timeout_ms=1000)
        msg = reader.consume()
        self.assertEqual(0, msg.offset)

        # a second reader on the same partition could create havoc:
        reader2 = self.topic.open_partition(0, 0)
        msg = reader.consume()
        self.assertEqual(1, msg.offset)
        del reader2
        msg = reader.consume()
        self.assertEqual(2, msg.offset)


if __name__ == "__main__":
    unittest.main()

