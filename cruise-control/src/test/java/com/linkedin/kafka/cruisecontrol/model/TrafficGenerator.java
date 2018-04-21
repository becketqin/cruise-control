/*
 *
 *  * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.model;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import joptsimple.internal.Strings;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(Parameterized.class)
public class TrafficGenerator {
  private static final String TOPIC = "topic-";
  private static final int TEST_TIME = 300000;
  private static final String CLUSTER = "localhost:9092";
  private static final String ZK_URL = "localhost:2181";
  private static final long REPORTING_INTERVAL_MS = 30000L;
  private static final long THROTTLE_INTERVAL_MS = 5;

  private final Settings _settings;

  @Parameterized.Parameters
  public static Collection<Object[]> settings() {
    List<Object[]> settings = new ArrayList<>();
    for (int numProducers : Arrays.asList(6, 5, 4, 3, 2, 1)) {
      for (int numConsumers : Arrays.asList(4, 3, 2, 1)) {
        for (int messageSize: Arrays.asList(200, 400, 800)) {
          for (int produceRate: Arrays.asList(6000, 4000, 2000, 1000, 500)) {
            settings.add(new Object[]{new Settings(numProducers, numConsumers, messageSize, produceRate, TEST_TIME)});
          }
        }
      }
    }
    return settings;
  }

  public TrafficGenerator(Settings settings) {
    _settings = settings;
  }

  public static void main(String[] agrs) {
    ZkUtils zkUtils = createZkUtils(ZK_URL);
    Consumer<String, String> consumer = createConsumer("deleteAllConsumer", CLUSTER);
    for (String topic : consumer.listTopics().keySet()) {
      if (topic.startsWith(TOPIC)) {
        System.out.println("deleting from source " + topic);
        deleteTopic(zkUtils, topic, consumer);
      }
    }
    zkUtils.close();
    consumer.close();
  }

  private static void deleteTopic(ZkUtils zkUtils, String topic, Consumer consumer) {
    try {
      AdminUtils.deleteTopic(zkUtils, topic);
      while (consumer.listTopics().containsKey(topic)) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      System.out.println("Received exception when deleting topic " + topic);
    }
  }

  @Test
  public void testTrafficGeneration() throws InterruptedException {
    System.out.println("Testing parameter: " + _settings);
    AtomicInteger topicSuffix = new AtomicInteger(0);
    List<Producer> producers = new ArrayList<>();
    List<ConsumeThread> consumeThreads = new ArrayList<>();
    ConcurrentMap<String, Integer> numMessagesMap = new ConcurrentHashMap<>();
    for (int i = 0; i < _settings._numConsumers; i++) {
      Consumer<String, String> consumer = createConsumer("ConsumeToDelete-" + i, CLUSTER);
      ConsumeThread t = new ConsumeThread(consumer, numMessagesMap);
      consumeThreads.add(t);
      t.start();
    }

    ScheduledExecutorService produceExecutor = Executors.newScheduledThreadPool(_settings._numProducers);

    for (int i = 0; i < _settings._numProducers; i++) {
      Producer<String, String> producer = createProducer();
      producers.add(producer);
      produceExecutor.scheduleAtFixedRate(new ProducerRunnable(createProducer(), topicSuffix),
                                          0, THROTTLE_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    try {
      Thread.sleep(_settings._testTime);
    } catch (InterruptedException e) {
      System.out.println("Received error when waiting for test to finish");
      e.printStackTrace();
    } finally {
      produceExecutor.shutdown();
      produceExecutor.awaitTermination(10, TimeUnit.SECONDS);
      producers.forEach(Producer::close);
      consumeThreads.forEach(ConsumeThread::shutdown);
      for (ConsumeThread consumeThread : consumeThreads) {
        consumeThread.awaitShudown();
      }
    }
  }

  private Producer<String, String> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.RETRIES_CONFIG, "10");
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
    props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    return new KafkaProducer<>(props);
  }

  private static Consumer<String, String> createConsumer(String groupId, String bootstrapServers) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
    props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "5000");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "9000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "8999");
    return new KafkaConsumer<>(props);
  }

  private static ZkUtils createZkUtils(String url) {
    ZkConnection connection = new ZkConnection(url);
    ZkClient client = new ZkClient(connection, 30000, new ZkStringSerializer());
    return new ZkUtils(client, connection, false);
  }

  private int produceToTopic(Producer<String, String> producer,
                             String topic,
                             int messageSize,
                             int numRecords) {
    long startMs = System.currentTimeMillis();
    int partIndex = 0;
    while (partIndex < numRecords) {
      final int partition = partIndex % 32;
      producer.send(new ProducerRecord<>(topic, partition, null,
                                         "value" + startMs + Strings.repeat('a', messageSize)),
                    new Callback() {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          if (e != null) {
            System.out.println("Failed to send message to partition " + partition);
            e.printStackTrace();
          }
        }
      });
      partIndex++;
    }
    return partIndex;
  }

  private class ProducerRunnable implements Runnable {
    private final Producer<String, String> _producer;
    private final AtomicInteger _topicSuffix;
    private final Consumer<String, String> _consumer;
    private final ZkUtils _sourceZkUtils;
    private final String _topic;
    private final long _startTime;
    private final AtomicLong _totalMessageSent;
    private volatile long lastReportTime;

    ProducerRunnable(Producer<String, String> producer,
                     AtomicInteger topicSuffix) {
      _producer = producer;
      _topicSuffix = topicSuffix;
      _consumer = createConsumer("produceThrottleConsumer", CLUSTER);
      _sourceZkUtils = createZkUtils(ZK_URL);
      _startTime = System.currentTimeMillis();
      int suffix = _topicSuffix.getAndIncrement();
      _topic = TOPIC + suffix;
      _totalMessageSent = new AtomicLong(0);
    }

    @Override
    public void run() {
      try {
        // Ensure the topic is created.
        if (!_consumer.listTopics().containsKey(_topic)) {
          try {
            AdminUtils.createTopic(_sourceZkUtils, _topic, 32, 2, new Properties(), RackAwareMode.Disabled$.MODULE$);
          } catch (Exception e) {
            System.out.println("Received exception when creating topic " + _topic);
            e.printStackTrace();
          }
          while (!_consumer.listTopics().containsKey(_topic)) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
        long now = System.currentTimeMillis();
        int numMessagesToSend =
            (int) (((double) (now - _startTime) / 1000) * _settings._produceRate - _totalMessageSent.get());
        long totalMessageSent =
            _totalMessageSent.addAndGet(produceToTopic(_producer, _topic, _settings._messageSize, numMessagesToSend));
        now = System.currentTimeMillis();
        if (now > lastReportTime + REPORTING_INTERVAL_MS) {
          lastReportTime = now;
          System.out.println("Produced " + totalMessageSent + " records to topic " + _topic + " in " + (now - _startTime) +
                                 " ms. Target produce rate " + _settings._produceRate + ". Actual produce rate " +
                                 (totalMessageSent * 1000 / (now - _startTime)));
        }
      } catch (Throwable t) {
        System.out.println("Failed to produce message");
        t.printStackTrace();
      }
    }
  }

  private class ConsumeThread extends Thread {
    private final Consumer<String, String> _consumer;
    private final ConcurrentMap<String, Integer> _numMessagesConsumed;
    private volatile boolean _shutdown = false;
    private final CountDownLatch _shutdownLatch = new CountDownLatch(1);

    ConsumeThread(Consumer<String, String> consumer,
                  ConcurrentMap<String, Integer> numMessagesConsumed) {
      _consumer = consumer;
      _numMessagesConsumed = numMessagesConsumed;
    }

    @Override
    public void run() {
      try {
        _consumer.subscribe(Pattern.compile(TOPIC + ".*"), new NoOpConsumerRebalanceListener());
        long lastLogging = System.currentTimeMillis();
        while (!_shutdown) {
          try {
            ConsumerRecords<String, String> records = _consumer.poll(1000);
            for (ConsumerRecord record : records) {
              _numMessagesConsumed.compute(record.topic(), (t, c) -> c == null ? 1 : c + 1);
            }
            long now = System.currentTimeMillis();
            if (now > lastLogging + REPORTING_INTERVAL_MS) {
              System.out.println("NumMessageConsumed: " + _numMessagesConsumed);
              lastLogging = now;
            }
          } catch (Exception e) {
            System.out.println("Got exception when consuming");
            e.printStackTrace();
          }
        }
        _consumer.close();
      } finally {
        _shutdownLatch.countDown();
      }
    }

    public void shutdown() {
      _shutdown = true;
    }

    public void awaitShudown() throws InterruptedException {
      _shutdownLatch.await();
    }
  }

  private static class ZkStringSerializer implements ZkSerializer {

    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
      return ((String) data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
      return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }
  }

  private static class Settings {
    private final int _numProducers;
    private final int _numConsumers;
    private final int _messageSize;
    private final int _produceRate;
    private final int _testTime;

    Settings(int numProducers,
             int numConsumers,
             int messageSize,
             int produceRate,
             int testTime) {
      _numConsumers = numConsumers;
      _numProducers = numProducers;
      _messageSize = messageSize;
      _produceRate = produceRate;
      _testTime = testTime;
    }

    @Override
    public String toString() {
      return String.format("numProducers=%d, numConsumers=%d, messageSize=%d, produceRate=%d, testTime=%d",
                           _numProducers, _numConsumers, _messageSize, _produceRate, _testTime);
    }
  }
}
