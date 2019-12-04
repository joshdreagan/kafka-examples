package com.shapira.examples;

import kafka.consumer.BaseConsumerRecord;
import kafka.tools.MirrorMaker;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.List;

public class TopicSwitchingHandler implements MirrorMaker.MirrorMakerMessageHandler {

  private final String topicPrefix;

  public TopicSwitchingHandler(String topicPrefix) {
    this.topicPrefix = topicPrefix;
  }

  @Override
  public List<ProducerRecord<byte[], byte[]>> handle(BaseConsumerRecord record) {
    return Collections.singletonList(new ProducerRecord<byte[], byte[]>(topicPrefix + "." + record.topic(), record.partition(), record.key(), record.value(), record.headers()));
  }
}
