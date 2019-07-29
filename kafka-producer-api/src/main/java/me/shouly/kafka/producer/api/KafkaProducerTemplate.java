package me.shouly.kafka.producer.api;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * .
 *
 * @author liangbing
 * @version v1.0
 * @date 2019-07-24 14:18
 */
@Slf4j
public class KafkaProducerTemplate<K, V> {

  private KafkaProducer<K, V> kafkaProducer;

  public KafkaProducerTemplate(Map<String, Object> properties) {
    kafkaProducer = new KafkaProducer<>(properties);
  }

  /**
   * send msg
   *
   * @param topic topic
   * @param msg msg
   */
  public void send(String topic, V msg) {
    send(topic, null, msg, null);
  }

  /**
   * send msg
   *
   * @param topic topic
   * @param key msg key
   * @param msg msg
   * @param partition partition
   */
  public void send(String topic, K key, V msg, Integer partition) {

    ProducerRecord<K, V> record = new ProducerRecord<>(topic, partition, key, msg);

    kafkaProducer.send(
        record,
        (metadata, exception) -> {
          log.info(
              "[ack success,topic:{},partition:{},offset:{}]",
              metadata.topic(),
              metadata.partition(),
              metadata.offset());

          if (exception != null) {
            log.error(exception.getMessage(), exception);
          }
        });
  }
}
