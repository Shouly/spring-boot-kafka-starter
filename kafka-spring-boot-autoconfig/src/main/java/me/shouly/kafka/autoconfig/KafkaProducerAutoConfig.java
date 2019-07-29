package me.shouly.kafka.autoconfig;

import java.util.HashMap;
import java.util.Map;

import me.shouly.kafka.producer.api.KafkaProducerTemplate;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * .
 *
 * @author liangbing
 * @version v1.0
 * @date 2019-07-24 14:16
 */
@Configuration
@ConditionalOnClass(KafkaProducerTemplate.class)
@EnableConfigurationProperties(KafkaProducerProperties.class)
public class KafkaProducerAutoConfig {

  @Autowired private KafkaProducerProperties kafkaProducerProperties;

  @Bean
  @ConditionalOnMissingBean(KafkaProducerTemplate.class)
  public KafkaProducerTemplate kafkaProducerTemplate() {

    Map<String, Object> properties = new HashMap<>(16);

    properties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerProperties.getBootstrapServers());
    properties.put(ProducerConfig.ACKS_CONFIG, kafkaProducerProperties.getAcks());
    properties.put(ProducerConfig.RETRIES_CONFIG, kafkaProducerProperties.getRetries().toString());
    properties.put(
        ProducerConfig.LINGER_MS_CONFIG, kafkaProducerProperties.getLingerMs().toString());
    properties.put(
        ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaProducerProperties.getBufferMemory().toString());
    properties.put(
        ProducerConfig.BATCH_SIZE_CONFIG, kafkaProducerProperties.getBatchSize().toString());
    properties.put(
        ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaProducerProperties.getCompressionType());
    properties.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProducerProperties.getKeySerializer());
    properties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProducerProperties.getValueSerializer());
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaProducerProperties.getClientId());

    return new KafkaProducerTemplate(properties);
  }
}
