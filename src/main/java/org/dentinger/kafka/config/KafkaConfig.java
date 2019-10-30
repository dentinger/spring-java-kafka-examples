package org.dentinger.kafka.config;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

@Configuration
public class KafkaConfig {

  @Autowired
  private KafkaProperties kafkaProperties;

  private ConcurrentKafkaListenerContainerFactory<String, Object> getBasetConcurrentKafkaListenerContainerFactory() {
    final JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>();
    jsonDeserializer.addTrustedPackages("*");

    ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

    factory.setConsumerFactory(
            new DefaultKafkaConsumerFactory<>(
                    kafkaProperties.buildConsumerProperties(),
                    new StringDeserializer(),
                    jsonDeserializer
            ));

    factory.setAckDiscarded(true);
    return factory;
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String,Object> nonFilteringkafkaListenerContainerFactory() {

    ConcurrentKafkaListenerContainerFactory<String, Object> factory = getBasetConcurrentKafkaListenerContainerFactory();

    return factory;
  }


}
