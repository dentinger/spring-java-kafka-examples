package org.dentinger.kafka;

import org.dentinger.kafka.consumer.ConsumerDirective;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.Map;

@SpringBootApplication
public class Temp {

    private static final Logger logger = LoggerFactory.getLogger(Temp.class);

    @Autowired
    private ConsumerDirective consumerDirective;

    @KafkaListener(topics = "TEST.TOPIC")
    public void consume(
            ConsumerRecord<String, Map> consumerRecord,
            @Payload String message) {

        if (!consumerDirective.isDrain()) {

            logger.info("Process records");
        }
        else {
            logger.info("Current state is to drain message: " + message);
        }
    }
}
