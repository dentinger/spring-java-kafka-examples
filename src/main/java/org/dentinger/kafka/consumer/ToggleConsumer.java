package org.dentinger.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

import static org.dentinger.kafka.consumer.ConsumerDirective.Directive;
import static org.dentinger.kafka.consumer.ConsumerDirective.Directive.PAUSE;
import static org.dentinger.kafka.consumer.ConsumerDirective.Directive.PROCESS;

@Component
public class ToggleConsumer {
  private static final Logger logger = LoggerFactory.getLogger(ToggleConsumer.class);
  private static final String TARGET_APP_KEY = "targetApplication";
  private static final String DIRECTIVE_KEY = "directive";
  private static final String TOGGLE_LISTENER_ID = "consumer.toggle.listener";

  private final String applicationName;
  private final KafkaListenerEndpointRegistry registry;
  private final ConsumerDirective consumerDirective;

  public ToggleConsumer(@Value("${spring.application.name}")String applicationName,
                        KafkaListenerEndpointRegistry registry,
                        ConsumerDirective consumerDirective) {

    this.applicationName = applicationName;
    this.registry = registry;
    this.consumerDirective = consumerDirective;
  }

  @KafkaListener(
          containerFactory = "nonFilteringkafkaListenerContainerFactory",
    groupId = "${application.consumer.toggle.groupId}",
    topics = "${application.consumer.toggle.topic}",
    id = TOGGLE_LISTENER_ID
  )
  public void consume(ConsumerRecord<String, Map> consumerRecord,
    @Payload Map message
  ) {

    logger.debug("Recieved a directive");
    String targetAppName = (String)message.get(TARGET_APP_KEY);

    if (applicationName.equals(targetAppName)) {

      final String directiveValue = (String)message.get(DIRECTIVE_KEY);
      final Directive directive = Directive.getDirectiveForValue(directiveValue);

      if (directive == null) {
        String errorMessage = "Invalid directive from message is invalid: "+ directiveValue;
        logger.error(errorMessage);
        throw new IllegalArgumentException(errorMessage);
      }

      consumerDirective.setCurrentDirective(directive);

      if (directive == PAUSE || directive == PROCESS) {

        registry.getAllListenerContainers().forEach(it -> {

            if (!TOGGLE_LISTENER_ID.equals(it.getListenerId())) {

              switch (directive) {
                case PAUSE:
                  it.pause();
                  break;
                case PROCESS:
                  it.resume();
                  break;
                default:
                  break;
              }
            }
          }
        );
      }
    }
  }
}
