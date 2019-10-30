# Kafka Toggle Lib



## Overview

This library is used to toggle on/off/drain Kafka consumers in an application.  It works by 
listening to a kafka topic and then performing the required directive on the listening application. The library works by listening on 
a "toggle" topic with a random consumer group and listens for a message that contains the application name and a directive.  Since kafka uses consumer groups and guarentees that a message will be read by one instance of a consume group, this library ensures that all instances of an application will process the message.
It accomplishes this by setting application.consumer.toggle.groupId to a random value.
```yaml
application:
  consumer:
    toggle:
      groupId: ${random.uuid}
 ```

For example, if an application config has the following:
```yaml
spring:
  application:
    name: toggle-lib
  kafka:
    bootstrap-servers: ${spring.embedded.kafka.brokers}
    consumer:
      properties:
        group.id: some-app-specific-group-id
      auto-offset-reset: earliest  
application:
  consumer:
    toggle:
      groupId: ${random.uuid}
```
The library would find all Kafka Listeners registered via spring and perform the requested directive on all of the listeners except the one listening to the toggle topic.


### Supported Directives:

* PROCESS - This tells an application to start processing the Kafka listeners.
* PAUSE - This tells an application to stop the Kafka Listeners (except the listener for the toggle topic).
* DRAIN - This tells an application to consume the data from the Kafka topics but NOT process it.  This will empty out the topic.

### Required Configuration
The library does not supply any default properties but it does REQUIRE some specific ones to be set in the 
application's config. The following properties should be set in the application.yml of the applications using this library.
```yaml
spring:
  application:
    name: toggle-lib
 
application:
  consumer:
    toggle:
      groupId: ${random.uuid}
      topic: TOGGLE.TOPIC
```


### How to use the library 
 When using this library, the application will need to add 
```org.dentinger.kafka ``` package to be component scanned.

This will ensure that the [KafkaConfig](src/main/java/org/dentinger/kafka/config/KafkaConfig.java) is loaded.  This makes sure that a default vanilla filter strategy is used. 

The [ConsumerDirective](src/main/java/org/dentinger/kafka/consumer/ConsumerDiretive.java) class should be injected into any class that has a kafka listener annotation. ConsumerDirective can then be checked when a message is received to see if the listener should drain the topic.

#### Sample code 
```java
@KafkaListener(topics = "${application.topic1.name}")
    public void consume(
      ConsumerRecord<String, Map> consumerRecord,
      @Payload Map payloadData) {

      if (!consumerDirective.isDrain()) {

          process(payloadData);
      }
      else {
          logger.debug("Current state is to drain message ");
      }
    }
```  



### How to build
```bash
./gradlew clean build
```




