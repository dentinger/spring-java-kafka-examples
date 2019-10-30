package org.dentinger.kafka.consumer;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.dentinger.kafka.Temp;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;


@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("unittest")
@DirtiesContext
@ContextConfiguration(classes = {Temp.class})
public class ConsumerDirectiveTest {

    private static final String TOPIC = "SOME.TOPIC";

    private static final String TEST_TOPIC = "TEST.TOPIC";

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, 1, TOPIC);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    ConsumerDirective consumerDirective;

    @Before
    public void setUp() {
        // wait until the partitions are assigned
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
                .getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer,
                    embeddedKafka.getEmbeddedKafka().getPartitionsPerTopic());
        }
    }


    @Test
    public void verifyThatDrainWillDrainATopic() {

        // get Logback Logger
        Logger fooLogger = (Logger) LoggerFactory.getLogger(Temp.class);

        // create and start a ListAppender
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        fooLogger.addAppender(listAppender);

        Map<String, String> toggleMessage = new HashMap<>();
        toggleMessage.put("targetApplication", "toggle-lib");
        toggleMessage.put("directive", "drain");

        kafkaTemplate.send(TOPIC, "testKey", toggleMessage);
        try {
            //wait up to 4 seconds to receive message
            Thread.sleep(3000);

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        
        kafkaTemplate.send(TEST_TOPIC, "testKey", "message");

        try {
            //wait up to 4 seconds to receive message
            Thread.sleep(5000);

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        List<ILoggingEvent> logsList = listAppender.list;
        MatcherAssert.assertThat(1, is(logsList.size()));
        MatcherAssert.assertThat(logsList.get(0).getMessage(), is("Current state is to drain message: \"message\""));

    }
}
