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
import org.springframework.boot.test.mock.mockito.MockBean;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("unittest")
@DirtiesContext
@ContextConfiguration(classes = {Temp.class})
public class ToggleConsumerTest {

    private static final String TOPIC = "SOME.TOPIC";

    private static final String TEST_TOPIC = "TEST.TOPIC";

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, 1, TOPIC);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @MockBean
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
    public void shouldErrorOnUnknownDirectiveMessage() {
        Map<String, String> toggleMessage = new HashMap<>();
        toggleMessage.put("targetApplication", "toggle-lib");
        toggleMessage.put("directive", "FLIP");

        // get Logback Logger
        Logger fooLogger = (Logger) LoggerFactory.getLogger(ToggleConsumer.class);

        // create and start a ListAppender
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        fooLogger.addAppender(listAppender);

        kafkaTemplate.send(TOPIC, "testKey", toggleMessage);

        try {
            //wait up to 4 seconds to receive message
            Thread.sleep(4000);

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        List<ILoggingEvent> logsList = listAppender.list;
        MatcherAssert.assertThat(2, is(logsList.size()));
        MatcherAssert.assertThat(logsList.get(1).getMessage(), containsString("Invalid directive"));
        kafkaListenerEndpointRegistry.getAllListenerContainers().forEach(
                it -> {
                    if (!"consumer.toggle.listener".equals(it.getListenerId())) {
                        assertThat(it.isRunning(), is(Boolean.TRUE));
                    } else {
                        assertThat(it.isRunning(), is(Boolean.TRUE));
                    }
                }
        );
    }

    @Test
    public void shouldHandleProcessMessage() {
        Map<String, String> toggleMessage = new HashMap<>();
        toggleMessage.put("targetApplication", "toggle-lib");
        toggleMessage.put("directive", "process");

        TestFuture testFuture = new TestFuture();

        doAnswer(invocation -> {
            String gtinId = invocation.getArgument(0).toString();
            testFuture.setValue(gtinId);
            return testFuture;
        }).when(consumerDirective).setCurrentDirective(any(ConsumerDirective.Directive.class));

        kafkaTemplate.send(TOPIC, "testKey", toggleMessage);

        try {
            //wait up to 4 seconds to receive message
            int count = 0;
            while (count++ <= 20 && testFuture.get() == null) {

                Thread.sleep(200);
            }

            assertThat(testFuture.get(), equalToIgnoringCase(toggleMessage.get("directive")));

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        kafkaListenerEndpointRegistry.getAllListenerContainers().forEach(
                it -> {
                    if (!"consumer.toggle.listener".equals(it.getListenerId())) {
                        assertThat(it.isPauseRequested(), is(Boolean.FALSE));
                        assertThat(it.isRunning(), is(Boolean.TRUE));
                    } else {
                        assertThat(it.isRunning(), is(Boolean.TRUE));
                    }
                }
        );
    }

    @Test
    public void shouldHandlePauseMessage() {

        Map<String, String> toggleMessage = new HashMap<>();
        toggleMessage.put("targetApplication", "toggle-lib");
        toggleMessage.put("directive", "pause");

        TestFuture testFuture = new TestFuture();

        doAnswer(invocation -> {
            String gtinId = invocation.getArgument(0).toString();
            testFuture.setValue(gtinId);
            return testFuture;
        }).when(consumerDirective).setCurrentDirective(any(ConsumerDirective.Directive.class));

        kafkaTemplate.send(TOPIC, "testKey", toggleMessage);

        try {
            //wait up to 4 seconds to receive message
            int count = 0;
            while (count++ <= 20 && testFuture.get() == null) {

                Thread.sleep(200);
            }

            assertThat(testFuture.get(), equalToIgnoringCase(toggleMessage.get("directive")));


        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }

        kafkaListenerEndpointRegistry.getAllListenerContainers().forEach(
                it -> {
                    if (!"consumer.toggle.listener".equals(it.getListenerId())) {
                        if(!it.isRunning()) fail("listener should be paused: " + it.getListenerId());

                    } else {
                        if(!it.isRunning()) fail("toggle listener should be running");
//                        assertThat(it.isRunning(), is(Boolean.TRUE));
                    }
                }
        );
    }

    @Test
    public void shouldHandleDrainMessage() {
        Map<String, String> toggleMessage = new HashMap<>();
        toggleMessage.put("targetApplication", "toggle-lib");
        toggleMessage.put("directive", "drain");

        TestFuture testFuture = new TestFuture();

        doAnswer(invocation -> {
            String gtinId = invocation.getArgument(0).toString();
            testFuture.setValue(gtinId);
            return testFuture;
        }).when(consumerDirective).setCurrentDirective(any(ConsumerDirective.Directive.class));

        kafkaTemplate.send(TOPIC, "testKey", toggleMessage);

        try {
            //wait up to 4 seconds to receive message
            int count = 0;
            while (count++ <= 20 && testFuture.get() == null) {

                Thread.sleep(200);
            }

            assertThat(testFuture.get(), equalToIgnoringCase(toggleMessage.get("directive")));

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        kafkaListenerEndpointRegistry.getAllListenerContainers().forEach(
                it -> {
                    if (!"consumer.toggle.listener".equals(it.getListenerId())) {
                        assertThat(it.isPauseRequested(), is(Boolean.FALSE));
                        assertThat(it.isRunning(), is(Boolean.TRUE));
                    } else {
                        assertThat(it.isRunning(), is(Boolean.TRUE));
                    }
                }
        );
    }

    @Test
    public void verifyThatDrainWillDrainATopic() {

        // get Logback Logger
        Logger fooLogger = (Logger) LoggerFactory.getLogger(Temp.class);

        // create and start a ListAppender
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        fooLogger.addAppender(listAppender);

        when(consumerDirective.isDrain()).thenReturn(Boolean.TRUE);


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

    private class TestFuture implements Future<String> {

        private String value;

        public void setValue(String value) {
            this.value = value;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public String get() throws InterruptedException, ExecutionException {
            return value;
        }

        @Override
        public String get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return value;
        }
    }

}
