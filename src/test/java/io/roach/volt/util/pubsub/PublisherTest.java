package io.roach.volt.util.pubsub;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jdbc.JdbcRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.boot.ShellRunnerAutoConfiguration;

import io.roach.volt.Application;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ConfigurationPropertiesScan(basePackageClasses = Application.class)
@SpringBootApplication(exclude = {
        BatchAutoConfiguration.class,
        JdbcRepositoriesAutoConfiguration.class,
        DataSourceAutoConfiguration.class,
        ShellRunnerAutoConfiguration.class,
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("interation-test")
public class PublisherTest {
    @Autowired
    private Publisher publisher;

    @Test
    public void testBasics() {
        Topic<String> topic1 = publisher.getTopic("test");
        Topic<String> topic2 = publisher.getTopic("test");
        Topic<String> topic3 = publisher.getTopic("test1");
        Assertions.assertSame(topic1, topic2);
        Assertions.assertNotSame(topic1, topic3);
        Assertions.assertNotSame(topic2, topic3);
    }

    @Test
    public void testSimpleTopic_withSyncPublish() {
        Topic<String> topic = publisher.getTopic("test");

        topic.addMessageListener(message -> {
            System.out.printf("A: %s\n", message.getPayload());
        });
        topic.addMessageListener(message -> {
            System.out.printf("B: %s\n", message.getPayload());
        });

        IntStream.rangeClosed(1, 10).forEach(value -> {
            topic.publish("hello " + value);
        });
    }

    @Test
    public void testSimpleTopic_withAsyncPublish() {
        Topic<Integer> topic = publisher.getTopic("test");

        Deque<Integer> q = new ConcurrentLinkedDeque<>();

        topic.addMessageListener(message -> {
            q.add(message.getPayload());
        });

        topic.addMessageListener(message -> {
            q.add(message.getPayload());
        });

        IntStream.rangeClosed(1, 10_000).forEach(value -> {
            topic.publishAsync(value);
        });

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new UndeclaredThrowableException(e);
        }

        Assertions.assertEquals(2 * 10_000, q.size());
    }
}
