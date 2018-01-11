import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.recovery.AutorecoveringChannel;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RMQProblemTest {
    private static final int QOS_PREFETCH = 64;
    private static final int NUM_MESSAGES_TO_PRODUCE = 10000;
    private static final int MESSAGE_PROCESSING_TIME_MS = 3000;
    private static final long MAX_TIME_BEFORE_FAIL_MS = TimeUnit.SECONDS.toMillis(60);
    private static final long QUEUE_TTL_MS = TimeUnit.MINUTES.toMillis(5);

    private AtomicInteger ackedMessages;
    private ExecutorService producerService;
    private AutorecoveringConnection producingConnection;
    private AutorecoveringChannel producingChannel;
    private AutorecoveringConnection consumingConnection;
    private AutorecoveringChannel consumingChannel;

    @Before
    public void setUp() throws Exception {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setAutomaticRecoveryEnabled(true);
        factory.setHost("localhost");

        ackedMessages = new AtomicInteger(0);
        producerService = Executors.newSingleThreadExecutor();
        producingConnection = (AutorecoveringConnection) factory.newConnection();
        producingChannel = (AutorecoveringChannel) producingConnection.createChannel();
        consumingConnection = (AutorecoveringConnection) factory.newConnection();
        consumingChannel = (AutorecoveringChannel) consumingConnection.createChannel();
    }

    @After
    public void tearDown() throws Exception {
        producerService.shutdownNow();
        consumingChannel.close();
        consumingConnection.close();
        producingChannel.close();
        producingConnection.close();
    }

    private void declareQueue(final Channel channel, final String queue) throws IOException {
        final Map<String, Object> queueArguments = new HashMap<>();
        queueArguments.put("x-expires", QUEUE_TTL_MS);
        channel.queueDeclare(queue, true, false, false, queueArguments);
    }

    private void produceMessagesInBackground(final Channel channel, final String queue) {
        final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().deliveryMode(2).build();
        producerService.execute(() -> IntStream.range(0, NUM_MESSAGES_TO_PRODUCE)
                .forEach(x -> {
                    try {
                        channel.basicPublish("", queue, false, properties, ("MSG NUM" + x).getBytes());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }));
    }

    private void startConsumer(String queue) throws IOException {
        consumingChannel.basicConsume(queue, false, "", false, false, null, new DefaultConsumer(consumingChannel) {
            @Override
            public void handleRecoverOk(String consumerTag) {
                System.out.println("Recovering. This doesn't get logged!!!");
            }

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                try {
                    Thread.sleep(MESSAGE_PROCESSING_TIME_MS);
                    consumingChannel.basicAck(envelope.getDeliveryTag(), false);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    ackedMessages.incrementAndGet();
                }
            }
        });

        consumingChannel.basicQos(QOS_PREFETCH);
    }

    private void registerRecoveryListener(final String type, final Recoverable recoverable) {
        recoverable.addRecoveryListener(new RecoveryListener() {
            @Override
            public void handleRecovery(Recoverable recoverable) {
                System.out.println("Recovery finished for " + type + " " + recoverable);
            }

            @Override
            public void handleRecoveryStarted(Recoverable recoverable) {
                System.out.println("Recovery Started for " + type + " " + recoverable);
            }
        });
    }

    @Test
    public void failureAndRecovery() throws IOException {
        final String queue = UUID.randomUUID().toString();
        final long startTime = System.currentTimeMillis();

        registerRecoveryListener("Consuming Channel", consumingChannel);
        registerRecoveryListener("Consuming Connection", consumingConnection);
        registerRecoveryListener("Producing Channel", producingChannel);
        registerRecoveryListener("Producing Connection", producingConnection);

        declareQueue(producingChannel, queue);
        produceMessagesInBackground(producingChannel, queue);
        startConsumer(queue);

        while (true) {
            final int numAckedMessages = ackedMessages.get();
            if (numAckedMessages < NUM_MESSAGES_TO_PRODUCE) {
                if (System.currentTimeMillis() - startTime > MAX_TIME_BEFORE_FAIL_MS) {
                    fail("We consume our messages within the specified time. We consumed " + numAckedMessages + " messages.");
                    break;
                }
                try {
                    Thread.sleep(1000);
                    System.out.println("Consumed so far - " + numAckedMessages);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                assertEquals(numAckedMessages, NUM_MESSAGES_TO_PRODUCE);
                System.out.println("DONE!");
                break;
            }
        }
    }
}
