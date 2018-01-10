import com.rabbitmq.client.*;
import org.junit.Test;
import ratelimiter.ThrottlingExecutorService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RMQProblemTest {
    private static final int NUM_WORKERS = 64;
    private static final int QOS_PREFETCH = 64;
    private static final int NUM_MESSAGES_TO_PRODUCE = 10000;
    private static final long MAX_TIME_BEFORE_FAIL_MS = TimeUnit.SECONDS.toMillis(60);
    private static final long QUEUE_TTL_MS = TimeUnit.MINUTES.toMillis(5);
    private static final Random RANDOM_NUM_GEN = new Random();

    private final ExecutorService ackerService = Executors.newSingleThreadExecutor();
    private final ExecutorService producerService = Executors.newSingleThreadExecutor();
//    private final ExecutorService workerService = Executors.newFixedThreadPool(NUM_WORKERS);
    private final ExecutorService workerService = ThrottlingExecutorService.createExecutorService(1000, 10, TimeUnit.SECONDS);

    private Runnable createAcker(final Channel channel, final AtomicInteger ackedMessages, final long deliveryTag, final boolean shouldRequeue) {
        return () -> {
            try {
                if (shouldRequeue) {
                    channel.basicReject(deliveryTag, true);
                } else {
                    channel.basicAck(deliveryTag, false);

                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (!shouldRequeue) {
                    ackedMessages.incrementAndGet();
                }
            }
        };
    }

    private Runnable createWorker(final Channel channel, final AtomicInteger ackedMessages, final Envelope envelope) {
        return () -> {
            try {
                Thread.sleep(100);
                ackerService.submit(createAcker(channel, ackedMessages, envelope.getDeliveryTag(), RANDOM_NUM_GEN.nextInt(100) > 1));
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    }

    private Runnable createProducer(final Channel channel, final String queue) {
        return () -> IntStream.range(0, NUM_MESSAGES_TO_PRODUCE)
                .forEach(x -> {
                    try {
                        channel.basicPublish("", queue, null, ("MSG NUM" + x).getBytes());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    private void declareQueue(final Channel channel, final String queue) throws IOException {
        final Map<String, Object> queueArguments = new HashMap<>();
        queueArguments.put("x-expires", QUEUE_TTL_MS);
        channel.queueDeclare(queue, true, false, false, queueArguments);
    }

    @Test
    public void shouldNotFail() throws IOException, TimeoutException {
        final ConnectionFactory factory = new ConnectionFactory();
        final String queue = UUID.randomUUID().toString();
        final long startTime = System.currentTimeMillis();
        factory.setHost("localhost");

        final Connection producingConnection = factory.newConnection();
        final Channel producingChannel = producingConnection.createChannel();
        declareQueue(producingChannel, queue);
        producerService.execute(createProducer(producingChannel, queue));

        final Connection consumingConnection = factory.newConnection();
        final Channel consumingChannel = consumingConnection.createChannel();
        final AtomicBoolean isFirstMessage = new AtomicBoolean(false);
        final AtomicInteger ackedMessages = new AtomicInteger(0);

        consumingChannel.basicConsume(queue, false, new DefaultConsumer(consumingChannel) {
            @Override
            public void handleRecoverOk(String consumerTag) {
                System.out.println("Recovering. This doesn't get logged!!!");
            }

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                if (!isFirstMessage.getAndSet(true)) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                workerService.execute(createWorker(consumingChannel, ackedMessages, envelope));
            }
        });

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        consumingChannel.basicQos(QOS_PREFETCH);

        while (true) {
            final int numAckedMessages = ackedMessages.get();
            if (numAckedMessages < NUM_MESSAGES_TO_PRODUCE) {
                if (System.currentTimeMillis() - startTime > MAX_TIME_BEFORE_FAIL_MS) {
                    fail("We consume our messages within the specified time. We consumed " + numAckedMessages + " messages.");
                    break;
                }
                try {
                    Thread.sleep(100);
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
