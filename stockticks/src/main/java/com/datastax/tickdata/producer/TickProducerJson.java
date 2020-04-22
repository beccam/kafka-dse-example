package com.datastax.tickdata.producer;

import com.datastax.tickdata.utils.ExchangeUtils;
import com.datastax.tickdata.utils.PropertyHelper;
import com.datastax.tickdata.utils.TickData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class TickProducerJson {

    private static final String TOPIC = "tick-stream-json";
    private static final int RECORDS_PER_STOCK = 1_000_000;

    // Report number of records sent every this many seconds.
    private static final long PROGRESS_REPORTING_INTERVAL = 5;

    private static Logger log = LoggerFactory.getLogger("TickProducer");

    public static void main(String[] args) throws InterruptedException {

        int events = Integer.parseInt(PropertyHelper.getProperty("noOfTicks", "61"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // Start a timer to measure how long this run takes overall.
        Instant start = Instant.now();

        final KafkaProducer<String, JsonNode> producer = new KafkaProducer<>(props);
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close, "Shutdown-thread"));

        TickGenerator generator = new TickGenerator(ExchangeUtils.getExchangeData());

        // Calculate the total number of records we expect to generate, an object
        // to keep track of the number of errors we encounter, and a latch that
        // will be signalled every time a "send" completes. This latch allows us
        // to wait for all sends to complete before terminating the program.
        int totalRecords = RECORDS_PER_STOCK * (events - 1);
        AtomicLong errorCount = new AtomicLong();
        CountDownLatch requestLatch = new CountDownLatch(totalRecords);

        // Create a counter to track the number of records we've successfully
        // created so far.
        final AtomicLong successCount = new AtomicLong();

        // This callback will be invoked whenever a send completes. It reports any
        // errors (and bumps the error-count) and signals the latch as described above.
        Callback postSender = (recordMetadata, e) -> {
            if (e != null) {
                log.error("Error adding to topic", e);
                errorCount.incrementAndGet();
            } else {
                successCount.incrementAndGet();
            }
            requestLatch.countDown();
        };

        ObjectMapper mapper = new ObjectMapper();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(
                ()->log.info("Successfully created {} Kafka records", successCount.get()),
                2, PROGRESS_REPORTING_INTERVAL, TimeUnit.SECONDS);

        for (int j = 0; j < RECORDS_PER_STOCK; j++) {
            for (int i = 0; i < events - 1; i++) {
                TickData tickData = generator.getStockWithRandomValue(i);
                tickData.setDateTime();
                producer.send(new ProducerRecord<>(TOPIC, tickData.getName(), mapper.valueToTree(tickData)), postSender);
            }
        }

        // Wait for sends to complete.
        requestLatch.await();

        // Stop the thread that periodically reports progress.
        scheduler.shutdown();
        producer.close();
        long duration = Duration.between(start, Instant.now()).getSeconds();
        log.info("Completed loading {}/{} records to Kafka in {} seconds",
                totalRecords - errorCount.get(), totalRecords, duration);
    }
}
