package com.epam.bdcc.kafka;

import com.epam.bdcc.htm.MonitoringRecord;
import com.epam.bdcc.utils.GlobalConstants;
import com.epam.bdcc.utils.PropertiesLoader;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TopicGenerator implements GlobalConstants {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicGenerator.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // load a properties file from class path, inside static method
        Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final boolean skipHeader = Boolean
                    .parseBoolean(applicationProperties.getProperty(GENERATOR_SKIP_HEADER_CONFIG));
            final long batchSleep = Long.parseLong(applicationProperties.getProperty(GENERATOR_BATCH_SLEEP_CONFIG));
            final int batchSize = Integer.parseInt(applicationProperties.getProperty(BATCH_SIZE_CONFIG));
            final String sampleFile = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            final String topicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);

            //TODO : Read the file (one_device_2015-2017.csv) and push records to Kafka raw topic.
            final KafkaProducer<String, MonitoringRecord> producer = KafkaHelper.createProducer();
            int count=0;
            try {
                Scanner in = new Scanner(new FileReader(sampleFile));
                while (in.hasNext()){
                    final String next = in.nextLine();
                    final String[] data = next.split(",");
                    final MonitoringRecord record = new MonitoringRecord(data);
                    System.out.println(record);
                    count+=1;
                    producer.send(new ProducerRecord<>(topicName,KafkaHelper.getKey(record), record)).get();
                }
                System.out.println(count);
            } catch (FileNotFoundException | ExecutionException | InterruptedException e) {
                e.getStackTrace();
            }

        }
    }

}
