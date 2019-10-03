/*
 * StreamTeam
 * Copyright (C) 2019  University of Basel
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ch.unibas.dmi.dbis.streamImporter;

import ch.unibas.dmi.dbis.streamImporter.dataItems.DataItem;
import ch.unibas.dmi.dbis.streamImporter.dataItems.MatchMetadataItem;
import ch.unibas.dmi.dbis.streamImporter.dataItems.PositionOutOfRangeException;
import ch.unibas.dmi.dbis.streamImporter.propertiesHelper.PropertyReadHelper;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.AbstractImmutableDataStreamElement;
import ch.unibas.dmi.dbis.streamTeam.dataStreamElements.football.MatchMetadataStreamElement;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * StreamImporter which consumes data stream elements from Kafka, converts them to MongoDB documents according to our schemata, and adds them to the corresponding MongoDB collection.
 */
public class StreamImporter {

    /**
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(StreamImporter.class);

    /**
     * KafkaConsumer
     */
    private final KafkaConsumer<String, byte[]> kafkaConsumer;

    /**
     * Poll timeout
     */
    private final long pollTimeout;

    /**
     * Interval in which the SubscriptionUpdater updates the subscriptions
     */
    private final long subscriptionInterval;

    /**
     * SubscriptionUpdater
     */
    private SubscriptionUpdater subscriptionUpdater;

    /**
     * Flag that indicates if the StreamImporter should continue polling new data stream elements or not
     */
    private boolean runFlag;

    /**
     * MongoDB collection for storing the match metadata
     */
    private MongoCollection<Document> matchesCollection;

    /**
     * MongoDB collection for storing the (atomic) events
     */
    private MongoCollection<Document> eventsCollection;

    /**
     * MongoDB collection for storing the non-atomic events
     */
    private MongoCollection<Document> nonatomicEventsCollection;

    /**
     * MongoDB collection for storing the statistics
     */
    private MongoCollection<Document> statisticsCollection;

    /**
     * MongoDB collection for storing the states
     */
    private MongoCollection<Document> statesCollection;

    /**
     * Map containing the generation timestamp (in ms) of the first data stream element for every match
     */
    private Map<String, Long> generationTimestampFirstDataStreamElementMap;

    /**
     * Map containing the video offset (in s) of the start of the match for every match
     */
    private Map<String, Long> matchStartVideoOffsetMap;

    /**
     * Wait list for data stream elements that cannot be added to MongoDB yet as the corresponding matchMetadata stream element has not been consumed yet
     */
    private ArrayDeque<AbstractImmutableDataStreamElement> waitList;

    /**
     * Creates and starts the StreamImporter.
     *
     * @param args Parameters
     */
    public static void main(String[] args) {
        String propertiesFilePath = "/streamImporter.properties";

        Properties properties = new Properties();
        try {
            //http://stackoverflow.com/questions/29070109/how-to-read-properties-file-inside-jar
            InputStream in = StreamImporter.class.getResourceAsStream(propertiesFilePath);
            properties.load(in);
        } catch (IOException e) {
            logger.error("Unable to load {}", propertiesFilePath, e);
            System.exit(1);
        }

        StreamImporter streamImporter = new StreamImporter(properties);
    }

    /**
     * StreamImporter constructor.
     *
     * @param properties Properties
     */
    public StreamImporter(Properties properties) {
        logger.info("Read properties");
        this.pollTimeout = PropertyReadHelper.readLongOrDie(properties, "kafka.pollTimeout");
        this.subscriptionInterval = PropertyReadHelper.readLongOrDie(properties, "kafka.subscriptionInterval");
        List<String> forbiddenTopics = PropertyReadHelper.readListOfStringsOrDie(properties, "kafka.forbiddenTopics");
        String brokerList = PropertyReadHelper.readStringOrDie(properties, "kafka.brokerList");
        String groupIdPrefix = PropertyReadHelper.readStringOrDie(properties, "kafka.groupIdPrefix");
        String connectionString = PropertyReadHelper.readStringOrDie(properties, "mongodb.connectionString");
        String databaseName = PropertyReadHelper.readStringOrDie(properties, "mongodb.database");

        logger.info("Initializing StreamConsumer");
        // https://kafka.apache.org/0100/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", groupIdPrefix + "_" + UUID.randomUUID().toString());
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        this.kafkaConsumer = new KafkaConsumer<>(props);

        this.subscriptionUpdater = new SubscriptionUpdater(forbiddenTopics);
        Thread subscriptionUpdaterThread = new Thread(this.subscriptionUpdater);
        subscriptionUpdaterThread.start();

        logger.info("Initialize MongoDB");
        MongoClientURI connectionURI = new MongoClientURI(connectionString);
        MongoClient mongoClient = new MongoClient(connectionURI);
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        this.matchesCollection = database.getCollection("matches");
        this.eventsCollection = database.getCollection("events");
        this.statesCollection = database.getCollection("states");
        this.nonatomicEventsCollection = database.getCollection("nonatomicEvents");
        this.statisticsCollection = database.getCollection("statistics");

        logger.info("Start consumption loop");
        this.generationTimestampFirstDataStreamElementMap = new HashMap<>();
        this.matchStartVideoOffsetMap = new HashMap<>();
        this.waitList = new ArrayDeque<>();
        this.runFlag = true;
        while (this.runFlag) {
            try {
                List<Document> matchDocuments = new LinkedList<>();
                List<Document> eventDocuments = new LinkedList<>();
                List<Document> nonatomicEventDocuments = new LinkedList<>();
                List<Document> statisticsDocuments = new LinkedList<>();
                List<Document> stateDocuments = new LinkedList<>();

                ConsumerRecords<String, byte[]> records;
                synchronized (this.kafkaConsumer) { // required for SubscriptionUpdater
                    records = this.kafkaConsumer.poll(this.pollTimeout);
                }
                for (ConsumerRecord<String, byte[]> record : records) {
                    String key = record.key();
                    Long sequenceNumber = record.offset();
                    byte[] contentByteArray = record.value();

                    try {
                        AbstractImmutableDataStreamElement dataStreamElement = AbstractImmutableDataStreamElement.generateDataStreamElementFromByteArray(key, contentByteArray, sequenceNumber, null, null);

                        if (!dataStreamElement.getStreamName().equals(record.topic())) {
                            logger.error("Cannot handle element ({}) since the stream name the data model assigns to the input stream element does not match the name of the Kafka topic via which it was received ({}).", dataStreamElement, record.topic());
                        } else {
                            handleDataStreamElement(dataStreamElement, matchDocuments, eventDocuments, nonatomicEventDocuments, statisticsDocuments, stateDocuments);
                        }
                    } catch (ClassNotFoundException | InvalidProtocolBufferException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
                        logger.info("Caught exception during generating data stream element from byte array: ", e);
                    }
                }

                for (int i = 0; i < this.waitList.size(); ++i) { // go once through the waitList (pollFirst & potentially addLast in handleDataStreamElement)
                    AbstractImmutableDataStreamElement dataStreamElement = this.waitList.pollFirst();

                    handleDataStreamElement(dataStreamElement, matchDocuments, eventDocuments, nonatomicEventDocuments, statisticsDocuments, stateDocuments);
                }

                insertMany(this.matchesCollection, matchDocuments);
                insertMany(this.eventsCollection, eventDocuments);
                insertMany(this.nonatomicEventsCollection, nonatomicEventDocuments);
                insertMany(this.statisticsCollection, statisticsDocuments);
                insertMany(this.statesCollection, stateDocuments);

            } catch (WakeupException e) {
                logger.info("Poll interrupted with wakeup call.");
            } catch (IllegalStateException e) {
                // No topic is subscribed yet
                try {
                    Thread.sleep(this.pollTimeout); // Otherwise 100% CPU usage until the first topic is subscribed
                } catch (InterruptedException e2) {
                    logger.trace("InterruptedException in main loop.", e2);
                }
            }
        }

        this.kafkaConsumer.close();
        this.subscriptionUpdater.subscriptionUpdaterRunFlag = false;
        logger.info("Closed StreamConsumer");
    }

    /**
     * Handles a data stream element polled as a record from Kafka or from the wait list.
     *
     * @param dataStreamElement       Data stream element
     * @param matchDocuments          Documents which have to be added to the matches collection (might contain already some documents which should not be deleted)
     * @param eventDocuments          Documents which have to be added to the (atomic) events collection (might contain already some documents which should not be deleted)
     * @param nonatomicEventDocuments Documents which have to be added to the non-atomic events collection (might contain already some documents which should not be deleted)
     * @param statisticsDocuments     Documents which have to be added to the statistics collection (might contain already some documents which should not be deleted)
     * @param stateDocuments          Documents which have to be added to the states collection (might contain already some documents which should not be deleted)
     */
    private void handleDataStreamElement(AbstractImmutableDataStreamElement dataStreamElement, List<Document> matchDocuments, List<Document> eventDocuments, List<Document> nonatomicEventDocuments, List<Document> statisticsDocuments, List<Document> stateDocuments) {
        try {
            if (dataStreamElement instanceof MatchMetadataStreamElement) {
                MatchMetadataItem matchMetadataItem = new MatchMetadataItem((MatchMetadataStreamElement) dataStreamElement);
                this.generationTimestampFirstDataStreamElementMap.put(matchMetadataItem.getMatchId(), matchMetadataItem.getGenerationTimestampFirstDataStreamElementOfTheMatch());
                this.matchStartVideoOffsetMap.put(matchMetadataItem.getMatchId(), matchMetadataItem.getMatchStartVideoOffset());
                matchDocuments.add(matchMetadataItem.toDocument());
            } else {
                Long generationTimestampFirstDataStreamElement = this.generationTimestampFirstDataStreamElementMap.get(dataStreamElement.getKey());
                Long matchStartVideoOffset = this.matchStartVideoOffsetMap.get(dataStreamElement.getKey());
                if (generationTimestampFirstDataStreamElement == null || matchStartVideoOffset == null) {
                    this.waitList.addLast(dataStreamElement);
                } else {
                    DataItem dataItem = new DataItem(dataStreamElement, generationTimestampFirstDataStreamElement, matchStartVideoOffset);

                    if (dataStreamElement.getStreamCategory().equals(AbstractImmutableDataStreamElement.StreamCategory.STATISTICS)) {
                        statisticsDocuments.add(dataItem.toDocument());
                    } else if (dataStreamElement.getStreamCategory().equals(AbstractImmutableDataStreamElement.StreamCategory.STATE)) {
                        stateDocuments.add(dataItem.toDocument());
                    } else if (dataStreamElement.getStreamCategory().equals(AbstractImmutableDataStreamElement.StreamCategory.EVENT)) {
                        if (dataStreamElement.isAtomic()) {
                            eventDocuments.add(dataItem.toDocument());
                        } else {
                            nonatomicEventDocuments.add(dataItem.toDocument());
                        }
                    }
                }
            }
        } catch (AbstractImmutableDataStreamElement.CannotRetrieveInformationException | PositionOutOfRangeException e) {
            logger.error("Caught exception during handling element: {}", dataStreamElement, e);
        }
    }

    /**
     * Inserts many documents into a MongoDB collection.
     *
     * @param collection MongoDB collection
     * @param documents  Documents
     */
    private void insertMany(MongoCollection collection, List<Document> documents) {
        try {
            if (!documents.isEmpty()) {
                collection.insertMany(documents);
            }
        } catch (MongoWriteException e) {
            logger.info("Cannot insert due to MongoWriteException: ", e);
        } catch (MongoBulkWriteException e) {
            logger.info("Cannot insert due to MongoBulkWriteException: ", e);
        }
    }

    /**
     * Updater for the subscriptions of the Kafka consumer.
     */
    private class SubscriptionUpdater implements Runnable {

        /**
         * Flag that indicates if the SubscriptionUpdater should update the subscriptions or not
         */
        private boolean subscriptionUpdaterRunFlag;

        /**
         * List of currently subscribed topics
         */
        private List<String> currentlySubscribeTopics;

        /**
         * List of forbidden topics
         */
        private final List<String> forbiddenTopics;

        /**
         * SubscriptionUpdater constructor.
         *
         * @param forbiddenTopics List of forbidden topics
         */
        private SubscriptionUpdater(List<String> forbiddenTopics) {
            this.subscriptionUpdaterRunFlag = true;
            this.currentlySubscribeTopics = new LinkedList<>();
            this.forbiddenTopics = forbiddenTopics;
            StringBuilder sb = new StringBuilder("Forbidden topics: ");
            for (String topic : forbiddenTopics) {
                sb.append(topic).append(" ");
            }
            logger.info(sb.toString());
        }

        /**
         * Continuously updates the subscriptions.
         */
        @Override
        public void run() {
            while (this.subscriptionUpdaterRunFlag) {
                synchronized (StreamImporter.this.kafkaConsumer) { // required for SubscriptionUpdater
                    Set<String> topicSet = StreamImporter.this.kafkaConsumer.listTopics().keySet();

                    List<String> topicsToSubscribe = new LinkedList<>();
                    for (String topic : topicSet) {
                        if (!topic.startsWith("__") && !topic.contains("changelog") && !topic.contains("metrics") && !this.forbiddenTopics.contains(topic)) {
                            topicsToSubscribe.add(topic);
                        }
                    }

                    if (haveTopicsChanged(topicsToSubscribe)) {
                        StreamImporter.this.kafkaConsumer.subscribe(topicsToSubscribe);
                        this.currentlySubscribeTopics = topicsToSubscribe;
                        StringBuilder sb = new StringBuilder("New subscription list: ");
                        for (String topic : topicsToSubscribe) {
                            sb.append(topic).append(" ");
                        }
                        logger.info(sb.toString());
                    }

                }

                try {
                    Thread.sleep(StreamImporter.this.subscriptionInterval);
                } catch (InterruptedException e) {
                    logger.trace("InterruptedException in SubscriptionUpdater", e);
                }
            }
        }

        /**
         * Check if the topics have changed.
         *
         * @param newTopicsToSubscribe New list of topics
         * @return True if the topics have changed
         */
        private boolean haveTopicsChanged(List<String> newTopicsToSubscribe) {
            if (this.currentlySubscribeTopics.size() != newTopicsToSubscribe.size()) {
                return true;
            } else {
                for (String topic : this.currentlySubscribeTopics) {
                    if (!newTopicsToSubscribe.contains(topic)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
