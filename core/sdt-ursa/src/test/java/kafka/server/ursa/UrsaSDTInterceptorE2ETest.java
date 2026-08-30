/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.ursa;

import kafka.server.ursa.sdt.UrsaSDTInterceptor;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.diskless.handlers.KafkaLogNaming;

import org.apache.commons.lang3.RandomStringUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.lakestream.ursa.compaction.OxiaCompactTaskManager;
import io.lakestream.ursa.compaction.task.CompactStreamTask;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.testcontainers.OxiaContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("UrsaSDTInterceptor E2E Tests (without Ursa Storage)")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
@Tag("integration")
public class UrsaSDTInterceptorE2ETest {

    private static final int NUM_RECORDS = 100;
    private static final int PRODUCE_TIMEOUT_SECONDS = 60;
    private static final String URSA_SDT_TEST_CLASS_PATH_PROPERTY = "ursa.sdt.test.class.path";

    private static KafkaClusterTestKit cluster;
    private static OxiaContainer oxiaContainer;
    private static Admin admin;
    private static AsyncOxiaClient oxiaClient;
    private static OxiaCompactTaskManager taskManager;

    @BeforeAll
    static void startCluster() throws Exception {
        var ursaSdtClassPath = System.getProperty(URSA_SDT_TEST_CLASS_PATH_PROPERTY);
        assertNotNull(ursaSdtClassPath, URSA_SDT_TEST_CLASS_PATH_PROPERTY + " is not set");
        oxiaContainer = new OxiaContainer(DockerImageName.parse("oxia/oxia:main"));
        oxiaContainer.start();
        cluster = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(2)
                        .setNumControllerNodes(1)
                .build())
                .setConfigProp(ServerLogConfigs.INTERCEPTOR_CLASS_NAME_CONFIG,
                    "kafka.server.ursa.sdt.UrsaSDTInterceptor")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_CLASS_PATH_CONFIG, ursaSdtClassPath)
                .setConfigProp(ServerLogConfigs.URSA_OXIA_SERVICE_URL_CONFIG,
                    String.format("oxia://%s/default", oxiaContainer.getServiceAddress()))
                .setConfigProp("clusterTailCompactDataVisibilityIntervalInSeconds", "3")
                .setConfigProp("clusterSdtEnabled", "true")
                .setConfigProp("offsets.topic.replication.factor", "1")
                .setConfigProp("transaction.state.log.replication.factor", "1")
                .setConfigProp("transaction.state.log.min.isr", "1")
                .build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        admin = cluster.admin();
        oxiaClient = OxiaClientBuilder.create(oxiaContainer.getServiceAddress())
            .namespace("default")
            .asyncClient()
            .get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        taskManager = new OxiaCompactTaskManager(oxiaClient);
    }

    @AfterAll
    static void stopCluster() throws Exception {
        try {
            if (admin != null) {
                admin.close();
            }
            if (oxiaClient != null) {
                oxiaClient.close();
            }
            if (cluster != null) {
                cluster.close();
            }
        } finally {
            if (oxiaContainer != null) {
                oxiaContainer.stop();
            }
        }
    }

    @Test
    @DisplayName("Should publish compaction tasks only from current live leader after leader move")
    void testPublishTasksAfterLiveLeaderMove() throws Exception {
        var currentTopic = "test-topic-" + RandomStringUtils.secure().nextAlphabetic(4);
        var topicPartition = new TopicPartition(currentTopic, 0);
        var topicId = createTopicWithAssignment(currentTopic, Map.of(0, List.of(0, 1)));

        waitForLeader(currentTopic, 0, 0);

        produceRecords(currentTopic, 0, 0, NUM_RECORDS);

        var topicNameInTask = taskTopicName(currentTopic, topicId, 0);

        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                var tasksInTopic = tasksForTopic(topicNameInTask);
                assertTaskCoverage(tasksInTopic, topicNameInTask, 0, NUM_RECORDS, Optional.of("0"));
            });
        var taskCountBeforeMove = tasksForTopic(topicNameInTask).size();

        admin.alterPartitionReassignments(Map.of(
            topicPartition,
            Optional.of(new NewPartitionReassignment(List.of(1, 0)))
        )).all().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        admin.electLeaders(ElectionType.PREFERRED, Set.of(topicPartition))
            .all().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        waitForLeader(currentTopic, 0, 1);

        produceRecords(currentTopic, 0, NUM_RECORDS, NUM_RECORDS);

        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                var tasksInTopic = tasksForTopic(topicNameInTask);
                assertTrue(tasksInTopic.size() > taskCountBeforeMove);
                assertLeaderMoveTaskCoverage(tasksInTopic, topicNameInTask);
            });
        var taskCountAfterMove = tasksForTopic(topicNameInTask).size();

        Awaitility.await().atMost(Duration.ofSeconds(15))
            .pollDelay(Duration.ofSeconds(8))
            .untilAsserted(() -> {
                var tasksInTopic = tasksForTopic(topicNameInTask);
                assertEquals(taskCountAfterMove, tasksInTopic.size());
                assertLeaderMoveTaskCoverage(tasksInTopic, topicNameInTask);
            });
    }

    @Test
    @DisplayName("Should not fail when single-replica leader moves to another live broker")
    void testPublishTasksAfterSingleReplicaLeaderMove() throws Exception {
        var currentTopic = "test-topic-" + RandomStringUtils.secure().nextAlphabetic(4);
        var topicPartition = new TopicPartition(currentTopic, 0);
        var topicId = createTopicWithAssignment(currentTopic, Map.of(0, List.of(0)));

        waitForLeader(currentTopic, 0, 0);
        waitForReplicas(currentTopic, 0, List.of(0));

        produceRecords(currentTopic, 0, 0, NUM_RECORDS);

        var topicNameInTask = taskTopicName(currentTopic, topicId, 0);

        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                var tasksInTopic = tasksForTopic(topicNameInTask);
                assertTaskCoverage(tasksInTopic, topicNameInTask, 0, NUM_RECORDS, Optional.of("0"));
            });
        var taskCountBeforeMove = tasksForTopic(topicNameInTask).size();

        admin.alterPartitionReassignments(Map.of(
            topicPartition,
            Optional.of(new NewPartitionReassignment(List.of(1)))
        )).all().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        waitForLeader(currentTopic, 0, 1);
        waitForReplicas(currentTopic, 0, List.of(1));

        Awaitility.await().pollDelay(Duration.ofSeconds(8)).atMost(Duration.ofSeconds(15))
            .untilAsserted(() -> {
                var tasksInTopic = tasksForTopic(topicNameInTask);
                assertEquals(taskCountBeforeMove, tasksInTopic.size());
                assertTaskCoverage(tasksInTopic, topicNameInTask, 0, NUM_RECORDS, Optional.of("0"));
            });

        produceRecords(currentTopic, 0, NUM_RECORDS, NUM_RECORDS);

        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                var tasksInTopic = tasksForTopic(topicNameInTask);
                assertTrue(tasksInTopic.size() > taskCountBeforeMove);
                assertLeaderMoveTaskCoverage(tasksInTopic, topicNameInTask);
            });
        var taskCountAfterMove = tasksForTopic(topicNameInTask).size();

        Awaitility.await().pollDelay(Duration.ofSeconds(8)).atMost(Duration.ofSeconds(15))
            .untilAsserted(() -> {
                var tasksInTopic = tasksForTopic(topicNameInTask);
                assertEquals(taskCountAfterMove, tasksInTopic.size());
                assertLeaderMoveTaskCoverage(tasksInTopic, topicNameInTask);
            });
    }

    @Test
    @DisplayName("Should publish compaction tasks")
    void testPublishTasksWithMultiplePartitions() throws Exception {
        var currentTopic = "test-topic-" + RandomStringUtils.secure().nextAlphabetic(4);
        var partitions = 5;
        var topicId = createTopicWithDefaults(currentTopic, partitions);
        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < partitions; j++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                        currentTopic,
                        j,
                        ("key-" + i).getBytes(StandardCharsets.UTF_8),
                        ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                producer.flush();
            }
        }

        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(5))
            .untilAsserted(() -> {
                for (int i = 0; i < partitions; i++) {
                    var topicName = taskTopicName(currentTopic, topicId, i);
                    var tasksInTopic = tasksForTopic(topicName);
                    assertTaskCoverage(tasksInTopic, topicName, 0, 10, Optional.empty());
                }
            });

    }

    @Test
    @DisplayName("Should publish multiple compaction tasks when producing data in batches")
    void testPublishMultipleCompactionTasks() throws Exception {
        var currentTopic = "test-topic-" + RandomStringUtils.secure().nextAlphabetic(4);
        var partitions = 1;
        var topicId = createTopicWithDefaults(currentTopic, partitions);

        // Produce first batch of records
        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = 0; i < NUM_RECORDS; i++) {
                for (int j = 0; j < partitions; j++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                        currentTopic,
                        j,
                        ("key-" + i).getBytes(StandardCharsets.UTF_8),
                        ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
            }
            producer.flush();
        }

        // Wait for first batch of tasks
        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(3))
            .untilAsserted(() -> {
                var topicName = taskTopicName(currentTopic, topicId, 0);
                var tasksInTopic = tasksForTopic(topicName);
                assertTaskCoverage(tasksInTopic, topicName, 0, NUM_RECORDS, Optional.empty());
            });
        var firstBatchTaskCount = tasksForTopic(taskTopicName(currentTopic, topicId, 0)).size();

        // Produce second batch of records
        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = NUM_RECORDS; i < NUM_RECORDS * 2; i++) {
                for (int j = 0; j < partitions; j++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                        currentTopic,
                        j,
                        ("key-" + i).getBytes(StandardCharsets.UTF_8),
                        ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
            }
            producer.flush();
        }

        // Wait for updated tasks with new offsets
        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(3))
            .untilAsserted(() -> {
                var topicName = taskTopicName(currentTopic, topicId, 0);
                var tasksInTopic = tasksForTopic(topicName);
                assertTrue(tasksInTopic.size() > firstBatchTaskCount);
                assertTaskCoverage(tasksInTopic, topicName, 0, NUM_RECORDS * 2L, Optional.empty());
            });
    }


    private Producer<byte[], byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private void produceRecords(String topicName, int partition, int startIndex, int numRecords) throws Exception {
        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = startIndex; i < startIndex + numRecords; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                    topicName,
                    partition,
                    ("key-" + i).getBytes(StandardCharsets.UTF_8),
                    ("value-" + i).getBytes(StandardCharsets.UTF_8));
                producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            producer.flush();
        }
    }


    private Uuid createTopicWithDefaults(String topicName, int partitions) throws Exception {
        NewTopic newTopic = new NewTopic(topicName, partitions, (short) 1);
        var result = admin.createTopics(Collections.singleton(newTopic));
        result.all().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return result.topicId(topicName).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private Uuid createTopicWithAssignment(String topicName, Map<Integer, List<Integer>> replicasAssignments)
            throws Exception {
        NewTopic newTopic = new NewTopic(topicName, replicasAssignments);
        var result = admin.createTopics(Collections.singleton(newTopic));
        result.all().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return result.topicId(topicName).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private void waitForLeader(String topicName, int partition, int expectedLeader) {
        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(1))
            .untilAsserted(() -> assertEquals(expectedLeader, leader(topicName, partition)));
    }

    private int leader(String topicName, int partition) throws Exception {
        var topicDescription = admin.describeTopics(Collections.singleton(topicName))
            .allTopicNames().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS).get(topicName);
        Node leader = topicDescription.partitions().get(partition).leader();
        assertNotNull(leader);
        return leader.id();
    }

    private void waitForReplicas(String topicName, int partition, List<Integer> expectedReplicas) {
        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(1))
            .untilAsserted(() -> assertEquals(expectedReplicas, replicas(topicName, partition)));
    }

    private List<Integer> replicas(String topicName, int partition) throws Exception {
        var topicDescription = admin.describeTopics(Collections.singleton(topicName))
            .allTopicNames().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS).get(topicName);
        return topicDescription.partitions().get(partition).replicas().stream()
            .map(Node::id)
            .toList();
    }

    private static String taskTopicName(String topicName, Uuid topicId, int partition) {
        return KafkaLogNaming.logName(new TopicIdPartition(
            topicId, new TopicPartition(topicName, partition)));
    }

    private List<CompactStreamTask> tasksForTopic(String topicName)
            throws Exception {
        var tasksInTopic = new ArrayList<CompactStreamTask>();
        var packagedTasks = taskManager.getAllTasks().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        for (var packagedTask : packagedTasks) {
            for (var subTask : packagedTask.getSubTasks()) {
                var task = taskManager.getCompactStreamTask(subTask)
                    .get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (task != null && topicName.equals(task.getTopic())) {
                    tasksInTopic.add(task);
                }
            }
        }
        Collections.sort(tasksInTopic);
        return tasksInTopic;
    }

    private static void assertLeaderMoveTaskCoverage(
            List<CompactStreamTask> tasks,
            String topicName) {
        var tasksBeforeMove = tasks.stream()
            .filter(task -> task.getEndOffset() <= NUM_RECORDS)
            .toList();
        var tasksAfterMove = tasks.stream()
            .filter(task -> task.getStartOffset() >= NUM_RECORDS)
            .toList();
        assertEquals(tasks.size(), tasksBeforeMove.size() + tasksAfterMove.size(),
            "A compaction task crossed the leader-move offset boundary: " + Arrays.toString(tasks.toArray()));
        assertTaskCoverage(tasksBeforeMove, topicName, 0, NUM_RECORDS, Optional.of("0"));
        assertTaskCoverage(tasksAfterMove, topicName, NUM_RECORDS, NUM_RECORDS * 2L, Optional.of("1"));
    }

    private static void assertTaskCoverage(
            List<CompactStreamTask> tasks,
            String topicName,
            long expectedStartOffset,
            long expectedEndOffset,
            Optional<String> expectedBrokerId) {
        assertFalse(tasks.isEmpty(), "Compaction tasks are not found for " + topicName);
        long nextOffset = expectedStartOffset;
        for (var task : tasks) {
            assertEquals(topicName, task.getTopic());
            assertEquals(nextOffset, task.getStartOffset(),
                "Compaction tasks must be contiguous and non-overlapping: " + Arrays.toString(tasks.toArray()));
            assertTrue(task.getEndOffset() > task.getStartOffset(),
                "Compaction task must contain a non-empty offset range: " + task);
            assertTrue(task.getEndOffset() <= expectedEndOffset,
                "Compaction task exceeds the expected offset range: " + task);
            assertEquals("KAFKA", task.getProperties().get("entryFormat"));
            expectedBrokerId.ifPresent(brokerId -> assertEquals(
                brokerId,
                task.getProperties().get(UrsaSDTInterceptor.PUBLISHER_BROKER_ID_PROPERTY)));
            nextOffset = task.getEndOffset();
        }
        assertEquals(expectedEndOffset, nextOffset,
            "Compaction tasks do not cover the expected offset range: " + Arrays.toString(tasks.toArray()));
    }
}
