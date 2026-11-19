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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.internals.StreamsConfigUtils;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.Task;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The empty directory a never-written store leaves is made by hand. The task is built directly rather than by
// TopologyTestDriver, whose constructor would leave the stores open when the task fails to initialize.
public class SegmentedStoreWithoutSegmentsTest {

    private static final String APPLICATION_ID = "segmented-store-without-segments";
    private static final String STORE_NAME = "never-written";
    private static final TaskId TASK_ID = new TaskId(0, 0);
    private static final Duration RETENTION = Duration.ofDays(2);
    private static final Duration WINDOW_SIZE = Duration.ofDays(1);

    private StreamTask task;

    private enum StoreType {
        WINDOW,
        TIMESTAMPED_WINDOW,
        TIMESTAMPED_WINDOW_WITH_HEADERS,
        TIME_ORDERED_WINDOW,
        SESSION,
        SESSION_WITH_HEADERS,
        TIME_ORDERED_SESSION
    }

    private static StoreBuilder<?> storeBuilder(final StoreType storeType) {
        switch (storeType) {
            case WINDOW:
                return Stores.windowStoreBuilder(
                    Stores.persistentWindowStore(STORE_NAME, RETENTION, WINDOW_SIZE, false),
                    Serdes.String(),
                    Serdes.Long());
            case TIMESTAMPED_WINDOW:
                return Stores.timestampedWindowStoreBuilder(
                    Stores.persistentTimestampedWindowStore(STORE_NAME, RETENTION, WINDOW_SIZE, false),
                    Serdes.String(),
                    Serdes.Long());
            case TIMESTAMPED_WINDOW_WITH_HEADERS:
                return Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStoreWithHeaders(STORE_NAME, RETENTION, WINDOW_SIZE, false),
                    Serdes.String(),
                    Serdes.Long());
            case TIME_ORDERED_WINDOW:
                return Stores.timestampedWindowStoreBuilder(
                    RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create(STORE_NAME, RETENTION, WINDOW_SIZE, false, true),
                    Serdes.String(),
                    Serdes.Long());
            case SESSION:
                return Stores.sessionStoreBuilder(
                    Stores.persistentSessionStore(STORE_NAME, RETENTION),
                    Serdes.String(),
                    Serdes.Long());
            case SESSION_WITH_HEADERS:
                return Stores.sessionStoreWithHeadersBuilder(
                    Stores.persistentSessionStoreWithHeaders(STORE_NAME, RETENTION),
                    Serdes.String(),
                    Serdes.Long());
            case TIME_ORDERED_SESSION:
                return Stores.sessionStoreBuilder(
                    new RocksDbTimeOrderedSessionBytesStoreSupplier(STORE_NAME, RETENTION.toMillis(), true),
                    Serdes.String(),
                    Serdes.Long());
            default:
                throw new IllegalArgumentException("unknown store type: " + storeType);
        }
    }

    private static class NoOpProcessor implements Processor<String, String, Void, Void> {
        @Override
        public void process(final Record<String, String> record) {
        }
    }

    private static ProcessorTopology topology(final StoreType storeType) {
        final TopologyWrapper topology = new TopologyWrapper();
        topology.addSource("source", new StringDeserializer(), new StringDeserializer(), "input");
        topology.addProcessor("processor", NoOpProcessor::new, "source");
        topology.addStateStore(storeBuilder(storeType), "processor");
        return topology.getInternalBuilder(APPLICATION_ID).buildTopology();
    }

    private static StreamTask createTask(final StoreType storeType, final File stateDir) {
        final StreamsConfig config = new StreamsConfig(exactlyOnce(stateDir));
        final ProcessorTopology topology = topology(storeType);
        final Set<TopicPartition> inputPartitions = Set.of(new TopicPartition("input", 0));
        final LogContext logContext = new LogContext();
        final MockTime time = new MockTime();
        final StreamsMetricsImpl metrics = new StreamsMetricsImpl(new Metrics(), "test-client", time);
        final ThreadCache cache = new ThreadCache(logContext, 0, metrics);
        final StateDirectory stateDirectory = new StateDirectory(config, time, true, false);
        final ProcessorStateManager stateManager = new ProcessorStateManager(
            TASK_ID,
            Task.TaskType.ACTIVE,
            StreamsConfigUtils.eosEnabled(config),
            config.getBoolean(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG),
            logContext,
            stateDirectory,
            time,
            topology.storeToChangelogTopic(),
            inputPartitions);

        return new StreamTask(
            TASK_ID,
            inputPartitions,
            topology,
            new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()),
            new TopologyConfig(null, config, new Properties()).getTaskConfig(),
            metrics,
            stateDirectory,
            cache,
            time,
            stateManager,
            new MockRecordCollector(),
            new ProcessorContextImpl(TASK_ID, config, stateManager, metrics, cache),
            logContext,
            false);
    }

    private static Properties exactlyOnce(final File stateDir) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getAbsolutePath());
        return props;
    }

    private static File storeDir(final File stateDir) {
        return new File(stateDir, APPLICATION_ID + File.separator + TASK_ID + File.separator + STORE_NAME);
    }

    @AfterEach
    public void closeTask() {
        if (task != null) {
            task.suspend();
            task.closeDirty();
        }
    }

    @ParameterizedTest
    @EnumSource(StoreType.class)
    public void shouldInitializeTaskWhoseStoreHasNoSegments(final StoreType storeType) {
        final File stateDir = TestUtils.tempDirectory();
        assertTrue(storeDir(stateDir).mkdirs());

        task = createTask(storeType, stateDir);
        assertDoesNotThrow(task::initializeIfNeeded);
    }

    // a segment that holds no offset may hold uncommitted writes
    @ParameterizedTest
    @EnumSource(StoreType.class)
    public void shouldTreatTaskAsCorruptedWhenSegmentHasNoOffset(final StoreType storeType) {
        final File stateDir = TestUtils.tempDirectory();
        assertTrue(new File(storeDir(stateDir), STORE_NAME + ".0").mkdirs());

        task = createTask(storeType, stateDir);
        assertThrows(TaskCorruptedException.class, task::initializeIfNeeded);
    }
}
