<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Provide enhancement to the framework to support the concept of apache storms TICK_TUPLE. Update the impacting classes and tests

Here's a comprehensive enhancement to add **TICK_TUPLE** support to the framework:

## 1. Constants.java (New)

```java
package com.trading.streaming.api;

/**
 * Constants for the streaming framework.
 * Similar to Apache Storm's Constants class.
 */
public class Constants {
    
    // System stream IDs
    public static final String SYSTEM_STREAM_ID = "__system";
    public static final String SYSTEM_TICK_STREAM_ID = "__tick";
    
    // System component IDs
    public static final String SYSTEM_COMPONENT_ID = "__system";
    
    // Configuration keys
    public static final String TOPOLOGY_TICK_TUPLE_FREQ_SECS = "topology.tick.tuple.freq.secs";
    public static final String TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS = "topology.enable.message.timeouts";
    public static final String TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";
    
    // Tick tuple field names
    public static final String TICK_TUPLE_FIELD = "tick";
    
    // Private constructor to prevent instantiation
    private Constants() {
        throw new UnsupportedOperationException("Constants class cannot be instantiated");
    }
    
    /**
     * Check if a tuple is a tick tuple.
     * 
     * @param tuple the tuple to check
     * @return true if the tuple is a tick tuple
     */
    public static boolean isTickTuple(Tuple tuple) {
        return tuple != null && 
               SYSTEM_COMPONENT_ID.equals(tuple.getSourceComponent()) &&
               SYSTEM_TICK_STREAM_ID.equals(tuple.getSourceStreamId());
    }
}
```


## 2. Enhanced Tuple Interface

```java
package com.trading.streaming.api;

import java.util.List;

/**
 * Enhanced Tuple interface with tick tuple support.
 */
public interface Tuple {
    
    // Value access by position
    Object getValue(int i);
    String getString(int i);
    Integer getInteger(int i);
    Long getLong(int i);
    Boolean getBoolean(int i);
    Double getDouble(int i);
    Float getFloat(int i);
    Short getShort(int i);
    Byte getByte(int i);
    
    // Value access by field name
    Object getValueByField(String field);
    String getStringByField(String field);
    Integer getIntegerByField(String field);
    Long getLongByField(String field);
    Boolean getBooleanByField(String field);
    Double getDoubleByField(String field);
    Float getFloatByField(String field);
    Short getShortByField(String field);
    Byte getByteByField(String field);
    
    // Metadata
    int size();
    List<Object> getValues();
    String getSourceComponent();
    String getSourceStreamId();
    Object getMessageId();
    
    /**
     * Check if this tuple is a tick tuple.
     * 
     * @return true if this is a tick tuple
     */
    default boolean isTickTuple() {
        return Constants.isTickTuple(this);
    }
}
```


## 3. Enhanced TupleImpl.java

```java
package com.trading.streaming.impl;

import com.trading.streaming.api.Constants;
import com.trading.streaming.api.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Enhanced tuple implementation with tick tuple support.
 */
public class TupleImpl implements Tuple {
    
    private final String sourceComponent;
    private final String sourceStreamId;
    private final List<Object> values;
    private final List<String> fields;
    private final Object messageId;
    
    public TupleImpl(String sourceComponent, String sourceStreamId, 
                     List<Object> values, List<String> fields, Object messageId) {
        this.sourceComponent = sourceComponent;
        this.sourceStreamId = sourceStreamId;
        this.values = new ArrayList<>(values);
        this.fields = new ArrayList<>(fields);
        this.messageId = messageId;
    }
    
    /**
     * Create a tick tuple.
     */
    public static Tuple createTickTuple() {
        return new TupleImpl(
            Constants.SYSTEM_COMPONENT_ID,
            Constants.SYSTEM_TICK_STREAM_ID,
            Arrays.asList(System.currentTimeMillis()),
            Arrays.asList(Constants.TICK_TUPLE_FIELD),
            null
        );
    }
    
    @Override
    public boolean isTickTuple() {
        return Constants.SYSTEM_COMPONENT_ID.equals(sourceComponent) &&
               Constants.SYSTEM_TICK_STREAM_ID.equals(sourceStreamId);
    }
    
    // ... rest of existing implementation unchanged ...
    
    @Override
    public Object getValue(int i) {
        return values.get(i);
    }
    
    @Override
    public String getString(int i) {
        return (String) getValue(i);
    }
    
    @Override
    public Integer getInteger(int i) {
        Object value = getValue(i);
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString());
    }
    
    @Override
    public Long getLong(int i) {
        Object value = getValue(i);
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString());
    }
    
    @Override
    public Boolean getBoolean(int i) {
        Object value = getValue(i);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }
    
    @Override
    public Double getDouble(int i) {
        Object value = getValue(i);
        if (value instanceof Double) {
            return (Double) value;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(value.toString());
    }
    
    @Override
    public Float getFloat(int i) {
        Object value = getValue(i);
        if (value instanceof Float) {
            return (Float) value;
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        return Float.parseFloat(value.toString());
    }
    
    @Override
    public Short getShort(int i) {
        Object value = getValue(i);
        if (value instanceof Short) {
            return (Short) value;
        }
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }
        return Short.parseShort(value.toString());
    }
    
    @Override
    public Byte getByte(int i) {
        Object value = getValue(i);
        if (value instanceof Byte) {
            return (Byte) value;
        }
        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }
        return Byte.parseByte(value.toString());
    }
    
    @Override
    public Object getValueByField(String field) {
        int index = fields.indexOf(field);
        if (index < 0) {
            throw new IllegalArgumentException("Field not found: " + field);
        }
        return getValue(index);
    }
    
    @Override
    public String getStringByField(String field) {
        return (String) getValueByField(field);
    }
    
    @Override
    public Integer getIntegerByField(String field) {
        int index = fields.indexOf(field);
        return getInteger(index);
    }
    
    @Override
    public Long getLongByField(String field) {
        int index = fields.indexOf(field);
        return getLong(index);
    }
    
    @Override
    public Boolean getBooleanByField(String field) {
        int index = fields.indexOf(field);
        return getBoolean(index);
    }
    
    @Override
    public Double getDoubleByField(String field) {
        int index = fields.indexOf(field);
        return getDouble(index);
    }
    
    @Override
    public Float getFloatByField(String field) {
        int index = fields.indexOf(field);
        return getFloat(index);
    }
    
    @Override
    public Short getShortByField(String field) {
        int index = fields.indexOf(field);
        return getShort(index);
    }
    
    @Override
    public Byte getByteByField(String field) {
        int index = fields.indexOf(field);
        return getByte(index);
    }
    
    @Override
    public int size() {
        return values.size();
    }
    
    @Override
    public List<Object> getValues() {
        return new ArrayList<>(values);
    }
    
    @Override
    public String getSourceComponent() {
        return sourceComponent;
    }
    
    @Override
    public String getSourceStreamId() {
        return sourceStreamId;
    }
    
    @Override
    public Object getMessageId() {
        return messageId;
    }
    
    @Override
    public String toString() {
        return String.format("Tuple{source=%s, stream=%s, values=%s}", 
                           sourceComponent, sourceStreamId, values);
    }
}
```


## 4. Enhanced BoltExecutor.java

```java
package com.trading.streaming.impl;

import com.trading.streaming.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Enhanced bolt executor with tick tuple support.
 */
public class BoltExecutor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BoltExecutor.class);
    
    private final String executorId;
    private final String componentId;
    private final IRichBolt bolt;
    private final LocalStreamingContext context;
    private final OutputCollector collector;
    private final Map<String, Object> conf;
    private final TopologyContext topologyContext;
    private final int queueCapacity;
    
    private final BlockingQueue<Tuple> inputQueue;
    private ScheduledExecutorService tickScheduler;
    private Integer tickFrequencySeconds;
    
    public BoltExecutor(String executorId, String componentId, IRichBolt bolt,
                       LocalStreamingContext context, OutputCollector collector,
                       Map<String, Object> conf, TopologyContext topologyContext,
                       int queueCapacity) {
        this.executorId = executorId;
        this.componentId = componentId;
        this.bolt = bolt;
        this.context = context;
        this.collector = collector;
        this.conf = conf;
        this.topologyContext = topologyContext;
        this.queueCapacity = queueCapacity;
        this.inputQueue = new LinkedBlockingQueue<>(queueCapacity);
        
        // Check for tick tuple configuration
        Object tickFreq = conf.get(Constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        if (tickFreq != null) {
            if (tickFreq instanceof Integer) {
                tickFrequencySeconds = (Integer) tickFreq;
            } else if (tickFreq instanceof String) {
                tickFrequencySeconds = Integer.parseInt((String) tickFreq);
            }
            
            if (tickFrequencySeconds != null && tickFrequencySeconds > 0) {
                startTickTupleScheduler();
                logger.info("Bolt executor {} configured with tick frequency: {} seconds", 
                           executorId, tickFrequencySeconds);
            }
        }
    }
    
    /**
     * Start the tick tuple scheduler.
     */
    private void startTickTupleScheduler() {
        tickScheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "TickScheduler-" + componentId);
            t.setDaemon(true);
            return t;
        });
        
        tickScheduler.scheduleAtFixedRate(
            this::emitTickTuple,
            tickFrequencySeconds,
            tickFrequencySeconds,
            TimeUnit.SECONDS
        );
        
        logger.debug("Started tick tuple scheduler for {} with frequency {} seconds", 
                    componentId, tickFrequencySeconds);
    }
    
    /**
     * Emit a tick tuple to the bolt.
     */
    private void emitTickTuple() {
        try {
            Tuple tickTuple = TupleImpl.createTickTuple();
            boolean added = inputQueue.offer(tickTuple, 100, TimeUnit.MILLISECONDS);
            
            if (!added) {
                logger.warn("Failed to add tick tuple to queue for {}, queue may be full", 
                           componentId);
            } else {
                logger.trace("Emitted tick tuple to {}", componentId);
            }
        } catch (InterruptedException e) {
            logger.debug("Tick tuple emission interrupted for {}", componentId);
            Thread.currentThread().interrupt();
        }
    }
    
    public void enqueue(Tuple tuple) {
        try {
            // Don't enqueue tick tuples from external sources
            if (tuple.isTickTuple()) {
                logger.warn("Received external tick tuple, ignoring");
                return;
            }
            
            inputQueue.put(tuple);
        } catch (InterruptedException e) {
            logger.error("Interrupted while enqueuing tuple", e);
            Thread.currentThread().interrupt();
        }
    }
    
    public int getQueueSize() {
        return inputQueue.size();
    }
    
    @Override
    public void run() {
        try {
            logger.info("Bolt executor {} starting for component {}", executorId, componentId);
            
            // Prepare bolt
            bolt.prepare(conf, topologyContext, collector);
            
            // Process tuples
            while (context.isRunning()) {
                try {
                    Tuple tuple = inputQueue.poll(100, TimeUnit.MILLISECONDS);
                    
                    if (tuple != null) {
                        try {
                            if (tuple.isTickTuple()) {
                                logger.trace("Processing tick tuple in {}", componentId);
                            }
                            
                            bolt.execute(tuple);
                            
                            // Tick tuples are auto-acked (system tuples don't need acking)
                            if (tuple.isTickTuple()) {
                                // Tick tuples are not tracked for acking
                                logger.trace("Tick tuple processed in {}", componentId);
                            }
                            
                        } catch (Exception e) {
                            logger.error("Error executing tuple in bolt {}", componentId, e);
                            if (!tuple.isTickTuple()) {
                                collector.fail(tuple);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    logger.info("Bolt executor {} interrupted", executorId);
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            
        } catch (Exception e) {
            logger.error("Fatal error in bolt executor {}", executorId, e);
        } finally {
            cleanup();
        }
    }
    
    private void cleanup() {
        logger.info("Bolt executor {} cleaning up", executorId);
        
        // Stop tick scheduler
        if (tickScheduler != null && !tickScheduler.isShutdown()) {
            tickScheduler.shutdown();
            try {
                if (!tickScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    tickScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                tickScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        try {
            bolt.cleanup();
        } catch (Exception e) {
            logger.error("Error during bolt cleanup", e);
        }
        
        logger.info("Bolt executor {} stopped", executorId);
    }
}
```


## 5. Example: BufferedAggregatorBolt Using Tick Tuples

```java
package com.trading.app.random.bolts;

import com.trading.streaming.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Example bolt that uses tick tuples for periodic buffer flushing.
 * Aggregates values and emits aggregates when tick tuple is received.
 */
public class BufferedAggregatorBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(BufferedAggregatorBolt.class);
    
    private OutputCollector collector;
    private Map<String, AggregateBuffer> buffers;
    private long totalProcessed = 0;
    private long tickCount = 0;
    
    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, 
                       OutputCollector collector) {
        this.collector = collector;
        this.buffers = new ConcurrentHashMap<>();
        
        logger.info("BufferedAggregatorBolt prepared with tick frequency: {} seconds",
                   conf.get(Constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS));
    }
    
    @Override
    public void execute(Tuple input) {
        // Check if this is a tick tuple
        if (input.isTickTuple()) {
            handleTickTuple(input);
            return;
        }
        
        // Process regular tuple
        try {
            Integer value = input.getInteger(0);
            String key = "default"; // Could be extracted from tuple
            
            AggregateBuffer buffer = buffers.computeIfAbsent(key, 
                k -> new AggregateBuffer());
            buffer.add(value);
            
            totalProcessed++;
            collector.ack(input);
            
            logger.trace("Added value {} to buffer {}, buffer size: {}", 
                        value, key, buffer.size());
            
        } catch (Exception e) {
            logger.error("Error processing tuple", e);
            collector.fail(input);
        }
    }
    
    /**
     * Handle tick tuple by flushing all buffers.
     */
    private void handleTickTuple(Tuple input) {
        tickCount++;
        
        logger.info("Received tick tuple #{}, flushing {} buffers", 
                   tickCount, buffers.size());
        
        long startTime = System.currentTimeMillis();
        int emittedCount = 0;
        
        // Flush all buffers
        for (Map.Entry<String, AggregateBuffer> entry : buffers.entrySet()) {
            String key = entry.getKey();
            AggregateBuffer buffer = entry.getValue();
            
            if (!buffer.isEmpty()) {
                Map<String, Object> aggregate = buffer.getAggregate();
                
                collector.emit(Arrays.asList(
                    key,
                    aggregate.get("count"),
                    aggregate.get("sum"),
                    aggregate.get("avg"),
                    aggregate.get("min"),
                    aggregate.get("max"),
                    System.currentTimeMillis()
                ));
                
                emittedCount++;
                buffer.clear();
                
                logger.debug("Flushed buffer {}: {}", key, aggregate);
            }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        
        logger.info("Tick #{}: Flushed {} buffers, emitted {} aggregates in {} ms, " +
                   "total processed: {}", 
                   tickCount, buffers.size(), emittedCount, duration, totalProcessed);
        
        // Note: Tick tuples are NOT acked/failed
    }
    
    @Override
    public void cleanup() {
        logger.info("BufferedAggregatorBolt cleanup - Total ticks: {}, Total processed: {}", 
                   tickCount, totalProcessed);
        
        // Final flush
        logger.info("Performing final flush of {} buffers", buffers.size());
        for (Map.Entry<String, AggregateBuffer> entry : buffers.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                logger.info("Buffer {} had {} unflushed values", 
                           entry.getKey(), entry.getValue().size());
            }
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "count", "sum", "avg", "min", "max", "timestamp"));
    }
    
    /**
     * Buffer for aggregating values.
     */
    private static class AggregateBuffer {
        private final List<Integer> values = new ArrayList<>();
        
        public void add(Integer value) {
            values.add(value);
        }
        
        public int size() {
            return values.size();
        }
        
        public boolean isEmpty() {
            return values.isEmpty();
        }
        
        public Map<String, Object> getAggregate() {
            Map<String, Object> result = new HashMap<>();
            
            if (values.isEmpty()) {
                result.put("count", 0);
                result.put("sum", 0L);
                result.put("avg", 0.0);
                result.put("min", 0);
                result.put("max", 0);
            } else {
                IntSummaryStatistics stats = values.stream()
                    .mapToInt(Integer::intValue)
                    .summaryStatistics();
                
                result.put("count", stats.getCount());
                result.put("sum", stats.getSum());
                result.put("avg", stats.getAverage());
                result.put("min", stats.getMin());
                result.put("max", stats.getMax());
            }
            
            return result;
        }
        
        public void clear() {
            values.clear();
        }
    }
}
```


## 6. Topology Configuration with Tick Tuples

### topology.yml

```yaml
name: "tick-tuple-example"

components:
  - id: "metricsCollector"
    className: "com.trading.app.random.metrics.SimpleMetricsCollector"

# Global tick configuration (can be overridden per bolt)
config:
  topology.tick.tuple.freq.secs: 10

spouts:
  - id: "number-generator"
    className: "com.trading.app.random.spouts.RandomNumberSpout"
    parallelism: 2
    outputFields:
      - "number"
      - "timestamp"

bolts:
  - id: "buffered-aggregator"
    className: "com.trading.app.random.bolts.BufferedAggregatorBolt"
    parallelism: 2
    # Override tick frequency for this bolt
    properties:
      - name: "tickFrequencySeconds"
        value: 5
    outputFields:
      - "key"
      - "count"
      - "sum"
      - "avg"
      - "min"
      - "max"
      - "timestamp"
  
  - id: "logger"
    className: "com.trading.app.random.bolts.LoggerBolt"
    parallelism: 1
    outputFields: []

streams:
  - name: "generator --> aggregator"
    from: "number-generator"
    to: "buffered-aggregator"
    grouping:
      type: SHUFFLE
  
  - name: "aggregator --> logger"
    from: "buffered-aggregator"
    to: "logger"
    grouping:
      type: SHUFFLE
```


### topology.properties

```properties
# Global tick tuple frequency (seconds)
topology.tick.tuple.freq.secs=10

# Enable/disable message timeouts
topology.enable.message.timeouts=true
topology.message.timeout.secs=30
```


## 7. Programmatic Tick Tuple Configuration

```java
package com.trading.app.examples;

import com.trading.streaming.api.*;
import com.trading.streaming.impl.LocalStreamingContext;
import com.trading.app.random.bolts.BufferedAggregatorBolt;
import com.trading.app.random.spouts.RandomNumberSpout;

import java.util.HashMap;
import java.util.Map;

public class TickTupleExample {
    
    public static void main(String[] args) {
        LocalStreamingContext context = new LocalStreamingContext();
        
        // Configure tick tuples
        Map<String, Object> config = new HashMap<>();
        config.put(Constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        
        // Register spout
        context.registerSpout(
            "number-generator",
            new RandomNumberSpout(),
            new Fields("number", "timestamp"),
            2
        );
        
        // Register bolt with tick tuple support
        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put("number-generator", Arrays.asList("default"));
        
        context.registerBolt(
            "buffered-aggregator",
            new BufferedAggregatorBolt(),
            new Fields("key", "count", "sum", "avg", "min", "max", "timestamp"),
            2,
            subscriptions,
            config  // Pass tick configuration
        );
        
        // Start topology
        context.start();
        
        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            context.stop();
        }));
    }
}
```


## 8. Tests for Tick Tuple Functionality

### TickTupleTest.java

```java
package com.trading.streaming.impl;

import com.trading.streaming.api.Constants;
import com.trading.streaming.api.Tuple;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class TickTupleTest {
    
    @Test
    @DisplayName("Should create tick tuple correctly")
    void testCreateTickTuple() {
        Tuple tickTuple = TupleImpl.createTickTuple();
        
        assertNotNull(tickTuple);
        assertTrue(tickTuple.isTickTuple());
        assertEquals(Constants.SYSTEM_COMPONENT_ID, tickTuple.getSourceComponent());
        assertEquals(Constants.SYSTEM_TICK_STREAM_ID, tickTuple.getSourceStreamId());
        assertEquals(1, tickTuple.size());
        assertNotNull(tickTuple.getLong(0)); // timestamp
    }
    
    @Test
    @DisplayName("Should identify tick tuples correctly")
    void testIsTickTuple() {
        Tuple tickTuple = TupleImpl.createTickTuple();
        assertTrue(tickTuple.isTickTuple());
        assertTrue(Constants.isTickTuple(tickTuple));
        
        Tuple regularTuple = new TupleImpl(
            "regular-component",
            "default",
            Arrays.asList("value"),
            Arrays.asList("field"),
            123L
        );
        assertFalse(regularTuple.isTickTuple());
        assertFalse(Constants.isTickTuple(regularTuple));
    }
    
    @Test
    @DisplayName("Should not identify regular tuples as tick tuples")
    void testRegularTupleNotTick() {
        Tuple tuple = new TupleImpl(
            "spout",
            "default",
            Arrays.asList(42, "data"),
            Arrays.asList("number", "text"),
            1L
        );
        
        assertFalse(tuple.isTickTuple());
    }
    
    @Test
    @DisplayName("Constants.isTickTuple should handle null")
    void testIsTickTupleHandlesNull() {
        assertFalse(Constants.isTickTuple(null));
    }
}
```


### BufferedAggregatorBoltTest.java

```java
package com.trading.app.random.bolts;

import com.trading.streaming.api.*;
import com.trading.streaming.impl.TupleImpl;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class BufferedAggregatorBoltTest {
    
    private BufferedAggregatorBolt bolt;
    private OutputCollector mockCollector;
    private TopologyContext mockContext;
    private AtomicInteger emitCount;
    
    @BeforeEach
    void setUp() {
        bolt = new BufferedAggregatorBolt();
        mockCollector = mock(OutputCollector.class);
        mockContext = mock(TopologyContext.class);
        when(mockContext.getThisComponentId()).thenReturn("test-aggregator");
        
        emitCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            emitCount.incrementAndGet();
            return null;
        }).when(mockCollector).emit(any());
    }
    
    @Test
    @DisplayName("Should buffer tuples and emit on tick")
    void testBufferAndFlushOnTick() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        
        bolt.prepare(conf, mockContext, mockCollector);
        
        // Send regular tuples
        for (int i = 1; i <= 10; i++) {
            Tuple tuple = createValueTuple(i);
            bolt.execute(tuple);
        }
        
        // Verify no emissions yet
        assertEquals(0, emitCount.get());
        verify(mockCollector, times(10)).ack(any());
        
        // Send tick tuple
        Tuple tickTuple = TupleImpl.createTickTuple();
        bolt.execute(tickTuple);
        
        // Verify aggregates were emitted
        assertTrue(emitCount.get() > 0);
        verify(mockCollector, atLeastOnce()).emit(any());
    }
    
    @Test
    @DisplayName("Should not ack/fail tick tuples")
    void testTickTuplesNotAcked() {
        Map<String, Object> conf = new HashMap<>();
        bolt.prepare(conf, mockContext, mockCollector);
        
        Tuple tickTuple = TupleImpl.createTickTuple();
        bolt.execute(tickTuple);
        
        // Tick tuples should not be acked or failed
        verify(mockCollector, never()).ack(tickTuple);
        verify(mockCollector, never()).fail(tickTuple);
    }
    
    @Test
    @DisplayName("Should handle multiple tick tuples")
    void testMultipleTicks() {
        Map<String, Object> conf = new HashMap<>();
        bolt.prepare(conf, mockContext, mockCollector);
        
        // Add some values
        for (int i = 1; i <= 5; i++) {
            bolt.execute(createValueTuple(i));
        }
        
        // First tick
        bolt.execute(TupleImpl.createTickTuple());
        int firstEmitCount = emitCount.get();
        assertTrue(firstEmitCount > 0);
        
        // Add more values
        for (int i = 6; i <= 10; i++) {
            bolt.execute(createValueTuple(i));
        }
        
        // Second tick
        bolt.execute(TupleImpl.createTickTuple());
        int secondEmitCount = emitCount.get();
        assertTrue(secondEmitCount > firstEmitCount);
    }
    
    @Test
    @DisplayName("Should handle tick when buffer is empty")
    void testTickWithEmptyBuffer() {
        Map<String, Object> conf = new HashMap<>();
        bolt.prepare(conf, mockContext, mockCollector);
        
        // Send tick without any data
        Tuple tickTuple = TupleImpl.createTickTuple();
        bolt.execute(tickTuple);
        
        // Should not emit anything
        assertEquals(0, emitCount.get());
    }
    
    @Test
    @DisplayName("Should clear buffers after tick")
    void testBuffersClearedAfterTick() {
        Map<String, Object> conf = new HashMap<>();
        bolt.prepare(conf, mockContext, mockCollector);
        
        // Add values and tick
        bolt.execute(createValueTuple(100));
        bolt.execute(TupleImpl.createTickTuple());
        
        int firstEmitCount = emitCount.get();
        
        // Add same value again and tick
        bolt.execute(createValueTuple(100));
        bolt.execute(TupleImpl.createTickTuple());
        
        // Should emit again (buffers were cleared)
        assertTrue(emitCount.get() > firstEmitCount);
    }
    
    private Tuple createValueTuple(int value) {
        return new TupleImpl(
            "test-spout",
            "default",
            Arrays.asList(value),
            Arrays.asList("number"),
            (long) value
        );
    }
}
```


### BoltExecutorTickTest.java

```java
package com.trading.streaming.impl;

import com.trading.streaming.api.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class BoltExecutorTickTest {
    
    private LocalStreamingContext mockContext;
    private TickReceivingBolt bolt;
    private BoltExecutor executor;
    
    @BeforeEach
    void setUp() {
        mockContext = mock(LocalStreamingContext.class);
        when(mockContext.isRunning()).thenReturn(true);
        bolt = new TickReceivingBolt();
    }
    
    @AfterEach
    void tearDown() {
        if (mockContext != null) {
            when(mockContext.isRunning()).thenReturn(false);
        }
    }
    
    @Test
    @DisplayName("BoltExecutor should emit tick tuples at configured frequency")
    void testTickTupleEmission() throws InterruptedException {
        OutputCollector collector = new OutputCollector(
            "test-bolt", mockContext, Arrays.asList("result"));
        
        Map<String, Object> conf = new HashMap<>();
        conf.put(Constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1); // 1 second
        
        executor = new BoltExecutor(
            "executor-1",
            "test-bolt",
            bolt,
            mockContext,
            collector,
            conf,
            new TopologyContext("test-topology", null, "test-bolt", 0),
            100
        );
        
        Thread executorThread = new Thread(executor);
        executorThread.start();
        
        // Wait for at least 2 tick tuples
        boolean received = bolt.tickLatch.await(3, TimeUnit.SECONDS);
        assertTrue(received, "Should receive tick tuples");
        assertTrue(bolt.tickCount.get() >= 2, 
                  "Should receive at least 2 tick tuples, got: " + bolt.tickCount.get());
        
        when(mockContext.isRunning()).thenReturn(false);
        executorThread.join(2000);
    }
    
    @Test
    @DisplayName("BoltExecutor should not emit ticks when frequency is 0")
    void testNoTicksWhenFrequencyZero() throws InterruptedException {
        OutputCollector collector = new OutputCollector(
            "test-bolt", mockContext, Arrays.asList("result"));
        
        Map<String, Object> conf = new HashMap<>();
        conf.put(Constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 0);
        
        executor = new BoltExecutor(
            "executor-1",
            "test-bolt",
            bolt,
            mockContext,
            collector,
            conf,
            new TopologyContext("test-topology", null, "test-bolt", 0),
            100
        );
        
        Thread executorThread = new Thread(executor);
        executorThread.start();
        
        Thread.sleep(1500);
        
        assertEquals(0, bolt.tickCount.get(), "Should not receive any tick tuples");
        
        when(mockContext.isRunning()).thenReturn(false);
        executorThread.join(1000);
    }
    
    @Test
    @DisplayName("BoltExecutor should handle both regular and tick tuples")
    void testMixedTuples() throws InterruptedException {
        OutputCollector collector = new OutputCollector(
            "test-bolt", mockContext, Arrays.asList("result"));
        
        Map<String, Object> conf = new HashMap<>();
        conf.put(Constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        
        executor = new BoltExecutor(
            "executor-1",
            "test-bolt",
            bolt,
            mockContext,
            collector,
            conf,
            new TopologyContext("test-topology", null, "test-bolt", 0),
            100
        );
        
        Thread executorThread = new Thread(executor);
        executorThread.start();
        
        // Enqueue regular tuples
        for (int i = 0; i < 5; i++) {
            Tuple tuple = new TupleImpl(
                "test-spout",
                "default",
                Arrays.asList(i),
                Arrays.asList("value"),
                (long) i
            );
            executor.enqueue(tuple);
        }
        
        // Wait for processing
        Thread.sleep(2500);
        
        assertTrue(bolt.regularCount.get() >= 5, 
                  "Should process regular tuples");
        assertTrue(bolt.tickCount.get() >= 2, 
                  "Should receive tick tuples");
        
        when(mockContext.isRunning()).thenReturn(false);
        executorThread.join(1000);
    }
    
    // Helper bolt that counts tick and regular tuples
    private static class TickReceivingBolt implements IRichBolt {
        final AtomicInteger tickCount = new AtomicInteger(0);
        final AtomicInteger regularCount = new AtomicInteger(0);
        final CountDownLatch tickLatch = new CountDownLatch(2);
        private OutputCollector collector;
        
        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, 
                           OutputCollector collector) {
            this.collector = collector;
        }
        
        @Override
        public void execute(Tuple input) {
            if (input.isTickTuple()) {
                tickCount.incrementAndGet();
                tickLatch.countDown();
            } else {
                regularCount.incrementAndGet();
                collector.ack(input);
            }
        }
        
        @Override
        public void cleanup() {}
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("result"));
        }
    }
}
```


## 9. Update LocalStreamingContext to Support Tick Configuration

```java
// Add this method to LocalStreamingContext
public void registerBolt(String id, IRichBolt bolt, Fields outputFields, 
                        int parallelism, Map<String, List<String>> subscriptions,
                        Map<String, Object> boltSpecificConfig) {
    // Merge with global config
    Map<String, Object> mergedConfig = new HashMap<>(this.config);
    if (boltSpecificConfig != null) {
        mergedConfig.putAll(boltSpecificConfig);
    }
    
    // Use mergedConfig when creating BoltExecutor
    // ... rest of implementation
}
```

This comprehensive enhancement adds full TICK_TUPLE support similar to Apache Storm! 🎯
<span style="display:none">[^1]</span>

<div align="center">⁂</div>

[^1]: Master-Implementation-Prompt_-Apache-Storm-Compati.md

