<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# convert the topology-simple.yml into the enhanced format. Add some components and config as required and provide their implementation

I'll convert the simple topology into an enhanced Flux-style format with components, configuration, and provide all the implementations.

## Enhanced topology.yml (Flux-Style)

```yaml
# Enhanced Topology Definition
name: "enhanced-random-data-topology"

# Components - Reusable beans with dependency injection
components:
  # Configuration object for number generator
  - id: "numberGeneratorConfig"
    className: "com.trading.app.random.config.NumberGeneratorConfig"
    properties:
      - name: "minValue"
        value: 1
      - name: "maxValue"
        value: 1000
      - name: "emitFrequencyMs"
        value: 100
      - name: "enableMetrics"
        value: true
  
  # Metrics collector component
  - id: "metricsCollector"
    className: "com.trading.app.random.metrics.SimpleMetricsCollector"
    configMethods:
      - name: "configure"
        args:
          - {
              "retention.period.seconds": 300,
              "publish.interval.seconds": 10
            }
  
  # Processing strategy for transformations
  - id: "processingStrategy"
    className: "com.trading.app.random.strategy.MultiplyStrategy"
    constructorArgs:
      - 2  # multiplier
    properties:
      - name: "enableLogging"
        value: true
  
  # Aggregation window configuration
  - id: "aggregationConfig"
    className: "com.trading.app.random.config.AggregationConfig"
    properties:
      - name: "windowSizeSeconds"
        value: 10
      - name: "slideIntervalSeconds"
        value: 5
      - name: "aggregationType"
        value: "AVG"

# Topology-level configuration
config:
  topology.workers: 2
  topology.max.spout.pending: 1000
  topology.message.timeout.secs: 30
  topology.executor.receive.buffer.size: 1024
  topology.executor.send.buffer.size: 1024
  topology.transfer.buffer.size: 32

# Spout definitions
spouts:
  - id: "random-number-generator"
    className: "com.trading.app.random.spouts.ConfigurableRandomNumberSpout"
    constructorArgs:
      - ref: "numberGeneratorConfig"
      - ref: "metricsCollector"
    parallelism: 2
    outputFields:
      - "number"
      - "timestamp"

  - id: "random-string-generator"
    className: "com.trading.app.random.spouts.RandomStringGeneratorSpout"
    parallelism: 1
    properties:
      - name: "stringLength"
        value: 10
      - name: "emitFrequencyMs"
        value: 200
    outputFields:
      - "text"
      - "timestamp"

# Bolt definitions
bolts:
  - id: "number-processor"
    className: "com.trading.app.random.bolts.StrategyBasedProcessorBolt"
    constructorArgs:
      - ref: "processingStrategy"
    parallelism: 4
    outputFields:
      - "original"
      - "processed"
      - "timestamp"

  - id: "number-aggregator"
    className: "com.trading.app.random.bolts.AggregatorBolt"
    constructorArgs:
      - ref: "aggregationConfig"
    parallelism: 2
    outputFields:
      - "aggregateValue"
      - "count"
      - "windowEnd"

  - id: "enrichment-bolt"
    className: "com.trading.app.random.bolts.EnrichmentBolt"
    parallelism: 2
    properties:
      - name: "enrichmentField"
        value: "metadata"
    outputFields:
      - "original"
      - "processed"
      - "metadata"
      - "timestamp"

  - id: "logger-bolt"
    className: "com.trading.app.random.bolts.FormattedLoggerBolt"
    constructorArgs:
      - "DATA_PROCESSING"
      - true  # pretty print
    parallelism: 1
    properties:
      - name: "metricsCollector"
        ref: "metricsCollector"
    outputFields: []

  - id: "metrics-reporter"
    className: "com.trading.app.random.bolts.MetricsReporterBolt"
    constructorArgs:
      - ref: "metricsCollector"
    parallelism: 1
    outputFields: []

# Stream definitions with grouping strategies
streams:
  - name: "numbers --> processor"
    from: "random-number-generator"
    to: "number-processor"
    grouping:
      type: SHUFFLE

  - name: "processor --> enrichment"
    from: "number-processor"
    to: "enrichment-bolt"
    grouping:
      type: SHUFFLE

  - name: "enrichment --> aggregator"
    from: "enrichment-bolt"
    to: "number-aggregator"
    grouping:
      type: FIELDS
      args: ["original"]

  - name: "enrichment --> logger"
    from: "enrichment-bolt"
    to: "logger-bolt"
    grouping:
      type: SHUFFLE

  - name: "aggregator --> metrics"
    from: "number-aggregator"
    to: "metrics-reporter"
    grouping:
      type: ALL

  - name: "strings --> logger"
    from: "random-string-generator"
    to: "logger-bolt"
    grouping:
      type: SHUFFLE
```


## Component Implementations

### NumberGeneratorConfig.java

```java
package com.trading.app.random.config;

/**
 * Configuration for random number generation.
 */
public class NumberGeneratorConfig {
    private int minValue = 1;
    private int maxValue = 100;
    private long emitFrequencyMs = 100;
    private boolean enableMetrics = false;
    
    public int getMinValue() {
        return minValue;
    }
    
    public void setMinValue(int minValue) {
        this.minValue = minValue;
    }
    
    public int getMaxValue() {
        return maxValue;
    }
    
    public void setMaxValue(int maxValue) {
        this.maxValue = maxValue;
    }
    
    public long getEmitFrequencyMs() {
        return emitFrequencyMs;
    }
    
    public void setEmitFrequencyMs(long emitFrequencyMs) {
        this.emitFrequencyMs = emitFrequencyMs;
    }
    
    public boolean isEnableMetrics() {
        return enableMetrics;
    }
    
    public void setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
    }
    
    @Override
    public String toString() {
        return String.format("NumberGeneratorConfig{min=%d, max=%d, frequency=%dms, metrics=%s}",
                           minValue, maxValue, emitFrequencyMs, enableMetrics);
    }
}
```


### SimpleMetricsCollector.java

```java
package com.trading.app.random.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple metrics collector for tracking component performance.
 */
public class SimpleMetricsCollector {
    private static final Logger logger = LoggerFactory.getLogger(SimpleMetricsCollector.class);
    
    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> timers = new ConcurrentHashMap<>();
    private final Map<String, Object> configuration = new ConcurrentHashMap<>();
    
    public void configure(Map<String, Object> config) {
        this.configuration.putAll(config);
        logger.info("Metrics collector configured: {}", config);
    }
    
    public void incrementCounter(String name) {
        counters.computeIfAbsent(name, k -> new AtomicLong(0)).incrementAndGet();
    }
    
    public void incrementCounter(String name, long delta) {
        counters.computeIfAbsent(name, k -> new AtomicLong(0)).addAndGet(delta);
    }
    
    public void recordTime(String name, long milliseconds) {
        timers.computeIfAbsent(name, k -> new AtomicLong(0)).addAndGet(milliseconds);
    }
    
    public long getCounter(String name) {
        AtomicLong counter = counters.get(name);
        return counter != null ? counter.get() : 0;
    }
    
    public long getTimer(String name) {
        AtomicLong timer = timers.get(name);
        return timer != null ? timer.get() : 0;
    }
    
    public Map<String, Long> getAllCounters() {
        Map<String, Long> snapshot = new ConcurrentHashMap<>();
        counters.forEach((key, value) -> snapshot.put(key, value.get()));
        return snapshot;
    }
    
    public Map<String, Long> getAllTimers() {
        Map<String, Long> snapshot = new ConcurrentHashMap<>();
        timers.forEach((key, value) -> snapshot.put(key, value.get()));
        return snapshot;
    }
    
    public void reset() {
        counters.clear();
        timers.clear();
    }
    
    public void printStats() {
        logger.info("=== Metrics Statistics ===");
        counters.forEach((name, value) -> 
            logger.info("Counter [{}]: {}", name, value.get()));
        timers.forEach((name, value) -> 
            logger.info("Timer [{}]: {}ms", name, value.get()));
    }
}
```


### MultiplyStrategy.java

```java
package com.trading.app.random.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processing strategy that multiplies values.
 */
public class MultiplyStrategy implements ProcessingStrategy {
    private static final Logger logger = LoggerFactory.getLogger(MultiplyStrategy.class);
    
    private final int multiplier;
    private boolean enableLogging = false;
    
    public MultiplyStrategy(int multiplier) {
        this.multiplier = multiplier;
    }
    
    public void setEnableLogging(boolean enableLogging) {
        this.enableLogging = enableLogging;
    }
    
    @Override
    public Integer process(Integer value) {
        Integer result = value * multiplier;
        if (enableLogging) {
            logger.debug("Processing: {} * {} = {}", value, multiplier, result);
        }
        return result;
    }
    
    @Override
    public String getName() {
        return "MultiplyStrategy(x" + multiplier + ")";
    }
}
```


### ProcessingStrategy.java (Interface)

```java
package com.trading.app.random.strategy;

/**
 * Strategy interface for value processing.
 */
public interface ProcessingStrategy {
    Integer process(Integer value);
    String getName();
}
```


### AggregationConfig.java

```java
package com.trading.app.random.config;

/**
 * Configuration for aggregation windows.
 */
public class AggregationConfig {
    private int windowSizeSeconds = 10;
    private int slideIntervalSeconds = 5;
    private String aggregationType = "AVG"; // AVG, SUM, MIN, MAX, COUNT
    
    public int getWindowSizeSeconds() {
        return windowSizeSeconds;
    }
    
    public void setWindowSizeSeconds(int windowSizeSeconds) {
        this.windowSizeSeconds = windowSizeSeconds;
    }
    
    public int getSlideIntervalSeconds() {
        return slideIntervalSeconds;
    }
    
    public void setSlideIntervalSeconds(int slideIntervalSeconds) {
        this.slideIntervalSeconds = slideIntervalSeconds;
    }
    
    public String getAggregationType() {
        return aggregationType;
    }
    
    public void setAggregationType(String aggregationType) {
        this.aggregationType = aggregationType;
    }
    
    @Override
    public String toString() {
        return String.format("AggregationConfig{window=%ds, slide=%ds, type=%s}",
                           windowSizeSeconds, slideIntervalSeconds, aggregationType);
    }
}
```


## Spout Implementations

### ConfigurableRandomNumberSpout.java

```java
package com.trading.app.random.spouts;

import com.trading.streaming.api.*;
import com.trading.app.random.config.NumberGeneratorConfig;
import com.trading.app.random.metrics.SimpleMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Configurable random number generator spout with metrics.
 */
public class ConfigurableRandomNumberSpout implements IRichSpout {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurableRandomNumberSpout.class);
    
    private final NumberGeneratorConfig config;
    private final SimpleMetricsCollector metricsCollector;
    private SpoutOutputCollector collector;
    private Random random;
    private AtomicLong messageIdCounter;
    private boolean active;
    private long lastEmitTime;
    
    public ConfigurableRandomNumberSpout(NumberGeneratorConfig config, 
                                        SimpleMetricsCollector metricsCollector) {
        this.config = config;
        this.metricsCollector = metricsCollector;
    }
    
    @Override
    public void open(Map<String, Object> conf, TopologyContext context, 
                     SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
        this.messageIdCounter = new AtomicLong(0);
        this.active = false;
        this.lastEmitTime = System.currentTimeMillis();
        
        logger.info("ConfigurableRandomNumberSpout opened with config: {}", config);
    }
    
    @Override
    public void nextTuple() {
        if (!active) return;
        
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastEmitTime < config.getEmitFrequencyMs()) {
            return; // Rate limiting
        }
        
        int randomNumber = random.nextInt(config.getMaxValue() - config.getMinValue() + 1) 
                          + config.getMinValue();
        long timestamp = System.currentTimeMillis();
        long messageId = messageIdCounter.incrementAndGet();
        
        collector.emit(Arrays.asList(randomNumber, timestamp), messageId);
        
        if (config.isEnableMetrics()) {
            metricsCollector.incrementCounter("spout.emitted");
        }
        
        lastEmitTime = currentTime;
        
        logger.debug("Emitted: number={}, timestamp={}, msgId={}", 
                    randomNumber, timestamp, messageId);
    }
    
    @Override
    public void ack(Object msgId) {
        if (config.isEnableMetrics()) {
            metricsCollector.incrementCounter("spout.acked");
        }
        logger.debug("Ack: {}", msgId);
    }
    
    @Override
    public void fail(Object msgId) {
        if (config.isEnableMetrics()) {
            metricsCollector.incrementCounter("spout.failed");
        }
        logger.warn("Fail: {}", msgId);
    }
    
    @Override
    public void activate() {
        active = true;
        logger.info("Spout activated");
    }
    
    @Override
    public void deactivate() {
        active = false;
        logger.info("Spout deactivated");
    }
    
    @Override
    public void close() {
        if (config.isEnableMetrics()) {
            metricsCollector.printStats();
        }
        logger.info("Spout closed");
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number", "timestamp"));
    }
}
```


### RandomStringGeneratorSpout.java

```java
package com.trading.app.random.spouts;

import com.trading.streaming.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates random strings.
 */
public class RandomStringGeneratorSpout implements IRichSpout {
    private static final Logger logger = LoggerFactory.getLogger(RandomStringGeneratorSpout.class);
    private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    
    private SpoutOutputCollector collector;
    private Random random;
    private AtomicLong messageIdCounter;
    private boolean active;
    private int stringLength = 10;
    private long emitFrequencyMs = 100;
    private long lastEmitTime;
    
    @Override
    public void open(Map<String, Object> conf, TopologyContext context, 
                     SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
        this.messageIdCounter = new AtomicLong(0);
        this.active = false;
        this.lastEmitTime = System.currentTimeMillis();
        
        logger.info("RandomStringGeneratorSpout opened");
    }
    
    public void setStringLength(int stringLength) {
        this.stringLength = stringLength;
    }
    
    public void setEmitFrequencyMs(long emitFrequencyMs) {
        this.emitFrequencyMs = emitFrequencyMs;
    }
    
    @Override
    public void nextTuple() {
        if (!active) return;
        
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastEmitTime < emitFrequencyMs) {
            return;
        }
        
        String randomString = generateRandomString();
        long timestamp = System.currentTimeMillis();
        long messageId = messageIdCounter.incrementAndGet();
        
        collector.emit(Arrays.asList(randomString, timestamp), messageId);
        
        lastEmitTime = currentTime;
        logger.debug("Emitted string: {}", randomString);
    }
    
    private String generateRandomString() {
        StringBuilder sb = new StringBuilder(stringLength);
        for (int i = 0; i < stringLength; i++) {
            sb.append(CHARS.charAt(random.nextInt(CHARS.length())));
        }
        return sb.toString();
    }
    
    @Override
    public void ack(Object msgId) {
        logger.debug("Ack: {}", msgId);
    }
    
    @Override
    public void fail(Object msgId) {
        logger.warn("Fail: {}", msgId);
    }
    
    @Override
    public void activate() {
        active = true;
        logger.info("Spout activated");
    }
    
    @Override
    public void deactivate() {
        active = false;
        logger.info("Spout deactivated");
    }
    
    @Override
    public void close() {
        logger.info("Spout closed");
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text", "timestamp"));
    }
}
```


## Bolt Implementations

### StrategyBasedProcessorBolt.java

```java
package com.trading.app.random.bolts;

import com.trading.streaming.api.*;
import com.trading.app.random.strategy.ProcessingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

/**
 * Processes numbers using a pluggable strategy.
 */
public class StrategyBasedProcessorBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(StrategyBasedProcessorBolt.class);
    
    private final ProcessingStrategy strategy;
    private OutputCollector collector;
    private long processedCount = 0;
    
    public StrategyBasedProcessorBolt(ProcessingStrategy strategy) {
        this.strategy = strategy;
    }
    
    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, 
                       OutputCollector collector) {
        this.collector = collector;
        logger.info("StrategyBasedProcessorBolt prepared with strategy: {}", 
                   strategy.getName());
    }
    
    @Override
    public void execute(Tuple input) {
        try {
            Integer original = input.getInteger(0);
            Long timestamp = input.getLong(1);
            
            Integer processed = strategy.process(original);
            processedCount++;
            
            collector.emit(input, Arrays.asList(original, processed, timestamp));
            collector.ack(input);
            
            if (processedCount % 100 == 0) {
                logger.info("Processed {} tuples with strategy: {}", 
                           processedCount, strategy.getName());
            }
            
        } catch (Exception e) {
            logger.error("Error processing tuple", e);
            collector.fail(input);
        }
    }
    
    @Override
    public void cleanup() {
        logger.info("Bolt cleanup. Total processed: {}", processedCount);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("original", "processed", "timestamp"));
    }
}
```


### EnrichmentBolt.java

```java
package com.trading.app.random.bolts;

import com.trading.streaming.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Enriches tuples with additional metadata.
 */
public class EnrichmentBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(EnrichmentBolt.class);
    
    private OutputCollector collector;
    private String enrichmentField = "metadata";
    private String componentId;
    private long enrichedCount = 0;
    
    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, 
                       OutputCollector collector) {
        this.collector = collector;
        this.componentId = context.getThisComponentId();
        logger.info("EnrichmentBolt prepared: componentId={}", componentId);
    }
    
    public void setEnrichmentField(String enrichmentField) {
        this.enrichmentField = enrichmentField;
    }
    
    @Override
    public void execute(Tuple input) {
        try {
            Integer original = input.getInteger(0);
            Integer processed = input.getInteger(1);
            Long timestamp = input.getLong(2);
            
            // Create metadata
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("enrichedBy", componentId);
            metadata.put("enrichedAt", System.currentTimeMillis());
            metadata.put("category", categorize(processed));
            metadata.put("sequenceNumber", ++enrichedCount);
            
            collector.emit(input, Arrays.asList(original, processed, metadata, timestamp));
            collector.ack(input);
            
        } catch (Exception e) {
            logger.error("Error enriching tuple", e);
            collector.fail(input);
        }
    }
    
    private String categorize(Integer value) {
        if (value < 100) return "LOW";
        if (value < 500) return "MEDIUM";
        if (value < 1000) return "HIGH";
        return "VERY_HIGH";
    }
    
    @Override
    public void cleanup() {
        logger.info("Bolt cleanup. Total enriched: {}", enrichedCount);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("original", "processed", "metadata", "timestamp"));
    }
}
```


### AggregatorBolt.java

```java
package com.trading.app.random.bolts;

import com.trading.streaming.api.*;
import com.trading.app.random.config.AggregationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Aggregates values over time windows.
 */
public class AggregatorBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(AggregatorBolt.class);
    
    private final AggregationConfig config;
    private OutputCollector collector;
    private Queue<TimestampedValue> window;
    private long lastWindowEnd;
    private long totalSum;
    private int totalCount;
    
    public AggregatorBolt(AggregationConfig config) {
        this.config = config;
    }
    
    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, 
                       OutputCollector collector) {
        this.collector = collector;
        this.window = new ConcurrentLinkedQueue<>();
        this.lastWindowEnd = System.currentTimeMillis();
        this.totalSum = 0;
        this.totalCount = 0;
        
        logger.info("AggregatorBolt prepared with config: {}", config);
        
        // Start window timer
        startWindowTimer();
    }
    
    @Override
    public void execute(Tuple input) {
        try {
            Integer original = input.getInteger(0);
            Integer processed = input.getInteger(1);
            Long timestamp = input.getLong(3);
            
            // Add to window
            window.add(new TimestampedValue(processed, timestamp));
            totalSum += processed;
            totalCount++;
            
            collector.ack(input);
            
        } catch (Exception e) {
            logger.error("Error aggregating tuple", e);
            collector.fail(input);
        }
    }
    
    private void startWindowTimer() {
        Thread timerThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(config.getSlideIntervalSeconds() * 1000L);
                    emitWindowAggregate();
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        timerThread.setDaemon(true);
        timerThread.start();
    }
    
    private void emitWindowAggregate() {
        if (window.isEmpty()) {
            return;
        }
        
        long currentTime = System.currentTimeMillis();
        long windowStartTime = currentTime - (config.getWindowSizeSeconds() * 1000L);
        
        // Remove old entries
        window.removeIf(tv -> tv.timestamp < windowStartTime);
        
        if (!window.isEmpty()) {
            double aggregateValue = calculateAggregate();
            
            collector.emit(Arrays.asList(aggregateValue, window.size(), currentTime));
            
            logger.info("Window aggregate [{}]: value={}, count={}", 
                       config.getAggregationType(), aggregateValue, window.size());
        }
        
        lastWindowEnd = currentTime;
    }
    
    private double calculateAggregate() {
        switch (config.getAggregationType().toUpperCase()) {
            case "SUM":
                return window.stream().mapToInt(tv -> tv.value).sum();
            case "AVG":
                return window.stream().mapToInt(tv -> tv.value).average().orElse(0.0);
            case "MIN":
                return window.stream().mapToInt(tv -> tv.value).min().orElse(0);
            case "MAX":
                return window.stream().mapToInt(tv -> tv.value).max().orElse(0);
            case "COUNT":
                return window.size();
            default:
                return 0.0;
        }
    }
    
    @Override
    public void cleanup() {
        logger.info("Bolt cleanup. Total processed: {}", totalCount);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("aggregateValue", "count", "windowEnd"));
    }
    
    private static class TimestampedValue {
        final int value;
        final long timestamp;
        
        TimestampedValue(int value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }
}
```


### FormattedLoggerBolt.java

```java
package com.trading.app.random.bolts;

import com.trading.streaming.api.*;
import com.trading.app.random.metrics.SimpleMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Logs tuples with formatted output.
 */
public class FormattedLoggerBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(FormattedLoggerBolt.class);
    
    private final String logPrefix;
    private final boolean prettyPrint;
    private OutputCollector collector;
    private SimpleMetricsCollector metricsCollector;
    private long loggedCount = 0;
    
    public FormattedLoggerBolt(String logPrefix, boolean prettyPrint) {
        this.logPrefix = logPrefix;
        this.prettyPrint = prettyPrint;
    }
    
    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, 
                       OutputCollector collector) {
        this.collector = collector;
        logger.info("FormattedLoggerBolt prepared: prefix={}, prettyPrint={}", 
                   logPrefix, prettyPrint);
    }
    
    public void setMetricsCollector(SimpleMetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }
    
    @Override
    public void execute(Tuple input) {
        try {
            loggedCount++;
            
            if (prettyPrint) {
                logPretty(input);
            } else {
                logger.info("{} [#{}]: {}", logPrefix, loggedCount, input.getValues());
            }
            
            if (metricsCollector != null) {
                metricsCollector.incrementCounter("logger.logged");
            }
            
            collector.ack(input);
            
        } catch (Exception e) {
            logger.error("Error logging tuple", e);
            collector.fail(input);
        }
    }
    
    private void logPretty(Tuple input) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n========== ").append(logPrefix).append(" [#")
          .append(loggedCount).append("] ==========\n");
        
        sb.append("  Source: ").append(input.getSourceComponent()).append("\n");
        sb.append("  Stream: ").append(input.getSourceStreamId()).append("\n");
        sb.append("  Size:   ").append(input.size()).append("\n");
        sb.append("  Values:\n");
        
        for (int i = 0; i < input.size(); i++) {
            Object value = input.getValue(i);
            sb.append(String.format("    [%d] %s: %s\n", 
                                   i, 
                                   value != null ? value.getClass().getSimpleName() : "null", 
                                   value));
        }
        
        sb.append("==========================================");
        logger.info(sb.toString());
    }
    
    @Override
    public void cleanup() {
        logger.info("Bolt cleanup. Total logged: {}", loggedCount);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields
    }
}
```


### MetricsReporterBolt.java

```java
package com.trading.app.random.bolts;

import com.trading.streaming.api.*;
import com.trading.app.random.metrics.SimpleMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Periodically reports metrics from the metrics collector.
 */
public class MetricsReporterBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(MetricsReporterBolt.class);
    
    private final SimpleMetricsCollector metricsCollector;
    private OutputCollector collector;
    private long lastReportTime;
    private long reportIntervalMs = 10000; // 10 seconds
    
    public MetricsReporterBolt(SimpleMetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }
    
    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, 
                       OutputCollector collector) {
        this.collector = collector;
        this.lastReportTime = System.currentTimeMillis();
        
        logger.info("MetricsReporterBolt prepared");
        
        // Start periodic reporting
        startPeriodicReporting();
    }
    
    @Override
    public void execute(Tuple input) {
        try {
            // Just trigger a report check on each tuple
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastReportTime >= reportIntervalMs) {
                reportMetrics();
                lastReportTime = currentTime;
            }
            
            collector.ack(input);
            
        } catch (Exception e) {
            logger.error("Error in metrics reporter", e);
            collector.fail(input);
        }
    }
    
    private void startPeriodicReporting() {
        Thread reporterThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(reportIntervalMs);
                    reportMetrics();
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        reporterThread.setDaemon(true);
        reporterThread.start();
    }
    
    private void reportMetrics() {
        logger.info("\n" + "=".repeat(80));
        logger.info("METRICS REPORT");
        logger.info("=".repeat(80));
        
        Map<String, Long> counters = metricsCollector.getAllCounters();
        if (!counters.isEmpty()) {
            logger.info("Counters:");
            counters.forEach((name, value) -> 
                logger.info("  {} = {}", name, value));
        }
        
        Map<String, Long> timers = metricsCollector.getAllTimers();
        if (!timers.isEmpty()) {
            logger.info("Timers:");
            timers.forEach((name, value) -> 
                logger.info("  {} = {}ms", name, value));
        }
        
        logger.info("=".repeat(80) + "\n");
    }
    
    @Override
    public void cleanup() {
        reportMetrics(); // Final report
        logger.info("Bolt cleanup");
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields
    }
}
```


## Updated logback.xml

```xml
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <appender name="FILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>logs/topology.log</file>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <fileNamePattern>logs/topology-%d{yyyy-MM-dd}.log</fileNamePattern>
            <maxHistory>7</maxHistory>
        </rollingPolicy>
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{50} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <logger name="com.trading.app.random" level="DEBUG"/>
    <logger name="com.trading.streaming" level="INFO"/>
    
    <root level="INFO">
        <appender-ref ref="STDOUT"/>
        <appender-ref ref="FILE"/>
    </root>
</configuration>
```

This enhanced topology demonstrates:

- ✅ **Components section** with dependency injection
- ✅ **Config section** for topology-level settings
- ✅ **Property injection** via setters
- ✅ **Method invocation** for configuration
- ✅ **Component references** using `ref:`
- ✅ **Stream definitions** with grouping strategies
- ✅ **Metrics collection** and reporting
- ✅ **Pluggable processing strategies**
- ✅ **Time-windowed aggregation**
- ✅ **Data enrichment pipeline**
- ✅ **Multiple spout types** working together
<span style="display:none">[^1]</span>

<div align="center">⁂</div>

[^1]: Master-Implementation-Prompt_-Apache-Storm-Compati.md

