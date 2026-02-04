<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# provide implementation of SimpleMetricsCollector

Here is the complete implementation of **SimpleMetricsCollector.java**:

```java
package com.trading.app.random.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Simple metrics collector for tracking component performance.
 * Thread-safe implementation using concurrent data structures.
 */
public class SimpleMetricsCollector {
    private static final Logger logger = LoggerFactory.getLogger(SimpleMetricsCollector.class);
    
    private final Map<String, LongAdder> counters = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> timers = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();
    private final Map<String, Object> configuration = new ConcurrentHashMap<>();
    
    /**
     * Configure the metrics collector with settings.
     */
    public void configure(Map<String, Object> config) {
        if (config != null) {
            this.configuration.putAll(config);
            logger.info("Metrics collector configured: {}", config);
        }
    }
    
    /**
     * Increment a counter by 1.
     */
    public void incrementCounter(String name) {
        counters.computeIfAbsent(name, k -> new LongAdder()).increment();
    }
    
    /**
     * Increment a counter by a specific delta.
     */
    public void incrementCounter(String name, long delta) {
        counters.computeIfAbsent(name, k -> new LongAdder()).add(delta);
    }
    
    /**
     * Decrement a counter by 1.
     */
    public void decrementCounter(String name) {
        counters.computeIfAbsent(name, k -> new LongAdder()).decrement();
    }
    
    /**
     * Record a time measurement in milliseconds.
     */
    public void recordTime(String name, long milliseconds) {
        timers.computeIfAbsent(name, k -> new LongAdder()).add(milliseconds);
    }
    
    /**
     * Record a time measurement in nanoseconds (converted to microseconds).
     */
    public void recordTimeNanos(String name, long nanoseconds) {
        long microseconds = nanoseconds / 1_000;
        timers.computeIfAbsent(name, k -> new LongAdder()).add(microseconds);
    }
    
    /**
     * Set a gauge value.
     */
    public void setGauge(String name, long value) {
        gauges.computeIfAbsent(name, k -> new AtomicLong(0)).set(value);
    }
    
    /**
     * Increment a gauge value.
     */
    public void incrementGauge(String name) {
        gauges.computeIfAbsent(name, k -> new AtomicLong(0)).incrementAndGet();
    }
    
    /**
     * Decrement a gauge value.
     */
    public void decrementGauge(String name) {
        gauges.computeIfAbsent(name, k -> new AtomicLong(0)).decrementAndGet();
    }
    
    /**
     * Get current counter value.
     */
    public long getCounter(String name) {
        LongAdder counter = counters.get(name);
        return counter != null ? counter.sum() : 0;
    }
    
    /**
     * Get current timer value.
     */
    public long getTimer(String name) {
        LongAdder timer = timers.get(name);
        return timer != null ? timer.sum() : 0;
    }
    
    /**
     * Get current gauge value.
     */
    public long getGauge(String name) {
        AtomicLong gauge = gauges.get(name);
        return gauge != null ? gauge.get() : 0;
    }
    
    /**
     * Get all counters as a snapshot.
     */
    public Map<String, Long> getAllCounters() {
        Map<String, Long> snapshot = new ConcurrentHashMap<>();
        counters.forEach((key, value) -> snapshot.put(key, value.sum()));
        return snapshot;
    }
    
    /**
     * Get all timers as a snapshot.
     */
    public Map<String, Long> getAllTimers() {
        Map<String, Long> snapshot = new ConcurrentHashMap<>();
        timers.forEach((key, value) -> snapshot.put(key, value.sum()));
        return snapshot;
    }
    
    /**
     * Get all gauges as a snapshot.
     */
    public Map<String, Long> getAllGauges() {
        Map<String, Long> snapshot = new ConcurrentHashMap<>();
        gauges.forEach((key, value) -> snapshot.put(key, value.get()));
        return snapshot;
    }
    
    /**
     * Get configuration value.
     */
    public Object getConfigValue(String key) {
        return configuration.get(key);
    }
    
    /**
     * Get configuration value with default.
     */
    public Object getConfigValue(String key, Object defaultValue) {
        return configuration.getOrDefault(key, defaultValue);
    }
    
    /**
     * Check if metrics are enabled.
     */
    public boolean isEnabled() {
        Object enabled = configuration.get("enabled");
        return enabled == null || Boolean.parseBoolean(enabled.toString());
    }
    
    /**
     * Reset all metrics.
     */
    public void reset() {
        counters.clear();
        timers.clear();
        gauges.clear();
        logger.info("All metrics reset");
    }
    
    /**
     * Reset a specific counter.
     */
    public void resetCounter(String name) {
        counters.remove(name);
    }
    
    /**
     * Reset a specific timer.
     */
    public void resetTimer(String name) {
        timers.remove(name);
    }
    
    /**
     * Reset a specific gauge.
     */
    public void resetGauge(String name) {
        gauges.remove(name);
    }
    
    /**
     * Print statistics to log.
     */
    public void printStats() {
        logger.info("\n" + "=".repeat(80));
        logger.info("METRICS STATISTICS");
        logger.info("=".repeat(80));
        
        if (!counters.isEmpty()) {
            logger.info("\nCounters:");
            counters.forEach((name, value) -> 
                logger.info("  {} = {}", name, value.sum()));
        }
        
        if (!timers.isEmpty()) {
            logger.info("\nTimers:");
            timers.forEach((name, value) -> 
                logger.info("  {} = {} μs", name, value.sum()));
        }
        
        if (!gauges.isEmpty()) {
            logger.info("\nGauges:");
            gauges.forEach((name, value) -> 
                logger.info("  {} = {}", name, value.get()));
        }
        
        logger.info("=".repeat(80) + "\n");
    }
    
    /**
     * Print detailed statistics with calculations.
     */
    public void printDetailedStats() {
        logger.info("\n" + "=".repeat(80));
        logger.info("DETAILED METRICS STATISTICS");
        logger.info("=".repeat(80));
        
        if (!counters.isEmpty()) {
            logger.info("\nCounters:");
            long totalCounters = 0;
            for (Map.Entry<String, LongAdder> entry : counters.entrySet()) {
                long value = entry.getValue().sum();
                totalCounters += value;
                logger.info("  {:50s} : {:,}", entry.getKey(), value);
            }
            logger.info("  " + "-".repeat(78));
            logger.info("  {:50s} : {:,}", "TOTAL", totalCounters);
        }
        
        if (!timers.isEmpty()) {
            logger.info("\nTimers (microseconds):");
            long totalTime = 0;
            for (Map.Entry<String, LongAdder> entry : timers.entrySet()) {
                long value = entry.getValue().sum();
                totalTime += value;
                double ms = value / 1000.0;
                double sec = ms / 1000.0;
                logger.info("  {:50s} : {:,} μs ({:.2f} ms, {:.2f} sec)", 
                           entry.getKey(), value, ms, sec);
            }
            logger.info("  " + "-".repeat(78));
            double totalMs = totalTime / 1000.0;
            double totalSec = totalMs / 1000.0;
            logger.info("  {:50s} : {:,} μs ({:.2f} ms, {:.2f} sec)", 
                       "TOTAL", totalTime, totalMs, totalSec);
        }
        
        if (!gauges.isEmpty()) {
            logger.info("\nGauges:");
            for (Map.Entry<String, AtomicLong> entry : gauges.entrySet()) {
                logger.info("  {:50s} : {:,}", entry.getKey(), entry.getValue().get());
            }
        }
        
        logger.info("=".repeat(80) + "\n");
    }
    
    /**
     * Get a summary of metrics as a formatted string.
     */
    public String getSummary() {
        StringBuilder sb = new StringBuilder();
        
        sb.append("Metrics Summary:\n");
        sb.append("  Counters: ").append(counters.size()).append("\n");
        sb.append("  Timers: ").append(timers.size()).append("\n");
        sb.append("  Gauges: ").append(gauges.size()).append("\n");
        
        long totalCounterValue = counters.values().stream()
            .mapToLong(LongAdder::sum)
            .sum();
        sb.append("  Total Counter Value: ").append(totalCounterValue).append("\n");
        
        long totalTimerValue = timers.values().stream()
            .mapToLong(LongAdder::sum)
            .sum();
        sb.append("  Total Timer Value: ").append(totalTimerValue).append(" μs\n");
        
        return sb.toString();
    }
    
    /**
     * Get metrics in JSON-like format for export.
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        
        // Counters
        sb.append("  \"counters\": {\n");
        int counterCount = 0;
        for (Map.Entry<String, LongAdder> entry : counters.entrySet()) {
            if (counterCount++ > 0) sb.append(",\n");
            sb.append("    \"").append(entry.getKey()).append("\": ")
              .append(entry.getValue().sum());
        }
        sb.append("\n  },\n");
        
        // Timers
        sb.append("  \"timers\": {\n");
        int timerCount = 0;
        for (Map.Entry<String, LongAdder> entry : timers.entrySet()) {
            if (timerCount++ > 0) sb.append(",\n");
            sb.append("    \"").append(entry.getKey()).append("\": ")
              .append(entry.getValue().sum());
        }
        sb.append("\n  },\n");
        
        // Gauges
        sb.append("  \"gauges\": {\n");
        int gaugeCount = 0;
        for (Map.Entry<String, AtomicLong> entry : gauges.entrySet()) {
            if (gaugeCount++ > 0) sb.append(",\n");
            sb.append("    \"").append(entry.getKey()).append("\": ")
              .append(entry.getValue().get());
        }
        sb.append("\n  }\n");
        
        sb.append("}");
        return sb.toString();
    }
    
    /**
     * Get the number of registered metrics.
     */
    public int getMetricCount() {
        return counters.size() + timers.size() + gauges.size();
    }
    
    /**
     * Check if a counter exists.
     */
    public boolean hasCounter(String name) {
        return counters.containsKey(name);
    }
    
    /**
     * Check if a timer exists.
     */
    public boolean hasTimer(String name) {
        return timers.containsKey(name);
    }
    
    /**
     * Check if a gauge exists.
     */
    public boolean hasGauge(String name) {
        return gauges.containsKey(name);
    }
    
    /**
     * Get all metric names.
     */
    public Map<String, String> getAllMetricNames() {
        Map<String, String> names = new ConcurrentHashMap<>();
        counters.keySet().forEach(name -> names.put(name, "COUNTER"));
        timers.keySet().forEach(name -> names.put(name, "TIMER"));
        gauges.keySet().forEach(name -> names.put(name, "GAUGE"));
        return names;
    }
    
    /**
     * Calculate average time per operation.
     */
    public double getAverageTime(String timerName, String counterName) {
        long totalTime = getTimer(timerName);
        long count = getCounter(counterName);
        
        if (count == 0) {
            return 0.0;
        }
        
        return (double) totalTime / count;
    }
    
    /**
     * Calculate throughput (operations per second).
     */
    public double getThroughput(String counterName, long durationMillis) {
        long count = getCounter(counterName);
        
        if (durationMillis == 0) {
            return 0.0;
        }
        
        return (count * 1000.0) / durationMillis;
    }
    
    @Override
    public String toString() {
        return String.format("SimpleMetricsCollector{counters=%d, timers=%d, gauges=%d, enabled=%s}",
                           counters.size(), timers.size(), gauges.size(), isEnabled());
    }
}
```


## Usage Examples

### 1. Basic Usage

```java
SimpleMetricsCollector metrics = new SimpleMetricsCollector();

// Configure
Map<String, Object> config = new HashMap<>();
config.put("enabled", true);
config.put("retention.period.seconds", 300);
metrics.configure(config);

// Increment counters
metrics.incrementCounter("messages.processed");
metrics.incrementCounter("bytes.sent", 1024);

// Record timing
long startTime = System.nanoTime();
// ... do work ...
long endTime = System.nanoTime();
metrics.recordTimeNanos("operation.latency", endTime - startTime);

// Set gauge
metrics.setGauge("queue.size", 42);

// Print stats
metrics.printStats();
```


### 2. In Spout/Bolt

```java
public class MySpout implements IRichSpout {
    private SimpleMetricsCollector metrics;
    
    public MySpout(SimpleMetricsCollector metrics) {
        this.metrics = metrics;
    }
    
    @Override
    public void nextTuple() {
        long start = System.nanoTime();
        
        // Emit tuple
        collector.emit(values, messageId);
        
        // Record metrics
        metrics.incrementCounter("spout.emitted");
        metrics.recordTimeNanos("spout.emit.latency", System.nanoTime() - start);
    }
    
    @Override
    public void ack(Object msgId) {
        metrics.incrementCounter("spout.acked");
    }
    
    @Override
    public void fail(Object msgId) {
        metrics.incrementCounter("spout.failed");
    }
}
```


### 3. Advanced Calculations

```java
// Calculate average processing time
double avgLatency = metrics.getAverageTime("bolt.processing.latency", "bolt.processed");
logger.info("Average latency: {:.2f} μs", avgLatency);

// Calculate throughput
long startTime = System.currentTimeMillis();
// ... run for some time ...
long duration = System.currentTimeMillis() - startTime;
double throughput = metrics.getThroughput("messages.processed", duration);
logger.info("Throughput: {:.2f} msgs/sec", throughput);

// Export to JSON
String json = metrics.toJson();
logger.info("Metrics: {}", json);
```


### 4. Detailed Statistics

```java
// Print detailed stats with calculations
metrics.printDetailedStats();

// Get summary
String summary = metrics.getSummary();
logger.info(summary);

// Check what metrics exist
Map<String, String> allMetrics = metrics.getAllMetricNames();
allMetrics.forEach((name, type) -> 
    logger.info("{}: {}", name, type));
```


### 5. Thread-Safe Usage

```java
// Multiple threads can safely update metrics
ExecutorService executor = Executors.newFixedThreadPool(10);

for (int i = 0; i < 100; i++) {
    executor.submit(() -> {
        metrics.incrementCounter("parallel.operations");
        
        long start = System.nanoTime();
        // Do work
        Thread.sleep(10);
        metrics.recordTimeNanos("parallel.latency", System.nanoTime() - start);
    });
}

executor.shutdown();
executor.awaitTermination(1, TimeUnit.MINUTES);

// All metrics will be accurate despite concurrent updates
metrics.printStats();
```

This implementation provides:

- ✅ Thread-safe counters, timers, and gauges using `LongAdder` and `AtomicLong`
- ✅ Configuration support
- ✅ Multiple output formats (console, JSON)
- ✅ Statistical calculations (averages, throughput)
- ✅ Detailed and summary reporting
- ✅ Metric lifecycle management (reset, check existence)
- ✅ High performance with low contention using `LongAdder`
<span style="display:none">[^1]</span>

<div align="center">⁂</div>

[^1]: Master-Implementation-Prompt_-Apache-Storm-Compati.md

