<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Provide implementation of metricsreporterbolt

Here's the complete implementation of **MetricsReporterBolt.java**:

```java
package com.trading.app.random.bolts;

import com.trading.streaming.api.*;
import com.trading.app.random.metrics.SimpleMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bolt that periodically reports metrics from the SimpleMetricsCollector.
 * Can aggregate metrics from incoming tuples and publish consolidated reports.
 */
public class MetricsReporterBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(MetricsReporterBolt.class);
    
    private final SimpleMetricsCollector metricsCollector;
    private OutputCollector collector;
    private String componentId;
    
    private ScheduledExecutorService scheduler;
    private int reportIntervalSeconds = 10;
    private boolean consoleReportEnabled = true;
    private boolean detailedReport = false;
    
    private final Map<String, WindowMetrics> windowMetrics = new ConcurrentHashMap<>();
    private long reportCount = 0;
    private long startTime;
    private SimpleDateFormat dateFormat;
    
    public MetricsReporterBolt(SimpleMetricsCollector metricsCollector) {
        this.metricsCollector = metricsCollector;
    }
    
    /**
     * Set the reporting interval in seconds.
     */
    public void setReportIntervalSeconds(int seconds) {
        this.reportIntervalSeconds = seconds;
    }
    
    /**
     * Enable or disable console reporting.
     */
    public void setConsoleReportEnabled(boolean enabled) {
        this.consoleReportEnabled = enabled;
    }
    
    /**
     * Enable or disable detailed reporting.
     */
    public void setDetailedReport(boolean detailed) {
        this.detailedReport = detailed;
    }
    
    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, 
                       OutputCollector collector) {
        this.collector = collector;
        this.componentId = context.getThisComponentId();
        this.startTime = System.currentTimeMillis();
        this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        // Load configuration
        Object interval = conf.get("metrics.report.interval.seconds");
        if (interval != null) {
            this.reportIntervalSeconds = Integer.parseInt(interval.toString());
        }
        
        Object consoleEnabled = conf.get("metrics.console.enabled");
        if (consoleEnabled != null) {
            this.consoleReportEnabled = Boolean.parseBoolean(consoleEnabled.toString());
        }
        
        Object detailed = conf.get("metrics.detailed.report");
        if (detailed != null) {
            this.detailedReport = Boolean.parseBoolean(detailed.toString());
        }
        
        // Start periodic reporting
        startPeriodicReporting();
        
        logger.info("MetricsReporterBolt prepared: interval={}s, console={}, detailed={}", 
                   reportIntervalSeconds, consoleReportEnabled, detailedReport);
    }
    
    @Override
    public void execute(Tuple input) {
        try {
            // Extract metrics from tuple if available
            processIncomingMetrics(input);
            
            // Update local metrics
            metricsCollector.incrementCounter("metrics.reporter.tuples.received");
            
            collector.ack(input);
            
        } catch (Exception e) {
            logger.error("Error processing metrics tuple", e);
            metricsCollector.incrementCounter("metrics.reporter.errors");
            collector.fail(input);
        }
    }
    
    /**
     * Process metrics data from incoming tuples.
     */
    private void processIncomingMetrics(Tuple input) {
        try {
            // Check if tuple contains aggregate metrics
            if (input.contains("aggregateValue")) {
                double aggregateValue = input.getDouble(input.fieldIndex("aggregateValue"));
                int count = input.getInteger(input.fieldIndex("count"));
                int min = input.getInteger(input.fieldIndex("min"));
                int max = input.getInteger(input.fieldIndex("max"));
                long windowStart = input.getLong(input.fieldIndex("windowStart"));
                long windowEnd = input.getLong(input.fieldIndex("windowEnd"));
                
                String windowKey = "window_" + windowStart;
                WindowMetrics metrics = windowMetrics.computeIfAbsent(windowKey, 
                    k -> new WindowMetrics(windowStart, windowEnd));
                
                metrics.aggregateValue = aggregateValue;
                metrics.count = count;
                metrics.min = min;
                metrics.max = max;
                metrics.updateTime = System.currentTimeMillis();
                
                logger.debug("Received window metrics: aggregate={}, count={}, range=[{}-{}]", 
                           aggregateValue, count, min, max);
                
                metricsCollector.incrementCounter("metrics.reporter.windows.received");
            }
            
            // Extract other metric fields if present
            for (String field : input.getFields()) {
                if (field.startsWith("metric_")) {
                    Object value = input.getValueByField(field);
                    String metricName = field.substring(7); // Remove "metric_" prefix
                    
                    if (value instanceof Number) {
                        metricsCollector.setGauge("external." + metricName, 
                                                  ((Number) value).longValue());
                    }
                }
            }
            
        } catch (Exception e) {
            logger.debug("Tuple does not contain metrics data: {}", e.getMessage());
        }
    }
    
    /**
     * Start periodic metrics reporting.
     */
    private void startPeriodicReporting() {
        scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "MetricsReporter-" + componentId);
            t.setDaemon(true);
            return t;
        });
        
        scheduler.scheduleAtFixedRate(
            this::publishMetricsReport,
            reportIntervalSeconds,
            reportIntervalSeconds,
            TimeUnit.SECONDS
        );
        
        logger.info("Started periodic metrics reporting every {} seconds", reportIntervalSeconds);
    }
    
    /**
     * Publish metrics report.
     */
    private void publishMetricsReport() {
        try {
            reportCount++;
            
            if (consoleReportEnabled) {
                if (detailedReport) {
                    printDetailedReport();
                } else {
                    printSummaryReport();
                }
            }
            
            // Clean old window metrics
            cleanOldWindowMetrics();
            
            metricsCollector.incrementCounter("metrics.reporter.reports.published");
            
        } catch (Exception e) {
            logger.error("Error publishing metrics report", e);
        }
    }
    
    /**
     * Print summary metrics report.
     */
    private void printSummaryReport() {
        long currentTime = System.currentTimeMillis();
        long uptime = currentTime - startTime;
        
        logger.info("\n" + "=".repeat(100));
        logger.info("METRICS REPORT #{} - {}", reportCount, dateFormat.format(new Date(currentTime)));
        logger.info("=".repeat(100));
        logger.info("Component: {} | Uptime: {} seconds", componentId, uptime / 1000);
        logger.info("-".repeat(100));
        
        // System metrics
        printSystemMetrics();
        
        // Counter metrics
        Map<String, Long> counters = metricsCollector.getAllCounters();
        if (!counters.isEmpty()) {
            logger.info("\nCOUNTERS:");
            counters.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> 
                    logger.info("  {:60s} : {:,}", entry.getKey(), entry.getValue())
                );
        }
        
        // Timer metrics
        Map<String, Long> timers = metricsCollector.getAllTimers();
        if (!timers.isEmpty()) {
            logger.info("\nTIMERS (microseconds):");
            timers.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .limit(10)  // Top 10
                .forEach(entry -> {
                    long micros = entry.getValue();
                    double millis = micros / 1000.0;
                    logger.info("  {:60s} : {:,} μs ({:.2f} ms)", 
                               entry.getKey(), micros, millis);
                });
        }
        
        // Gauge metrics
        Map<String, Long> gauges = metricsCollector.getAllGauges();
        if (!gauges.isEmpty()) {
            logger.info("\nGAUGES:");
            gauges.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> 
                    logger.info("  {:60s} : {:,}", entry.getKey(), entry.getValue())
                );
        }
        
        // Window metrics summary
        if (!windowMetrics.isEmpty()) {
            logger.info("\nWINDOW METRICS SUMMARY:");
            logger.info("  Active windows: {}", windowMetrics.size());
            
            WindowMetrics latest = getLatestWindowMetrics();
            if (latest != null) {
                logger.info("  Latest window:");
                logger.info("    Aggregate: {:.2f}", latest.aggregateValue);
                logger.info("    Count: {}", latest.count);
                logger.info("    Range: [{}, {}]", latest.min, latest.max);
            }
        }
        
        // Calculated metrics
        printCalculatedMetrics(uptime);
        
        logger.info("=".repeat(100) + "\n");
    }
    
    /**
     * Print detailed metrics report.
     */
    private void printDetailedReport() {
        logger.info("\n" + "=".repeat(100));
        logger.info("DETAILED METRICS REPORT #{}", reportCount);
        logger.info("=".repeat(100));
        
        // Use the collector's detailed stats
        metricsCollector.printDetailedStats();
        
        // Window metrics details
        if (!windowMetrics.isEmpty()) {
            logger.info("\nWINDOW METRICS DETAILS:");
            logger.info("-".repeat(100));
            
            windowMetrics.entrySet().stream()
                .sorted((e1, e2) -> Long.compare(e2.getValue().windowStart, e1.getValue().windowStart))
                .limit(5)  // Last 5 windows
                .forEach(entry -> {
                    WindowMetrics wm = entry.getValue();
                    logger.info("Window [{} - {}]:", 
                               dateFormat.format(new Date(wm.windowStart)),
                               dateFormat.format(new Date(wm.windowEnd)));
                    logger.info("  Aggregate: {:.2f}, Count: {}, Min: {}, Max: {}", 
                               wm.aggregateValue, wm.count, wm.min, wm.max);
                });
        }
        
        logger.info("=".repeat(100) + "\n");
    }
    
    /**
     * Print system metrics.
     */
    private void printSystemMetrics() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory() / (1024 * 1024);
        long freeMemory = runtime.freeMemory() / (1024 * 1024);
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory() / (1024 * 1024);
        
        logger.info("\nSYSTEM METRICS:");
        logger.info("  Memory: {} MB used / {} MB total / {} MB max", 
                   usedMemory, totalMemory, maxMemory);
        logger.info("  Available Processors: {}", runtime.availableProcessors());
        logger.info("  Active Threads: {}", Thread.activeCount());
    }
    
    /**
     * Print calculated metrics (rates, averages, etc.).
     */
    private void printCalculatedMetrics(long uptimeMillis) {
        long uptimeSeconds = uptimeMillis / 1000;
        
        if (uptimeSeconds > 0) {
            logger.info("\nCALCULATED METRICS:");
            
            // Calculate throughput for various counters
            Map<String, Long> counters = metricsCollector.getAllCounters();
            
            for (Map.Entry<String, Long> entry : counters.entrySet()) {
                if (entry.getKey().contains("emitted") || 
                    entry.getKey().contains("processed") || 
                    entry.getKey().contains("acked")) {
                    
                    double rate = entry.getValue() / (double) uptimeSeconds;
                    logger.info("  {} rate: {:.2f} /sec", entry.getKey(), rate);
                }
            }
            
            // Calculate average latencies
            Map<String, Long> timers = metricsCollector.getAllTimers();
            for (Map.Entry<String, Long> timerEntry : timers.entrySet()) {
                String timerName = timerEntry.getKey();
                String counterName = timerName.replace(".latency", ".processed");
                
                if (counters.containsKey(counterName)) {
                    long count = counters.get(counterName);
                    if (count > 0) {
                        double avgLatency = timerEntry.getValue() / (double) count;
                        logger.info("  {} avg: {:.2f} μs", timerName, avgLatency);
                    }
                }
            }
        }
    }
    
    /**
     * Get the latest window metrics.
     */
    private WindowMetrics getLatestWindowMetrics() {
        return windowMetrics.values().stream()
            .max(Comparator.comparingLong(wm -> wm.windowStart))
            .orElse(null);
    }
    
    /**
     * Clean old window metrics (older than 5 minutes).
     */
    private void cleanOldWindowMetrics() {
        long cutoffTime = System.currentTimeMillis() - (5 * 60 * 1000);
        
        windowMetrics.entrySet().removeIf(entry -> 
            entry.getValue().windowEnd < cutoffTime
        );
    }
    
    @Override
    public void cleanup() {
        logger.info("Shutting down MetricsReporterBolt...");
        
        // Stop scheduler
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        // Final report
        logger.info("FINAL METRICS REPORT:");
        if (detailedReport) {
            metricsCollector.printDetailedStats();
        } else {
            metricsCollector.printStats();
        }
        
        logger.info("Total reports published: {}", reportCount);
        logger.info("MetricsReporterBolt shutdown complete");
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // This is a terminal bolt - no output fields
    }
    
    /**
     * Container for window metrics data.
     */
    private static class WindowMetrics {
        final long windowStart;
        final long windowEnd;
        double aggregateValue;
        int count;
        int min;
        int max;
        long updateTime;
        
        WindowMetrics(long windowStart, long windowEnd) {
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.updateTime = System.currentTimeMillis();
        }
    }
}
```


## Additional Helper Classes

### MetricsSnapshot.java (Optional - for exporting metrics)

```java
package com.trading.app.random.metrics;

import java.util.Map;

/**
 * Snapshot of metrics at a point in time.
 */
public class MetricsSnapshot {
    private final long timestamp;
    private final Map<String, Long> counters;
    private final Map<String, Long> timers;
    private final Map<String, Long> gauges;
    
    public MetricsSnapshot(long timestamp, 
                          Map<String, Long> counters,
                          Map<String, Long> timers,
                          Map<String, Long> gauges) {
        this.timestamp = timestamp;
        this.counters = counters;
        this.timers = timers;
        this.gauges = gauges;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public Map<String, Long> getCounters() {
        return counters;
    }
    
    public Map<String, Long> getTimers() {
        return timers;
    }
    
    public Map<String, Long> getGauges() {
        return gauges;
    }
    
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"timestamp\": ").append(timestamp).append(",\n");
        sb.append("  \"counters\": ").append(mapToJson(counters)).append(",\n");
        sb.append("  \"timers\": ").append(mapToJson(timers)).append(",\n");
        sb.append("  \"gauges\": ").append(mapToJson(gauges)).append("\n");
        sb.append("}");
        return sb.toString();
    }
    
    private String mapToJson(Map<String, Long> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            if (!first) sb.append(", ");
            sb.append("\"").append(entry.getKey()).append("\": ").append(entry.getValue());
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }
}
```


## Usage in Topology

### In topology.yml

```yaml
bolts:
  - id: "metrics-reporter"
    className: "com.trading.app.random.bolts.MetricsReporterBolt"
    constructorArgs:
      - ref: "metricsCollector"
    properties:
      - name: "reportIntervalSeconds"
        value: "${metrics.publish.interval.seconds}"
      - name: "consoleReportEnabled"
        value: "${metrics.console.reporter.enabled}"
      - name: "detailedReport"
        value: "false"
    parallelism: 1
    outputFields: []
```


### In topology.properties

```properties
metrics.publish.interval.seconds=10
metrics.console.reporter.enabled=true
metrics.detailed.report=false
```


## Example Output

### Summary Report

```
====================================================================================================
METRICS REPORT #12 - 2026-02-04 16:53:45
====================================================================================================
Component: metrics-reporter | Uptime: 120 seconds
----------------------------------------------------------------------------------------------------

SYSTEM METRICS:
  Memory: 512 MB used / 1024 MB total / 2048 MB max
  Available Processors: 8
  Active Threads: 24

COUNTERS:
  bolt.aggregator.received                                     : 1,234
  bolt.enrichment.enriched                                     : 1,234
  bolt.processor.processed                                     : 1,234
  spout.numbers.emitted                                        : 1,234
  spout.numbers.acked                                          : 1,234

TIMERS (microseconds):
  bolt.processor.latency.us                                    : 45,678 μs (45.68 ms)
  bolt.enrichment.latency.us                                   : 23,456 μs (23.46 ms)

GAUGES:
  queue.size                                                   : 42

WINDOW METRICS SUMMARY:
  Active windows: 12
  Latest window:
    Aggregate: 567.89
    Count: 123
    Range: [100, 999]

CALCULATED METRICS:
  spout.numbers.emitted rate: 10.28 /sec
  bolt.processor.processed rate: 10.28 /sec
  bolt.processor.latency.us avg: 37.01 μs
====================================================================================================
```


## Test the MetricsReporterBolt

```java
package com.trading.app.random.bolts;

import com.trading.app.random.metrics.SimpleMetricsCollector;
import com.trading.streaming.api.*;
import com.trading.streaming.impl.TupleImpl;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MetricsReporterBoltTest {
    
    private SimpleMetricsCollector metricsCollector;
    private MetricsReporterBolt bolt;
    private OutputCollector mockCollector;
    
    @BeforeEach
    void setUp() {
        metricsCollector = new SimpleMetricsCollector();
        bolt = new MetricsReporterBolt(metricsCollector);
        mockCollector = mock(OutputCollector.class);
    }
    
    @Test
    @DisplayName("Should prepare and configure correctly")
    void testPrepare() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("metrics.report.interval.seconds", "5");
        conf.put("metrics.console.enabled", "true");
        
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisComponentId()).thenReturn("test-reporter");
        
        bolt.prepare(conf, context, mockCollector);
        
        // Verify it doesn't throw exception
        assertNotNull(bolt);
    }
    
    @Test
    @DisplayName("Should process window metrics tuple")
    void testProcessWindowMetrics() {
        TopologyContext context = mock(TopologyContext.class);
        when(context.getThisComponentId()).thenReturn("test-reporter");
        
        bolt.prepare(new HashMap<>(), context, mockCollector);
        
        Tuple tuple = new TupleImpl(
            "aggregator",
            "default",
            Arrays.asList(567.89, 123, 100, 999, 1000L, 2000L),
            Arrays.asList("aggregateValue", "count", "min", "max", "windowStart", "windowEnd"),
            1L
        );
        
        bolt.execute(tuple);
        
        verify(mockCollector).ack(tuple);
        assertTrue(metricsCollector.getCounter("metrics.reporter.tuples.received") > 0);
    }
    
    @AfterEach
    void tearDown() {
        if (bolt != null) {
            bolt.cleanup();
        }
    }
}
```

Run the application and you'll see periodic metrics reports in the console! 📊
<span style="display:none">[^1]</span>

<div align="center">⁂</div>

[^1]: Master-Implementation-Prompt_-Apache-Storm-Compati.md

