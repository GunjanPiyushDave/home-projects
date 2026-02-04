<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# convert the topology-simple.yml into the enhanced format including some property substitutions. Add some components and config as required and provide their implementation. convert the simple topology into an enhanced Flux-style format with components, configuration, and provide all the implementations.

use the random data topology for your response

I'll provide a comprehensive enhanced random data topology with property substitution, components, and all implementations.

## 1. topology.properties

```properties
# Application Configuration
app.name=EnhancedRandomDataTopology
app.environment=development
app.version=1.0.0

# Parallelism Configuration
parallelism.spout.random.number=2
parallelism.spout.random.string=1
parallelism.bolt.processor=4
parallelism.bolt.filter=2
parallelism.bolt.aggregator=2
parallelism.bolt.enrichment=2
parallelism.bolt.logger=1
parallelism.bolt.metrics=1

# Number Generator Configuration
generator.min.value=1
generator.max.value=1000
generator.emit.frequency.ms=100
generator.batch.size=10
generator.enable.metrics=true

# String Generator Configuration
string.generator.length=10
string.generator.charset=ALPHANUMERIC
string.generator.emit.frequency.ms=200

# Processing Strategy Configuration
strategy.type=MULTIPLY
strategy.multiplier=2
strategy.enable.logging=true

# Filter Configuration
filter.threshold=500
filter.operator=GREATER_THAN
filter.pass.through.on.error=false

# Aggregation Configuration
aggregation.window.size.seconds=10
aggregation.slide.interval.seconds=5
aggregation.type=AVG
aggregation.emit.partial.results=true

# Enrichment Configuration
enrichment.source=METADATA_SERVICE
enrichment.cache.enabled=true
enrichment.cache.ttl.seconds=300
enrichment.field.name=enriched_metadata

# Metrics Configuration
metrics.retention.period.seconds=300
metrics.publish.interval.seconds=10
metrics.enabled=true
metrics.console.reporter.enabled=true

# Logging Configuration
logger.prefix=RANDOM_DATA_PROCESSING
logger.pretty.print=true
logger.log.level=DEBUG
logger.include.timestamp=true

# Topology Configuration
topology.workers=2
topology.max.spout.pending=1000
topology.message.timeout.secs=30
topology.executor.receive.buffer.size=1024
topology.executor.send.buffer.size=1024
topology.transfer.buffer.size=32
topology.debug=false
```


## 2. Enhanced topology.yml

```yaml
# Enhanced Random Data Topology with Property Substitution
name: "${app.name}"

# Components - Reusable beans with dependency injection
components:
  # Number Generator Configuration
  - id: "numberGeneratorConfig"
    className: "com.trading.app.random.config.NumberGeneratorConfig"
    properties:
      - name: "minValue"
        value: "${generator.min.value}"
      - name: "maxValue"
        value: "${generator.max.value}"
      - name: "emitFrequencyMs"
        value: "${generator.emit.frequency.ms}"
      - name: "batchSize"
        value: "${generator.batch.size}"
      - name: "enableMetrics"
        value: "${generator.enable.metrics}"
  
  # String Generator Configuration
  - id: "stringGeneratorConfig"
    className: "com.trading.app.random.config.StringGeneratorConfig"
    properties:
      - name: "length"
        value: "${string.generator.length}"
      - name: "charset"
        value: "${string.generator.charset}"
      - name: "emitFrequencyMs"
        value: "${string.generator.emit.frequency.ms}"
  
  # Metrics Collector
  - id: "metricsCollector"
    className: "com.trading.app.random.metrics.SimpleMetricsCollector"
    configMethods:
      - name: "configure"
        args:
          - {
              "retention.period.seconds": "${metrics.retention.period.seconds}",
              "publish.interval.seconds": "${metrics.publish.interval.seconds}",
              "enabled": "${metrics.enabled}",
              "console.reporter.enabled": "${metrics.console.reporter.enabled}"
            }
  
  # Processing Strategy Factory
  - id: "strategyFactory"
    className: "com.trading.app.random.strategy.ProcessingStrategyFactory"
  
  # Processing Strategy
  - id: "processingStrategy"
    className: "com.trading.app.random.strategy.MultiplyStrategy"
    constructorArgs:
      - "${strategy.multiplier}"
    properties:
      - name: "enableLogging"
        value: "${strategy.enable.logging}"
  
  # Filter Configuration
  - id: "filterConfig"
    className: "com.trading.app.random.config.FilterConfig"
    properties:
      - name: "threshold"
        value: "${filter.threshold}"
      - name: "operator"
        value: "${filter.operator}"
      - name: "passThroughOnError"
        value: "${filter.pass.through.on.error}"
  
  # Aggregation Configuration
  - id: "aggregationConfig"
    className: "com.trading.app.random.config.AggregationConfig"
    properties:
      - name: "windowSizeSeconds"
        value: "${aggregation.window.size.seconds}"
      - name: "slideIntervalSeconds"
        value: "${aggregation.slide.interval.seconds}"
      - name: "aggregationType"
        value: "${aggregation.type}"
      - name: "emitPartialResults"
        value: "${aggregation.emit.partial.results}"
  
  # Enrichment Configuration
  - id: "enrichmentConfig"
    className: "com.trading.app.random.config.EnrichmentConfig"
    properties:
      - name: "source"
        value: "${enrichment.source}"
      - name: "cacheEnabled"
        value: "${enrichment.cache.enabled}"
      - name: "cacheTtlSeconds"
        value: "${enrichment.cache.ttl.seconds}"
      - name: "fieldName"
        value: "${enrichment.field.name}"
  
  # Logger Configuration
  - id: "loggerConfig"
    className: "com.trading.app.random.config.LoggerConfig"
    properties:
      - name: "prefix"
        value: "${logger.prefix}"
      - name: "prettyPrint"
        value: "${logger.pretty.print}"
      - name: "logLevel"
        value: "${logger.log.level}"
      - name: "includeTimestamp"
        value: "${logger.include.timestamp}"

# Topology-level configuration
config:
  topology.name: "${app.name}"
  topology.environment: "${app.environment}"
  topology.version: "${app.version}"
  topology.workers: "${topology.workers}"
  topology.max.spout.pending: "${topology.max.spout.pending}"
  topology.message.timeout.secs: "${topology.message.timeout.secs}"
  topology.executor.receive.buffer.size: "${topology.executor.receive.buffer.size}"
  topology.executor.send.buffer.size: "${topology.executor.send.buffer.size}"
  topology.transfer.buffer.size: "${topology.transfer.buffer.size}"
  topology.debug: "${topology.debug}"

# Spout definitions
spouts:
  - id: "random-number-generator"
    className: "com.trading.app.random.spouts.ConfigurableRandomNumberSpout"
    constructorArgs:
      - ref: "numberGeneratorConfig"
      - ref: "metricsCollector"
    parallelism: "${parallelism.spout.random.number}"
    outputFields:
      - "number"
      - "timestamp"
      - "sequence"

  - id: "random-string-generator"
    className: "com.trading.app.random.spouts.ConfigurableRandomStringSpout"
    constructorArgs:
      - ref: "stringGeneratorConfig"
      - ref: "metricsCollector"
    parallelism: "${parallelism.spout.random.string}"
    outputFields:
      - "text"
      - "timestamp"
      - "length"

# Bolt definitions
bolts:
  - id: "number-processor"
    className: "com.trading.app.random.bolts.StrategyBasedProcessorBolt"
    constructorArgs:
      - ref: "processingStrategy"
      - ref: "metricsCollector"
    parallelism: "${parallelism.bolt.processor}"
    outputFields:
      - "original"
      - "processed"
      - "timestamp"
      - "sequence"

  - id: "number-filter"
    className: "com.trading.app.random.bolts.ConfigurableFilterBolt"
    constructorArgs:
      - ref: "filterConfig"
    parallelism: "${parallelism.bolt.filter}"
    outputFields:
      - "original"
      - "processed"
      - "timestamp"
      - "sequence"
      - "filter_passed"

  - id: "number-aggregator"
    className: "com.trading.app.random.bolts.WindowedAggregatorBolt"
    constructorArgs:
      - ref: "aggregationConfig"
      - ref: "metricsCollector"
    parallelism: "${parallelism.bolt.aggregator}"
    outputFields:
      - "aggregateValue"
      - "count"
      - "min"
      - "max"
      - "windowStart"
      - "windowEnd"

  - id: "data-enrichment"
    className: "com.trading.app.random.bolts.ConfigurableEnrichmentBolt"
    constructorArgs:
      - ref: "enrichmentConfig"
      - ref: "metricsCollector"
    parallelism: "${parallelism.bolt.enrichment}"
    outputFields:
      - "original"
      - "processed"
      - "enriched_metadata"
      - "timestamp"
      - "sequence"

  - id: "formatted-logger"
    className: "com.trading.app.random.bolts.ConfigurableLoggerBolt"
    constructorArgs:
      - ref: "loggerConfig"
      - ref: "metricsCollector"
    parallelism: "${parallelism.bolt.logger}"
    outputFields: []

  - id: "metrics-reporter"
    className: "com.trading.app.random.bolts.MetricsReporterBolt"
    constructorArgs:
      - ref: "metricsCollector"
    parallelism: "${parallelism.bolt.metrics}"
    outputFields: []

# Stream definitions with grouping strategies
streams:
  - name: "numbers --> processor"
    from: "random-number-generator"
    to: "number-processor"
    grouping:
      type: SHUFFLE

  - name: "processor --> filter"
    from: "number-processor"
    to: "number-filter"
    grouping:
      type: SHUFFLE

  - name: "filter --> enrichment"
    from: "number-filter"
    to: "data-enrichment"
    grouping:
      type: FIELDS
      args: ["sequence"]

  - name: "enrichment --> aggregator"
    from: "data-enrichment"
    to: "number-aggregator"
    grouping:
      type: FIELDS
      args: ["original"]

  - name: "enrichment --> logger"
    from: "data-enrichment"
    to: "formatted-logger"
    grouping:
      type: SHUFFLE

  - name: "aggregator --> logger"
    from: "number-aggregator"
    to: "formatted-logger"
    grouping:
      type: SHUFFLE

  - name: "aggregator --> metrics"
    from: "number-aggregator"
    to: "metrics-reporter"
    grouping:
      type: ALL

  - name: "strings --> logger"
    from: "random-string-generator"
    to: "formatted-logger"
    grouping:
      type: SHUFFLE
```


## 3. Configuration Classes

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
    private int batchSize = 1;
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
    
    public int getBatchSize() {
        return batchSize;
    }
    
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
    
    public boolean isEnableMetrics() {
        return enableMetrics;
    }
    
    public void setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
    }
    
    @Override
    public String toString() {
        return String.format("NumberGeneratorConfig{min=%d, max=%d, frequency=%dms, batch=%d, metrics=%s}",
                           minValue, maxValue, emitFrequencyMs, batchSize, enableMetrics);
    }
}
```


### StringGeneratorConfig.java

```java
package com.trading.app.random.config;

/**
 * Configuration for random string generation.
 */
public class StringGeneratorConfig {
    private int length = 10;
    private String charset = "ALPHANUMERIC"; // ALPHANUMERIC, ALPHA, NUMERIC
    private long emitFrequencyMs = 100;
    
    public int getLength() {
        return length;
    }
    
    public void setLength(int length) {
        this.length = length;
    }
    
    public String getCharset() {
        return charset;
    }
    
    public void setCharset(String charset) {
        this.charset = charset;
    }
    
    public long getEmitFrequencyMs() {
        return emitFrequencyMs;
    }
    
    public void setEmitFrequencyMs(long emitFrequencyMs) {
        this.emitFrequencyMs = emitFrequencyMs;
    }
    
    @Override
    public String toString() {
        return String.format("StringGeneratorConfig{length=%d, charset=%s, frequency=%dms}",
                           length, charset, emitFrequencyMs);
    }
}
```


### FilterConfig.java

```java
package com.trading.app.random.config;

/**
 * Configuration for filtering operations.
 */
public class FilterConfig {
    private int threshold = 50;
    private String operator = "GREATER_THAN"; // GREATER_THAN, LESS_THAN, EQUALS, NOT_EQUALS
    private boolean passThroughOnError = false;
    
    public int getThreshold() {
        return threshold;
    }
    
    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }
    
    public String getOperator() {
        return operator;
    }
    
    public void setOperator(String operator) {
        this.operator = operator;
    }
    
    public boolean isPassThroughOnError() {
        return passThroughOnError;
    }
    
    public void setPassThroughOnError(boolean passThroughOnError) {
        this.passThroughOnError = passThroughOnError;
    }
    
    @Override
    public String toString() {
        return String.format("FilterConfig{threshold=%d, operator=%s, passThrough=%s}",
                           threshold, operator, passThroughOnError);
    }
}
```


### AggregationConfig.java (Enhanced)

```java
package com.trading.app.random.config;

/**
 * Enhanced configuration for aggregation windows.
 */
public class AggregationConfig {
    private int windowSizeSeconds = 10;
    private int slideIntervalSeconds = 5;
    private String aggregationType = "AVG"; // AVG, SUM, MIN, MAX, COUNT
    private boolean emitPartialResults = false;
    
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
    
    public boolean isEmitPartialResults() {
        return emitPartialResults;
    }
    
    public void setEmitPartialResults(boolean emitPartialResults) {
        this.emitPartialResults = emitPartialResults;
    }
    
    @Override
    public String toString() {
        return String.format("AggregationConfig{window=%ds, slide=%ds, type=%s, partial=%s}",
                           windowSizeSeconds, slideIntervalSeconds, aggregationType, emitPartialResults);
    }
}
```


### EnrichmentConfig.java

```java
package com.trading.app.random.config;

/**
 * Configuration for data enrichment.
 */
public class EnrichmentConfig {
    private String source = "METADATA_SERVICE";
    private boolean cacheEnabled = true;
    private int cacheTtlSeconds = 300;
    private String fieldName = "metadata";
    
    public String getSource() {
        return source;
    }
    
    public void setSource(String source) {
        this.source = source;
    }
    
    public boolean isCacheEnabled() {
        return cacheEnabled;
    }
    
    public void setCacheEnabled(boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
    }
    
    public int getCacheTtlSeconds() {
        return cacheTtlSeconds;
    }
    
    public void setCacheTtlSeconds(int cacheTtlSeconds) {
        this.cacheTtlSeconds = cacheTtlSeconds;
    }
    
    public String getFieldName() {
        return fieldName;
    }
    
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
    
    @Override
    public String toString() {
        return String.format("EnrichmentConfig{source=%s, cache=%s, ttl=%ds, field=%s}",
                           source, cacheEnabled, cacheTtlSeconds, fieldName);
    }
}
```


### LoggerConfig.java

```java
package com.trading.app.random.config;

/**
 * Configuration for logging behavior.
 */
public class LoggerConfig {
    private String prefix = "DATA";
    private boolean prettyPrint = false;
    private String logLevel = "INFO";
    private boolean includeTimestamp = true;
    
    public String getPrefix() {
        return prefix;
    }
    
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
    
    public boolean isPrettyPrint() {
        return prettyPrint;
    }
    
    public void setPrettyPrint(boolean prettyPrint) {
        this.prettyPrint = prettyPrint;
    }
    
    public String getLogLevel() {
        return logLevel;
    }
    
    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }
    
    public boolean isIncludeTimestamp() {
        return includeTimestamp;
    }
    
    public void setIncludeTimestamp(boolean includeTimestamp) {
        this.includeTimestamp = includeTimestamp;
    }
    
    @Override
    public String toString() {
        return String.format("LoggerConfig{prefix=%s, pretty=%s, level=%s, timestamp=%s}",
                           prefix, prettyPrint, logLevel, includeTimestamp);
    }
}
```


## 4. Strategy Classes

### ProcessingStrategyFactory.java

```java
package com.trading.app.random.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating processing strategies.
 */
public class ProcessingStrategyFactory {
    private static final Logger logger = LoggerFactory.getLogger(ProcessingStrategyFactory.class);
    
    public ProcessingStrategy createStrategy(String type, Object... args) {
        logger.info("Creating strategy: {} with args: {}", type, args);
        
        switch (type.toUpperCase()) {
            case "MULTIPLY":
                int multiplier = args.length > 0 ? (int) args[0] : 2;
                return new MultiplyStrategy(multiplier);
            case "ADD":
                int addend = args.length > 0 ? (int) args[0] : 10;
                return new AddStrategy(addend);
            case "SQUARE":
                return new SquareStrategy();
            default:
                throw new IllegalArgumentException("Unknown strategy type: " + type);
        }
    }
}
```


### AddStrategy.java

```java
package com.trading.app.random.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processing strategy that adds a constant value.
 */
public class AddStrategy implements ProcessingStrategy {
    private static final Logger logger = LoggerFactory.getLogger(AddStrategy.class);
    
    private final int addend;
    private boolean enableLogging = false;
    
    public AddStrategy(int addend) {
        this.addend = addend;
    }
    
    public void setEnableLogging(boolean enableLogging) {
        this.enableLogging = enableLogging;
    }
    
    @Override
    public Integer process(Integer value) {
        Integer result = value + addend;
        if (enableLogging) {
            logger.debug("Processing: {} + {} = {}", value, addend, result);
        }
        return result;
    }
    
    @Override
    public String getName() {
        return "AddStrategy(+" + addend + ")";
    }
}
```


### SquareStrategy.java

```java
package com.trading.app.random.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processing strategy that squares values.
 */
public class SquareStrategy implements ProcessingStrategy {
    private static final Logger logger = LoggerFactory.getLogger(SquareStrategy.class);
    
    private boolean enableLogging = false;
    
    public void setEnableLogging(boolean enableLogging) {
        this.enableLogging = enableLogging;
    }
    
    @Override
    public Integer process(Integer value) {
        Integer result = value * value;
        if (enableLogging) {
            logger.debug("Processing: {}^2 = {}", value, result);
        }
        return result;
    }
    
    @Override
    public String getName() {
        return "SquareStrategy(x^2)";
    }
}
```


## 5. Enhanced Spout Implementations

### ConfigurableRandomNumberSpout.java (Enhanced)

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
 * Enhanced configurable random number generator spout with batch support.
 */
public class ConfigurableRandomNumberSpout implements IRichSpout {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurableRandomNumberSpout.class);
    
    private final NumberGeneratorConfig config;
    private final SimpleMetricsCollector metricsCollector;
    private SpoutOutputCollector collector;
    private Random random;
    private AtomicLong messageIdCounter;
    private AtomicLong sequenceCounter;
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
        this.sequenceCounter = new AtomicLong(0);
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
        
        // Emit batch
        for (int i = 0; i < config.getBatchSize(); i++) {
            int randomNumber = random.nextInt(config.getMaxValue() - config.getMinValue() + 1) 
                              + config.getMinValue();
            long timestamp = System.currentTimeMillis();
            long messageId = messageIdCounter.incrementAndGet();
            long sequence = sequenceCounter.incrementAndGet();
            
            collector.emit(Arrays.asList(randomNumber, timestamp, sequence), messageId);
            
            if (config.isEnableMetrics()) {
                metricsCollector.incrementCounter("spout.numbers.emitted");
            }
            
            logger.trace("Emitted: number={}, timestamp={}, sequence={}, msgId={}", 
                        randomNumber, timestamp, sequence, messageId);
        }
        
        lastEmitTime = currentTime;
        
        if (config.isEnableMetrics() && sequenceCounter.get() % 100 == 0) {
            logger.debug("Total emitted: {}", sequenceCounter.get());
        }
    }
    
    @Override
    public void ack(Object msgId) {
        if (config.isEnableMetrics()) {
            metricsCollector.incrementCounter("spout.numbers.acked");
        }
        logger.trace("Ack: {}", msgId);
    }
    
    @Override
    public void fail(Object msgId) {
        if (config.isEnableMetrics()) {
            metricsCollector.incrementCounter("spout.numbers.failed");
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
            logger.info("Spout closing - Total emitted: {}", sequenceCounter.get());
            metricsCollector.printStats();
        }
        logger.info("Spout closed");
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number", "timestamp", "sequence"));
    }
}
```


### ConfigurableRandomStringSpout.java

```java
package com.trading.app.random.spouts;

import com.trading.streaming.api.*;
import com.trading.app.random.config.StringGeneratorConfig;
import com.trading.app.random.metrics.SimpleMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Configurable random string generator spout.
 */
public class ConfigurableRandomStringSpout implements IRichSpout {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurableRandomStringSpout.class);
    
    private static final String ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String NUMERIC = "0123456789";
    private static final String ALPHANUMERIC = ALPHA + NUMERIC;
    
    private final StringGeneratorConfig config;
    private final SimpleMetricsCollector metricsCollector;
    private SpoutOutputCollector collector;
    private Random random;
    private AtomicLong messageIdCounter;
    private boolean active;
    private long lastEmitTime;
    private String characterSet;
    
    public ConfigurableRandomStringSpout(StringGeneratorConfig config, 
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
        
        // Determine character set
        switch (config.getCharset().toUpperCase()) {
            case "ALPHA":
                characterSet = ALPHA;
                break;
            case "NUMERIC":
                characterSet = NUMERIC;
                break;
            case "ALPHANUMERIC":
            default:
                characterSet = ALPHANUMERIC;
                break;
        }
        
        logger.info("ConfigurableRandomStringSpout opened with config: {}", config);
    }
    
    @Override
    public void nextTuple() {
        if (!active) return;
        
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastEmitTime < config.getEmitFrequencyMs()) {
            return;
        }
        
        String randomString = generateRandomString();
        long timestamp = System.currentTimeMillis();
        long messageId = messageIdCounter.incrementAndGet();
        
        collector.emit(Arrays.asList(randomString, timestamp, randomString.length()), messageId);
        
        metricsCollector.incrementCounter("spout.strings.emitted");
        
        lastEmitTime = currentTime;
        logger.trace("Emitted string: {} (length={})", randomString, randomString.length());
    }
    
    private String generateRandomString() {
        StringBuilder sb = new StringBuilder(config.getLength());
        for (int i = 0; i < config.getLength(); i++) {
            sb.append(characterSet.charAt(random.nextInt(characterSet.length())));
        }
        return sb.toString();
    }
    
    @Override
    public void ack(Object msgId) {
        metricsCollector.incrementCounter("spout.strings.acked");
        logger.trace("Ack: {}", msgId);
    }
    
    @Override
    public void fail(Object msgId) {
        metricsCollector.incrementCounter("spout.strings.failed");
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
        logger.info("Spout closing - Total emitted: {}", messageIdCounter.get());
        logger.info("Spout closed");
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text", "timestamp", "length"));
    }
}
```


## 6. Enhanced Bolt Implementations

### StrategyBasedProcessorBolt.java (Enhanced)

```java
package com.trading.app.random.bolts;

import com.trading.streaming.api.*;
import com.trading.app.random.strategy.ProcessingStrategy;
import com.trading.app.random.metrics.SimpleMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

/**
 * Enhanced processor bolt with metrics support.
 */
public class StrategyBasedProcessorBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(StrategyBasedProcessorBolt.class);
    
    private final ProcessingStrategy strategy;
    private final SimpleMetricsCollector metricsCollector;
    private OutputCollector collector;
    private long processedCount = 0;
    
    public StrategyBasedProcessorBolt(ProcessingStrategy strategy, 
                                     SimpleMetricsCollector metricsCollector) {
        this.strategy = strategy;
        this.metricsCollector = metricsCollector;
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
        long startTime = System.nanoTime();
        
        try {
            Integer original = input.getInteger(0);
            Long timestamp = input.getLong(1);
            Long sequence = input.getLong(2);
            
            Integer processed = strategy.process(original);
            processedCount++;
            
            collector.emit(input, Arrays.asList(original, processed, timestamp, sequence));
            collector.ack(input);
            
            metricsCollector.incrementCounter("bolt.processor.processed");
            
            long duration = (System.nanoTime() - startTime) / 1_000; // microseconds
            metricsCollector.recordTime("bolt.processor.latency.us", duration);
            
            if (processedCount % 1000 == 0) {
                logger.info("Processed {} tuples with strategy: {}", 
                           processedCount, strategy.getName());
            }
            
        } catch (Exception e) {
            logger.error("Error processing tuple", e);
            metricsCollector.incrementCounter("bolt.processor.errors");
            collector.fail(input);
        }
    }
    
    @Override
    public void cleanup() {
        logger.info("Bolt cleanup. Total processed: {}", processedCount);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("original", "processed", "timestamp", "sequence"));
    }
}
```


### ConfigurableFilterBolt.java

```java
package com.trading.app.random.bolts;

import com.trading.streaming.api.*;
import com.trading.app.random.config.FilterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

/**
 * Configurable filter bolt that filters tuples based on configuration.
 */
public class ConfigurableFilterBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurableFilterBolt.class);
    
    private final FilterConfig config;
    private OutputCollector collector;
    private long totalCount = 0;
    private long passedCount = 0;
    private long filteredCount = 0;
    
    public ConfigurableFilterBolt(FilterConfig config) {
        this.config = config;
    }
    
    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, 
                       OutputCollector collector) {
        this.collector = collector;
        logger.info("ConfigurableFilterBolt prepared with config: {}", config);
    }
    
    @Override
    public void execute(Tuple input) {
        try {
            totalCount++;
            
            Integer original = input.getInteger(0);
            Integer processed = input.getInteger(1);
            Long timestamp = input.getLong(2);
            Long sequence = input.getLong(3);
            
            boolean passed = evaluateFilter(processed);
            
            if (passed) {
                passedCount++;
                collector.emit(input, Arrays.asList(original, processed, timestamp, sequence, true));
                logger.trace("Tuple passed filter: {} (value={})", sequence, processed);
            } else {
                filteredCount++;
                logger.trace("Tuple filtered out: {} (value={})", sequence, processed);
            }
            
            collector.ack(input);
            
            if (totalCount % 100 == 0) {
                double passRate = (passedCount * 100.0) / totalCount;
                logger.debug("Filter stats: total={}, passed={}, filtered={}, pass rate={:.2f}%", 
                           totalCount, passedCount, filteredCount, passRate);
            }
            
        } catch (Exception e) {
            logger.error("Error filtering tuple", e);
            if (config.isPassThroughOnError()) {
                collector.emit(input, input.getValues());
            }
            collector.fail(input);
        }
    }
    
    private boolean evaluateFilter(Integer value) {
        switch (config.getOperator().toUpperCase()) {
            case "GREATER_THAN":
                return value > config.getThreshold();
            case "LESS_THAN":
                return value < config.getThreshold();
            case "EQUALS":
                return value == config.getThreshold();
            case "NOT_EQUALS":
                return value != config.getThreshold();
            case "GREATER_THAN_OR_EQUAL":
                return value >= config.getThreshold();
            case "LESS_THAN_OR_EQUAL":
                return value <= config.getThreshold();
            default:
                logger.warn("Unknown operator: {}", config.getOperator());
                return true;
        }
    }
    
    @Override
    public void cleanup() {
        double passRate = totalCount > 0 ? (passedCount * 100.0) / totalCount : 0;
        logger.info("Filter cleanup - Total: {}, Passed: {}, Filtered: {}, Pass Rate: {:.2f}%", 
                   totalCount, passedCount, filteredCount, passRate);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("original", "processed", "timestamp", "sequence", "filter_passed"));
    }
}
```


### WindowedAggregatorBolt.java

```java
package com.trading.app.random.bolts;

import com.trading.streaming.api.*;
import com.trading.app.random.config.AggregationConfig;
import com.trading.app.random.metrics.SimpleMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Windowed aggregator bolt with configurable windows and aggregation types.
 */
public class WindowedAggregatorBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(WindowedAggregatorBolt.class);
    
    private final AggregationConfig config;
    private final SimpleMetricsCollector metricsCollector;
    private OutputCollector collector;
    private Queue<TimestampedValue> window;
    private long lastWindowEmit;
    private long totalTuplesProcessed;
    
    public WindowedAggregatorBolt(AggregationConfig config, 
                                 SimpleMetricsCollector metricsCollector) {
        this.config = config;
        this.metricsCollector = metricsCollector;
    }
    
    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, 
                       OutputCollector collector) {
        this.collector = collector;
        this.window = new ConcurrentLinkedQueue<>();
        this.lastWindowEmit = System.currentTimeMillis();
        this.totalTuplesProcessed = 0;
        
        logger.info("WindowedAggregatorBolt prepared with config: {}", config);
        
        startWindowTimer();
    }
    
    @Override
    public void execute(Tuple input) {
        try {
            Integer processed = input.getInteger(1);
            Long timestamp = input.getLong(2);
            
            window.add(new TimestampedValue(processed, timestamp));
            totalTuplesProcessed++;
            
            metricsCollector.incrementCounter("bolt.aggregator.received");
            
            collector.ack(input);
            
        } catch (Exception e) {
            logger.error("Error aggregating tuple", e);
            metricsCollector.incrementCounter("bolt.aggregator.errors");
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
                    logger.info("Window timer interrupted");
                    break;
                } catch (Exception e) {
                    logger.error("Error in window timer", e);
                }
            }
        });
        timerThread.setDaemon(true);
        timerThread.setName("WindowAggregator-Timer");
        timerThread.start();
    }
    
    private void emitWindowAggregate() {
        long currentTime = System.currentTimeMillis();
        long windowStartTime = currentTime - (config.getWindowSizeSeconds() * 1000L);
        
        // Remove expired entries
        window.removeIf(tv -> tv.timestamp < windowStartTime);
        
        if (window.isEmpty() && !config.isEmitPartialResults()) {
            return;
        }
        
        // Calculate aggregates
        IntSummaryStatistics stats = window.stream()
            .mapToInt(tv -> tv.value)
            .summaryStatistics();
        
        double aggregateValue = calculateAggregate(stats);
        int count = (int) stats.getCount();
        int min = count > 0 ? stats.getMin() : 0;
        int max = count > 0 ? stats.getMax() : 0;
        
        collector.emit(Arrays.asList(
            aggregateValue, 
            count, 
            min, 
            max,
            windowStartTime,
            currentTime
        ));
        
        metricsCollector.incrementCounter("bolt.aggregator.windows.emitted");
        
        logger.info("Window aggregate [{}]: value={:.2f}, count={}, min={}, max={}, range=[{}-{}]", 
                   config.getAggregationType(), aggregateValue, count, min, max,
                   new Date(windowStartTime), new Date(currentTime));
        
        lastWindowEmit = currentTime;
    }
    
    private double calculateAggregate(IntSummaryStatistics stats) {
        if (stats.getCount() == 0) {
            return 0.0;
        }
        
        switch (config.getAggregationType().toUpperCase()) {
            case "SUM":
                return stats.getSum();
            case "AVG":
                return stats.getAverage();
            case "MIN":
                return stats.getMin();
            case "MAX":
                return stats.getMax();
            case "COUNT":
                return stats.getCount();
            default:
                return stats.getAverage();
        }
    }
    
    @Override
    public void cleanup() {
        emitWindowAggregate(); // Final window
        logger.info("Bolt cleanup. Total tuples processed: {}", totalTuplesProcessed);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("aggregateValue", "count", "min", "max", 
                                   "windowStart", "windowEnd"));
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


### ConfigurableEnrichmentBolt.java

```java
package com.trading.app.random.bolts;

import com.trading.streaming.api.*;
import com.trading.app.random.config.EnrichmentConfig;
import com.trading.app.random.metrics.SimpleMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Configurable enrichment bolt with caching support.
 */
public class ConfigurableEnrichmentBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurableEnrichmentBolt.class);
    
    private final EnrichmentConfig config;
    private final SimpleMetricsCollector metricsCollector;
    private OutputCollector collector;
    private String componentId;
    private long enrichedCount = 0;
    private Map<String, CachedMetadata> cache;
    
    public ConfigurableEnrichmentBolt(EnrichmentConfig config, 
                                     SimpleMetricsCollector metricsCollector) {
        this.config = config;
        this.metricsCollector = metricsCollector;
    }
    
    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, 
                       OutputCollector collector) {
        this.collector = collector;
        this.componentId = context.getThisComponentId();
        
        if (config.isCacheEnabled()) {
            this.cache = new ConcurrentHashMap<>();
            startCacheCleanupTimer();
        }
        
        logger.info("ConfigurableEnrichmentBolt prepared: componentId={}, config={}", 
                   componentId, config);
    }
    
    @Override
    public void execute(Tuple input) {
        try {
            Integer original = input.getInteger(0);
            Integer processed = input.getInteger(1);
            Long timestamp = input.getLong(2);
            Long sequence = input.getLong(3);
            
            Map<String, Object> metadata = enrichData(original, processed, sequence);
            
            collector.emit(input, Arrays.asList(original, processed, metadata, timestamp, sequence));
            collector.ack(input);
            
            enrichedCount++;
            metricsCollector.incrementCounter("bolt.enrichment.enriched");
            
            if (enrichedCount % 100 == 0) {
                logger.debug("Enriched {} tuples", enrichedCount);
            }
            
        } catch (Exception e) {
            logger.error("Error enriching tuple", e);
            metricsCollector.incrementCounter("bolt.enrichment.errors");
            collector.fail(input);
        }
    }
    
    private Map<String, Object> enrichData(Integer original, Integer processed, Long sequence) {
        String cacheKey = "key-" + (original % 100); // Simple cache key strategy
        
        // Check cache if enabled
        if (config.isCacheEnabled() && cache != null) {
            CachedMetadata cached = cache.get(cacheKey);
            if (cached != null && !cached.isExpired()) {
                metricsCollector.incrementCounter("bolt.enrichment.cache.hits");
                return cached.metadata;
            }
            metricsCollector.incrementCounter("bolt.enrichment.cache.misses");
        }
        
        // Create enrichment metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("enrichedBy", componentId);
        metadata.put("enrichedAt", System.currentTimeMillis());
        metadata.put("source", config.getSource());
        metadata.put("category", categorize(processed));
        metadata.put("sequenceNumber", enrichedCount);
        metadata.put("originalValue", original);
        metadata.put("processedValue", processed);
        metadata.put("sequence", sequence);
        metadata.put("tags", Arrays.asList("random", "processed", categorize(processed)));
        
        // Add to cache if enabled
        if (config.isCacheEnabled() && cache != null) {
            cache.put(cacheKey, new CachedMetadata(metadata, config.getCacheTtlSeconds()));
        }
        
        return metadata;
    }
    
    private String categorize(Integer value) {
        if (value < 100) return "VERY_LOW";
        if (value < 500) return "LOW";
        if (value < 1000) return "MEDIUM";
        if (value < 1500) return "HIGH";
        return "VERY_HIGH";
    }
    
    private void startCacheCleanupTimer() {
        Thread cleanupThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(60000); // Clean every minute
                    cleanCache();
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        cleanupThread.setDaemon(true);
        cleanupThread.setName("EnrichmentCache-Cleanup");
        cleanupThread.start();
    }
    
    private void cleanCache() {
        if (cache == null) return;
        
        int before = cache.size();
        cache.entrySet().removeIf(entry -> entry.getValue().isExpired());
        int after = cache.size();
        
        if (before != after) {
            logger.debug("Cache cleanup: removed {} expired entries ({} -> {})", 
                        before - after, before, after);
        }
    }
    
    @Override
    public void cleanup() {
        logger.info("Bolt cleanup. Total enriched: {}", enrichedCount);
        if (cache != null) {
            logger.info("Final cache size: {}", cache.size());
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("original", "processed", config.getFieldName(), 
                                   "timestamp", "sequence"));
    }
    
    private static class CachedMetadata {
        final Map<String, Object> metadata;
        final long expiryTime;
        
        CachedMetadata(Map<String, Object> metadata, int ttlSeconds) {
            this.metadata = metadata;
            this.expiryTime = System.currentTimeMillis() + (ttlSeconds * 1000L);
        }
        
        boolean isExpired() {
            return System.currentTimeMillis() > expiryTime;
        }
    }
}
```


### ConfigurableLoggerBolt.java

```java
package com.trading.app.random.bolts;

import com.trading.streaming.api.*;
import com.trading.app.random.config.LoggerConfig;
import com.trading.app.random.metrics.SimpleMetricsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Configurable logger bolt with formatting options.
 */
public class ConfigurableLoggerBolt implements IRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurableLoggerBolt.class);
    
    private final LoggerConfig config;
    private final SimpleMetricsCollector metricsCollector;
    private OutputCollector collector;
    private long loggedCount = 0;
    private SimpleDateFormat dateFormat;
    
    public ConfigurableLoggerBolt(LoggerConfig config, 
                                 SimpleMetricsCollector metricsCollector) {
        this.config = config;
        this.metricsCollector = metricsCollector;
    }
    
    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, 
                       OutputCollector collector) {
        this.collector = collector;
        this.dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        
        logger.info("ConfigurableLoggerBolt prepared with config: {}", config);
    }
    
    @Override
    public void execute(Tuple input) {
        try {
            loggedCount++;
            
            if (config.isPrettyPrint()) {
                logPretty(input);
            } else {
                logSimple(input);
            }
            
            metricsCollector.incrementCounter("bolt.logger.logged");
            
            collector.ack(input);
            
        } catch (Exception e) {
            logger.error("Error logging tuple", e);
            metricsCollector.incrementCounter("bolt.logger.errors");
            collector.fail(input);
        }
    }
    
    private void logSimple(Tuple input) {
        StringBuilder sb = new StringBuilder();
        sb.append(config.getPrefix()).append(" ");
        
        if (config.isIncludeTimestamp()) {
            sb.append("[").append(dateFormat.format(new Date())).append("] ");
        }
        
        sb.append("[#").append(loggedCount).append("] ");
        sb.append(input.getSourceComponent()).append(" -> ");
        sb.append(input.getValues());
        
        logAtLevel(sb.toString());
    }
    
    private void logPretty(Tuple input) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n").append("=".repeat(80)).append("\n");
        sb.append(config.getPrefix()).append(" [#").append(loggedCount).append("]\n");
        sb.append("=".repeat(80)).append("\n");
        
        if (config.isIncludeTimestamp()) {
            sb.append("  Time:   ").append(dateFormat.format(new Date())).append("\n");
        }
        
        sb.append("  Source: ").append(input.getSourceComponent()).append("\n");
        sb.append("  Stream: ").append(input.getSourceStreamId()).append("\n");
        sb.append("  Size:   ").append(input.size()).append("\n");
        sb.append("  Values:\n");
        
        for (int i = 0; i < input.size(); i++) {
            Object value = input.getValue(i);
            String typeName = value != null ? value.getClass().getSimpleName() : "null";
            String valueStr = formatValue(value);
            sb.append(String.format("    [%d] %-15s : %s\n", i, typeName, valueStr));
        }
        
        sb.append("=".repeat(80));
        
        logAtLevel(sb.toString());
    }
    
    private String formatValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof Map) {
            return formatMap((Map<?, ?>) value);
        }
        if (value instanceof Date || value instanceof Long) {
            return value.toString();
        }
        return value.toString();
    }
    
    private String formatMap(Map<?, ?> map) {
        if (map.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{\n");
        map.forEach((key, val) -> 
            sb.append("        ").append(key).append(": ").append(val).append("\n"));
        sb.append("      }");
        return sb.toString();
    }
    
    private void logAtLevel(String message) {
        switch (config.getLogLevel().toUpperCase()) {
            case "TRACE":
                logger.trace(message);
                break;
            case "DEBUG":
                logger.debug(message);
                break;
            case "INFO":
                logger.info(message);
                break;
            case "WARN":
                logger.warn(message);
                break;
            case "ERROR":
                logger.error(message);
                break;
            default:
                logger.info(message);
        }
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


## 7. Enhanced Application Class

### EnhancedRandomDataApplication.java

```java
package com.trading.app.random;

import com.trading.streaming.config.PropertyResolver;
import com.trading.streaming.config.TopologyLoader;
import com.trading.streaming.impl.LocalStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Enhanced Random Data Application with property substitution support.
 */
public class EnhancedRandomDataApplication {
    private static final Logger logger = LoggerFactory.getLogger(EnhancedRandomDataApplication.class);
    
    public static void main(String[] args) {
        logger.info("Starting Enhanced Random Data Topology...");
        
        // Create context
        LocalStreamingContext context = new LocalStreamingContext();
        TopologyLoader loader = new TopologyLoader(context);
        
        // Load topology based on command line args or defaults
        if (args.length >= 2) {
            // Custom property file and topology file
            String propertiesFile = args[0];
            String topologyFile = args[1];
            logger.info("Loading custom configuration: {} and {}", propertiesFile, topologyFile);
            loader.loadTopology(topologyFile, propertiesFile);
        } else if (args.length == 1) {
            // Custom property file, default topology
            String propertiesFile = args[0];
            logger.info("Loading custom properties: {}", propertiesFile);
            loader.getPropertyResolver().loadPropertiesFromResource(propertiesFile);
            loader.loadTopology("/topology.yml");
        } else {
            // Default: auto-load topology.properties and topology.yml
            logger.info("Loading default configuration");
            loader.loadTopology();
        }
        
        // Display loaded properties
        PropertyResolver resolver = loader.getPropertyResolver();
        logger.info("Loaded {} properties", resolver.getPropertyCount());
        
        // Log some key properties
        logProperty(resolver, "app.name");
        logProperty(resolver, "app.environment");
        logProperty(resolver, "generator.min.value");
        logProperty(resolver, "generator.max.value");
        logProperty(resolver, "parallelism.bolt.processor");
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Enhanced Random Data Topology...");
            context.stop();
            logger.info("Topology stopped successfully");
        }));
        
        logger.info("=".repeat(80));
        logger.info("Enhanced Random Data Topology started successfully!");
        logger.info("Press Ctrl+C to stop");
        logger.info("=".repeat(80));
    }
    
    private static void logProperty(PropertyResolver resolver, String key) {
        String value = resolver.getProperty(key);
        if (value != null) {
            logger.info("  {} = {}", key, value);
        }
    }
}
```


## 8. Build Configuration

### storm-app-random-data/build.gradle (Enhanced)

```gradle
plugins {
    id 'application'
    id 'distribution'
}

description = 'Enhanced Random Data Generation Streaming Application'

dependencies {
    implementation project(':storm-framework-core')
}

application {
    mainClass = 'com.trading.app.random.EnhancedRandomDataApplication'
    applicationName = 'enhanced-random-data-app'
    
    applicationDefaultJvmArgs = [
        '-Xmx1g',
        '-Xms512m',
        '-XX:+UseG1GC',
        '-XX:MaxGCPauseMillis=200',
        '-Dfile.encoding=UTF-8'
    ]
}

// Run with default configuration
tasks.register('runDefault', JavaExec) {
    group = 'application'
    description = 'Run with default topology.properties and topology.yml'
    mainClass = application.mainClass
    classpath = sourceSets.main.runtimeClasspath
    jvmArgs = application.applicationDefaultJvmArgs
}

// Run with custom properties
tasks.register('runWithProperties', JavaExec) {
    group = 'application'
    description = 'Run with custom properties file'
    mainClass = application.mainClass
    classpath = sourceSets.main.runtimeClasspath
    jvmArgs = application.applicationDefaultJvmArgs
    
    def propsFile = project.findProperty('props') ?: 'topology.properties'
    args = [propsFile]
}

// Run with environment-specific config
tasks.register('runDev', JavaExec) {
    group = 'application'
    description = 'Run with development configuration'
    mainClass = application.mainClass
    classpath = sourceSets.main.runtimeClasspath
    jvmArgs = application.applicationDefaultJvmArgs
    args = ['dev.properties', 'topology.yml']
}

tasks.register('runProd', JavaExec) {
    group = 'application'
    description = 'Run with production configuration'
    mainClass = application.mainClass
    classpath = sourceSets.main.runtimeClasspath
    jvmArgs = application.applicationDefaultJvmArgs
    args = ['prod.properties', 'topology.yml']
}

// Create fat JAR
tasks.register('fatJar', Jar) {
    group = 'build'
    description = 'Create a fat JAR with all dependencies'
    archiveBaseName = "${project.name}-all"
    archiveClassifier = 'all'
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    
    manifest {
        attributes(
            'Main-Class': application.mainClass.get(),
            'Implementation-Title': project.description,
            'Implementation-Version': project.version
        )
    }
    
    from {
        configurations.runtimeClasspath.collect { 
            it.isDirectory() ? it : zipTree(it) 
        }
    }
    
    with jar
    exclude 'META-INF/*.RSA', 'META-INF/*.SF', 'META-INF/*.DSA'
}
```


## 9. Running the Application

```bash
# Run with default configuration
./gradlew :storm-app-random-data:run

# Run with custom properties
./gradlew :storm-app-random-data:runWithProperties -Pprops=custom.properties

# Run with development config
./gradlew :storm-app-random-data:runDev

# Run with production config
./gradlew :storm-app-random-data:runProd

# Run with system property overrides
./gradlew :storm-app-random-data:run \
  -Dgenerator.max.value=5000 \
  -Dparallelism.bolt.processor=8 \
  -Dlogger.pretty.print=true

# Build and run fat JAR
./gradlew :storm-app-random-data:fatJar
java -jar storm-app-random-data/build/libs/storm-app-random-data-all-1.0.0.jar

# Run fat JAR with custom properties
java -jar storm-app-random-data-all-1.0.0.jar custom.properties topology.yml
```

This comprehensive implementation provides:

- ✅ Full property substitution support
- ✅ 8+ configurable components
- ✅ Environment-specific configurations
- ✅ Advanced features (caching, windowing, filtering)
- ✅ Metrics collection and reporting
- ✅ Flexible deployment options

