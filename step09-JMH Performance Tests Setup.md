<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

## JMH Performance Tests Setup

### 1. Project Structure

```
storm-framework-parent/
├── storm-framework-core/
│   ├── src/
│   │   ├── main/java/
│   │   ├── test/java/
│   │   └── jmh/java/                          # JMH benchmarks
│   │       └── com/trading/streaming/
│   │           └── benchmarks/
│   │               ├── ThroughputBenchmark.java
│   │               ├── LatencyBenchmark.java
│   │               ├── SerializationBenchmark.java
│   │               └── MemoryBenchmark.java
│   └── build.gradle
├── storm-app-random-data/
│   ├── src/
│   │   ├── main/java/
│   │   ├── test/java/
│   │   └── jmh/java/                          # App-specific benchmarks
│   │       └── com/trading/app/random/
│   │           └── benchmarks/
│   │               ├── RandomDataBenchmark.java
│   │               └── ProcessingStrategyBenchmark.java
│   └── build.gradle
└── settings.gradle
```


### 2. Enhanced build.gradle (storm-framework-core)

```gradle
plugins {
    id 'java-library'
    id 'me.champeau.jmh' version '0.7.2'  // JMH plugin
}

group = 'com.trading.streaming'
version = '1.0.0'

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

dependencies {
    // Main dependencies
    implementation 'org.slf4j:slf4j-api:2.0.7'
    implementation 'ch.qos.logback:logback-classic:1.4.11'
    implementation 'com.fasterxml.jackson.core:jackson-databind:2.15.2'
    implementation 'com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.15.2'
    
    // Test dependencies
    testImplementation 'org.junit.jupiter:junit-jupiter:5.9.3'
    testImplementation 'org.mockito:mockito-core:5.3.1'
    testImplementation 'org.mockito:mockito-junit-jupiter:5.3.1'
    testImplementation 'org.awaitility:awaitility:4.2.0'
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
    
    // JMH dependencies (automatically added by plugin, but can be explicit)
    jmh 'org.openjdk.jmh:jmh-core:1.37'
    jmh 'org.openjdk.jmh:jmh-generator-annprocess:1.37'
}

// JMH Configuration
jmh {
    // JMH version
    jmhVersion = '1.37'
    
    // Benchmark mode (default: Throughput)
    // includes = ['.*Benchmark.*']  // Regex to include benchmarks
    // excludes = ['.*OldBenchmark.*']  // Regex to exclude benchmarks
    
    // Number of benchmark iterations
    iterations = 3
    
    // Number of warmup iterations
    warmupIterations = 2
    
    // Number of benchmark forks
    fork = 2
    
    // JVM arguments
    jvmArgs = ['-Xms2g', '-Xmx2g']
    
    // Output format: JSON, CSV, TEXT
    resultsFormat = 'JSON'
    
    // Results file
    resultFormat = 'JSON'
    resultsFile = project.file("${buildDir}/reports/jmh/results.json")
    
    // Human-readable output
    humanOutputFile = project.file("${buildDir}/reports/jmh/human.txt")
    
    // Timeout per iteration
    timeOnIteration = '10s'
    
    // Time unit for results
    timeUnit = 'ms'
    
    // Profilers: 'gc', 'stack', 'cl', 'comp', 'hs_rt', 'hs_thr'
    // profilers = ['gc', 'stack']
    
    // Duplicate class strategy
    duplicateClassesStrategy = DuplicatesStrategy.WARN
    
    // Zip results
    zip64 = true
}

// Task to run all benchmarks
tasks.register('benchmarkAll') {
    group = 'benchmark'
    description = 'Run all JMH benchmarks'
    dependsOn jmh
}

// Task to run specific benchmark
tasks.register('benchmarkThroughput', JavaExec) {
    group = 'benchmark'
    description = 'Run throughput benchmarks only'
    dependsOn jmhClasses
    
    classpath = sourceSets.jmh.runtimeClasspath
    mainClass = 'org.openjdk.jmh.Main'
    args = ['.*ThroughputBenchmark.*', '-rf', 'json']
}

tasks.register('benchmarkLatency', JavaExec) {
    group = 'benchmark'
    description = 'Run latency benchmarks only'
    dependsOn jmhClasses
    
    classpath = sourceSets.jmh.runtimeClasspath
    mainClass = 'org.openjdk.jmh.Main'
    args = ['.*LatencyBenchmark.*', '-rf', 'json']
}

// Quick benchmark (fewer iterations for development)
tasks.register('benchmarkQuick', JavaExec) {
    group = 'benchmark'
    description = 'Run quick benchmarks for development'
    dependsOn jmhClasses
    
    classpath = sourceSets.jmh.runtimeClasspath
    mainClass = 'org.openjdk.jmh.Main'
    args = [
        '.*Benchmark.*',
        '-i', '1',           // 1 iteration
        '-wi', '1',          // 1 warmup iteration
        '-f', '1',           // 1 fork
        '-rf', 'text'        // Text format
    ]
}

test {
    useJUnitPlatform()
    
    testLogging {
        events "passed", "skipped", "failed"
        exceptionFormat "full"
        showStandardStreams = false
    }
    
    maxParallelForks = Runtime.runtime.availableProcessors().intdiv(2) ?: 1
}
```


### 3. Example JMH Benchmarks

#### ThroughputBenchmark.java

```java
package com.trading.streaming.benchmarks;

import com.trading.streaming.api.*;
import com.trading.streaming.impl.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark for measuring streaming throughput.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class ThroughputBenchmark {
    
    private LocalStreamingContext context;
    private AtomicLong counter;
    
    @Setup(Level.Trial)
    public void setup() {
        context = new LocalStreamingContext();
        counter = new AtomicLong(0);
        
        // Setup topology
        SimpleSpout spout = new SimpleSpout(counter);
        SimpleBolt bolt = new SimpleBolt();
        
        Fields fields = new Fields("value");
        Map<String, List<String>> subscriptions = new HashMap<>();
        subscriptions.put("spout", Arrays.asList("default"));
        
        context.registerSpout("spout", spout, fields, 4);
        context.registerBolt("bolt", bolt, fields, 4, subscriptions);
        context.start();
    }
    
    @TearDown(Level.Trial)
    public void tearDown() {
        if (context != null) {
            context.stop();
        }
    }
    
    @Benchmark
    public void measureThroughput(Blackhole blackhole) throws InterruptedException {
        Thread.sleep(100);
        blackhole.consume(counter.get());
    }
    
    // Helper classes
    private static class SimpleSpout implements IRichSpout {
        private final AtomicLong counter;
        private SpoutOutputCollector collector;
        private boolean active;
        
        SimpleSpout(AtomicLong counter) {
            this.counter = counter;
        }
        
        @Override
        public void open(Map<String, Object> conf, TopologyContext context, 
                        SpoutOutputCollector collector) {
            this.collector = collector;
        }
        
        @Override
        public void nextTuple() {
            if (active) {
                collector.emit(Arrays.asList(counter.incrementAndGet()));
            }
        }
        
        @Override
        public void activate() { active = true; }
        @Override
        public void deactivate() { active = false; }
        @Override
        public void ack(Object msgId) {}
        @Override
        public void fail(Object msgId) {}
        @Override
        public void close() {}
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("value"));
        }
    }
    
    private static class SimpleBolt implements IRichBolt {
        private OutputCollector collector;
        
        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context, 
                           OutputCollector collector) {
            this.collector = collector;
        }
        
        @Override
        public void execute(Tuple input) {
            collector.ack(input);
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


#### LatencyBenchmark.java

```java
package com.trading.streaming.benchmarks;

import com.trading.streaming.impl.TupleImpl;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for measuring tuple processing latency.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class LatencyBenchmark {
    
    private TupleImpl tuple;
    
    @Setup(Level.Trial)
    public void setup() {
        tuple = new TupleImpl(
            "test-component",
            "default",
            Arrays.asList("value1", 123, true),
            Arrays.asList("field1", "field2", "field3"),
            12345L
        );
    }
    
    @Benchmark
    public void measureTupleCreation(Blackhole blackhole) {
        TupleImpl newTuple = new TupleImpl(
            "component",
            "stream",
            Arrays.asList("data", 42, false),
            Arrays.asList("f1", "f2", "f3"),
            System.currentTimeMillis()
        );
        blackhole.consume(newTuple);
    }
    
    @Benchmark
    public void measureTupleAccess(Blackhole blackhole) {
        String value = tuple.getString(0);
        Integer number = tuple.getInteger(1);
        Boolean flag = tuple.getBoolean(2);
        
        blackhole.consume(value);
        blackhole.consume(number);
        blackhole.consume(flag);
    }
    
    @Benchmark
    public void measureFieldLookup(Blackhole blackhole) {
        String value = tuple.getStringByField("field1");
        Integer number = tuple.getIntegerByField("field2");
        Boolean flag = tuple.getBooleanByField("field3");
        
        blackhole.consume(value);
        blackhole.consume(number);
        blackhole.consume(flag);
    }
}
```


#### ProcessingStrategyBenchmark.java

```java
package com.trading.app.random.benchmarks;

import com.trading.app.random.strategy.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * Benchmark for processing strategies.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class ProcessingStrategyBenchmark {
    
    private ProcessingStrategy multiplyStrategy;
    private ProcessingStrategy addStrategy;
    private ProcessingStrategy squareStrategy;
    private ProcessingStrategy compositeStrategy;
    
    @Param({"10", "100", "1000"})
    private int inputValue;
    
    @Setup
    public void setup() {
        multiplyStrategy = new MultiplyStrategy(2);
        addStrategy = new AddStrategy(10);
        squareStrategy = new SquareStrategy();
        compositeStrategy = new CompositeStrategy(
            new MultiplyStrategy(2),
            new AddStrategy(10),
            new SquareStrategy()
        );
    }
    
    @Benchmark
    public void multiplyStrategy(Blackhole blackhole) {
        Integer result = multiplyStrategy.process(inputValue);
        blackhole.consume(result);
    }
    
    @Benchmark
    public void addStrategy(Blackhole blackhole) {
        Integer result = addStrategy.process(inputValue);
        blackhole.consume(result);
    }
    
    @Benchmark
    public void squareStrategy(Blackhole blackhole) {
        Integer result = squareStrategy.process(inputValue);
        blackhole.consume(result);
    }
    
    @Benchmark
    public void compositeStrategy(Blackhole blackhole) {
        Integer result = compositeStrategy.process(inputValue);
        blackhole.consume(result);
    }
}
```


### 4. Running JMH Benchmarks

#### Command Line Options

```bash
# Run all benchmarks
./gradlew jmh

# Run all benchmarks with custom output
./gradlew jmh --args='-rf json -rff results.json'

# Run specific benchmark
./gradlew jmh --args='ThroughputBenchmark'

# Run benchmarks matching pattern
./gradlew jmh --args='.*Latency.*'

# Run with custom iterations and forks
./gradlew jmh --args='-i 3 -wi 2 -f 1'

# Run quick benchmark for development
./gradlew benchmarkQuick

# Run specific benchmark types
./gradlew benchmarkThroughput
./gradlew benchmarkLatency

# List available benchmarks
./gradlew jmh --args='-l'

# Get help
./gradlew jmh --args='-h'

# Run with profilers
./gradlew jmh --args='-prof gc'
./gradlew jmh --args='-prof stack'

# Run with specific JVM args
./gradlew jmh --args='-jvmArgs "-Xms4g -Xmx4g"'

# Detailed output
./gradlew jmh --args='-v EXTRA'
```


#### JMH Command Line Arguments Reference

```bash
# Benchmark selection
-l                          # List benchmarks
<regex>                     # Run benchmarks matching regex

# Execution control
-i <n>                      # Number of measurement iterations
-wi <n>                     # Number of warmup iterations
-f <n>                      # Number of forks
-t <n>                      # Number of threads
-to <time>                  # Timeout per iteration

# Output control
-rf <format>                # Result format: json, csv, scsv, text
-rff <file>                 # Result file
-o <file>                   # Human-readable output file
-v <level>                  # Verbosity: SILENT, NORMAL, EXTRA

# Profilers
-prof <profiler>            # Run with profiler
-prof gc                    # GC profiler
-prof stack                 # Stack profiler
-prof cl                    # Classloader profiler
-prof comp                  # JIT compiler profiler

# Mode control
-bm <mode>                  # Benchmark mode: Throughput, AverageTime, etc.
-tu <unit>                  # Time unit: ns, us, ms, s
```


### 5. Gradle Tasks Summary

```bash
# All benchmarks
./gradlew jmh
./gradlew benchmarkAll

# Quick development benchmark
./gradlew benchmarkQuick

# Specific benchmarks
./gradlew benchmarkThroughput
./gradlew benchmarkLatency

# Clean JMH artifacts
./gradlew cleanJmh

# View benchmark reports
ls -la storm-framework-core/build/reports/jmh/
cat storm-framework-core/build/reports/jmh/human.txt
```


### 6. CI/CD Integration

#### GitHub Actions Workflow

```yaml
name: Performance Benchmarks

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]
  schedule:
    - cron: '0 0 * * 0'  # Weekly

jobs:
  benchmark:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up JDK 17
      uses: actions/setup-java@v3
      with:
        java-version: '17'
        distribution: 'temurin'
        
    - name: Cache Gradle packages
      uses: actions/cache@v3
      with:
        path: ~/.gradle/caches
        key: ${{ runner.os }}-gradle-${{ hashFiles('**/*.gradle') }}
        
    - name: Run benchmarks
      run: ./gradlew jmh
      
    - name: Upload benchmark results
      uses: actions/upload-artifact@v3
      with:
        name: benchmark-results
        path: |
          **/build/reports/jmh/results.json
          **/build/reports/jmh/human.txt
```


### 7. View Results

```bash
# View human-readable results
cat build/reports/jmh/human.txt

# View JSON results
cat build/reports/jmh/results.json

# Parse JSON with jq
cat build/reports/jmh/results.json | jq '.[] | {benchmark: .benchmark, score: .primaryMetric.score}'
```


### Summary

**Project Structure:**

- `src/jmh/java/` - JMH benchmark source files
- `build/reports/jmh/` - Benchmark results

**Run Benchmarks:**

```bash
./gradlew jmh                    # All benchmarks (production)
./gradlew benchmarkQuick         # Quick run (development)
./gradlew benchmarkThroughput    # Specific benchmark
```

**Best Practices:**

- Keep benchmarks separate from unit tests
- Use appropriate warmup and measurement iterations
- Run on dedicated hardware for consistent results
- Use `Blackhole` to prevent dead code elimination
- Profile with `-prof gc` to understand GC impact
<span style="display:none">[^1]</span>

<div align="center">⁂</div>

[^1]: Master-Implementation-Prompt_-Apache-Storm-Compati.md

