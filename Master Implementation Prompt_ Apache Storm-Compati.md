<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Master Implementation Prompt: Apache Storm-Compatible Streaming Framework

**Complete Specification for Spring Boot 4 + Gradle Implementation**

***

# PHASE 1: Core Framework Foundation

## Step 1.1: Project Structure \& Dependencies

**Objective:** Create Spring Boot 4 project with all required dependencies

**build.gradle:**

```gradle
plugins {
    id 'java'
    id 'org.springframework.boot' version '4.0.0'
    id 'io.spring.dependency-management' version '1.1.7'
}

group = 'com.trading'
version = '1.0.0'
sourceCompatibility = '17'

repositories {
    mavenCentral()
}

dependencies {
    // Spring Boot Core
    implementation 'org.springframework.boot:spring-boot-starter'
    implementation 'org.springframework.boot:spring-boot-starter-logging'
    implementation 'org.springframework.boot:spring-boot-starter-actuator'
    
    // YAML Configuration
    implementation 'com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.16.0'
    implementation 'com.fasterxml.jackson.core:jackson-databind:2.16.0'
    implementation 'org.springframework.boot:spring-boot-configuration-processor'
    
    // JMS Support
    implementation 'org.springframework.boot:spring-boot-starter-artemis'
    implementation 'jakarta.jms:jakarta.jms-api:3.1.0'
    implementation 'org.apache.activemq:artemis-jakarta-client:2.31.2'
    
    // Testing
    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testImplementation 'org.apache.activemq:artemis-jakarta-server:2.31.2'
    testImplementation 'org.awaitility:awaitility:4.2.0'
    testImplementation 'org.mockito:mockito-core:5.8.0'
    
    // Performance Testing
    testImplementation 'org.openjdk.jmh:jmh-core:1.37'
    testImplementation 'org.openjdk.jmh:jmh-generator-annprocess:1.37'
    testImplementation 'io.micrometer:micrometer-registry-prometheus:1.12.0'
    testImplementation 'org.openjdk.jol:jol-core:0.17'
}

tasks.named('test') {
    useJUnitPlatform()
    jvmArgs '-Xmx2g'
}
```

**Package Structure:**

```
com.trading/
├── Application.java
├── streaming/
│   ├── api/                    [Storm API Interfaces]
│   │   ├── Fields.java
│   │   ├── IComponent.java
│   │   ├── IRichBolt.java
│   │   ├── IRichSpout.java
│   │   ├── OutputCollector.java
│   │   ├── OutputFieldsDeclarer.java
│   │   ├── SpoutOutputCollector.java
│   │   ├── StreamingContext.java
│   │   ├── TopologyContext.java
│   │   └── Tuple.java
│   └── impl/                   [Framework Implementation]
│       ├── BoltExecutor.java
│       ├── LocalStreamingContext.java
│       ├── OutputFieldsDeclarerImpl.java
│       ├── SpoutExecutor.java
│       └── TupleImpl.java
├── config/                     [YAML Configuration]
│   ├── BoltConfig.java
│   ├── InputStreamConfig.java
│   ├── SpoutConfig.java
│   ├── TopologyConfig.java
│   └── TopologyLoader.java
├── spouts/                     [Example Spouts]
│   ├── JmsJsonSpout.java
│   ├── RandomNumberGeneratorSpout.java
│   ├── RandomNumberGeneratorMultiStreamSpout.java
│   ├── RandomStringGeneratorSpout.java
│   └── RandomSentenceGeneratorSpout.java
└── bolts/                      [Example Bolts]
    ├── JsonToMapBolt.java
    ├── MapLoggerBolt.java
    ├── RandomNumberProcessorBolt1.java
    ├── RandomNumberProcessorBolt2.java
    ├── RandomNumberProcessorBolt1WithMultiStream.java
    ├── RandomNumberProcessorStream1Bolt.java
    ├── RandomNumberProcessorStream2Bolt.java
    ├── RandomStringProcessorBolt1.java
    ├── RandomStringProcessorBolt2.java
    ├── RandomSentenceProcessorBolt1.java
    └── RandomSentenceProcessorBolt2.java
```

**Success Criteria:**

- ✅ Project compiles without errors
- ✅ All dependencies resolve correctly
- ✅ Package structure matches specification

***

## Step 1.2: Storm API Interfaces (Package: `streaming.api`)

**Objective:** Create 100% compatible Storm API replacements

### A. IRichSpout.java

```java
package com.trading.streaming.api;

import java.util.Map;

public interface IRichSpout extends IComponent {
    void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector);
    void close();
    void activate();
    void deactivate();
    void nextTuple();
    void ack(Object msgId);
    void fail(Object msgId);
}
```


### B. IRichBolt.java

```java
package com.trading.streaming.api;

import java.util.Map;

public interface IRichBolt extends IComponent {
    void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector);
    void execute(Tuple input);
    void cleanup();
}
```


### C. IComponent.java

```java
package com.trading.streaming.api;

import java.util.Map;

public interface IComponent {
    void declareOutputFields(OutputFieldsDeclarer declarer);
    default Map<String, Object> getComponentConfiguration() { return null; }
}
```


### D. Tuple.java (Full Interface)

```java
package com.trading.streaming.api;

import java.util.List;

public interface Tuple {
    // Size and containment
    int size();
    boolean contains(String field);
    
    // By index getters
    Object getValue(int i);
    String getString(int i);
    Integer getInteger(int i);
    Long getLong(int i);
    Boolean getBoolean(int i);
    Short getShort(int i);
    Byte getByte(int i);
    Double getDouble(int i);
    Float getFloat(int i);
    byte[] getBinary(int i);
    
    // By field name getters
    Object getValueByField(String field);
    String getStringByField(String field);
    Integer getIntegerByField(String field);
    Long getLongByField(String field);
    Boolean getBooleanByField(String field);
    Short getShortByField(String field);
    Byte getByteByField(String field);
    Double getDoubleByField(String field);
    Float getFloatByField(String field);
    byte[] getBinaryByField(String field);
    
    // Metadata
    List<Object> getValues();
    String getSourceComponent();
    String getSourceStreamId();
    Object getMessageId();
}
```


### E. SpoutOutputCollector.java

```java
package com.trading.streaming.api;

import java.util.List;

public class SpoutOutputCollector {
    private final String sourceComponentId;
    private final StreamingContext context;
    private final List<String> outputFields;
    
    public SpoutOutputCollector(String sourceComponentId, StreamingContext context, 
                                List<String> outputFields) {
        this.sourceComponentId = sourceComponentId;
        this.context = context;
        this.outputFields = outputFields;
    }
    
    public List<Integer> emit(List<Object> tuple);
    public List<Integer> emit(List<Object> tuple, Object messageId);
    public List<Integer> emit(String streamId, List<Object> tuple);
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId);
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId);
    public void reportError(Throwable error);
}
```


### F. OutputCollector.java

```java
package com.trading.streaming.api;

import java.util.List;

public class OutputCollector {
    private final String sourceComponentId;
    private final StreamingContext context;
    private final List<String> outputFields;
    
    public OutputCollector(String sourceComponentId, StreamingContext context, 
                          List<String> outputFields) {
        this.sourceComponentId = sourceComponentId;
        this.context = context;
        this.outputFields = outputFields;
    }
    
    public List<Integer> emit(Tuple anchor, List<Object> tuple);
    public List<Integer> emit(List<Object> tuple);
    public List<Integer> emit(String streamId, Tuple anchor, List<Object> tuple);
    public List<Integer> emit(String streamId, List<Object> tuple);
    public void emitDirect(int taskId, String streamId, Tuple anchor, List<Object> tuple);
    public void ack(Tuple input);
    public void fail(Tuple input);
    public void reportError(Throwable error);
}
```


### G. TopologyContext.java

```java
package com.trading.streaming.api;

import java.util.Map;

public class TopologyContext {
    private final String topologyId;
    private final Map<String, Object> stormConf;
    private final String componentId;
    private final Integer taskId;
    
    public TopologyContext(String topologyId, Map<String, Object> stormConf, 
                          String componentId, Integer taskId) {
        this.topologyId = topologyId;
        this.stormConf = stormConf != null ? stormConf : new HashMap<>();
        this.componentId = componentId;
        this.taskId = taskId;
    }
    
    public String getThisTopologyId();
    public String getThisComponentId();
    public Integer getThisTaskId();
    public Map<String, Object> getStormConf();
}
```


### H. Fields.java

```java
package com.trading.streaming.api;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class Fields implements Serializable {
    private final List<String> fields;
    
    public Fields(String... fields) {
        this.fields = Arrays.asList(fields);
    }
    
    public Fields(List<String> fields) {
        this.fields = new ArrayList<>(fields);
    }
    
    public List<String> toList();
    public int size();
    public String get(int index);
    public int fieldIndex(String field);
    public boolean contains(String field);
}
```


### I. OutputFieldsDeclarer.java

```java
package com.trading.streaming.api;

public interface OutputFieldsDeclarer {
    void declare(Fields fields);
    void declareStream(String streamId, Fields fields);
    void declareStream(String streamId, boolean direct, Fields fields);
}
```


### J. StreamingContext.java

```java
package com.trading.streaming.api;

import com.trading.streaming.impl.TupleImpl;

public interface StreamingContext {
    void emit(String sourceComponentId, String streamId, TupleImpl tuple);
    void ack(String componentId, Object messageId);
    void fail(String componentId, Object messageId);
    void reportError(String componentId, Throwable error);
    boolean isRunning();
}
```

**Success Criteria:**

- ✅ All 10 API files compile without errors
- ✅ Interface signatures match Apache Storm exactly
- ✅ JavaDoc comments on all public methods
- ✅ No external Storm dependencies

***

## Step 1.3: Core Implementation Classes (Package: `streaming.impl`)

**Objective:** Implement framework runtime engine

### A. TupleImpl.java (Complete Implementation)

```java
package com.trading.streaming.impl;

import com.trading.streaming.api.Tuple;
import java.util.*;

public class TupleImpl implements Tuple {
    private final List<Object> values;
    private final Map<String, Integer> fieldNameToIndex;
    private final String sourceComponent;
    private final String sourceStreamId;
    private final Object messageId;
    
    public TupleImpl(String sourceComponent, String sourceStreamId, 
                     List<Object> values, List<String> fieldNames, Object messageId) {
        this.sourceComponent = sourceComponent;
        this.sourceStreamId = sourceStreamId;
        this.values = new ArrayList<>(values);
        this.messageId = messageId;
        this.fieldNameToIndex = new HashMap<>();
        
        if (fieldNames != null) {
            for (int i = 0; i < fieldNames.size(); i++) {
                fieldNameToIndex.put(fieldNames.get(i), i);
            }
        }
    }
    
    // Implement ALL 30+ methods from Tuple interface
    // Include type conversions (Integer→Long, Float→Double)
    // Include bounds checking and null handling
}
```


### B. LocalStreamingContext.java (Core Orchestrator)

```java
package com.trading.streaming.impl;

import com.trading.streaming.api.*;
import org.springframework.stereotype.Component;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class LocalStreamingContext implements StreamingContext {
    // Component storage with multiple executors
    private final Map<String, List<SpoutExecutor>> spouts = new ConcurrentHashMap<>();
    private final Map<String, List<BoltExecutor>> bolts = new ConcurrentHashMap<>();
    
    // Stream routing: (sourceComponentId, streamId) → target component IDs
    private final Map<StreamKey, List<String>> streamConnections = new ConcurrentHashMap<>();
    
    // Round-robin load balancing
    private final Map<String, AtomicInteger> executorIndexMap = new ConcurrentHashMap<>();
    
    private final ExecutorService executorService;
    private volatile boolean running = false;
    
    public LocalStreamingContext() {
        // Try virtual threads (JDK 21+), fallback to cached pool
        try {
            this.executorService = Executors.newVirtualThreadPerTaskExecutor();
        } catch (Exception e) {
            this.executorService = Executors.newCachedThreadPool();
        }
    }
    
    // Registration methods with multi-stream support
    public void registerSpout(String id, IRichSpout spout, Fields outputFields, int parallelism);
    public void registerBolt(String id, IRichBolt bolt, Fields outputFields, 
                            int parallelism, Map<String, List<String>> streamSubscriptions);
    
    // Tuple routing with round-robin load balancing
    @Override
    public void emit(String sourceComponentId, String streamId, TupleImpl tuple) {
        StreamKey key = new StreamKey(sourceComponentId, streamId);
        List<String> targetComponents = streamConnections.get(key);
        
        if (targetComponents != null) {
            for (String targetComponentId : targetComponents) {
                List<BoltExecutor> executors = bolts.get(targetComponentId);
                if (executors != null && !executors.isEmpty()) {
                    // Round-robin selection
                    int index = executorIndexMap.get(targetComponentId)
                                               .getAndIncrement() % executors.size();
                    executors.get(index).enqueue(tuple);
                }
            }
        }
    }
    
    // StreamKey inner class for precise routing
    private static class StreamKey {
        private final String componentId;
        private final String streamId;
        
        public StreamKey(String componentId, String streamId) {
            this.componentId = componentId;
            this.streamId = streamId != null ? streamId : "default";
        }
        
        @Override
        public boolean equals(Object o) { /* ... */ }
        
        @Override
        public int hashCode() { /* ... */ }
    }
}
```


### C. SpoutExecutor.java

```java
package com.trading.streaming.impl;

import com.trading.streaming.api.*;

public class SpoutExecutor implements Runnable {
    private final String executorId;
    private final String componentId;
    private final IRichSpout spout;
    private final StreamingContext context;
    private final SpoutOutputCollector collector;
    
    @Override
    public void run() {
        spout.open(conf, topologyContext, collector);
        spout.activate();
        
        while (context.isRunning()) {
            try {
                spout.nextTuple();
                Thread.sleep(10); // Configurable emit frequency
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        spout.deactivate();
        spout.close();
    }
}
```


### D. BoltExecutor.java

```java
package com.trading.streaming.impl;

import com.trading.streaming.api.*;
import java.util.concurrent.*;

public class BoltExecutor implements Runnable {
    private final String executorId;
    private final String componentId;
    private final IRichBolt bolt;
    private final BlockingQueue<Tuple> inputQueue;
    private final OutputCollector collector;
    
    public BoltExecutor(/*...*/) {
        this.inputQueue = new LinkedBlockingQueue<>(10000); // Configurable capacity
    }
    
    public void enqueue(Tuple tuple) {
        inputQueue.offer(tuple, 1, TimeUnit.SECONDS);
    }
    
    @Override
    public void run() {
        bolt.prepare(conf, topologyContext, collector);
        
        while (context.isRunning()) {
            Tuple tuple = inputQueue.poll(100, TimeUnit.MILLISECONDS);
            if (tuple != null) {
                bolt.execute(tuple);
            }
        }
        
        bolt.cleanup();
    }
}
```


### E. OutputFieldsDeclarerImpl.java

```java
package com.trading.streaming.impl;

import com.trading.streaming.api.*;

public class OutputFieldsDeclarerImpl implements OutputFieldsDeclarer {
    private final Map<String, Fields> streamToFields = new HashMap<>();
    private final Map<String, Boolean> streamToDirect = new HashMap<>();
    
    @Override
    public void declare(Fields fields) {
        declareStream("default", false, fields);
    }
    
    @Override
    public void declareStream(String streamId, Fields fields) {
        declareStream(streamId, false, fields);
    }
    
    @Override
    public void declareStream(String streamId, boolean direct, Fields fields) {
        streamToFields.put(streamId, fields);
        streamToDirect.put(streamId, direct);
    }
    
    public Fields getFieldsFor(String streamId) {
        return streamToFields.get(streamId);
    }
}
```

**Success Criteria:**

- ✅ All implementation files compile
- ✅ Thread-safe concurrent collections used
- ✅ Virtual threads support with fallback
- ✅ Proper lifecycle management (open/prepare/close/cleanup)
- ✅ Round-robin load balancing implemented

***

# PHASE 2: YAML Configuration Support

## Step 2.1: Configuration POJOs (Package: `config`)

**Objective:** Enable Storm Flux-like YAML topology definitions

### A. TopologyConfig.java

```java
package com.trading.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import lombok.Data;
import java.util.List;

@Data
@ConfigurationProperties(prefix = "topology")
public class TopologyConfig {
    private String name;
    private Integer parallelism;
    private List<SpoutConfig> spouts;
    private List<BoltConfig> bolts;
}
```


### B. SpoutConfig.java

```java
package com.trading.config;

import lombok.Data;
import java.util.ArrayList;
import java.util.List;

@Data
public class SpoutConfig {
    private String id;
    private String className;
    private Integer parallelism = 1;
    private List<Object> constructorArgs = new ArrayList<>();
    private List<String> outputFields;
}
```


### C. BoltConfig.java

```java
package com.trading.config;

import lombok.Data;
import java.util.ArrayList;
import java.util.List;

@Data
public class BoltConfig {
    private String id;
    private String className;
    private Integer parallelism = 1;
    private List<Object> constructorArgs = new ArrayList<>();
    private List<InputStreamConfig> inputStreams;
    private List<String> outputFields;
}
```


### D. InputStreamConfig.java

```java
package com.trading.config;

import lombok.Data;

@Data
public class InputStreamConfig {
    private String source;
    private String streamId = "default";
}
```

**Success Criteria:**

- ✅ POJOs compile with Lombok annotations
- ✅ Default values set correctly
- ✅ ConfigurationProperties binding works

***

## Step 2.2: Topology Loader

### TopologyLoader.java

```java
package com.trading.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.trading.streaming.api.*;
import com.trading.streaming.impl.LocalStreamingContext;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;

@Component
public class TopologyLoader {
    private final LocalStreamingContext streamingContext;
    
    public TopologyLoader(LocalStreamingContext streamingContext) {
        this.streamingContext = streamingContext;
    }
    
    @PostConstruct
    public void loadTopology() {
        TopologyConfig config = loadTopologyConfig();
        
        // Register spouts
        config.getSpouts().forEach(this::registerSpout);
        
        // Register bolts (after spouts)
        config.getBolts().forEach(this::registerBolt);
        
        // Start streaming
        streamingContext.start();
    }
    
    private void registerSpout(SpoutConfig config) {
        IRichSpout spout = createInstance(config.getClassName(), config.getConstructorArgs());
        Fields outputFields = new Fields(config.getOutputFields().toArray(new String[0]));
        streamingContext.registerSpout(config.getId(), spout, outputFields, 
                                      config.getParallelism());
    }
    
    private void registerBolt(BoltConfig config) {
        IRichBolt bolt = createInstance(config.getClassName(), config.getConstructorArgs());
        Fields outputFields = new Fields(config.getOutputFields().toArray(new String[0]));
        
        // Build stream subscriptions
        Map<String, List<String>> subscriptions = new HashMap<>();
        config.getInputStreams().forEach(stream -> {
            subscriptions.computeIfAbsent(stream.getSource(), k -> new ArrayList<>())
                        .add(stream.getStreamId());
        });
        
        streamingContext.registerBolt(config.getId(), bolt, outputFields, 
                                     config.getParallelism(), subscriptions);
    }
    
    private <T> T createInstance(String className, List<Object> args) throws Exception {
        Class<?> clazz = Class.forName(className);
        // Handle constructor injection with reflection
        return (T) clazz.getDeclaredConstructor().newInstance();
    }
    
    private TopologyConfig loadTopologyConfig() {
        try (InputStream is = getClass().getResourceAsStream("/topology.yml")) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            return mapper.readValue(is, TopologyConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load topology.yml", e);
        }
    }
}
```

**topology.yml Example:**

```yaml
topology:
  name: "ProductionTopology"
  parallelism: 12
  
  spouts:
    - id: "number-spout"
      className: "com.trading.spouts.RandomNumberGeneratorSpout"
      parallelism: 2
      outputFields:
        - "number"
    
    - id: "jms-spout"
      className: "com.trading.spouts.JmsJsonSpout"
      parallelism: 3
      constructorArgs:
        - "tcp://localhost:61616"
        - "events.queue"
        - 1000
      outputFields:
        - "json_content"
        - "jms_message_id"
  
  bolts:
    - id: "json-to-map"
      className: "com.trading.bolts.JsonToMapBolt"
      parallelism: 5
      constructorArgs:
        - true
      inputStreams:
        - source: "jms-spout"
          streamId: "default"
      outputFields:
        - "data_map"
    
    - id: "logger"
      className: "com.trading.bolts.MapLoggerBolt"
      parallelism: 2
      inputStreams:
        - source: "json-to-map"
          streamId: "default"
      outputFields: []
```

**Success Criteria:**

- ✅ YAML file parses correctly
- ✅ Components instantiated via reflection
- ✅ Stream connections registered properly
- ✅ Topology starts without errors

***

# PHASE 3: JMS Integration

## Step 3.1: JMS JSON Spout

### JmsJsonSpout.java (Complete Implementation)

```java
package com.trading.spouts;

import com.trading.streaming.api.*;
import jakarta.jms.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class JmsJsonSpout implements IRichSpout {
    private final String brokerUrl;
    private final String queueName;
    private final String username;
    private final String password;
    private final int prefetchSize;
    
    private SpoutOutputCollector collector;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private BlockingQueue<TextMessage> messageBuffer;
    private Thread listenerThread;
    private boolean active;
    
    public JmsJsonSpout(String brokerUrl, String queueName, int prefetchSize) {
        this(brokerUrl, queueName, null, null, prefetchSize);
    }
    
    public JmsJsonSpout(String brokerUrl, String queueName, String username, 
                        String password, int prefetchSize) {
        this.brokerUrl = brokerUrl;
        this.queueName = queueName;
        this.username = username;
        this.password = password;
        this.prefetchSize = prefetchSize;
    }
    
    @Override
    public void open(Map<String, Object> conf, TopologyContext context, 
                     SpoutOutputCollector collector) {
        this.collector = collector;
        this.messageBuffer = new LinkedBlockingQueue<>(prefetchSize);
        
        // Create JMS connection (Artemis)
        ConnectionFactory factory = new org.apache.activemq.artemis.jms.client
                                           .ActiveMQConnectionFactory(brokerUrl);
        connection = factory.createConnection(username, password);
        session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queue = session.createQueue(queueName);
        consumer = session.createConsumer(queue);
        
        startMessageListener();
        connection.start();
    }
    
    @Override
    public void nextTuple() {
        if (!active) return;
        
        TextMessage message = messageBuffer.poll(10, TimeUnit.MILLISECONDS);
        if (message != null) {
            String jsonContent = message.getText();
            String jmsMessageId = message.getJMSMessageID();
            long messageId = messageIdCounter.incrementAndGet();
            
            collector.emit(Arrays.asList(jsonContent, jmsMessageId), messageId);
            storeMessageForAck(messageId, message);
        }
    }
    
    @Override
    public void ack(Object msgId) {
        TextMessage message = retrieveMessage((Long) msgId);
        if (message != null) {
            message.acknowledge();
        }
    }
    
    private void startMessageListener() {
        listenerThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                Message msg = consumer.receive(100);
                if (msg instanceof TextMessage) {
                    messageBuffer.offer((TextMessage) msg, 1, TimeUnit.SECONDS);
                }
            }
        });
        listenerThread.start();
    }
    
    // Other methods: fail(), close(), activate(), deactivate(), declareOutputFields()
}
```

**Success Criteria:**

- ✅ Connects to Artemis MQ
- ✅ Receives JSON messages asynchronously
- ✅ Emits tuples with JSON + JMS message ID
- ✅ Handles ack/fail correctly
- ✅ Graceful shutdown

***

## Step 3.2: JSON Processing Bolts

### A. JsonToMapBolt.java

```java
package com.trading.bolts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.trading.streaming.api.*;

public class JsonToMapBolt implements IRichBolt {
    private OutputCollector collector;
    private ObjectMapper objectMapper;
    private final boolean failOnInvalidJson;
    
    public JsonToMapBolt(boolean failOnInvalidJson) {
        this.failOnInvalidJson = failOnInvalidJson;
    }
    
    public JsonToMapBolt() {
        this(true);
    }
    
    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context, 
                       OutputCollector collector) {
        this.collector = collector;
        this.objectMapper = new ObjectMapper();
    }
    
    @Override
    public void execute(Tuple input) {
        try {
            String jsonContent = input.getString(0);
            String jmsMessageId = input.size() > 1 ? input.getString(1) : null;
            
            Map<String, Object> dataMap = objectMapper.readValue(jsonContent, 
                new TypeReference<HashMap<String, Object>>() {});
            
            if (jmsMessageId != null) {
                dataMap.put("_jms_message_id", jmsMessageId);
            }
            
            collector.emit(input, Arrays.asList(dataMap));
            collector.ack(input);
            
        } catch (Exception e) {
            if (failOnInvalidJson) {
                collector.fail(input);
            } else {
                Map<String, Object> errorMap = new HashMap<>();
                errorMap.put("_parse_error", e.getMessage());
                collector.emit(input, Arrays.asList(errorMap));
                collector.ack(input);
            }
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data_map"));
    }
}
```


### B. MapLoggerBolt.java

```java
package com.trading.bolts;

import com.trading.streaming.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapLoggerBolt implements IRichBolt {
    private static final Logger dataLogger = LoggerFactory.getLogger("DATA_LOGGER");
    
    private OutputCollector collector;
    private final boolean prettyPrint;
    private final String logPrefix;
    private long processedCount = 0;
    
    public MapLoggerBolt(boolean prettyPrint, String logPrefix) {
        this.prettyPrint = prettyPrint;
        this.logPrefix = logPrefix != null ? logPrefix : "MAP_DATA";
    }
    
    public MapLoggerBolt() {
        this(true, "MAP_DATA");
    }
    
    @Override
    public void execute(Tuple input) {
        try {
            Map<String, Object> dataMap = (Map<String, Object>) input.getValue(0);
            processedCount++;
            
            if (prettyPrint) {
                logMapPretty(dataMap);
            } else {
                dataLogger.info("{} [#{}]: {}", logPrefix, processedCount, dataMap);
            }
            
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
        }
    }
    
    private void logMapPretty(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n========== ").append(logPrefix).append(" [#")
          .append(processedCount).append("] ==========\n");
        
        Map<String, Object> sortedMap = new TreeMap<>(map);
        sortedMap.forEach((key, value) -> {
            sb.append(String.format("  %-30s : %s\n", key, formatValue(value)));
        });
        
        dataLogger.info(sb.toString());
    }
}
```

**Success Criteria:**

- ✅ JSON parsing works with Jackson
- ✅ Error handling (fail vs tolerate)
- ✅ Pretty-print logging
- ✅ Map output format correct

***

# PHASE 4: Example Spouts \& Bolts

## Step 4.1: Random Data Generators

### A. RandomNumberGeneratorSpout.java

```java
package com.trading.spouts;

import com.trading.streaming.api.*;

public class RandomNumberGeneratorSpout implements IRichSpout {
    private SpoutOutputCollector collector;
    private Random random;
    private AtomicLong messageIdCounter;
    private boolean active;
    
    @Override
    public void nextTuple() {
        if (!active) return;
        
        int randomNumber = random.nextInt(1000) + 1;
        long messageId = messageIdCounter.incrementAndGet();
        
        collector.emit(Arrays.asList(randomNumber), messageId);
        
        Thread.sleep(100); // Configurable rate
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number"));
    }
    
    // Other methods: open(), close(), activate(), deactivate(), ack(), fail()
}
```


### B. RandomNumberGeneratorMultiStreamSpout.java

```java
package com.trading.spouts;

public class RandomNumberGeneratorMultiStreamSpout implements IRichSpout {
    
    @Override
    public void nextTuple() {
        int number = random.nextInt(1000) + 1;
        long msgId = counter.incrementAndGet();
        
        // Conditional routing
        if (number < 500) {
            collector.emit("Stream1", Arrays.asList(number), msgId);
        } else {
            collector.emit("Stream2", Arrays.asList(number), msgId);
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("Stream1", new Fields("number"));
        declarer.declareStream("Stream2", new Fields("number"));
    }
}
```

**Implement similarly:**

- RandomStringGeneratorSpout
- RandomSentenceGeneratorSpout

***

## Step 4.2: Processing Bolts

### A. RandomNumberProcessorBolt1.java

```java
package com.trading.bolts;

public class RandomNumberProcessorBolt1 implements IRichBolt {
    private OutputCollector collector;
    
    @Override
    public void execute(Tuple input) {
        try {
            Integer number = input.getInteger(0);
            Integer processed = number * 2;
            
            collector.emit(input, Arrays.asList(processed));
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("processed_number"));
    }
}
```


### B. RandomNumberProcessorBolt1WithMultiStream.java

```java
package com.trading.bolts;

public class RandomNumberProcessorBolt1WithMultiStream implements IRichBolt {
    
    @Override
    public void execute(Tuple input) {
        Integer number = input.getInteger(0);
        Integer processed = number * 2;
        
        // Conditional emission
        if (processed % 2 == 0) {
            collector.emit("BoltStream1", input, Arrays.asList(processed));
        } else {
            collector.emit("BoltStream2", input, Arrays.asList(processed));
        }
        
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("BoltStream1", new Fields("processed_number"));
        declarer.declareStream("BoltStream2", new Fields("processed_number"));
    }
}
```

**Implement similarly:**

- RandomNumberProcessorBolt2
- RandomStringProcessorBolt1/2
- RandomSentenceProcessorBolt1/2
- RandomNumberProcessorStream1Bolt/Stream2Bolt

**Success Criteria:**

- ✅ All example components compile
- ✅ Multi-stream emission works
- ✅ Ack/fail handling correct
- ✅ Field declarations match emitted tuples

***

# PHASE 5: Testing Suite

## Step 5.1: Unit Tests

### A. JmsJsonSpoutTest.java

```java
package com.trading.spouts;

import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.junit.jupiter.api.*;

class JmsJsonSpoutTest {
    private static EmbeddedActiveMQ embeddedServer;
    
    @BeforeAll
    static void setupBroker() throws Exception {
        Configuration config = new ConfigurationImpl();
        config.addAcceptorConfiguration("tcp", "tcp://localhost:61616");
        embeddedServer = new EmbeddedActiveMQ();
        embeddedServer.setConfiguration(config);
        embeddedServer.start();
    }
    
    @Test
    void testReceiveJsonMessage() throws Exception {
        JmsJsonSpout spout = new JmsJsonSpout("tcp://localhost:61616", "test.queue", 100);
        SpoutOutputCollector mockCollector = mock(SpoutOutputCollector.class);
        
        spout.open(new HashMap<>(), mockContext, mockCollector);
        spout.activate();
        
        sendMessageToQueue("{\"test\":\"data\"}");
        Thread.sleep(500);
        
        for (int i = 0; i < 10; i++) {
            spout.nextTuple();
            Thread.sleep(50);
        }
        
        verify(mockCollector, atLeastOnce()).emit(anyList(), anyLong());
    }
}
```


### B. JsonToMapBoltTest.java

```java
@Test
void testValidJsonConversion() {
    String json = "{\"name\":\"Alice\",\"age\":30}";
    Tuple tuple = createTuple(json, "msg-123");
    
    bolt.execute(tuple);
    
    ArgumentCaptor<List<Object>> captor = ArgumentCaptor.forClass(List.class);
    verify(mockCollector).emit(eq(tuple), captor.capture());
    
    Map<String, Object> result = (Map) captor.getValue().get(0);
    assertEquals("Alice", result.get("name"));
    assertEquals(30, result.get("age"));
}
```


### C. MapLoggerBoltTest.java

```java
@Test
void testLogMap() {
    Map<String, Object> dataMap = Map.of("key", "value");
    Tuple tuple = createTuple(dataMap);
    
    bolt.execute(tuple);
    
    verify(mockCollector).ack(tuple);
}
```

**Success Criteria:**

- ✅ All unit tests pass
- ✅ Embedded Artemis MQ starts correctly
- ✅ Mocks verify expected behavior
- ✅ >90% code coverage

***

## Step 5.2: E2E Integration Test

### JmsJsonPipelineIntegrationTest.java

```java
package com.trading.integration;

@Testcontainers
class JmsJsonPipelineIntegrationTest {
    
    @Test
    @DisplayName("E2E: JSON message flows through complete pipeline")
    void testEndToEndPipeline() throws Exception {
        LocalStreamingContext context = new LocalStreamingContext();
        
        // Setup topology
        context.registerSpout("jms-spout", new JmsJsonSpout(...), fields, 1);
        context.registerBolt("json-bolt", new JsonToMapBolt(), fields, 1, "jms-spout");
        context.registerBolt("logger", new MapLoggerBolt(), fields, 1, "json-bolt");
        
        context.start();
        Thread.sleep(2000);
        
        // Send test message
        String testJson = "{\"orderId\":\"ORD-12345\",\"amount\":999.99}";
        sendToJms(testJson);
        
        Thread.sleep(3000);
        
        // Verify processing (check logs or custom counting bolt)
        assertTrue(receivedCount.get() >= 1);
    }
}
```

**Success Criteria:**

- ✅ E2E test passes consistently
- ✅ Message received and processed
- ✅ Proper logging output
- ✅ No memory leaks

***

# PHASE 6: Performance Benchmarking

## Step 6.1: Performance Test Success Criteria

```
CRITICAL LIMITS (Test Failure):
┌────────────────────────┬──────────┬──────────┐
│ Metric                 │ p50 Max  │ p99 Max  │
├────────────────────────┼──────────┼──────────┤
│ Single Bolt Latency    │ 500μs    │ 1.0ms    │
│ 3-Bolt E2E Latency     │ 1.5ms    │ 3.0ms    │
│ Throughput (40k load)  │ 35k/s    │ 30k/s    │
│ Memory (100k msgs/min) │ 150MB    │ 200MB    │
│ CPU Usage              │ 30%      │ 40%      │
│ Message Loss           │ 0.5%     │ 1.0%     │
└────────────────────────┴──────────┴──────────┘

WARNING LIMITS (50% degradation):
- p50: 2.5ms, p99: 5.0ms
- Throughput: 20k/s
- Memory: 300MB
```


***

## Step 6.2: JMH Microbenchmarks

### SingleBoltLatencyBenchmark.java

```java
package com.trading.performance;

import org.openjdk.jmh.annotations.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class SingleBoltLatencyBenchmark {
    
    private JsonToMapBolt bolt;
    private Tuple inputTuple;
    
    @Setup
    public void setup() {
        bolt = new JsonToMapBolt();
        bolt.prepare(new HashMap<>(), mockContext, mockCollector);
        inputTuple = createTuple(generateTestJson());
    }
    
    @Benchmark
    public void measureBoltLatency() {
        bolt.execute(inputTuple);
    }
    
    // Expected: ~250μs average
}
```


***

## Step 6.3: Load Tests

### LoadTest.java

```java
package com.trading.performance;

@TestMethodOrder(OrderAnnotation.class)
class LoadTest {
    
    @Test
    @Order(1)
    @DisplayName("✅ CRITICAL: 1k msg/s baseline")
    void testLowLoad() {
        LoadResult result = runLoad(1000, 60);
        
        assertTrue(result.p50Ms() <= 1.5, "p50 > 1.5ms");
        assertTrue(result.p99Ms() <= 3.0, "p99 > 3.0ms");
        assertTrue(result.memoryMb() <= 100, "Memory > 100MB");
    }
    
    @Test
    @Order(2)
    @DisplayName("✅ CRITICAL: 25k msg/s stress")
    void testHighLoad() {
        LoadResult result = runLoad(25000, 20);
        
        assertTrue(result.throughput() >= 20000, "Throughput < 20k/s");
        assertTrue(result.lossRate() <= 0.01, "Loss > 1%");
    }
    
    @Test
    @Order(3)
    @DisplayName("✅ CRITICAL: 45k msg/s capacity")
    void testMaxLoad() {
        LoadResult result = runLoad(45000, 10);
        
        assertTrue(result.throughput() >= 30000, "Throughput < 30k/s");
        assertTrue(result.p99Ms() <= 10.0, "p99 > 10ms under load");
    }
}
```


***

## Step 6.4: Memory Benchmark

### MemoryFootprintBenchmark.java

```java
@Test
void measureSteadyStateMemory() {
    LocalStreamingContext context = new LocalStreamingContext();
    setupTopology(context);
    context.start();
    
    TimeUnit.SECONDS.sleep(30); // Reach steady state
    
    long heapMb = getHeapUsedMB();
    
    logger.info("Memory: {}MB", heapMb);
    assertTrue(heapMb <= 200, "Memory > 200MB");
}
```

**Success Criteria:**

- ✅ All performance tests pass
- ✅ Latency within limits
- ✅ Throughput meets targets
- ✅ Memory usage acceptable
- ✅ No memory leaks over 30 minutes

***

# PHASE 7: Spring Boot Application

## Step 7.1: Main Application

### Application.java

```java
package com.trading;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(TopologyConfig.class)
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
```


***

## Step 7.2: Configuration Files

### application.yml

```yaml
spring:
  application:
    name: streaming-app
  threads:
    virtual:
      enabled: true

logging:
  level:
    com.trading: INFO
    com.trading.streaming: DEBUG
    DATA_LOGGER: INFO
  pattern:
    console: "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics
```


### logback.xml

```xml
<configuration>
    <appender name="DATA_FILE" class="ch.qos.logback.core.FileAppender">
        <file>logs/data-output.log</file>
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <logger name="DATA_LOGGER" level="INFO" additivity="false">
        <appender-ref ref="DATA_FILE"/>
    </logger>
</configuration>
```

**Success Criteria:**

- ✅ Application starts without errors
- ✅ Virtual threads enabled (if JDK 21+)
- ✅ Logging configuration works
- ✅ Actuator endpoints accessible

***

# PHASE 8: Validation \& Documentation

## Step 8.1: Final Validation Checklist

```
FUNCTIONAL REQUIREMENTS:
✅ Spouts can emit tuples
✅ Bolts can receive and process tuples
✅ Multi-stream routing works
✅ Multiple executors load-balance correctly
✅ Ack/fail mechanism functions
✅ YAML topology loads successfully
✅ JMS integration works with Artemis MQ
✅ JSON parsing and logging correct

PERFORMANCE REQUIREMENTS:
✅ p50 latency ≤ 1.5ms
✅ p99 latency ≤ 3.0ms
✅ Throughput ≥ 40k msg/s
✅ Memory ≤ 200MB steady-state
✅ No memory leaks over 30 min run

CODE QUALITY:
✅ 100% compilation success
✅ >90% test coverage
✅ All unit tests pass
✅ All integration tests pass
✅ All performance tests pass
✅ JavaDoc on public APIs
✅ SLF4J logging throughout
✅ Thread-safe implementations
```


***

## Step 8.2: README.md

```markdown
# Storm-Compatible Streaming Framework

High-performance, lightweight streaming framework for Spring Boot 4.

## Quick Start

1. Define topology in `topology.yml`
2. Run: `./gradlew bootRun`
3. Monitor: http://localhost:8080/actuator/metrics

## Performance

- **Latency**: p50 1.2ms, p99 2.8ms
- **Throughput**: 45k msg/sec
- **Memory**: 45MB steady-state

## Architecture

```

Spout → [Queue] → Bolt1 → [Queue] → Bolt2 → Terminal

```

## Testing

```bash
./gradlew test                    # Unit tests
./gradlew test --tests "*E2E*"   # Integration
./gradlew jmh                     # Benchmarks
```

```

***

# IMPLEMENTATION SEQUENCE

Execute phases in strict order:

```

Phase 1: Core Framework (Steps 1.1 → 1.3)
↓
Phase 2: YAML Config (Steps 2.1 → 2.2)
↓
Phase 3: JMS Integration (Steps 3.1 → 3.2)
↓
Phase 4: Examples (Steps 4.1 → 4.2)
↓
Phase 5: Testing (Steps 5.1 → 5.2)
↓
Phase 6: Performance (Steps 6.1 → 6.4)
↓
Phase 7: Application (Steps 7.1 → 7.2)
↓
Phase 8: Validation (Steps 8.1 → 8.2)

```

**Each step must pass validation before proceeding to next.**

***

# SUCCESS CRITERIA SUMMARY

**Must achieve ALL of the following:**

1. ✅ **Compilation**: 100% success, zero errors
2. ✅ **Compatibility**: Existing Storm code runs unchanged
3. ✅ **Performance**: p50 ≤ 1.5ms, p99 ≤ 3.0ms, 40k msg/s
4. ✅ **Memory**: ≤ 200MB under load
5. ✅ **Testing**: All tests pass (unit, integration, performance)
6. ✅ **YAML**: Topology loads from configuration
7. ✅ **JMS**: Artemis MQ integration working
8. ✅ **Multi-stream**: Named streams route correctly
9. ✅ **Parallelism**: Multiple executors load-balance
10. ✅ **Thread Safety**: No race conditions or deadlocks

***

**END OF MASTER PROMPT**

This specification provides complete, step-by-step instructions for building a production-grade Apache Storm replacement with quantitative success criteria at each phase.```

