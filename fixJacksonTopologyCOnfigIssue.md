<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# while running get the exception in TopologyConfig  - unrecognized field topology

The error "unrecognized field topology" occurs because the YAML file has a root `topology:` element, but without Spring's `@ConfigurationProperties`, Jackson doesn't know to skip to that nested level. Here's the fix:

## Solution 1: Update TopologyConfig.java (Add wrapper)

```java
package com.trading.streaming.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * Root configuration wrapper for YAML topology files.
 */
public class TopologyConfigWrapper {
    @JsonProperty("topology")
    private TopologyConfig topology;
    
    public TopologyConfig getTopology() {
        return topology;
    }
    
    public void setTopology(TopologyConfig topology) {
        this.topology = topology;
    }
}
```


## Solution 2: Update TopologyConfig.java (Simplified)

```java
package com.trading.streaming.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Topology configuration POJO.
 */
public class TopologyConfig {
    private String name;
    private Integer parallelism;
    private List<SpoutConfig> spouts = new ArrayList<>();
    private List<BoltConfig> bolts = new ArrayList<>();
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public Integer getParallelism() {
        return parallelism;
    }
    
    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }
    
    public List<SpoutConfig> getSpouts() {
        return spouts;
    }
    
    public void setSpouts(List<SpoutConfig> spouts) {
        this.spouts = spouts;
    }
    
    public List<BoltConfig> getBolts() {
        return bolts;
    }
    
    public void setBolts(List<BoltConfig> bolts) {
        this.bolts = bolts;
    }
}
```


## Solution 3: Update TopologyLoader.java (Recommended Fix)

```java
package com.trading.streaming.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.trading.streaming.api.*;
import com.trading.streaming.impl.LocalStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.*;

/**
 * Loads topology configuration from YAML file and registers components.
 */
public class TopologyLoader {
    private static final Logger logger = LoggerFactory.getLogger(TopologyLoader.class);
    
    private final LocalStreamingContext streamingContext;
    
    public TopologyLoader(LocalStreamingContext streamingContext) {
        this.streamingContext = streamingContext;
    }
    
    /**
     * Load topology from classpath resource topology.yml
     */
    public void loadTopology() {
        loadTopology("/topology.yml");
    }
    
    /**
     * Load topology from specified YAML file
     */
    public void loadTopology(String resourcePath) {
        TopologyConfig config = loadTopologyConfig(resourcePath);
        
        logger.info("Loading topology: {}", config.getName());
        
        // Register spouts
        if (config.getSpouts() != null) {
            config.getSpouts().forEach(this::registerSpout);
        }
        
        // Register bolts (after spouts)
        if (config.getBolts() != null) {
            config.getBolts().forEach(this::registerBolt);
        }
        
        // Start streaming
        streamingContext.start();
        
        logger.info("Topology '{}' loaded and started successfully", config.getName());
    }
    
    private void registerSpout(SpoutConfig config) {
        try {
            logger.info("Registering spout: {} (class: {})", config.getId(), config.getClassName());
            
            IRichSpout spout = createInstance(config.getClassName(), config.getConstructorArgs());
            Fields outputFields = new Fields(config.getOutputFields().toArray(new String[^0]));
            
            streamingContext.registerSpout(
                config.getId(), 
                spout, 
                outputFields, 
                config.getParallelism()
            );
            
            logger.info("Spout '{}' registered with parallelism {}", 
                       config.getId(), config.getParallelism());
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to register spout: " + config.getId(), e);
        }
    }
    
    private void registerBolt(BoltConfig config) {
        try {
            logger.info("Registering bolt: {} (class: {})", config.getId(), config.getClassName());
            
            IRichBolt bolt = createInstance(config.getClassName(), config.getConstructorArgs());
            Fields outputFields = new Fields(config.getOutputFields().toArray(new String[^0]));
            
            // Build stream subscriptions
            Map<String, List<String>> subscriptions = new HashMap<>();
            if (config.getInputStreams() != null) {
                config.getInputStreams().forEach(stream -> {
                    subscriptions.computeIfAbsent(stream.getSource(), k -> new ArrayList<>())
                                .add(stream.getStreamId());
                });
            }
            
            streamingContext.registerBolt(
                config.getId(), 
                bolt, 
                outputFields, 
                config.getParallelism(), 
                subscriptions
            );
            
            logger.info("Bolt '{}' registered with parallelism {}", 
                       config.getId(), config.getParallelism());
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to register bolt: " + config.getId(), e);
        }
    }
    
    @SuppressWarnings("unchecked")
    private <T> T createInstance(String className, List<Object> args) throws Exception {
        Class<?> clazz = Class.forName(className);
        
        if (args == null || args.isEmpty()) {
            return (T) clazz.getDeclaredConstructor().newInstance();
        }
        
        // Convert argument types for constructor matching
        Class<?>[] paramTypes = args.stream()
            .map(arg -> {
                if (arg instanceof Integer) return int.class;
                if (arg instanceof Long) return long.class;
                if (arg instanceof Boolean) return boolean.class;
                if (arg instanceof Double) return double.class;
                if (arg instanceof Float) return float.class;
                return arg.getClass();
            })
            .toArray(Class[]::new);
        
        Constructor<?> constructor = findMatchingConstructor(clazz, paramTypes);
        return (T) constructor.newInstance(args.toArray());
    }
    
    private Constructor<?> findMatchingConstructor(Class<?> clazz, Class<?>[] paramTypes) 
            throws NoSuchMethodException {
        try {
            return clazz.getDeclaredConstructor(paramTypes);
        } catch (NoSuchMethodException e) {
            // Try to find a compatible constructor
            for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
                Class<?>[] constructorParams = constructor.getParameterTypes();
                if (isCompatible(constructorParams, paramTypes)) {
                    return constructor;
                }
            }
            throw e;
        }
    }
    
    private boolean isCompatible(Class<?>[] constructorParams, Class<?>[] paramTypes) {
        if (constructorParams.length != paramTypes.length) {
            return false;
        }
        
        for (int i = 0; i < constructorParams.length; i++) {
            if (!constructorParams[i].isAssignableFrom(paramTypes[i]) &&
                !isBoxingCompatible(constructorParams[i], paramTypes[i])) {
                return false;
            }
        }
        return true;
    }
    
    private boolean isBoxingCompatible(Class<?> constructorParam, Class<?> argType) {
        return (constructorParam == int.class && argType == Integer.class) ||
               (constructorParam == long.class && argType == Long.class) ||
               (constructorParam == boolean.class && argType == Boolean.class) ||
               (constructorParam == double.class && argType == Double.class) ||
               (constructorParam == float.class && argType == Float.class);
    }
    
    private TopologyConfig loadTopologyConfig(String resourcePath) {
        try (InputStream is = getClass().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new RuntimeException("Configuration file not found: " + resourcePath);
            }
            
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            
            // Read the YAML tree
            var rootNode = mapper.readTree(is);
            
            // Check if there's a "topology" root element
            if (rootNode.has("topology")) {
                // Deserialize the nested "topology" node
                return mapper.treeToValue(rootNode.get("topology"), TopologyConfig.class);
            } else {
                // Deserialize the entire document (no nested structure)
                return mapper.treeToValue(rootNode, TopologyConfig.class);
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to load topology configuration from " + resourcePath, e);
        }
    }
}
```


## Complete Config Classes (No Spring Annotations)

### SpoutConfig.java

```java
package com.trading.streaming.config;

import java.util.ArrayList;
import java.util.List;

public class SpoutConfig {
    private String id;
    private String className;
    private Integer parallelism = 1;
    private List<Object> constructorArgs = new ArrayList<>();
    private List<String> outputFields = new ArrayList<>();
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getClassName() {
        return className;
    }
    
    public void setClassName(String className) {
        this.className = className;
    }
    
    public Integer getParallelism() {
        return parallelism;
    }
    
    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }
    
    public List<Object> getConstructorArgs() {
        return constructorArgs;
    }
    
    public void setConstructorArgs(List<Object> constructorArgs) {
        this.constructorArgs = constructorArgs;
    }
    
    public List<String> getOutputFields() {
        return outputFields;
    }
    
    public void setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
    }
}
```


### BoltConfig.java

```java
package com.trading.streaming.config;

import java.util.ArrayList;
import java.util.List;

public class BoltConfig {
    private String id;
    private String className;
    private Integer parallelism = 1;
    private List<Object> constructorArgs = new ArrayList<>();
    private List<InputStreamConfig> inputStreams = new ArrayList<>();
    private List<String> outputFields = new ArrayList<>();
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public String getClassName() {
        return className;
    }
    
    public void setClassName(String className) {
        this.className = className;
    }
    
    public Integer getParallelism() {
        return parallelism;
    }
    
    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }
    
    public List<Object> getConstructorArgs() {
        return constructorArgs;
    }
    
    public void setConstructorArgs(List<Object> constructorArgs) {
        this.constructorArgs = constructorArgs;
    }
    
    public List<InputStreamConfig> getInputStreams() {
        return inputStreams;
    }
    
    public void setInputStreams(List<InputStreamConfig> inputStreams) {
        this.inputStreams = inputStreams;
    }
    
    public List<String> getOutputFields() {
        return outputFields;
    }
    
    public void setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
    }
}
```


### InputStreamConfig.java

```java
package com.trading.streaming.config;

public class InputStreamConfig {
    private String source;
    private String streamId = "default";
    
    public String getSource() {
        return source;
    }
    
    public void setSource(String source) {
        this.source = source;
    }
    
    public String getStreamId() {
        return streamId;
    }
    
    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }
}
```


## Verify Your topology.yml Format

Make sure your `topology.yml` looks like this:

```yaml
topology:
  name: "RandomDataTopology"
  parallelism: 4
  
  spouts:
    - id: "number-generator"
      className: "com.trading.app.random.spouts.RandomNumberGeneratorSpout"
      parallelism: 2
      outputFields:
        - "number"
  
  bolts:
    - id: "processor-1"
      className: "com.trading.app.random.bolts.RandomNumberProcessorBolt1"
      parallelism: 4
      inputStreams:
        - source: "number-generator"
          streamId: "default"
      outputFields:
        - "processed_number"
```

The key fix in `TopologyLoader.loadTopologyConfig()` is:

```java
// Check if there's a "topology" root element
if (rootNode.has("topology")) {
    // Deserialize the nested "topology" node
    return mapper.treeToValue(rootNode.get("topology"), TopologyConfig.class);
}
```

This handles the YAML structure with the `topology:` root element correctly, without needing Spring's `@ConfigurationProperties` annotation.
<span style="display:none">[^1]</span>

<div align="center">⁂</div>

[^1]: Master-Implementation-Prompt_-Apache-Storm-Compati.md

