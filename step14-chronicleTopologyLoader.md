<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

## **TopologyLoader.java** (Chronicle Queue Compatible - No Major Changes Needed)

Actually, **TopologyLoader doesn't need Chronicle-specific changes** because it works at a higher abstraction level. However, here are **minor enhancements** to support the new features:

***

## **TopologyLoader.java** (Enhanced for Chronicle + Performance)

```java
package com.trading.streaming.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.trading.streaming.api.*;
import com.trading.streaming.impl.LocalStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Loads topology configuration from YAML and registers components.
 * Compatible with Chronicle Queue backend (transparent).
 */
public class TopologyLoader {
    private static final Logger logger = LoggerFactory.getLogger(TopologyLoader.class);
    
    private final LocalStreamingContext streamingContext;
    private final ComponentFactory componentFactory;
    private final PropertyResolver propertyResolver;
    private final List<TaskHook> taskHooks = new ArrayList<>();
    
    public TopologyLoader(LocalStreamingContext streamingContext) {
        this.streamingContext = streamingContext;
        this.componentFactory = new ComponentFactory();
        this.propertyResolver = new PropertyResolver();
    }
    
    /**
     * Load topology from classpath resource.
     */
    public void loadTopology() {
        loadTopology("/topology.yml");
    }
    
    /**
     * Load topology from specific classpath resource.
     */
    public void loadTopology(String resourcePath) {
        try (InputStream yamlStream = getClass().getResourceAsStream(resourcePath)) {
            if (yamlStream == null) {
                throw new IllegalArgumentException("Topology file not found: " + resourcePath);
            }
            
            // Load properties if present
            String propsPath = resourcePath.replace(".yml", ".properties");
            loadPropertiesIfPresent(propsPath);
            
            // Parse YAML
            String yamlContent = new String(yamlStream.readAllBytes());
            loadTopologyFromYaml(yamlContent);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to load topology from " + resourcePath, e);
        }
    }
    
    /**
     * Load topology from YAML string and properties file.
     */
    public void loadTopology(String yamlContent, String propertiesPath) {
        loadPropertiesIfPresent(propertiesPath);
        loadTopologyFromYaml(yamlContent);
    }
    
    /**
     * Load topology from YAML string with Properties object.
     */
    public void loadTopologyWithProperties(String yamlContent, Properties properties) {
        if (properties != null) {
            propertyResolver.addProperties(properties);
        }
        loadTopologyFromYaml(yamlContent);
    }
    
    /**
     * Load properties file if present.
     */
    private void loadPropertiesIfPresent(String propertiesPath) {
        try (InputStream propsStream = getClass().getResourceAsStream(propertiesPath)) {
            if (propsStream != null) {
                Properties props = new Properties();
                props.load(propsStream);
                propertyResolver.addProperties(props);
                logger.info("Loaded properties from {}", propertiesPath);
            }
        } catch (Exception e) {
            logger.debug("No properties file found at {}", propertiesPath);
        }
    }
    
    /**
     * Load topology from YAML content.
     */
    private void loadTopologyFromYaml(String yamlContent) {
        try {
            // Resolve ${} placeholders in YAML
            String resolvedYaml = propertyResolver.resolve(yamlContent);
            
            // Parse YAML
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            TopologyConfig config = mapper.readValue(resolvedYaml, TopologyConfig.class);
            
            logger.info("Loading topology: {}", config.getName());
            
            // Set topology metadata
            streamingContext.setTopologyId(config.getName());
            
            // 1. LOAD TASK HOOKS FIRST (before components)
            if (config.getTaskHooks() != null && !config.getTaskHooks().isEmpty()) {
                loadTaskHooks(config.getTaskHooks());
            }
            
            // 2. APPLY GLOBAL CONFIG (includes Chronicle Queue settings)
            if (config.getConfig() != null) {
                Map<String, Object> resolvedConfig = resolveConfigMap(config.getConfig());
                streamingContext.setGlobalConfig(resolvedConfig);
                
                // Log Chronicle-specific settings
                logChronicleSettings(resolvedConfig);
            }
            
            // 3. CREATE SHARED COMPONENTS
            if (config.getComponents() != null && !config.getComponents().isEmpty()) {
                resolveComponentProperties(config.getComponents());
                componentFactory.createComponents(config.getComponents());
                logger.info("Created {} shared components", config.getComponents().size());
            }
            
            // 4. REGISTER SPOUTS
            if (config.getSpouts() != null) {
                for (SpoutConfig spoutConfig : config.getSpouts()) {
                    registerSpout(spoutConfig);
                }
            }
            
            // 5. REGISTER BOLTS
            if (config.getBolts() != null) {
                for (BoltConfig boltConfig : config.getBolts()) {
                    registerBolt(boltConfig);
                }
            }
            
            // 6. LOG STREAM CONNECTIONS
            if (config.getStreams() != null && !config.getStreams().isEmpty()) {
                logStreamConnections(config.getStreams());
            }
            
            // 7. START TOPOLOGY
            streamingContext.start();
            
            logger.info("Topology '{}' loaded and started successfully", config.getName());
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to load topology from YAML", e);
        }
    }
    
    /**
     * Load and instantiate task hooks.
     */
    private void loadTaskHooks(List<String> hookClassNames) {
        logger.info("Initializing {} task hooks", hookClassNames.size());
        
        for (String hookClassName : hookClassNames) {
            try {
                Class<?> clazz = Class.forName(hookClassName);
                
                if (!TaskHook.class.isAssignableFrom(clazz)) {
                    throw new IllegalArgumentException(
                        "Hook class does not implement TaskHook: " + hookClassName);
                }
                
                TaskHook hook = (TaskHook) clazz.getDeclaredConstructor().newInstance();
                taskHooks.add(hook);
                
                logger.info("Registered TaskHook: {}", hookClassName);
                
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate TaskHook: " + hookClassName, e);
            }
        }
        
        // Inject hooks into streaming context
        streamingContext.setTaskHooks(taskHooks);
    }
    
    /**
     * Log Chronicle Queue specific settings.
     */
    private void logChronicleSettings(Map<String, Object> config) {
        Object queuePath = config.get("chronicle.queue.path");
        Object queueCapacity = config.get("chronicle.queue.capacity");
        Object rollCycle = config.get("chronicle.queue.rollCycle");
        
        if (queuePath != null || queueCapacity != null || rollCycle != null) {
            logger.info("Chronicle Queue settings: path={}, capacity={}, rollCycle={}", 
                       queuePath, queueCapacity, rollCycle);
        }
    }
    
    /**
     * Resolve config map (${} substitution).
     */
    private Map<String, Object> resolveConfigMap(Map<String, Object> config) {
        Map<String, Object> resolved = new HashMap<>();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String) {
                resolved.put(entry.getKey(), propertyResolver.resolve((String) value));
            } else {
                resolved.put(entry.getKey(), value);
            }
        }
        return resolved;
    }
    
    /**
     * Resolve component properties (${} substitution).
     */
    private void resolveComponentProperties(List<ComponentConfig> components) {
        for (ComponentConfig component : components) {
            if (component.getProperties() != null) {
                for (PropertyConfig prop : component.getProperties()) {
                    if (prop.getValue() instanceof String) {
                        String resolved = propertyResolver.resolve((String) prop.getValue());
                        prop.setValue(resolved);
                    }
                }
            }
        }
    }
    
    /**
     * Register a spout.
     */
    private void registerSpout(SpoutConfig spoutConfig) {
        try {
            logger.info("Registering spout: {}", spoutConfig.getId());
            
            // Create spout instance
            IRichSpout spout = createSpout(spoutConfig);
            
            // Get output fields
            Fields outputFields = getOutputFields(spout, spoutConfig);
            
            // Get parallelism (default 1)
            int parallelism = spoutConfig.getParallelism() != null ? 
                             spoutConfig.getParallelism() : 1;
            
            // Register with streaming context
            streamingContext.registerSpout(
                spoutConfig.getId(),
                spout,
                outputFields,
                parallelism
            );
            
            logger.info("Registered spout '{}' with parallelism {}", 
                       spoutConfig.getId(), parallelism);
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to register spout: " + spoutConfig.getId(), e);
        }
    }
    
    /**
     * Register a bolt.
     */
    private void registerBolt(BoltConfig boltConfig) {
        try {
            logger.info("Registering bolt: {}", boltConfig.getId());
            
            // Create bolt instance
            IRichBolt bolt = createBolt(boltConfig);
            
            // Get output fields
            Fields outputFields = getOutputFields(bolt, boltConfig);
            
            // Get parallelism (default 1)
            int parallelism = boltConfig.getParallelism() != null ? 
                             boltConfig.getParallelism() : 1;
            
            // Build subscriptions map
            Map<String, List<String>> subscriptions = buildSubscriptions(boltConfig);
            
            // Register with streaming context
            streamingContext.registerBolt(
                boltConfig.getId(),
                bolt,
                outputFields,
                parallelism,
                subscriptions
            );
            
            logger.info("Registered bolt '{}' with parallelism {} and {} subscriptions", 
                       boltConfig.getId(), parallelism, subscriptions.size());
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to register bolt: " + boltConfig.getId(), e);
        }
    }
    
    /**
     * Create spout instance with properties.
     */
    private IRichSpout createSpout(SpoutConfig config) throws Exception {
        // Use ComponentFactory if available
        Object existing = componentFactory.getComponent(config.getId());
        if (existing instanceof IRichSpout) {
            return (IRichSpout) existing;
        }
        
        // Create new instance
        Class<?> clazz = Class.forName(config.getClassName());
        
        // Try constructor with args
        IRichSpout spout;
        if (config.getConstructorArgs() != null && !config.getConstructorArgs().isEmpty()) {
            spout = createWithConstructorArgs(clazz, config.getConstructorArgs());
        } else {
            spout = (IRichSpout) clazz.getDeclaredConstructor().newInstance();
        }
        
        // Apply properties
        if (config.getProperties() != null) {
            for (PropertyConfig prop : config.getProperties()) {
                setPropertyWithConversion(spout, prop.getName(), prop.getValue());
            }
        }
        
        // Invoke config methods
        if (config.getConfigMethods() != null) {
            for (ConfigMethodConfig method : config.getConfigMethods()) {
                invokeMethodWithConversion(spout, method);
            }
        }
        
        return spout;
    }
    
    /**
     * Create bolt instance with properties.
     */
    private IRichBolt createBolt(BoltConfig config) throws Exception {
        // Check component factory
        Object existing = componentFactory.getComponent(config.getId());
        if (existing instanceof IRichBolt) {
            return (IRichBolt) existing;
        }
        
        // Create new instance
        Class<?> clazz = Class.forName(config.getClassName());
        
        // Try constructor with args
        IRichBolt bolt;
        if (config.getConstructorArgs() != null && !config.getConstructorArgs().isEmpty()) {
            bolt = createWithConstructorArgs(clazz, config.getConstructorArgs());
        } else {
            bolt = (IRichBolt) clazz.getDeclaredConstructor().newInstance();
        }
        
        // Apply properties
        if (config.getProperties() != null) {
            for (PropertyConfig prop : config.getProperties()) {
                setPropertyWithConversion(bolt, prop.getName(), prop.getValue());
            }
        }
        
        // Invoke config methods
        if (config.getConfigMethods() != null) {
            for (ConfigMethodConfig method : config.getConfigMethods()) {
                invokeMethodWithConversion(bolt, method);
            }
        }
        
        return bolt;
    }
    
    /**
     * Create instance with constructor args.
     */
    @SuppressWarnings("unchecked")
    private <T> T createWithConstructorArgs(Class<?> clazz, List<Object> args) throws Exception {
        // Resolve refs
        List<Object> resolvedArgs = new ArrayList<>();
        for (Object arg : args) {
            if (arg instanceof String && ((String) arg).startsWith("ref:")) {
                String ref = ((String) arg).substring(4);
                resolvedArgs.add(componentFactory.getComponent(ref));
            } else {
                resolvedArgs.add(arg);
            }
        }
        
        // Find matching constructor
        Constructor<?>[] constructors = clazz.getDeclaredConstructors();
        for (Constructor<?> constructor : constructors) {
            if (constructor.getParameterCount() == resolvedArgs.size()) {
                return (T) constructor.newInstance(resolvedArgs.toArray());
            }
        }
        
        throw new IllegalArgumentException("No matching constructor found for " + clazz);
    }
    
    /**
     * Set property with type conversion.
     */
    private void setPropertyWithConversion(Object instance, String propName, Object value) 
            throws Exception {
        
        String methodName = "set" + propName.substring(0, 1).toUpperCase() + propName.substring(1);
        Method method = findSetterMethod(instance.getClass(), methodName);
        
        if (method == null) {
            logger.warn("No setter found for property '{}' on {}", propName, instance.getClass());
            return;
        }
        
        Class<?> paramType = method.getParameterTypes()[^0];
        Object convertedValue = convertValue(value, paramType);
        
        method.invoke(instance, convertedValue);
    }
    
    /**
     * Find setter method by name.
     */
    private Method findSetterMethod(Class<?> clazz, String methodName) {
        for (Method method : clazz.getMethods()) {
            if (method.getName().equals(methodName) && method.getParameterCount() == 1) {
                return method;
            }
        }
        return null;
    }
    
    /**
     * Convert value to target type.
     */
    private Object convertValue(Object value, Class<?> targetType) {
        if (value == null) return null;
        
        // Handle refs
        if (value instanceof String && ((String) value).startsWith("ref:")) {
            String ref = ((String) value).substring(4);
            return componentFactory.getComponent(ref);
        }
        
        // Direct assignment if types match
        if (targetType.isInstance(value)) {
            return value;
        }
        
        // String conversion
        if (value instanceof String) {
            return convertFromString((String) value, targetType);
        }
        
        // Number conversions
        if (value instanceof Number) {
            Number num = (Number) value;
            if (targetType == int.class || targetType == Integer.class) return num.intValue();
            if (targetType == long.class || targetType == Long.class) return num.longValue();
            if (targetType == double.class || targetType == Double.class) return num.doubleValue();
            if (targetType == float.class || targetType == Float.class) return num.floatValue();
        }
        
        return value;
    }
    
    /**
     * Convert from string to target type.
     */
    private Object convertFromString(String value, Class<?> targetType) {
        if (targetType == String.class) return value;
        if (targetType == int.class || targetType == Integer.class) return Integer.parseInt(value);
        if (targetType == long.class || targetType == Long.class) return Long.parseLong(value);
        if (targetType == double.class || targetType == Double.class) return Double.parseDouble(value);
        if (targetType == boolean.class || targetType == Boolean.class) return Boolean.parseBoolean(value);
        if (targetType.isEnum()) return Enum.valueOf((Class<Enum>) targetType, value);
        return value;
    }
    
    /**
     * Invoke config method with conversion.
     */
    private void invokeMethodWithConversion(Object instance, ConfigMethodConfig methodConfig) 
            throws Exception {
        Method method = instance.getClass().getMethod(methodConfig.getName(), Object.class);
        method.invoke(instance, methodConfig.getArgs().toArray());
    }
    
    /**
     * Get output fields from component.
     */
    private Fields getOutputFields(Object component, Object config) {
        // Check config first
        List<String> fieldsFromConfig = null;
        if (config instanceof SpoutConfig) {
            fieldsFromConfig = ((SpoutConfig) config).getOutputFields();
        } else if (config instanceof BoltConfig) {
            fieldsFromConfig = ((BoltConfig) config).getOutputFields();
        }
        
        if (fieldsFromConfig != null && !fieldsFromConfig.isEmpty()) {
            return new Fields(fieldsFromConfig);
        }
        
        // Fallback to declareOutputFields
        return new Fields("value");
    }
    
    /**
     * Build subscriptions map from bolt config.
     */
    private Map<String, List<String>> buildSubscriptions(BoltConfig boltConfig) {
        Map<String, List<String>> subscriptions = new HashMap<>();
        
        if (boltConfig.getInputStreams() != null) {
            for (InputStreamConfig input : boltConfig.getInputStreams()) {
                subscriptions.put(
                    input.getComponentId(),
                    input.getStreamIds() != null ? input.getStreamIds() : 
                        Collections.singletonList("default")
                );
            }
        }
        
        return subscriptions;
    }
    
    /**
     * Log stream connections.
     */
    private void logStreamConnections(List<StreamConfig> streams) {
        logger.info("Stream connections:");
        for (StreamConfig stream : streams) {
            logger.info("  {} -> {}", stream.getFrom(), stream.getTo());
        }
    }
    
    /**
     * Get task hooks (for testing).
     */
    public List<TaskHook> getTaskHooks() {
        return Collections.unmodifiableList(taskHooks);
    }
    
    /**
     * Get property resolver (for testing).
     */
    public PropertyResolver getPropertyResolver() {
        return propertyResolver;
    }
}
```


***

## **Key Points:**

### **1. Chronicle Queue is Transparent**

```java
// TopologyLoader doesn't know about Chronicle Queue
// It just calls:
streamingContext.registerBolt(id, bolt, fields, parallelism, subscriptions);

// LocalStreamingContext handles Chronicle Queue internally
```


### **2. Optional Chronicle Configuration**

```yaml
# topology.yml
name: "high-performance-topology"

config:
  # Chronicle Queue settings (optional - has defaults)
  chronicle.queue.path: "/var/chronicle"
  chronicle.queue.capacity: 100000
  chronicle.queue.rollCycle: "HOURLY"
  
  # Tick tuples
  topology.tick.tuple.freq.secs: 10

spouts:
  - id: "fast-spout"
    className: "com.example.FastSpout"
    parallelism: 4
```


### **3. No Code Changes Needed**

TopologyLoader works exactly the same whether using:

- BlockingQueue (old)
- Chronicle Queue (new)
- Any other queue implementation


### **4. Task Hooks Loaded First**

```java
// Ensures hooks are injected before any components start
loadTaskHooks(config.getTaskHooks());
streamingContext.setTaskHooks(taskHooks);
// Then register spouts/bolts
```


***

## **Summary:**

✅ **No Chronicle-specific changes needed** in TopologyLoader
✅ **Transparent queue backend** swap
✅ **Task hooks support** added
✅ **Config property resolution** for Chronicle settings
✅ **Backwards compatible** with existing topologies

**TopologyLoader is queue-agnostic by design!** 🎯
<span style="display:none">[^1]</span>

<div align="center">⁂</div>

[^1]: Master-Implementation-Prompt_-Apache-Storm-Compati.md

