<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

## **Correct Hooks Integration (Using Your TopologyContext)**

You're right! Here are the **exact changes** for your actual `TopologyContext`:

***

## **1. TopologyContext.java** (Add Hook Support)

```java
package com.trading.streaming.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TopologyContext {
    private final String topologyId;
    private final Map<String, Object> stormConf;
    private final String componentId;
    private final Integer taskId;
    
    // ADD THIS: Hook storage
    private final List<TaskHook> taskHooks = new ArrayList<>();
    
    public TopologyContext(String topologyId,
                          Map<String, Object> stormConf,
                          String componentId,
                          Integer taskId) {
        this.topologyId = topologyId;
        this.stormConf = stormConf;
        this.componentId = componentId;
        this.taskId = taskId;
    }
    
    // ADD THESE: Hook management
    public void addTaskHook(TaskHook hook) {
        taskHooks.add(hook);
    }
    
    public List<TaskHook> getTaskHooks() {
        return Collections.unmodifiableList(taskHooks);
    }
    
    // Existing getters
    public String getTopologyId() { return topologyId; }
    public Map<String, Object> getStormConf() { return stormConf; }
    public String getThisComponentId() { return componentId; }
    public Integer getThisTaskId() { return taskId; }
}
```


***

## **2. LocalStreamingContext.java** (Inject Hooks)

```java
package com.trading.streaming.impl;

import com.trading.streaming.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalStreamingContext {
    private static final Logger logger = LoggerFactory.getLogger(LocalStreamingContext.class);
    
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<String, List<BoltExecutor>> boltExecutors = new ConcurrentHashMap<>();
    private final Map<String, List<SpoutExecutor>> spoutExecutors = new ConcurrentHashMap<>();
    private final List<Thread> executorThreads = new ArrayList<>();
    private final AtomicInteger taskIdGenerator = new AtomicInteger(0);
    
    // ADD THIS: Store hooks and config
    private List<TaskHook> taskHooks = new ArrayList<>();
    private Map<String, Object> globalConfig = new HashMap<>();
    private String topologyId = "local-topology";
    
    // ADD THIS: Setters for TopologyLoader
    public void setTaskHooks(List<TaskHook> hooks) {
        this.taskHooks = new ArrayList<>(hooks);
        logger.info("Registered {} task hooks", hooks.size());
    }
    
    public void setGlobalConfig(Map<String, Object> config) {
        this.globalConfig = new HashMap<>(config);
    }
    
    public void setTopologyId(String topologyId) {
        this.topologyId = topologyId;
    }
    
    // MODIFY: registerBolt to use hook-enabled TopologyContext
    public void registerBolt(String componentId, IRichBolt bolt, Fields outputFields,
                            int parallelism, Map<String, List<String>> subscriptions) {
        
        for (int i = 0; i < parallelism; i++) {
            Integer taskId = taskIdGenerator.incrementAndGet();
            String executorId = componentId + "-" + taskId;
            
            // CREATE TopologyContext WITH HOOKS
            TopologyContext topologyContext = createTopologyContextWithHooks(
                componentId, taskId);
            
            // Create OutputCollector
            OutputCollector collector = new OutputCollector(
                componentId, this, outputFields.toList());
            collector.setTopologyContext(topologyContext); // Add this setter
            
            // Create BoltExecutor
            BoltExecutor executor = new BoltExecutor(
                executorId,
                componentId,
                bolt,
                this,
                collector,
                globalConfig,
                topologyContext,
                65536
            );
            
            boltExecutors.computeIfAbsent(componentId, k -> new ArrayList<>()).add(executor);
            
            // Start thread
            Thread executorThread = new Thread(executor, "bolt-" + executorId);
            executorThread.setDaemon(false);
            executorThreads.add(executorThread);
        }
        
        logger.info("Registered bolt '{}' with {} executors", componentId, parallelism);
    }
    
    // MODIFY: registerSpout to use hook-enabled TopologyContext
    public void registerSpout(String componentId, IRichSpout spout, Fields outputFields,
                             int parallelism) {
        
        for (int i = 0; i < parallelism; i++) {
            Integer taskId = taskIdGenerator.incrementAndGet();
            String executorId = componentId + "-" + taskId;
            
            // CREATE TopologyContext WITH HOOKS
            TopologyContext topologyContext = createTopologyContextWithHooks(
                componentId, taskId);
            
            // Create SpoutOutputCollector
            SpoutOutputCollector collector = new SpoutOutputCollector(
                componentId, this, outputFields.toList());
            collector.setTopologyContext(topologyContext); // Add this setter
            
            // Create SpoutExecutor
            SpoutExecutor executor = new SpoutExecutor(
                executorId,
                componentId,
                spout,
                this,
                collector,
                globalConfig,
                topologyContext
            );
            
            spoutExecutors.computeIfAbsent(componentId, k -> new ArrayList<>()).add(executor);
            
            // Start thread
            Thread executorThread = new Thread(executor, "spout-" + executorId);
            executorThread.setDaemon(false);
            executorThreads.add(executorThread);
        }
        
        logger.info("Registered spout '{}' with {} executors", componentId, parallelism);
    }
    
    // ADD THIS: Helper method to create TopologyContext with hooks
    private TopologyContext createTopologyContextWithHooks(String componentId, Integer taskId) {
        // Create TopologyContext
        TopologyContext ctx = new TopologyContext(
            topologyId,
            globalConfig,
            componentId,
            taskId
        );
        
        // INJECT HOOKS
        for (TaskHook hook : taskHooks) {
            ctx.addTaskHook(hook);
            
            // Call prepare() ONCE per hook per task
            try {
                hook.prepare(globalConfig, ctx);
                logger.debug("Prepared hook {} for component {} task {}", 
                           hook.getClass().getSimpleName(), componentId, taskId);
            } catch (Exception e) {
                logger.error("Failed to prepare hook {} for {} task {}", 
                           hook.getClass().getSimpleName(), componentId, taskId, e);
            }
        }
        
        return ctx;
    }
    
    // Existing methods...
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting topology '{}'", topologyId);
            executorThreads.forEach(Thread::start);
        }
    }
    
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping topology '{}'", topologyId);
            executorThreads.forEach(thread -> {
                thread.interrupt();
                try {
                    thread.join(5000);
                } catch (InterruptedException ignored) {}
            });
        }
    }
    
    public boolean isRunning() {
        return running.get();
    }
}
```


***

## **3. BoltExecutor.java** (Fire Hook Events)

```java
public class BoltExecutor implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(BoltExecutor.class);
    
    private final String executorId;
    private final String componentId;
    private final IRichBolt bolt;
    private final LocalStreamingContext context;
    private final OutputCollector collector;
    private final Map<String, Object> conf;
    private final TopologyContext topologyContext;
    private final ChronicleQueueInputHandler inputHandler;
    
    @Override
    public void run() {
        try {
            log.info("BoltExecutor {} starting for component {}", executorId, componentId);
            
            // Prepare bolt
            bolt.prepare(conf, topologyContext, collector);
            
            // FIRE HOOK: onBoltPrepare
            for (TaskHook hook : topologyContext.getTaskHooks()) {
                try {
                    hook.onBoltPrepare(componentId, topologyContext.getThisTaskId());
                } catch (Exception e) {
                    log.error("Hook onBoltPrepare failed", e);
                }
            }
            
            // Process tuples
            while (context.isRunning()) {
                Tuple tuple = inputHandler.take();
                if (tuple != null) {
                    try {
                        // FIRE HOOK: onExecute
                        for (TaskHook hook : topologyContext.getTaskHooks()) {
                            try {
                                hook.onExecute(componentId, topologyContext.getThisTaskId(), tuple);
                            } catch (Exception e) {
                                log.error("Hook onExecute failed", e);
                            }
                        }
                        
                        bolt.execute(tuple);
                        
                    } catch (Exception e) {
                        log.error("Error executing tuple in bolt {}", componentId, e);
                        
                        // FIRE HOOK: onFail
                        for (TaskHook hook : topologyContext.getTaskHooks()) {
                            try {
                                hook.onFail(componentId, topologyContext.getThisTaskId(), tuple, e);
                            } catch (Exception he) {
                                log.error("Hook onFail failed", he);
                            }
                        }
                        
                        collector.fail(tuple);
                    } finally {
                        ((TupleImpl) tuple).release();
                    }
                }
            }
            
        } finally {
            cleanup();
        }
    }
    
    private void cleanup() {
        log.info("BoltExecutor {} cleaning up", executorId);
        
        // FIRE HOOK: onBoltCleanup
        for (TaskHook hook : topologyContext.getTaskHooks()) {
            try {
                hook.onBoltCleanup(componentId, topologyContext.getThisTaskId());
            } catch (Exception e) {
                log.error("Hook onBoltCleanup failed", e);
            }
        }
        
        try {
            bolt.cleanup();
        } catch (Exception e) {
            log.error("Bolt cleanup failed", e);
        }
        
        inputHandler.close();
    }
}
```


***

## **4. OutputCollector.java** (Fire Hook Events)

```java
public class OutputCollector {
    private final String componentId;
    private final LocalStreamingContext context;
    private final List<String> outputFields;
    
    // ADD THIS: Store TopologyContext reference
    private TopologyContext topologyContext;
    
    public void setTopologyContext(TopologyContext ctx) {
        this.topologyContext = ctx;
    }
    
    public void emit(List<Object> values) {
        Tuple tuple = TupleImpl.create(componentId, "default", values, outputFields);
        
        // FIRE HOOK: onEmit
        if (topologyContext != null) {
            for (TaskHook hook : topologyContext.getTaskHooks()) {
                try {
                    hook.onEmit(componentId, topologyContext.getThisTaskId(), tuple, "default");
                } catch (Exception e) {
                    log.error("Hook onEmit failed", e);
                }
            }
        }
        
        routeTuple(tuple);
        ((TupleImpl) tuple).release();
    }
    
    public void ack(Tuple tuple) {
        // FIRE HOOK: onAck
        if (topologyContext != null) {
            for (TaskHook hook : topologyContext.getTaskHooks()) {
                try {
                    hook.onAck(componentId, topologyContext.getThisTaskId(), tuple);
                } catch (Exception e) {
                    log.error("Hook onAck failed", e);
                }
            }
        }
        
        // existing ack logic...
    }
    
    public void fail(Tuple tuple) {
        // FIRE HOOK: onFail
        if (topologyContext != null) {
            for (TaskHook hook : topologyContext.getTaskHooks()) {
                try {
                    hook.onFail(componentId, topologyContext.getThisTaskId(), tuple, null);
                } catch (Exception e) {
                    log.error("Hook onFail failed", e);
                }
            }
        }
        
        // existing fail logic...
    }
}
```


***

## **5. SpoutOutputCollector.java** (Fire Hook Events)

```java
public class SpoutOutputCollector {
    private final String componentId;
    private final LocalStreamingContext context;
    private final List<String> outputFields;
    
    // ADD THIS
    private TopologyContext topologyContext;
    
    public void setTopologyContext(TopologyContext ctx) {
        this.topologyContext = ctx;
    }
    
    public void emit(List<Object> values, String streamId) {
        Tuple tuple = TupleImpl.create(componentId, streamId, values, outputFields);
        
        // FIRE HOOK: onEmit
        if (topologyContext != null) {
            for (TaskHook hook : topologyContext.getTaskHooks()) {
                try {
                    hook.onEmit(componentId, topologyContext.getThisTaskId(), tuple, streamId);
                } catch (Exception e) {
                    log.error("Hook onEmit failed", e);
                }
            }
        }
        
        routeTuple(tuple);
        ((TupleImpl) tuple).release();
    }
}
```


***

## **KEY CHANGES SUMMARY:**

| Component | Change | Why |
| :-- | :-- | :-- |
| **TopologyContext** | Add `taskHooks` list + getters | Store hooks per executor |
| **LocalStreamingContext** | `createTopologyContextWithHooks()` | Inject + prepare hooks |
| **BoltExecutor** | Fire `onBoltPrepare/Execute/Cleanup/Fail` | Lifecycle events |
| **OutputCollector** | Add `setTopologyContext()`, fire `onEmit/Ack/Fail` | Tuple events |
| **SpoutOutputCollector** | Add `setTopologyContext()`, fire `onEmit` | Spout events |

**Now hooks work with your actual TopologyContext structure!** ✅
<span style="display:none">[^1]</span>

<div align="center">⁂</div>

[^1]: Master-Implementation-Prompt_-Apache-Storm-Compati.md

