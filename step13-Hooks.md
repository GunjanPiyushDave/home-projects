<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Enhance the framework to provide implementation of hooks which can be specified in the topology yml config and can used as hooks on bolts and spouts

You can mirror Apache Storm’s **task hooks** by adding a small hook SPI, wiring it into spouts/bolts, and allowing hook classes to be configured in your topology YAML.

Below is a focused design and concrete code you can drop into your framework.

***

## 1. Hook SPI

Create a simple hook interface plus a base class.

```java
// src/main/java/com/trading/streaming/api/TaskHook.java
package com.trading.streaming.api;

import java.util.Map;

public interface TaskHook {

    default void prepare(Map<String, Object> conf, TopologyContext context) {}

    default void onSpoutOpen(String componentId, int taskIndex) {}

    default void onSpoutClose(String componentId, int taskIndex) {}

    default void onBoltPrepare(String componentId, int taskIndex) {}

    default void onBoltCleanup(String componentId, int taskIndex) {}

    default void onEmit(String componentId, int taskIndex, Tuple tuple, String streamId) {}

    default void onExecute(String componentId, int taskIndex, Tuple tuple) {}

    default void onAck(String componentId, int taskIndex, Tuple tuple) {}

    default void onFail(String componentId, int taskIndex, Tuple tuple, Throwable cause) {}
}
```

Optional base:

```java
// src/main/java/com/trading/streaming/api/BaseTaskHook.java
package com.trading.streaming.api;

import java.util.Map;

public abstract class BaseTaskHook implements TaskHook {
    @Override public void prepare(Map<String, Object> conf, TopologyContext context) {}
}
```

And add to `TopologyContext` a place to store hooks:

```java
// TopologyContext.java – add:
private final java.util.List<TaskHook> taskHooks = new java.util.ArrayList<>();

public void addTaskHook(TaskHook hook) {
    taskHooks.add(hook);
}

public java.util.List<TaskHook> getTaskHooks() {
    return java.util.Collections.unmodifiableList(taskHooks);
}
```


***

## 2. YAML configuration model

Extend your YAML config POJOs to carry hook class names, similar to Storm’s `topology.auto.task.hooks`.[^1]

```java
// In TopologyConfig.java
private java.util.List<String> taskHooks;

public java.util.List<String> getTaskHooks() { return taskHooks; }
public void setTaskHooks(java.util.List<String> taskHooks) { this.taskHooks = taskHooks; }
```

Example `topology.yml`:

```yaml
name: "random-with-hooks"

taskHooks:
  - "com.trading.app.random.hooks.LoggingTaskHook"
  - "com.trading.app.random.hooks.MetricsTaskHook"

spouts:
  - id: "numbers"
    className: "com.trading.app.random.spouts.RandomNumberSpout"
    parallelism: 1
    outputFields: ["value"]

bolts:
  - id: "processor"
    className: "com.trading.app.random.bolts.ProcessingBolt"
    parallelism: 1
    outputFields: ["result"]
```

You can optionally also allow **per-component hooks** later by adding `hooks` to `SpoutConfig` / `BoltConfig`, but start with topology‑level hooks.

***

## 3. TopologyLoader: instantiate and attach hooks

Enhance `TopologyLoader` to:

1. Instantiate all `TaskHook` implementations from YAML.
2. Attach them to `TopologyContext` so executors can use them.

Add a field:

```java
// TopologyLoader.java
private final java.util.List<TaskHook> taskHooks = new java.util.ArrayList<>();

public java.util.List<TaskHook> getTaskHooks() {
    return java.util.Collections.unmodifiableList(taskHooks);
}
```

After loading `TopologyConfig` in `loadTopology(String resourcePath)`:

```java
// After TopologyConfig config = loadTopologyConfig(resourcePath);
if (config.getTaskHooks() != null && !config.getTaskHooks().isEmpty()) {
    logger.info("Initializing {} task hooks", config.getTaskHooks().size());
    for (String hookClassName : config.getTaskHooks()) {
        try {
            Class<?> clazz = Class.forName(hookClassName);
            Object instance = clazz.getDeclaredConstructor().newInstance();
            if (!(instance instanceof TaskHook)) {
                throw new IllegalArgumentException(
                    "Hook class does not implement TaskHook: " + hookClassName);
            }
            TaskHook hook = (TaskHook) instance;
            taskHooks.add(hook);
            logger.info("Registered TaskHook: {}", hookClassName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate TaskHook: " + hookClassName, e);
        }
    }
}
```

When you create `TopologyContext` for an executor (spout or bolt), inject the hooks and call `prepare` once:

```java
// Wherever you construct TopologyContext for an executor, e.g. in LocalStreamingContext:
TopologyContext ctx = new TopologyContext(
    topologyName,
    componentIdToTaskIndexMap,
    componentId,
    taskIndex
);

// Attach hooks
for (TaskHook hook : topologyLoader.getTaskHooks()) {
    ctx.addTaskHook(hook);
    hook.prepare(globalConf, ctx);
}
```

Make sure `LocalStreamingContext` has access to the `TopologyLoader` or to the `taskHooks` list (e.g. pass it in the constructor or via setter).

***

## 4. Wire hooks into SpoutExecutor and BoltExecutor

### 4.1 SpoutExecutor

In your spout executor (or wherever you call `IRichSpout` methods):

- Fire `onSpoutOpen` when spout opens.
- Fire `onEmit` when tuples are emitted.
- Fire `onAck`/`onFail` on ack/fail.
- Fire `onSpoutClose` on shutdown.

Example:

```java
// SpoutExecutor.java – inside run() / open()
spout.open(conf, topologyContext, spoutOutputCollector);
for (TaskHook hook : topologyContext.getTaskHooks()) {
    hook.onSpoutOpen(componentId, taskIndex);
}

// When emitting
public void emit(java.util.List<Object> values, String streamId) {
    Tuple tuple = new TupleImpl(componentId, streamId, values, outputFields, nextMsgId());
    // existing routing logic...
    for (TaskHook hook : topologyContext.getTaskHooks()) {
        hook.onEmit(componentId, taskIndex, tuple, streamId);
    }
}

// When acking
public void ack(Tuple tuple) {
    // existing logic
    for (TaskHook hook : topologyContext.getTaskHooks()) {
        hook.onAck(componentId, taskIndex, tuple);
    }
}

// When failing
public void fail(Tuple tuple, Throwable cause) {
    // existing logic
    for (TaskHook hook : topologyContext.getTaskHooks()) {
        hook.onFail(componentId, taskIndex, tuple, cause);
    }
}

// On cleanup
spout.close();
for (TaskHook hook : topologyContext.getTaskHooks()) {
    hook.onSpoutClose(componentId, taskIndex);
}
```

Adapt to your actual method names (you already have an `OutputCollector` wrapper; call hooks from there if that’s simpler).

***

### 4.2 BoltExecutor

In `BoltExecutor` (you already have `execute`, `cleanup`):

```java
// After bolt.prepare(...)
bolt.prepare(conf, topologyContext, collector);
for (TaskHook hook : topologyContext.getTaskHooks()) {
    hook.onBoltPrepare(componentId, taskIndex);
}

// When processing a tuple
bolt.execute(tuple);
for (TaskHook hook : topologyContext.getTaskHooks()) {
    hook.onExecute(componentId, taskIndex, tuple);
}

// On ack/fail from collector – call hooks there
// OutputCollector.ack(...)
for (TaskHook hook : topologyContext.getTaskHooks()) {
    hook.onAck(componentId, taskIndex, tuple);
}
// OutputCollector.fail(...)
for (TaskHook hook : topologyContext.getTaskHooks()) {
    hook.onFail(componentId, taskIndex, tuple, cause);
}

// In BoltExecutor.cleanup()
bolt.cleanup();
for (TaskHook hook : topologyContext.getTaskHooks()) {
    hook.onBoltCleanup(componentId, taskIndex);
}
```

Again, wire at the most central place: either in `BoltExecutor` or inside your `OutputCollector` implementation.

***

## 5. Example hook implementation

A simple hook that logs events and counts tuples:

```java
// src/main/java/com/trading/app/random/hooks/LoggingTaskHook.java
package com.trading.app.random.hooks;

import com.trading.streaming.api.BaseTaskHook;
import com.trading.streaming.api.TopologyContext;
import com.trading.streaming.api.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class LoggingTaskHook extends BaseTaskHook {
    private static final Logger logger = LoggerFactory.getLogger(LoggingTaskHook.class);

    private final Map<String, AtomicLong> emitted = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> executed = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> acks = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> fails = new ConcurrentHashMap<>();

    @Override
    public void prepare(Map<String, Object> conf, TopologyContext context) {
        logger.info("LoggingTaskHook prepared for topology {}", context.getTopologyName());
    }

    private String key(String componentId, int taskIndex) {
        return componentId + "-" + taskIndex;
    }

    @Override
    public void onSpoutOpen(String componentId, int taskIndex) {
        logger.info("Spout opened: {}-{}", componentId, taskIndex);
    }

    @Override
    public void onBoltPrepare(String componentId, int taskIndex) {
        logger.info("Bolt prepared: {}-{}", componentId, taskIndex);
    }

    @Override
    public void onEmit(String componentId, int taskIndex, Tuple tuple, String streamId) {
        emitted.computeIfAbsent(key(componentId, taskIndex), k -> new AtomicLong())
               .incrementAndGet();
    }

    @Override
    public void onExecute(String componentId, int taskIndex, Tuple tuple) {
        executed.computeIfAbsent(key(componentId, taskIndex), k -> new AtomicLong())
                .incrementAndGet();
    }

    @Override
    public void onAck(String componentId, int taskIndex, Tuple tuple) {
        acks.computeIfAbsent(key(componentId, taskIndex), k -> new AtomicLong())
            .incrementAndGet();
    }

    @Override
    public void onFail(String componentId, int taskIndex, Tuple tuple, Throwable cause) {
        fails.computeIfAbsent(key(componentId, taskIndex), k -> new AtomicLong())
             .incrementAndGet();
        logger.warn("Tuple failed in {}-{}: {}", componentId, taskIndex, cause.toString());
    }

    @Override
    public void onBoltCleanup(String componentId, int taskIndex) {
        String k = key(componentId, taskIndex);
        logger.info("Cleanup {}-{}: emitted={}, executed={}, acks={}, fails={}",
                componentId, taskIndex,
                emitted.getOrDefault(k, new AtomicLong()).get(),
                executed.getOrDefault(k, new AtomicLong()).get(),
                acks.getOrDefault(k, new AtomicLong()).get(),
                fails.getOrDefault(k, new AtomicLong()).get());
    }
}
```

Configure it in `topology.yml` as shown earlier.

***

## 6. Tests

### 6.1 Hook wiring test

```java
// src/test/java/com/trading/streaming/config/TopologyLoaderHooksTest.java
package com.trading.streaming.config;

import com.trading.streaming.api.TaskHook;
import com.trading.streaming.impl.LocalStreamingContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TopologyLoaderHooksTest {

    @Test
    void shouldLoadHooksFromYaml() {
        LocalStreamingContext ctx = new LocalStreamingContext();
        TopologyLoader loader = new TopologyLoader(ctx);

        loader.loadTopology("/topology-with-hooks.yml");

        List<TaskHook> hooks = loader.getTaskHooks();
        assertFalse(hooks.isEmpty());
        assertTrue(hooks.stream().anyMatch(
            h -> h.getClass().getName().equals("com.trading.app.random.hooks.LoggingTaskHook")));
    }
}
```


### 6.2 Hook callback test on bolt

```java
// src/test/java/com/trading/streaming/impl/BoltExecutorHookTest.java
package com.trading.streaming.impl;

import com.trading.streaming.api.*;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class BoltExecutorHookTest {

    static class TestHook extends BaseTaskHook {
        AtomicBoolean prepared = new AtomicBoolean(false);
        AtomicBoolean cleaned = new AtomicBoolean(false);
        AtomicInteger executes = new AtomicInteger(0);

        @Override
        public void prepare(Map<String, Object> conf, TopologyContext context) {
            prepared.set(true);
        }

        @Override
        public void onBoltPrepare(String componentId, int taskIndex) {
            prepared.set(true);
        }

        @Override
        public void onExecute(String componentId, int taskIndex, Tuple tuple) {
            executes.incrementAndGet();
        }

        @Override
        public void onBoltCleanup(String componentId, int taskIndex) {
            cleaned.set(true);
        }
    }

    static class NoopBolt implements IRichBolt {
        @Override public void prepare(Map<String, Object> conf, TopologyContext context, OutputCollector collector) {}
        @Override public void execute(Tuple input) {}
        @Override public void cleanup() {}
        @Override public void declareOutputFields(OutputFieldsDeclarer declarer) {}
    }

    @Test
    void hooksShouldBeNotified() throws Exception {
        LocalStreamingContext ctx = new LocalStreamingContext();
        Map<String, Object> conf = new HashMap<>();

        TestHook hook = new TestHook();
        TopologyContext topologyContext = new TopologyContext("test", null, "bolt", 0);
        topologyContext.addTaskHook(hook);
        hook.prepare(conf, topologyContext);

        OutputCollector collector = new OutputCollector("bolt", ctx, Collections.emptyList());
        BoltExecutor executor = new BoltExecutor(
            "exec-1", "bolt", new NoopBolt(), ctx, collector, conf, topologyContext, 100);

        // Run executor in a short-lived thread
        Thread t = new Thread(() -> {
            // Simulate one tuple then stop
            Tuple tuple = new TupleImpl("spout", "default",
                    Collections.singletonList(1), Collections.singletonList("v"), 1L);
            executor.enqueue(tuple);
            ctx.start(); // make isRunning() true
            new Thread(executor).start();
            try { Thread.sleep(200); } catch (InterruptedException ignored) {}
            ctx.stop(); // make isRunning() false
        });
        t.start();
        t.join(1000);

        assertTrue(hook.prepared.get());
        assertTrue(hook.executes.get() > 0);
        // cleanup will be invoked when executor exits
    }
}
```


***

## 7. Summary of what you add

- **New SPI**: `TaskHook`, `BaseTaskHook`.
- **Topology model**: `taskHooks` list in `TopologyConfig` (YAML).
- **Loader**: instantiate hook classes, keep them in `TopologyLoader`, inject and prepare them via `TopologyContext`.
- **Executors / OutputCollector**: call hook methods on spout open/close, bolt prepare/cleanup, emit, execute, ack, fail.
- **Example hook \& tests**: `LoggingTaskHook`, YAML config, loader and executor tests.

This gives you a Storm‑style hook mechanism configurable from YAML, applicable to all bolts and spouts in the topology.[^1]
<span style="display:none">[^10][^11][^12][^13][^14][^15][^16][^2][^3][^4][^5][^6][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://storm.apache.org/releases/2.6.1/Hooks.html

[^2]: Master-Implementation-Prompt_-Apache-Storm-Compati.md

[^3]: https://storm.apache.org/releases/2.4.0/javadocs/org/apache/storm/hooks/class-use/IWorkerHook.html

[^4]: https://storm.apache.org/releases/1.2.4/javadocs/org/apache/storm/hooks/IWorkerHook.html

[^5]: https://storm.apache.org/releases/2.6.1/javadocs/org/apache/storm/hooks/IWorkerHook.html

[^6]: https://stackoverflow.com/questions/48541603/using-storm-ieventlogger-for-logging-events-into-db

[^7]: https://atlas.apache.org/1.0.0/Hook-Storm.html

[^8]: https://atlas.apache.org/1.2.0/Hook-Storm.html

[^9]: https://github.com/ooyala/metrics_storm

[^10]: https://storm.apache.org/releases/1.2.4/Hooks.html

[^11]: https://storm.apache.org/releases/2.6.0/Tutorial.html

[^12]: https://stackoverflow.com/questions/35572979/apache-storm-topology-using-flux-yaml-file

[^13]: https://github.com/apache/storm/blob/master/storm-client/src/jvm/org/apache/storm/Config.java

[^14]: https://github.com/apache/storm/blob/master/conf/defaults.yaml

[^15]: https://storm.apache.org/documentation/Home.html

[^16]: https://storm.apache.org/releases/2.7.1/Hooks.html

