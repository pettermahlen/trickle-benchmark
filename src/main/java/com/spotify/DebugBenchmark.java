package com.spotify;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import com.spotify.trickle.Graph;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 * Compare the performance with and without debugging.
 */
public class DebugBenchmark {

  @State(Scope.Thread)
  public static class TrickleGraph {
    ListeningExecutorService executor;
    Graph<Long> result;

    @Setup
    public void setupExecutor() {
      executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
      result = GraphSetup.graph(executor);
      GraphSetup.useExecutor = true;
    }

    @TearDown
    public void shutdownExecutor() {
      result = null;
      executor.shutdown();
      executor = null;
    }

  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public long benchmarkNoDebug(TrickleGraph graph) throws ExecutionException, InterruptedException {
    return graph.result.bind(GraphSetup.HEARTBEAT_ENDPOINT, String.valueOf(System.currentTimeMillis())).debug(false).run().get();
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public long benchmarkDebug(TrickleGraph graph) throws ExecutionException, InterruptedException {
    return graph.result.bind(GraphSetup.HEARTBEAT_ENDPOINT, String.valueOf(System.currentTimeMillis())).debug(true).run().get();
  }

}
