/*
 * Copyright 2013-2014 Spotify AB. All rights reserved.
 *
 * The contents of this file are licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.spotify;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import rx.Observable;

/**
 * Runs benchmarks using JMH: http://openjdk.java.net/projects/code-tools/jmh/.
 */
public class TrickleVsXBenchmark {

  @State(Scope.Thread)
  public static class TrickleGraph {
    ListeningExecutorService executor;
    Graph<Long> result;

    @Setup
    public void setupExecutor() {
      executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
      result = GraphSetup.graph(executor);
    }

    @TearDown
    public void shutdownExecutor() {
      executor.shutdown();
      executor = null;
    }


  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public long benchmarkImmediateTrickle(TrickleGraph graph) throws ExecutionException, InterruptedException {
    GraphSetup.useExecutor = false;
    return runTrickle(graph);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public long benchmarkExecutorTrickle(TrickleGraph graph) throws ExecutionException, InterruptedException {
    GraphSetup.useExecutor = true;
    return runTrickle(graph);
  }

  private long runTrickle(TrickleGraph graph) throws InterruptedException, ExecutionException {
    return graph.result.bind(GraphSetup.HEARTBEAT_ENDPOINT, String.valueOf(System.currentTimeMillis())).run().get();
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public long benchmarkImmediateGuava(final TrickleGraph graph) throws ExecutionException, InterruptedException {
    GraphSetup.useExecutor = false;
    return runGuava(graph);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public long benchmarkExecutorGuava(final TrickleGraph graph) throws ExecutionException, InterruptedException {
    GraphSetup.useExecutor = true;
    return runGuava(graph);
  }

  private long runGuava(final TrickleGraph graph) throws InterruptedException, ExecutionException {
    final String hey = String.valueOf(System.currentTimeMillis());

    ListenableFuture<Integer> endpointFuture = GraphSetup.fetchEndpoint(hey, graph.executor);
    ListenableFuture<Boolean> heartbeatFuture = Futures.transform(endpointFuture, new AsyncFunction<Integer, Boolean>() {
      @Override
      public ListenableFuture<Boolean> apply(Integer input) throws Exception {
        return GraphSetup.putHeartbeat(hey, graph.executor);
      }
    });
    ListenableFuture<Void> serial = Futures.transform(heartbeatFuture, new AsyncFunction<Boolean, Void>() {
      @Override
      public ListenableFuture<Void> apply(Boolean input) throws Exception {
        return GraphSetup.updateSerialCall(graph.executor);
      }
    });

    ListenableFuture<Long> result = Futures.transform(serial, new AsyncFunction<Void, Long>() {
      @Override
      public ListenableFuture<Long> apply(Void input) throws Exception {
        return GraphSetup.heartbeatIntervalMillis(graph.executor);
      }
    });

    return result.get();
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public long benchmarkImmediateRx(final TrickleGraph graph) {
    GraphSetup.useExecutor = false;
    return runRx(graph);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  public long benchmarkExecutorRx(final TrickleGraph graph) {
    GraphSetup.useExecutor = true;
    return runRx(graph);
  }

  private long runRx(final TrickleGraph graph) {
    final String hey = String.valueOf(System.currentTimeMillis());

    return Observable
        .from(GraphSetup.fetchEndpoint(hey, graph.executor))
        .flatMap(new rx.util.functions.Func1<Integer, Observable<Boolean>>() {
          @Override
          public Observable<Boolean> call(Integer integer) {
            return Observable.from(GraphSetup.putHeartbeat(hey, graph.executor));
          }
        }).flatMap(new rx.util.functions.Func1<Boolean, Observable<?>>() {
          @Override
          public Observable<?> call(Boolean aBoolean) {
            return Observable.from(GraphSetup.updateSerialCall(graph.executor));
          }
        }).flatMap(new rx.util.functions.Func1<Object, Observable<? extends Long>>() {
          @Override
          public Observable<? extends Long> call(Object o) {
            return Observable.from(GraphSetup.heartbeatIntervalMillis(graph.executor));
          }
        }).toBlockingObservable().single();
  }


  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(TrickleVsXBenchmark.class.getSimpleName())
        .warmupIterations(3)
        .measurementIterations(5)
        .forks(1)
//        .addProfiler(ProfilerType.STACK)
//        .addProfiler(ProfilerType.HS_GC)
//        .addProfiler(ProfilerType.HS_RT)
        .build();

    new Runner(opt).run();
  }
}
