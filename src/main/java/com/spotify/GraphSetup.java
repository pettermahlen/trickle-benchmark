package com.spotify;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import com.spotify.trickle.Func0;
import com.spotify.trickle.Func1;
import com.spotify.trickle.Graph;
import com.spotify.trickle.Input;

import java.util.concurrent.Callable;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.spotify.trickle.Trickle.call;

/**
 * TODO: document!
 */
public class GraphSetup {

  public static final Input<String> HEARTBEAT_ENDPOINT = Input.named("heartbeat");
  static boolean useExecutor = false;

  static ListenableFuture<Long> heartbeatIntervalMillis(ListeningExecutorService executor) {
    if (useExecutor) {
      return executor.submit(new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          return System.currentTimeMillis();
        }
      });
    }

    return immediateFuture(System.currentTimeMillis());
  }

  static ListenableFuture<Void> updateSerialCall(ListeningExecutorService executor) {
    if (useExecutor) {
      return executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          return null;
        }
      });
    }

    return immediateFuture(null);
  }

  static ListenableFuture<Boolean> putHeartbeat(final String arg, ListeningExecutorService executor) {
    if (useExecutor) {
      return executor.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return arg.hashCode() % 2 == 0;
        }
      });
    }

    return immediateFuture(arg.hashCode() % 2 == 0);
  }

  static ListenableFuture<Integer> fetchEndpoint(final String arg,
                                                 ListeningExecutorService executor) {
    if (useExecutor) {
      return executor.submit(new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          return arg.length();
        }
      });
    }

    return immediateFuture(arg.length());
  }

  static Func1<String, Integer> endpointFunc(
      final ListeningExecutorService executorService) {
    return new Func1<String, Integer>() {
      @Override
      public ListenableFuture<Integer> run(String arg) {
        return fetchEndpoint(arg, executorService);
      }
    };
  }

  static Func1<String, Boolean> heartbeatFunc(final ListeningExecutorService executorService) {
    return new Func1<String, Boolean>() {
      @Override
      public ListenableFuture<Boolean> run(String arg) {
        return putHeartbeat(arg, executorService);
      }
    };
  }

  static Func1<Integer, Void> updateSerialFunc(final ListeningExecutorService executorService) {
    return new Func1<Integer, Void>() {
      @Override
      public ListenableFuture<Void> run(Integer arg) {
        return updateSerialCall(executorService);
      }
    };
  }

  static Func0<Long> resultFunc(final ListeningExecutorService executorService) {
    return new Func0<Long>() {
      @Override
      public ListenableFuture<Long> run() {
        return heartbeatIntervalMillis(executorService);
      }
    };
  }

  static Graph<Long> graph(ListeningExecutorService executorService) {
    Graph<Integer> endpoint = call(endpointFunc(executorService)).with(GraphSetup.HEARTBEAT_ENDPOINT);
    Graph<Boolean> putHeartbeat = call(heartbeatFunc(executorService)).with(GraphSetup.HEARTBEAT_ENDPOINT).after(endpoint);
    Graph<Void> serial = call(updateSerialFunc(executorService)).with(endpoint).after(putHeartbeat);

    return call(resultFunc(executorService)).after(serial);
  }
}
