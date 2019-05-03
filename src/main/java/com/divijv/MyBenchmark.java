package com.divijv;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@Threads(32)
@Fork(1)
@Warmup(iterations = 2, time = 20)
@Measurement(iterations = 5, time = 20)
public class MyBenchmark {
    @Benchmark
    @OperationsPerInvocation(500)
    public void shortQuery(BenchmarkSetup setup) {
        List<CompletableFuture<ResultSet>> futures = new ArrayList<>();

        for (int i = 0; i < 500; i++) {
            futures.add(setup.client.submitAsync(EmptyGraph.instance().traversal().V(1).id()));
        }

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        List<CompletableFuture<List<Result>>> rsfs = allFutures
                .thenApply((v) -> {
                    return futures.stream()
                                  .map(f -> f.join())
                                  .map(rs -> rs.all())
                                  .collect(Collectors.toList());

                }).join();
        List<Long> ans = CompletableFuture.allOf(rsfs.toArray(new CompletableFuture[0])).thenApply(list -> {
            return rsfs.stream().map(f -> f.join())
                       .flatMap(lres -> lres.stream()).map(res -> res.getLong()).collect(Collectors.toList());
        }).join();

        if (!ans.stream().allMatch((id) -> (id == 1L))) {
            throw new IllegalArgumentException("failed bench");
        }
    }

    @Benchmark
    @OperationsPerInvocation(100)
    public void longQuery(BenchmarkSetup setup) {
        List<CompletableFuture<ResultSet>> futures = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            futures.add(setup.client.submitAsync(EmptyGraph.instance().traversal().V().id()));
        }

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        List<CompletableFuture<List<Result>>> rsfs = allFutures
                .thenApply((v) -> {
                    return futures.stream()
                                  .map(f -> f.join())
                                  .map(rs -> rs.all())
                                  .collect(Collectors.toList());

                }).join();
        List<Long> ans = CompletableFuture.allOf(rsfs.toArray(new CompletableFuture[0])).thenApply(list -> {
            return rsfs.stream().map(f -> f.join())
                       .flatMap(lres -> lres.stream()).map(res -> res.getLong()).collect(Collectors.toList());
        }).join();

        if (ans.size() != 100*100) {
            throw new IllegalArgumentException("failed bench" + ans.size());
        }
    }

    @OperationsPerInvocation(10)
    @Benchmark
    public void largeResultQuery(BenchmarkSetup setup) throws ExecutionException, InterruptedException {
        final int resultCountToGenerate = 20000;
        final String fatty = IntStream.range(0, 175).mapToObj(String::valueOf).collect(Collectors.joining());
        final String fattyX = "['" + fatty + "'] * " + resultCountToGenerate;
        List<CompletableFuture<ResultSet>> futures = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            futures.add(setup.client.submitAsync(fattyX));
        }

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        List<CompletableFuture<List<Result>>> rsfs = allFutures
                .thenApply((v) -> {
                    return futures.stream()
                                  .map(f -> f.join())
                                  .map(rs -> rs.all())
                                  .collect(Collectors.toList());

                }).join();

        CompletableFuture.allOf(rsfs.toArray(new CompletableFuture[0])).thenApply(list -> {
            return rsfs.stream().map(f -> f.join())
                       .flatMap(lres -> lres.stream())
                       .map(res -> res.getObject())
                       .collect(Collectors.toList());
        }).join();
    }
}
