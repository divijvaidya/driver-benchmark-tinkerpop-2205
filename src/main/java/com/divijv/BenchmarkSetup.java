package com.divijv;

import groovy.util.logging.Slf4j;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

@Slf4j
@State(Scope.Benchmark)
public class BenchmarkSetup {
    private static Logger log = LoggerFactory.getLogger(BenchmarkSetup.class);

    protected GraphTraversalSource g;
    public static final int NUM_INPUT_VERTICES = 100;

    private static final int PORT = 8182;
    private Cluster cluster;
    protected Client client;

    @Setup
    public void init() throws Exception {
        log.info("Setup:Start Init the cluster.");
        final Cluster.Builder builder = Cluster.build();
        builder.addContactPoint("<ADD host endpoint>");
        builder.port(PORT);
        builder.maxConnectionPoolSize(1000);
        builder.maxSimultaneousUsagePerConnection(20);
        builder.maxInProcessPerConnection(20);
        cluster = builder.create();
        client = cluster.connect().alias("g");

        this.g = traversal().withRemote(DriverRemoteConnection.using(client));

        log.info("Setup:Finished Init the cluster.");

        this.addBenchmarkData();
    }

    private void addBenchmarkData() throws Exception {
        log.info("Setup:Start Adding benchmark data.");

        try {
            // Remove existing data
            g.V().drop().iterate();

            // Add new data
            for (int i = 1; i <= NUM_INPUT_VERTICES; i++) {
                g.addV().property(T.id, i).next();
            }

            // Verify new data
            long dataInDB = g.V().count().next();
            if (dataInDB != NUM_INPUT_VERTICES) {
                throw new IllegalStateException("Actual number of vertices in DB:" + dataInDB + " Expected: " + NUM_INPUT_VERTICES);
            }
            log.info("Setup:Finished adding benchmark data.");
        } catch (Exception ex) {
            this.close();
        }
    }

    @TearDown
    public void close() throws Exception {
        log.info("Close:Start Closing the cluster.");
        this.g.close();
        this.cluster.close();
        log.info("Close:Finished Closing the cluster.");
    }
}
