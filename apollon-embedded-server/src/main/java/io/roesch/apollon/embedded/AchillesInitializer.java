/*
 *    Copyright 2021 Johannes Roesch
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package io.roesch.apollon.embedded;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import io.roesch.apollon.exception.ApollonException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static io.roesch.apollon.embedded.CassandraEmbeddedConfigParameters.*;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class AchillesInitializer {
    private static final AtomicBoolean STARTED = new AtomicBoolean(false);

    private static final Logger LOGGER = LoggerFactory.getLogger(AchillesInitializer.class);

    private static final Pattern KEYSPACE_NAME_PATTERN = Pattern.compile("[a-zA-Z][_a-zA-Z0-9]{0,31}");

    private CqlSession singletonSession;

    void initializeFromParameters(String cassandraHost, TypedMap parameters) throws ApollonException {
        synchronized (STARTED) {
            final String keyspaceName = extractAndValidateKeyspaceName(parameters);
            final Boolean durableWrite = parameters.getTyped(KEYSPACE_DURABLE_WRITE);
            try {

                if (!STARTED.get()) {
                    LOGGER.debug("Creating cluster and session singletons");
                    singletonSession = initializeCluster(cassandraHost, parameters);
                    createKeyspaceIfNeeded(singletonSession, keyspaceName, durableWrite);
                    executeStartupScripts(singletonSession, parameters);
                    STARTED.getAndSet(true);
                } else {
                    LOGGER.debug("Cluster and session singletons already created");
                    createKeyspaceIfNeeded(singletonSession, keyspaceName, durableWrite);
                    executeStartupScripts(singletonSession, parameters);
                }
            } catch (UnknownHostException e) {
                throw new ApollonException(e);
            }
        }
    }

    private CqlSession initializeCluster(String cassandraHost, TypedMap parameters) throws UnknownHostException {

        String hostname;
        int cqlPort;

        if (isNotBlank(cassandraHost) && cassandraHost.contains(":")) {
            String[] split = cassandraHost.split(":");
            hostname = split[0];
            cqlPort = Integer.parseInt(split[1]);
        } else {
            hostname = parameters.getTyped(RPC_ADDRESS);
            cqlPort = parameters.getTyped(CASSANDRA_CQL_PORT);
        }

        final InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(hostname), cqlPort);
        return createCluster(address, parameters);
    }


    private CqlSession createCluster(InetSocketAddress endpoint, TypedMap parameters) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Creating Cluster object with host/port {}/{} and parameters {}", endpoint.getHostString(), endpoint.getPort(), parameters);
        }
//        final String clusterName = parameters.getTyped(CLUSTER_NAME);
//        final Compression compression = parameters.getTyped(COMPRESSION_TYPE);
//        final LoadBalancingPolicy loadBalancingPolicy = parameters.getTyped(LOAD_BALANCING_POLICY);
//        final RetryPolicy retryPolicy = parameters.getTyped(RETRY_POLICY);
//        final ReconnectionPolicy reconnectionPolicy = parameters.getTyped(RECONNECTION_POLICY);
//        final SocketOptions socketOptions = new SocketOptions();
//        socketOptions.setKeepAlive(true);
//        socketOptions.setConnectTimeoutMillis(15000);
//        socketOptions.setReadTimeoutMillis(30000);

        InetSocketAddress e = null;
        try {
            e = new InetSocketAddress(InetAddress.getByName("localhost"), endpoint.getPort());
        } catch (UnknownHostException unknownHostException) {
            unknownHostException.printStackTrace();
        }

//        final EndPoint defaultEndpoint = new DefaultEndPoint(e);
        final CqlSession session = CqlSession.builder()
//                .addContactEndPoint(defaultEndpoint)
                .build();

//        Cluster cluster = Cluster.builder()
//                .addContactPoint(host)
//                .withPort(cqlPort)
//                .withClusterName(clusterName)
//                .withCompression(compression)
//                .withLoadBalancingPolicy(loadBalancingPolicy)
//                .withRetryPolicy(retryPolicy)
//                .withReconnectionPolicy(reconnectionPolicy)
//                .withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED)
//                .withSocketOptions(socketOptions)
//                .withoutJMXReporting()
//                .build();

        // Add Cluster for shutdown process
        ServerStarter.CASSANDRA_EMBEDDED.getShutdownHook().addSession(session);

        return session;
    }

    private String extractAndValidateKeyspaceName(TypedMap parameters) {
        String keyspaceName = parameters.getTyped(DEFAULT_KEYSPACE_NAME);
        Validator.validateNotBlank(keyspaceName, "The provided keyspace name should not be blank");
        Validator.validateTrue(KEYSPACE_NAME_PATTERN.matcher(keyspaceName).matches(),
                "The provided keyspace name '%s' should match the " + "following pattern : '%s'", keyspaceName,
                KEYSPACE_NAME_PATTERN.pattern());
        return keyspaceName;
    }

    private void createKeyspaceIfNeeded(CqlSession session, String keyspaceName, Boolean keyspaceDurableWrite) {
        final Map<String, Object> replicationOptions = new HashMap<>();
        replicationOptions.put("class", "SimpleStrategy");
        replicationOptions.put("replication_factor", 1);

        final SimpleStatement statement = SchemaBuilder.createKeyspace(keyspaceName).ifNotExists().withReplicationOptions(replicationOptions).withDurableWrites(keyspaceDurableWrite).build();

        LOGGER.info("Creating keyspace : " + statement.getQuery());

        session.execute(statement);
    }

    private void executeStartupScripts(CqlSession session, TypedMap parameters) {
        ScriptExecutor scriptExecutor = null;
        List<String> scriptLocations = parameters.getTypedOr(SCRIPT_LOCATIONS, new ArrayList<>());
        if (scriptLocations.size() > 0) {
            scriptExecutor = new ScriptExecutor(session);
            scriptLocations.forEach(scriptExecutor::executeScript);
        }

        final Map<String, Map<String, Object>> scriptTemplates = parameters.getTypedOr(SCRIPT_TEMPLATES, new HashMap<>());
        if (scriptTemplates.size() > 0) {
            scriptExecutor = scriptExecutor == null
                    ? new ScriptExecutor(session)
                    : scriptExecutor;

            final ScriptExecutor executor = scriptExecutor;

            scriptTemplates
                    .entrySet()
                    .forEach(entry -> executor.executeScriptTemplate(entry.getKey(), entry.getValue()));
        }
    }

    public CqlSession getSingletonSession() {
        return singletonSession;
    }
}
