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

package io.github.johannesroesch.apollon.embedded;

import com.google.common.collect.ImmutableSet;
import io.github.johannesroesch.apollon.exception.ApollonException;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.johannesroesch.apollon.embedded.ApollonCassandraConfig.*;
import static io.github.johannesroesch.apollon.embedded.CassandraEmbeddedConfigParameters.*;
import static java.util.concurrent.TimeUnit.SECONDS;

public enum ServerStarter {
    CASSANDRA_EMBEDDED;

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerStarter.class);

    private static final OrderedShutdownHook orderedShutdownHook = new OrderedShutdownHook();
    private static int cqlPort;

    private static int thriftPort;

    private static int storageRandomPort() {
        return PortFinder.findAvailableBetween(7001, 7500);
    }

    private static int storageSslRandomPort() {
        return PortFinder.findAvailableBetween(7501, 7999);
    }

    private static int jxmRandomPort() {
        return PortFinder.findAvailableBetween(7501, 7999);
    }

    private static int cqlRandomPort() {
        return PortFinder.findFirstAvailableBetween(9042, 9499);
    }

    private static int thriftRandomPort() {
        return PortFinder.findFirstAvailableBetween(9160, 9999);
    }

    public void startServer(String cassandraHost, TypedMap parameters) {
        if (StringUtils.isBlank(cassandraHost)) {

            LOGGER.debug("Do start embedded Cassandra server ");
            validateDataFolders(parameters);
            cleanCassandraDataFiles(parameters);
            randomizePortsIfNeeded(parameters);


            // Start embedded server
            CASSANDRA_EMBEDDED.start(parameters);
        }
    }

    public void checkAndConfigurePorts(TypedMap parameters) {
        LOGGER.trace("Check and configure Thrift/CQL ports");
        Integer cqlPort = parameters.getTyped(CASSANDRA_CQL_PORT);
        Integer thriftPort = parameters.getTyped(CASSANDRA_THRIFT_PORT);
        if (cqlPort != null && ServerStarter.cqlPort != cqlPort) {
            throw new IllegalArgumentException(String.format("An embedded Cassandra server is already listening to CQL port '%s', the specified CQL port '%s' does not match", ServerStarter.cqlPort, cqlPort));
        } else {
            parameters.put(CASSANDRA_CQL_PORT, ServerStarter.cqlPort);
        }

        if (thriftPort != null && ServerStarter.thriftPort != thriftPort) {
            throw new IllegalArgumentException(String.format("An embedded Cassandra server is already listening to Thrift port '%s', the specified Thrift port '%s' does not match", ServerStarter.thriftPort, thriftPort));
        } else {
            parameters.put(CASSANDRA_THRIFT_PORT, ServerStarter.thriftPort);
        }
    }

    public OrderedShutdownHook getShutdownHook() {
        return orderedShutdownHook;
    }

    public void shutdown() {
        orderedShutdownHook.callShutDown();
    }

    private void start(final TypedMap parameters) {
        if (isAlreadyRunning() && CassandraEmbeddedServer.embeddedServerStarted) {
            LOGGER.debug("Cassandra is already running, not starting new one");
            return;
        }

        final String triggersDir = createTriggersFolder();

        LOGGER.info(" Cassandra listen address = {}", parameters.<String>getTyped(LISTEN_ADDRESS));
        LOGGER.info(" Cassandra RPC address = {}", parameters.<String>getTyped(RPC_ADDRESS));
        LOGGER.info(" Cassandra broadcast address = {}", parameters.<String>getTyped(BROADCAST_ADDRESS));
        LOGGER.info(" Cassandra RPC broadcast address = {}", parameters.<String>getTyped(BROADCAST_RPC_ADDRESS));
        LOGGER.info(" Random embedded Cassandra RPC port/Thrift port = {}", parameters.<Integer>getTyped(CASSANDRA_THRIFT_PORT));
        LOGGER.info(" Random embedded Cassandra Native port/CQL port = {}", parameters.<Integer>getTyped(CASSANDRA_CQL_PORT));
        LOGGER.info(" Random embedded Cassandra Storage port = {}", parameters.<Integer>getTyped(CASSANDRA_STORAGE_PORT));
        LOGGER.info(" Random embedded Cassandra Storage SSL port = {}", parameters.<Integer>getTyped(CASSANDRA_STORAGE_SSL_PORT));
        LOGGER.info(" Random embedded Cassandra Remote JMX port = {}", System.getProperty("com.sun.management.jmxremote.port", "null"));
        LOGGER.info(" Embedded Cassandra triggers directory = {}", triggersDir);

        LOGGER.info("Starting Cassandra...");

        System.setProperty("cassandra.triggers_dir", triggersDir);
        System.setProperty("cassandra.embedded.concurrent.reads", parameters.getTypedOr(CASSANDRA_CONCURRENT_READS, 32).toString());
        System.setProperty("cassandra.embedded.concurrent.writes", parameters.getTypedOr(CASSANDRA_CONCURRENT_WRITES, 32).toString());
        System.setProperty("cassandra-foreground", "true");

        final boolean useUnsafeCassandra = parameters.getTyped(USE_UNSAFE_CASSANDRA_DAEMON);

        if (useUnsafeCassandra) {
            System.setProperty("cassandra-num-tokens", "1");
        }

        System.setProperty("cassandra.config.loader", "io.github.johannesroesch.apollon.embedded.ApollonCassandraConfig");

        final CountDownLatch startupLatch = new CountDownLatch(1);

        ThreadFactory threadFactory = new DefaultThreadFactory("Apollon");

        final ExecutorService executor = Executors.newSingleThreadExecutor(threadFactory);
        final AtomicReference<CassandraDaemon> daemonRef = new AtomicReference<>();
        executor.execute(() -> {
            if (useUnsafeCassandra) {
                LOGGER.warn("******* WARNING, starting unsafe embedded Cassandra daemon. This should be only used for unit testing or development and not for production !");
            }

            CassandraDaemon cassandraDaemon = useUnsafeCassandra
                    ? new ApollonCassandraDaemon() : new CassandraDaemon();

            cassandraDaemon.completeSetup();
            cassandraDaemon.activate();
            daemonRef.getAndSet(cassandraDaemon);
            startupLatch.countDown();
        });


        try {
            startupLatch.await(30, SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Timeout starting Cassandra embedded", e);
            throw new IllegalStateException("Timeout starting Cassandra embedded", e);
        }

        if (parameters.containsKey(SHUTDOWN_HOOK)) {
            CassandraShutDownHook shutDownHook = parameters.getTyped(SHUTDOWN_HOOK);
            shutDownHook.addCassandraDaemonRef(daemonRef);
            shutDownHook.addOrderedShutdownHook(orderedShutdownHook);
            shutDownHook.addExecutorService(executor);
        } else {
            // Generate an OrderedShutdownHook to shutdown all connections from java clients before closing the server
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Calling stop on Embedded Cassandra server");
                daemonRef.get().stop();

                LOGGER.info("Calling shutdown on all Cluster instances");
                // First call shutdown on all registered Java driver Cluster instances
                orderedShutdownHook.callShutDown();

                LOGGER.info("Shutting down embedded Cassandra server");
                // Then shutdown the server
                executor.shutdownNow();
            }));
        }

    }

    private void validateDataFolders(Map<String, Object> parameters) {
        final String dataFolder = (String) parameters.get(DATA_FILE_FOLDER);
        final String commitLogFolder = (String) parameters.get(COMMIT_LOG_FOLDER);
        final String savedCachesFolder = (String) parameters.get(SAVED_CACHES_FOLDER);
        final String hintsFolder = (String) parameters.get(HINTS_FOLDER);
        final String cdcRawFolder = (String) parameters.get(CDC_RAW_FOLDER);

        LOGGER.debug(" Embedded Cassandra data directory = {}", dataFolder);
        LOGGER.debug(" Embedded Cassandra commitlog directory = {}", commitLogFolder);
        LOGGER.debug(" Embedded Cassandra saved caches directory = {}", savedCachesFolder);
        LOGGER.debug(" Embedded Cassandra hints directory = {}", hintsFolder);
        LOGGER.debug(" Embedded Cassandra cdc_raw directory = {}", cdcRawFolder);

        validateFolder(dataFolder);
        validateFolder(commitLogFolder);
        validateFolder(savedCachesFolder);
        validateFolder(hintsFolder);
        validateFolder(cdcRawFolder);

        System.setProperty(ACHILLES_EMBEDDED_CASSANDRA_DATA_FOLDER, dataFolder);
        System.setProperty(ACHILLES_EMBEDDED_CASSANDRA_COMMITLOG_FOLDER, commitLogFolder);
        System.setProperty(ACHILLES_EMBEDDED_CASSANDRA_SAVED_CACHES_FOLDER, savedCachesFolder);
        System.setProperty(ACHILLES_EMBEDDED_CASSANDRA_HINTS_FOLDER, hintsFolder);
        System.setProperty(ACHILLES_EMBEDDED_CASSANDRA_CDC_RAW_FOLDER, cdcRawFolder);

    }

    private void validateFolder(String folderPath) {
        String currentUser = System.getProperty("user.name");
        final File folder = new File(folderPath);
        if (!DEFAULT_ACHILLES_TEST_FOLDERS.contains(folderPath)) {
            ValidationHelper.validateTrue(folder.exists(), "Folder '%s' does not exist", folder.getAbsolutePath());
            ValidationHelper.validateTrue(folder.isDirectory(), "Folder '%s' is not a directory", folder.getAbsolutePath());
            ValidationHelper.validateTrue(folder.canRead(), "No read credential. Please grant read permission for the current user '%s' on folder '%s'", currentUser, folder.getAbsolutePath());
            ValidationHelper.validateTrue(folder.canWrite(), "No write credential. Please grant write permission for the current user '%s' on folder '%s'", currentUser, folder.getAbsolutePath());
        } else if (!folder.exists()) {
            try {
                LOGGER.info("Creating folder : {}", folder.getAbsolutePath());
                FileUtils.forceMkdir(folder);
            } catch (IOException e) {
                throw new RuntimeException("Cannot create Cassandra data folder " + folderPath, e);
            }
        } else {
            LOGGER.info("Using existing data folder for unit tests : {}", folder.getAbsolutePath());
        }
    }

    private void cleanCassandraDataFiles(TypedMap parameters) {
        if (parameters.<Boolean>getTyped(CLEAN_CASSANDRA_DATA_FILES)) {
            final ImmutableSet<String> dataFolders = ImmutableSet.<String>builder()
                    .add(parameters.<String>getTyped(DATA_FILE_FOLDER))
                    .add(parameters.<String>getTyped(COMMIT_LOG_FOLDER))
                    .add(parameters.<String>getTyped(SAVED_CACHES_FOLDER)).build();
            for (String dataFolder : dataFolders) {
                File dataFolderFile = new File(dataFolder);
                if (dataFolderFile.exists() && dataFolderFile.isDirectory()) {
                    LOGGER.info("Cleaning up embedded Cassandra data directory '{}'", dataFolderFile.getAbsolutePath());
                    try {
                        FileUtils.cleanDirectory(dataFolderFile);
                    } catch (IOException e) {
                        throw new ApollonException(String.format("Cannot clean data folder %s", dataFolder));
                    }
                }
            }
        }
    }

    private void randomizePortsIfNeeded(TypedMap parameters) {
        final Integer portThrift = extractAndValidatePort(Optional.ofNullable(parameters.get(CASSANDRA_THRIFT_PORT))
                .orElseGet(ServerStarter::thriftRandomPort), CASSANDRA_THRIFT_PORT);
        final Integer protCql = extractAndValidatePort(Optional.ofNullable(parameters.get(CASSANDRA_CQL_PORT))
                .orElseGet(ServerStarter::cqlRandomPort), CASSANDRA_CQL_PORT);
        final Integer storagePort = extractAndValidatePort(Optional.ofNullable(parameters.get(CASSANDRA_STORAGE_PORT))
                .orElseGet(ServerStarter::storageRandomPort), CASSANDRA_STORAGE_PORT);
        final Integer storageSSLPort = extractAndValidatePort(
                Optional.ofNullable(parameters.get(CASSANDRA_STORAGE_SSL_PORT))
                        .orElseGet(ServerStarter::storageSslRandomPort), CASSANDRA_STORAGE_SSL_PORT);

        final Integer jmxPort = extractAndValidatePort(Optional.ofNullable(parameters.get(CASSANDRA_JMX_PORT))
                .orElseGet(ServerStarter::jxmRandomPort), CASSANDRA_JMX_PORT);


        parameters.put(CASSANDRA_THRIFT_PORT, portThrift);
        parameters.put(CASSANDRA_CQL_PORT, protCql);
        parameters.put(CASSANDRA_STORAGE_PORT, storagePort);
        parameters.put(CASSANDRA_STORAGE_SSL_PORT, storageSSLPort);

        System.setProperty(ACHILLES_EMBEDDED_CASSANDRA_LISTEN_ADDRESS, parameters.getTyped(LISTEN_ADDRESS));
        System.setProperty(ACHILLES_EMBEDDED_CASSANDRA_RPC_ADDRESS, parameters.getTyped(RPC_ADDRESS));
        System.setProperty(ACHILLES_EMBEDDED_CASSANDRA_BROADCAST_ADDRESS, parameters.getTyped(BROADCAST_ADDRESS));
        System.setProperty(ACHILLES_EMBEDDED_CASSANDRA_BROADCAST_RPC_ADDRESS, parameters.getTyped(BROADCAST_RPC_ADDRESS));

        System.setProperty(ACHILLES_EMBEDDED_CASSANDRA_THRIFT_PORT, portThrift.toString());
        System.setProperty(ACHILLES_EMBEDDED_CASSANDRA_CQL_PORT, protCql.toString());
        System.setProperty(ACHILLES_EMBEDDED_CASSANDRA_STORAGE_PORT, storagePort.toString());
        System.setProperty(ACHILLES_EMBEDDED_CASSANDRA_STORAGE_SSL_PORT, storageSSLPort.toString());

        System.setProperty("cassandra.jmx.local.port", jmxPort.toString());
        System.setProperty("cassandra.skip_wait_for_gossip_to_settle", "0");

        ServerStarter.cqlPort = protCql;
        ServerStarter.thriftPort = portThrift;
    }

    private Integer extractAndValidatePort(Object port, String portLabel) {
        ValidationHelper.validateTrue(port instanceof Integer, "The provided '%s' port should be an integer", portLabel);
        ValidationHelper.validateTrue((Integer) port > 0, "The provided '%s' port should positive", portLabel);
        return (Integer) port;

    }

    private String createTriggersFolder() {
        LOGGER.trace("Create triggers folder");
        final File triggersDir = new File(DEFAULT_ACHILLES_TEST_TRIGGERS_FOLDER);
        if (!triggersDir.exists()) {
            triggersDir.mkdir();
        }
        return triggersDir.getAbsolutePath();
    }

    private boolean isAlreadyRunning() {
        LOGGER.trace("Check whether an embedded Cassandra is already running");
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            MBeanInfo mBeanInfo = mbs.getMBeanInfo(new ObjectName("org.apache.cassandra.db:type=StorageService"));
            return mBeanInfo != null;
        } catch (InstanceNotFoundException e) {
            return false;
        } catch (IntrospectionException | ReflectionException | MalformedObjectNameException e) {
            throw new IllegalStateException("Cannot check if cassandra is already running", e);
        }

    }
}
