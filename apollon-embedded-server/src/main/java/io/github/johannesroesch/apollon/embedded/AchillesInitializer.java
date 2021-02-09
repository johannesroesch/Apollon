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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import static io.github.johannesroesch.apollon.embedded.CassandraEmbeddedConfigParameters.*;

public class AchillesInitializer {
    private static final AtomicBoolean STARTED = new AtomicBoolean(false);

    private static final Logger LOGGER = LoggerFactory.getLogger(AchillesInitializer.class);

    private static final Pattern KEYSPACE_NAME_PATTERN = Pattern.compile("[a-zA-Z][_a-zA-Z0-9]{0,31}");

    private CqlSession singletonSession;

    void initializeFromParameters(TypedMap parameters) {
        synchronized (STARTED) {
            final String keyspaceName = extractAndValidateKeyspaceName(parameters);
            final Boolean durableWrite = parameters.getTyped(KEYSPACE_DURABLE_WRITE);

            if (!STARTED.get()) {
                LOGGER.debug("Creating cluster and session singletons");
                singletonSession = initializeSession();
                createKeyspaceIfNeeded(singletonSession, keyspaceName, durableWrite);
                executeStartupScripts(singletonSession, parameters);
                STARTED.getAndSet(true);
            } else {
                LOGGER.debug("Cluster and session singletons already created");
                createKeyspaceIfNeeded(singletonSession, keyspaceName, durableWrite);
                executeStartupScripts(singletonSession, parameters);
            }
        }
    }

    private CqlSession initializeSession() {
        final CqlSession session = CqlSession.builder().build();

        // Add session for shutdown process
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

        LOGGER.info("Creating keyspace : {}", statement.getQuery());

        session.execute(statement);
    }

    private void executeStartupScripts(CqlSession session, TypedMap parameters) {
        ScriptExecutor scriptExecutor = null;
        List<String> scriptLocations = parameters.getTypedOr(SCRIPT_LOCATIONS, new ArrayList<>());
        if (!scriptLocations.isEmpty()) {
            scriptExecutor = new ScriptExecutor(session);
            scriptLocations.forEach(scriptExecutor::executeScript);
        }

        final Map<String, Map<String, Object>> scriptTemplates = parameters.getTypedOr(SCRIPT_TEMPLATES, new HashMap<>());
        if (!scriptTemplates.isEmpty()) {
            scriptExecutor = scriptExecutor == null
                    ? new ScriptExecutor(session)
                    : scriptExecutor;

            final ScriptExecutor executor = scriptExecutor;

            scriptTemplates.forEach(executor::executeScriptTemplate);
        }
    }

    public CqlSession getSingletonSession() {
        return singletonSession;
    }
}
