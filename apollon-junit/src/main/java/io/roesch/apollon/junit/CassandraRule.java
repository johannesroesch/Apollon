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

package io.roesch.apollon.junit;

import com.datastax.oss.driver.api.core.CqlSession;
import io.roesch.apollon.embedded.*;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class CassandraRule extends ExternalResource {

    // Default statement cache for unit testing
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRule.class);

    private final TypedMap cassandraParams;
    private final CassandraShutDownHook cassandraShutDownHook = new CassandraShutDownHook();
    private CassandraEmbeddedServer server;
    private CqlSession session;
    private Consumer<CqlSession> doBefore = s -> {
    };
    private Consumer<CqlSession> doAfter = s -> {
    };

    private CassandraRule(final TypedMap parameters, final Consumer<CqlSession> doBefore, final Consumer<CqlSession> doAfter) {
        this.cassandraParams = parameters;
        this.doBefore = doBefore;
        this.doAfter = doAfter;
    }

    public static Builder builder() {
        return new Builder();
    }

    public CqlSession getNativeSession() {
        return this.session;
    }

    private CassandraEmbeddedServer buildServer() {
        return CassandraEmbeddedServerBuilder
                .builder()
                .withParams(cassandraParams)
                .withShutdownHook(cassandraShutDownHook)
                .cleanDataFilesAtStartup(true)
                .buildServer();
    }

    private CqlSession buildSession(CassandraEmbeddedServer server) {
        return server.getNativeSession();
    }

    @Override
    protected void before() throws Throwable {
        this.server = buildServer();
        this.session = this.server.getNativeSession();
        doBefore.accept(session);
    }

    @Override
    protected void after() {
        doAfter.accept(session);
    }

    public void shutdown() throws InterruptedException {
        session.close();
        cassandraShutDownHook.shutDownNow();
    }

    public static class Builder {
        private Consumer<CqlSession> doBefore = s -> {
        };
        private Consumer<CqlSession> doAfter = s -> {
        };
        private TypedMap parameters = CassandraEmbeddedConfigParameters.getDefaultParameters();

        private Builder() {
        }

        public CassandraRule build() {
            return new CassandraRule(parameters, doBefore, doAfter);
        }

        public Builder doBeforeTest(final Consumer<CqlSession> doBefore) {
            this.doBefore = doBefore;
            return this;
        }

        public Builder doAfterTest(final Consumer<CqlSession> doAfter) {
            this.doAfter = doAfter;
            return this;
        }

        public Builder withParameters(final TypedMap parameters) {
            this.parameters = CassandraEmbeddedConfigParameters.mergeWithDefaultParameters(parameters);
            return this;
        }
    }
}
