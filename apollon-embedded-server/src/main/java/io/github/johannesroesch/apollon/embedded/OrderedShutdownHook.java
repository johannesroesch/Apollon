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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class OrderedShutdownHook {
    private static final Logger log = LoggerFactory.getLogger(OrderedShutdownHook.class);

    private final Set<CqlSession> sessions = new CopyOnWriteArraySet<>();

    void addSession(CqlSession session) {
        sessions.add(session);
    }


    void callShutDown() {
        sessions.forEach(session -> {
            log.info(String.format("Call shutdown on Session instance '%s'", session.toString()));
            session.close();
        });
    }
}
