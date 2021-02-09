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
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * Facility class to execute a CQL script file or a plain CQL statement
 */
public class ScriptExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScriptExecutor.class);

    private static final String COMMA = ";";
    private static final String BATCH_BEGIN = "BEGIN";
    private static final String BATCH_APPLY = "APPLY";

    private static final String CODE_DELIMITER_START = "^\\s*(?:AS)?\\s*\\$\\$\\s*$";
    private static final String CODE_DELIMITER_END = "^\\s*\\$\\$\\s*;\\s*$";
    private static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([a-z][a-zA-Z0-9_]*)\\}");
    private static final Pattern SPECIAL_REGEX_CHARS = Pattern.compile("[\\{\\}\\(\\)\\[\\]\\.\\+\\*\\?\\^\\$\\\\\\|]");

    private static final Map<String, Object> EMPTY_MAP = new HashMap<>();

    private final CqlSession session;

    public ScriptExecutor(CqlSession session) {
        this.session = session;
    }

    /**
     * Execute a CQL script file located in the class path
     *
     * @param scriptLocation the location of the script file in the class path
     */
    public void executeScript(String scriptLocation) {
        executeScriptTemplate(scriptLocation, EMPTY_MAP);
    }

    /**
     * Execute a CQL script template located in the class path and
     * inject provided values into the template to produce the actual script
     *
     * @param scriptTemplateLocation the location of the script template in the class path
     * @param values                 template values
     */
    public void executeScriptTemplate(String scriptTemplateLocation, Map<String, Object> values) {
        final List<SimpleStatement> statements = buildStatements(loadScriptAsLines(scriptTemplateLocation, values));
        for (SimpleStatement statement : statements) {
            LOGGER.debug("\tSCRIPT : {}\n", statement.getQuery());
            session.execute(statement);
        }
    }

    /**
     * Execute a plain CQL string statement
     *
     * @param statement plain CQL string statement
     * @return the resultSet
     */
    public ResultSet execute(String statement) {
        return session.execute(statement);
    }

    /**
     * Execute a CQL statement
     *
     * @param statement CQL statement
     * @return the resultSet
     */
    public ResultSet execute(Statement statement) {
        return session.execute(statement);
    }

    /**
     * Execute a plain CQL string statement asynchronously
     *
     * @param statement the CQL string statement
     * @return CompletableFuture&lt;ResultSet&gt;
     */
    public CompletableFuture<AsyncResultSet> executeAsync(String statement) {
        return session.executeAsync(statement).toCompletableFuture();
    }

    /**
     * Execute a CQL statement asynchronously
     *
     * @param statement CQL statement
     * @return CompletableFuture&lt;ResultSet&gt;
     */
    public CompletableFuture<AsyncResultSet> executeAsync(Statement statement) {
        return session.executeAsync(statement).toCompletableFuture();
    }

    protected List<String> loadScriptAsLines(String scriptLocation) {
        return loadScriptAsLines(scriptLocation, EMPTY_MAP);
    }

    protected List<String> loadScriptAsLines(String scriptLocation, Map<String, Object> variables) {

        InputStream inputStream = this.getClass().getResourceAsStream("/" + scriptLocation);

        ValidationHelper.validateNotNull(inputStream, "Cannot find CQL script file at location '%s'", scriptLocation);

        Scanner scanner = new Scanner(inputStream);
        List<String> lines = new ArrayList<>();
        while (scanner.hasNextLine()) {
            String nextLine = maybeReplaceVariables(scanner, variables);
            if (isNotBlank(nextLine)) {
                lines.add(nextLine);
            }
        }
        return lines;
    }

    private String maybeReplaceVariables(Scanner scanner, Map<String, Object> variables) {
        String nextLine = scanner.nextLine().trim();
        if (isNotBlank(nextLine) && !variables.isEmpty()) {
            final Matcher matcher = VARIABLE_PATTERN.matcher(nextLine);
            while (matcher.find()) {
                final String group = matcher.group(1);
                ValidationHelper.validateTrue(variables.containsKey(group),
                        "Cannot find value for variable ${%s} in the variable map provided to ScriptExecutor", group);
                final String replacement = SPECIAL_REGEX_CHARS.matcher(variables.get(group).toString()).replaceAll("\\\\$0");
                nextLine = nextLine.replaceFirst("\\$\\{" + group + "\\}", replacement);

            }
        }
        return nextLine;
    }

    protected List<SimpleStatement> buildStatements(List<String> lines) {
        List<SimpleStatement> statements = new ArrayList<>();
        StringBuilder statement = new StringBuilder();
        boolean batch = false;
        boolean codeBlock = false;
        StringBuilder batchStatement = new StringBuilder();
        for (String line : lines) {
            if (line.trim().startsWith(BATCH_BEGIN)) {
                batch = true;
            }
            if (line.trim().matches(CODE_DELIMITER_START)) {
                codeBlock = !codeBlock;
            }

            if (batch) {
                batchStatement.append(" ").append(line);
                if (line.trim().startsWith(BATCH_APPLY)) {
                    batch = false;
                    statements.add(SimpleStatement.newInstance(batchStatement.toString()));
                    batchStatement = new StringBuilder();
                }
            } else if (codeBlock) {
                statement.append(line);
                if (line.trim().matches(CODE_DELIMITER_END)) {
                    codeBlock = false;
                    statements.add(SimpleStatement.newInstance(statement.toString()));
                    statement = new StringBuilder();
                }
            } else {
                statement.append(line);
                if (line.trim().endsWith(COMMA)) {
                    statements.add(SimpleStatement.newInstance(statement.toString()));
                    statement = new StringBuilder();
                } else {
                    statement.append(" ");
                }
            }
        }
        return statements;
    }

    public CqlSession getSession() {
        return session;
    }
}
