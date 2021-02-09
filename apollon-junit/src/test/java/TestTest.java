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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import io.github.johannesroesch.apollon.junit.CassandraRule;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.assertTrue;

@FixMethodOrder(value = MethodSorters.NAME_ASCENDING)
public class TestTest {
    @ClassRule
    public static final CassandraRule cassandraRule = CassandraRule.builder().build();

    @Test
    public void test1createKeyspace() {
        final CqlSession session = cassandraRule.getNativeSession();
        final ResultSet rs = session.execute(SchemaBuilder.createKeyspace("bla").ifNotExists().withSimpleStrategy(1).build());
        assertTrue(rs.wasApplied());
    }

    @Test
    public void test2createTable() {
        final CqlSession session = cassandraRule.getNativeSession();
        final ResultSet rs = session.execute(SchemaBuilder.createTable("bla", "TEST").ifNotExists().withPartitionKey("a", new PrimitiveType(ProtocolConstants.DataType.INT)).build());
        assertTrue(rs.wasApplied());
    }
}
