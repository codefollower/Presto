/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.qinsql.test.presto;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.sql.analyzer.AbstractAnalyzerTest;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.transaction.TransactionBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class PrestoTest {

    public static void main(String[] args) {
        testLocalQueryRunner();
        testAnalyzer();
        testGroupingSets();
    }

    private static void testLocalQueryRunner() {
        Session session = TestingSession.testSessionBuilder()
                .setIdentity(new Identity("test_current_user", Optional.empty())).build();
        QueryRunner runner = new LocalQueryRunner(session);
        String sql = "SELECT CURRENT_USER";
        MaterializedResult result = runner.execute(runner.getDefaultSession(), sql).toTestTypes();
        result.forEach(row -> {
            System.out.println(row);
        });
        runner.close();
    }

    private static void testAnalyzer() {
        PrestoAnalyzerTest analyzer = new PrestoAnalyzerTest();
        analyzer.setup();
        analyzer.testInsert();
        analyzer.testCTAS();
    }

    private static class PrestoAnalyzerTest extends AbstractAnalyzerTest {
        public void testInsert() {
            assertUtilizedTableColumns("INSERT INTO t2 SELECT a, b FROM t1",
                    ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b")));
        }

        public void testCTAS() {
            assertUtilizedTableColumns("CREATE TABLE foo AS SELECT a, b FROM t1",
                    ImmutableMap.of(QualifiedObjectName.valueOf("tpch.s1.t1"), ImmutableSet.of("a", "b")));
        }

        @SuppressWarnings("deprecation")
        private void assertUtilizedTableColumns(String query, Map<QualifiedObjectName, Set<String>> expected) {
            TransactionBuilder.transaction(transactionManager, accessControl).singleStatement().readUncommitted()
                    .readOnly().execute(CLIENT_SESSION, session -> {
                        Analyzer analyzer = createAnalyzer(session, metadata, WarningCollector.NOOP);
                        Statement statement = SQL_PARSER.createStatement(query);
                        Analysis analysis = analyzer.analyze(statement);
                        org.junit.Assert.assertEquals(
                                analysis.getUtilizedTableColumnReferences().values().stream().findFirst().get(),
                                expected);
                    });
        }
    }

    public static void testGroupingSets() { 
        Session session = TestingSession.testSessionBuilder()
            .setCatalog("local")
            .setSchema("default")
            .build();
        QueryRunner runner = new LocalQueryRunner(session);
        // runner.execute(runner.getDefaultSession(),"CREATE TABLE IF NOT EXISTS orders (f1 bigint,f1 bigint)");
        runner.execute(runner.getDefaultSession(),
                "SELECT k, count(DISTINCT x) FILTER (WHERE y = 100), count(DISTINCT x) FILTER (WHERE y = 200) FROM " +
                        "(VALUES " +
                        "   (1, 1, 100)," +
                        "   (1, 1, 200)," +
                        "   (1, 2, 100)," +
                        "   (1, 3, 300)," +
                        "   (2, 1, 100)," +
                        "   (2, 10, 100)," +
                        "   (2, 20, 100)," +
                        "   (2, 20, 200)," +
                        "   (2, 30, 300)," +
                        "   (2, 40, 100)" +
                        ") t(k, x, y) " +
                        "GROUP BY GROUPING SETS ((), (k))");
        runner.execute(runner.getDefaultSession(),
                "VALUES " +
                        "(1, BIGINT '2', BIGINT '1'), " +
                        "(2, BIGINT '4', BIGINT '1'), " +
                        "(CAST(NULL AS INTEGER), BIGINT '5', BIGINT '2')");
        runner.close();
    }
}
