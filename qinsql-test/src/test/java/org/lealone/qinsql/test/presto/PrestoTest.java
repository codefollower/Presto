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

import java.util.Optional;

import com.facebook.presto.Session;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingSession;

public class PrestoTest {

    public static void main(String[] args) {
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
}
