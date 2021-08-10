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

import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.server.TestHttpRequestSessionContext.createFunctionAdd;
import static com.facebook.presto.server.TestHttpRequestSessionContext.createSqlFunctionIdAdd;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.google.common.collect.ImmutableList;

public class PrestoClientTest {

    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private static final SqlFunctionId SQL_FUNCTION_ID_ADD = createSqlFunctionIdAdd();
    private static final SqlInvokedFunction SQL_FUNCTION_ADD = createFunctionAdd();
    private static final String SERIALIZED_SQL_FUNCTION_ID_ADD = jsonCodec(SqlFunctionId.class)
            .toJson(SQL_FUNCTION_ID_ADD);
    private static final String SERIALIZED_SQL_FUNCTION_ADD = jsonCodec(SqlInvokedFunction.class)
            .toJson(SQL_FUNCTION_ADD);

    public static URI uriFor(String path) throws Exception, URISyntaxException {
        return HttpUriBuilder.uriBuilderFrom(new URL("http://127.0.0.1:51449").toURI()).replacePath(path).build();
    }

    protected static String urlEncode(String value) {
        try {
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    public static void main(String[] args) throws URISyntaxException, Exception {
        HttpClient client = new JettyHttpClient();// start query
        Request request = preparePost().setUri(uriFor("/v1/statement"))
                .setBodyGenerator(createStaticBodyGenerator("show catalogs", UTF_8)).setHeader(PRESTO_USER, "user")
                .setHeader(PRESTO_SOURCE, "source").setHeader(PRESTO_CATALOG, "catalog")
                .setHeader(PRESTO_SCHEMA, "schema").setHeader(PRESTO_CLIENT_INFO, "{\"clientVersion\":\"testVersion\"}")
                .addHeader(PRESTO_SESSION, QUERY_MAX_MEMORY + "=1GB")
                .addHeader(PRESTO_SESSION, JOIN_DISTRIBUTION_TYPE + "=partitioned," + HASH_PARTITION_COUNT + " = 43")
                .addHeader(PRESTO_PREPARED_STATEMENT, "foo=select * from bar")
                .addHeader(PRESTO_SESSION_FUNCTION, format("%s=%s", urlEncode(SERIALIZED_SQL_FUNCTION_ID_ADD),
                        urlEncode(SERIALIZED_SQL_FUNCTION_ADD)))
                .build();

        QueryResults queryResults = client.execute(request, createJsonResponseHandler(QUERY_RESULTS_CODEC));

        ImmutableList.Builder<List<Object>> data = ImmutableList.builder();
        while (queryResults.getNextUri() != null) {
            queryResults = client.execute(prepareGet().setUri(queryResults.getNextUri()).build(),
                    createJsonResponseHandler(QUERY_RESULTS_CODEC));

            if (queryResults.getData() != null) {
                data.addAll(queryResults.getData());
            }
        }
        client.close();
    }

}
