/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.qinsql.engine.server;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.server.TcpServer;

public class QinSQLServer extends TcpServer implements AsyncConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(QinSQLServer.class);

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public String getType() {
        return QinSQLServerEngine.NAME;
    }

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        super.start();

        try {
            startPresto();
        } catch (Exception e) {
            logger.error("Failed to start presto", e);
        }
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        super.stop();
    }

    private void startPresto() throws Exception {
    }
}
