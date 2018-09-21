/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.sql.client.interfaces;


public class QueryResult {
    private int m_execId;
    private boolean m_success;
    private SqlSchema m_schema;

    public QueryResult(int execId, SqlSchema schema, Boolean success) {
        if(schema == null)
            throw new IllegalArgumentException();
        m_execId = execId;
        m_schema = schema;
        m_success = success;
    }

    public int getExecutionId() {
        return m_execId;
    }

    public SqlSchema getSchema() {
        return m_schema;
    }

    public boolean succeeded() {
        return m_success;
    }
}
