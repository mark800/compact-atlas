/**
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
package org.apache.compactatlas.graphdb.janus.query;

import org.apache.compactatlas.graphdb.janus.AtlasJanusGraph;
import org.apache.compactatlas.graphdb.api.AtlasGraphQuery;
import org.apache.compactatlas.graphdb.janus.AtlasJanusEdge;
import org.apache.compactatlas.graphdb.janus.AtlasJanusVertex;
import org.apache.compactatlas.graphdb.common.tinkerpop.query.NativeTinkerpopGraphQuery;
import org.apache.compactatlas.graphdb.common.tinkerpop.query.NativeTinkerpopQueryFactory;
import org.apache.compactatlas.graphdb.common.tinkerpop.query.TinkerpopGraphQuery;

/**
 * Janus implementation of TinkerpopGraphQuery.
 */
public class AtlasJanusGraphQuery extends TinkerpopGraphQuery<AtlasJanusVertex, AtlasJanusEdge>
        implements NativeTinkerpopQueryFactory<AtlasJanusVertex, AtlasJanusEdge> {

    public AtlasJanusGraphQuery(AtlasJanusGraph graph, boolean isChildQuery) {
        super(graph, isChildQuery);
    }

    public AtlasJanusGraphQuery(AtlasJanusGraph graph) {
        super(graph);
    }

    @Override
    public AtlasGraphQuery<AtlasJanusVertex, AtlasJanusEdge> createChildQuery() {
        return new AtlasJanusGraphQuery((AtlasJanusGraph) graph, true);
    }

    @Override
    protected NativeTinkerpopQueryFactory<AtlasJanusVertex, AtlasJanusEdge> getQueryFactory() {
        return this;
    }

    @Override
    public NativeTinkerpopGraphQuery<AtlasJanusVertex, AtlasJanusEdge> createNativeTinkerpopQuery() {
        return new NativeJanusGraphQuery((AtlasJanusGraph) graph);
    }
}
