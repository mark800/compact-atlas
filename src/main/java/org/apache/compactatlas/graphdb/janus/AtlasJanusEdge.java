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
package org.apache.compactatlas.graphdb.janus;

import org.apache.compactatlas.graphdb.api.AtlasEdge;
import org.apache.compactatlas.graphdb.api.AtlasVertex;
import org.apache.tinkerpop.gremlin.structure.Edge;

/**
 * Janus implementation of AtlasEdge.
 */
public class AtlasJanusEdge extends AtlasJanusElement<Edge> implements AtlasEdge<AtlasJanusVertex, AtlasJanusEdge> {


    public AtlasJanusEdge(AtlasJanusGraph graph, Edge edge) {
        super(graph, edge);
    }

    @Override
    public String getLabel() {
        return getWrappedElement().label();
    }

    @Override
    public AtlasJanusEdge getE() {

        return this;
    }

    @Override
    public AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> getInVertex() {
        return GraphDbObjectFactory.createVertex(graph, getWrappedElement().inVertex());
    }

    @Override
    public AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> getOutVertex() {
        return GraphDbObjectFactory.createVertex(graph, getWrappedElement().outVertex());
    }

}
