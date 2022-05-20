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

import org.apache.compactatlas.graphdb.api.AtlasEdgeLabel;
import org.janusgraph.core.EdgeLabel;

/**
 *
 */
public class AtlasJanusEdgeLabel implements AtlasEdgeLabel {

    private final EdgeLabel wrapped;

    public AtlasJanusEdgeLabel(EdgeLabel toWrap) {
        wrapped = toWrap;
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasPropertyKey#getName()
     */
    @Override
    public String getName() {
        return wrapped.name();
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37*result + wrapped.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof AtlasJanusEdgeLabel)) {
            return false;
        }
        AtlasJanusEdgeLabel otherKey = (AtlasJanusEdgeLabel)other;
        return otherKey.wrapped.equals(wrapped);

    }
}
