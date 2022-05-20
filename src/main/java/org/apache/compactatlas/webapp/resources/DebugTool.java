package org.apache.compactatlas.webapp.resources;

import org.apache.compactatlas.graphdb.janus.AtlasJanusGraphDatabase;
import org.apache.compactatlas.quickstart.hive.AddClassification;
import org.apache.compactatlas.quickstart.hive.ImportHiveTable;
import org.apache.compactatlas.quickstart.hive.SetGlossary;
import org.apache.compactatlas.quickstart.hive.SetLineage;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphIndexQuery;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.P.*;

@RequestMapping("cmd")
@RestController
public class DebugTool {
    @Autowired
    AtlasJanusGraphDatabase janusGraphDatabase;

    @GetMapping("/printSchema")
    public String testHello1() {
        JanusGraph janusGraph = janusGraphDatabase.getGraphInstance();

        JanusGraphManagement mgmt = janusGraph.openManagement();
        System.out.println("schema:" + mgmt.printSchema());
        return "hello";
    }

    @GetMapping("/vertices")
    public String testHello2() {
        JanusGraph janusGraph = janusGraphDatabase.getGraphInstance();
        final GraphTraversalSource g = janusGraph.traversal();

        final List<Vertex> vertices = g.V().toList();
        for (Vertex v : vertices) {
            System.out.println("v.label:" + v.toString());
            System.out.println("--------------");
            final Iterator<VertexProperty<Object>> properties = v.properties();
            while (properties.hasNext()) {
                VertexProperty<Object> ap = properties.next();
                System.out.println(ap.key() + ":" + ap.value() + ";");
            }
            System.out.println("######");
        }
        return "hello";
    }

    @GetMapping("/edges")
    public String testHello3() {
        JanusGraph janusGraph = janusGraphDatabase.getGraphInstance();

        final GraphTraversalSource g = janusGraph.traversal();
        final List<Edge> edges = g.E().toList();
        for (Edge e : edges) {
            System.out.println("e.label:" + e.label());
            final Iterator<Property<Object>> properties = e.properties();
            while (properties.hasNext()) {
                Property<Object> ap = properties.next();
                System.out.print("pkey:" + ap.key() + ",pvalue:" + ap.value() + ";");
            }
            System.out.println("");
        }
        return "hello";
    }

    @GetMapping("/query")
    public String directquery(@RequestParam("qs") String qs) {
        JanusGraph janusGraph = janusGraphDatabase.getGraphInstance();

        Iterator<JanusGraphIndexQuery.Result<JanusGraphVertex>> results =
                janusGraph.indexQuery("vertexAindex", qs).vertexStream().iterator();

        while (results.hasNext()) {
            JanusGraphIndexQuery.Result<JanusGraphVertex> aResult = results.next();
            JanusGraphVertex element = aResult.getElement();
            Set<String> keys = element.keys();
            for (String key : keys) {
                try {
                    System.out.println(key + ":" + element.value(key));
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
            }
            System.out.println("----------");
        }
        return "hello";
    }

    @GetMapping("/kvquery")
    public String directquery(@RequestParam("key") String key, @RequestParam("val") String val) {
        JanusGraph janusGraph = janusGraphDatabase.getGraphInstance();
        Vertex retv = janusGraph.traversal().V().has(key, eq(val)).next();

        Set<String> keys = retv.keys();
        for (String akey : keys) {
            System.out.println(akey + ":" + retv.value(akey));
        }
        return "hello";
    }
}