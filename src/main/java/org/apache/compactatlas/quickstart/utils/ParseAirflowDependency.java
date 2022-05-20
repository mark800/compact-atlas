package org.apache.compactatlas.quickstart.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class ParseAirflowDependency {
    private static final Logger LOG = LoggerFactory.getLogger(ParseAirflowDependency.class);
    private static String depMark = " >> ";

    public static Map<String, Set<String>> getDepinfoFromAirflowConfigFile(String fileName) {
        List<String> contents = getFileLines(fileName);

        Map<String, Set<String>> retDependencyInfo = new HashMap<>();

        StringBuilder depLine = new StringBuilder();
        for (String aline : contents) {
            //concat dep lines to one line
            if (depLine.length() > 5 && aline.trim().length() > 0 && !aline.trim().startsWith("#")) {
                depLine.append(aline.trim());
                continue;
            }
            if (depLine.length() > 5 && (aline.trim().length() == 0 || aline.trim().startsWith("#"))) {
                parseDep(depLine, retDependencyInfo);
                depLine = new StringBuilder();
                continue;
            }
            //find dep new start: contains depMark, or start with '['. start_line. until empty lien, end_line
            if (aline.trim().length() > 5 && (aline.contains(depMark) || aline.trim().startsWith("["))
                    && !aline.trim().startsWith("#")) {
                depLine.append(aline.trim());
            }
        }
        return retDependencyInfo;
    }

    public static void parseDep(StringBuilder depLine, Map<String, Set<String>> retDepInfo) {
        String[] p2 = depLine.toString().split(depMark);
        String output = p2[1].trim();
        Set<String> inputs = new HashSet<>();
        String ins = p2[0].trim();
        if (ins.startsWith("[") && ins.endsWith("]")) {
            String pureInputs = ins.substring(1, ins.length() - 1);
            String[] ts = pureInputs.split(",");
            for (String at : ts) {
                inputs.add(at.trim());
            }
        } else if (ins.startsWith("[") || ins.endsWith("]")) {
            //error
            LOG.error("[] mark not appear at the same time!");
        } else {
            //only one input table
            inputs.add(ins);
        }
        retDepInfo.put(output, inputs);
    }

    public static List<String> getFileLines(String fileName) {
        try {
            Stream<String> lines = Files.lines(Paths.get(fileName));
            List<String> ret = new LinkedList<>();
            lines.forEach(l -> ret.add(l));
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}

