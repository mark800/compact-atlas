package org.apache.compactatlas.quickstart.hive;

import org.apache.compactatlas.repository.repository.patches.AtlasPatchRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

public class HiveTableUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasPatchRegistry.class);
    private static final String jdbcUrl = "jdbc:hive2://datanode03:10000/";
    private static final String jdbcUser = "atlas";
    private static final String SEPRATOR = "-#-";


    public static class ColumnInfo {
        private String columnName;
        private String comment;
        private String type;

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    public static class TableInfo {
        private String dbName;
        private String tableName;
        private String type;
        private String location;
        private boolean temporary;
        private long createTime;//milliseconds
        private long lastAccessTime;
        private String owner;
        private String retention;
        private String comment;
        private String viewOriginalText;
        private String viewExpandedText;
        private List<ColumnInfo> partitionKeys;
        private List<ColumnInfo> columns;
        private String createTableString;

        public String getCreateTableString() {
            return createTableString;
        }

        public void setCreateTableString(String createTableString) {
            this.createTableString = createTableString;
        }

        public long getCreateTime() {
            return createTime;
        }

        public void setCreateTime(long createTime) {
            this.createTime = createTime;
        }

        public long getLastAccessTime() {
            return lastAccessTime;
        }

        public void setLastAccessTime(long lastAccessTime) {
            this.lastAccessTime = lastAccessTime;
        }

        public String getOwner() {
            return owner;
        }

        public void setOwner(String owner) {
            this.owner = owner;
        }

        public String getRetention() {
            return retention;
        }

        public void setRetention(String retention) {
            this.retention = retention;
        }

        public String getComment() {
            return comment;
        }

        public void setComment(String comment) {
            this.comment = comment;
        }

        public String getViewOriginalText() {
            return viewOriginalText;
        }

        public void setViewOriginalText(String viewOriginalText) {
            this.viewOriginalText = viewOriginalText;
        }

        public String getViewExpandedText() {
            return viewExpandedText;
        }

        public void setViewExpandedText(String viewExpandedText) {
            this.viewExpandedText = viewExpandedText;
        }

        public List<ColumnInfo> getPartitionKeys() {
            return partitionKeys;
        }

        public void setPartitionKeys(List<ColumnInfo> partitionKeys) {
            this.partitionKeys = partitionKeys;
        }

        public List<ColumnInfo> getColumns() {
            return columns;
        }

        public void setColumns(List<ColumnInfo> columns) {
            this.columns = columns;
        }

        public boolean isTemporary() {
            return temporary;
        }

        public void setTemporary(boolean temporary) {
            this.temporary = temporary;
        }

        public String getDbName() {
            return dbName;
        }

        public void setDbName(String dbName) {
            this.dbName = dbName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("cols:");
            for (ColumnInfo col : columns) {
                sb.append(col.columnName);
                sb.append(",");
            }
            sb.append("\n");
            sb.append("partitions:");
            for (ColumnInfo col : partitionKeys) {
                sb.append(col.columnName);
                sb.append(",");
            }
            sb.append("\n");
            sb.append(dbName + "," + tableName);
            sb.append("\nlocation:");
            sb.append(location);
            return sb.toString();
        }
    }

    public static TableInfo getHiveTableInfo(String db, String table, String path) {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setDbName(db);
        tableInfo.setTableName(table);
        List<ColumnInfo> columns = new LinkedList<>();
        List<ColumnInfo> partitions = new LinkedList<>();
        List<String> ret = getQueryResult_debug(table, path);
        if (ret == null) {
            return tableInfo;
        }
        String mark = "init";
        for (int i = 0; i < ret.size(); i++) {
            String[] cols = ret.get(i).split(SEPRATOR);
            if (cols == null || cols.length < 1) {
                continue;
            }
            String col0 = cols[0];
            if (col0.startsWith("# col_name") && !mark.equals("partition")) {
                mark = "col_name";
            } else if (col0.startsWith("# Partition")) {
                mark = "partition";
            } else if (col0.startsWith("# Detailed")) {
                mark = "detailed";
            } else if (mark.equals("col_name")) {
                if (cols.length < 2 || cols[0].length() < 1 || cols[0].equals("null")) {
                    continue;
                }
                ColumnInfo aColumn = new ColumnInfo();
                aColumn.setColumnName(cols[0]);
                aColumn.setType(cols[1]);
                if (cols.length > 2) {
                    aColumn.setComment(cols[2]);
                }
                columns.add(aColumn);
            } else if (mark.equals("partition")) {
                if (cols.length < 2 || cols[0].length() < 1 || cols[0].equals("null") || cols[0].startsWith("#")) {
                    continue;
                }
                ColumnInfo aColumn = new ColumnInfo();
                aColumn.setColumnName(cols[0]);
                aColumn.setType(cols[1]);
                if (cols.length > 2) {
                    aColumn.setComment(cols[2]);
                }
                partitions.add(aColumn);
            } else if (col0.startsWith("Owner:")) {
                tableInfo.setOwner(cols[1].trim());
            } else if (col0.startsWith("CreateTime:")) {
                tableInfo.setCreateTime(getTime(cols[1].trim()));
            } else if (col0.startsWith("Location:")) {
                tableInfo.setLocation(cols[1].trim());
            } else if (col0.startsWith("Table Type:")) {
                tableInfo.setType(cols[1].trim());
            }
        }
        tableInfo.setPartitionKeys(partitions);
        tableInfo.setColumns(columns);
        return tableInfo;
    }

    private static long getTime(String dts) {
        SimpleDateFormat f = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
        try {
            Date d = f.parse(dts);
            long milliseconds = d.getTime();
            return milliseconds;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static List<String> getQueryResult_debug(String table, String path) {
        try {
            Stream<String> lines = Files.lines(Paths.get(path + "/" + table + ".txt"));
            List<String> ret = new LinkedList<>();
            lines.forEach(l -> ret.add(l));
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static List<String> getAllTables(String dbName, String path) {
        List<String> ret = new LinkedList<>();
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            ret = getTableNames_debug(path);
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (resultSet != null) {
                    statement.close();
                }
                if (resultSet != null) {
                    connection.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return ret;
    }

    private static List<String> getTableNames_debug(String path) {
        List<String> ret = new ArrayList<>();
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
                String fname = listOfFiles[i].getName();
                if (fname.endsWith(".txt")) {
                    ret.add(fname.substring(0, fname.length() - 4));
                }
            }
        }
        return ret;
    }

    public static List<String> getAllDbs() {

        List<String> ret = new LinkedList<>();
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = DriverManager.getConnection(jdbcUrl + "default", jdbcUser, null);

            statement = connection.createStatement();
            resultSet = statement.executeQuery("show databases");
            while (resultSet.next()) {
                String name = resultSet.getString(1);
                ret.add(name);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (resultSet != null) {
                    statement.close();
                }
                if (resultSet != null) {
                    connection.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return ret;
    }
}
