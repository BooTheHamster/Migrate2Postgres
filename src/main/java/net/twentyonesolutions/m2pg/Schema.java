package net.twentyonesolutions.m2pg;

import com.google.gson.internal.LinkedTreeMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.twentyonesolutions.m2pg.Config.CHARACTER_MAXIMUM_LENGTH;
import static net.twentyonesolutions.m2pg.Config.COLUMN_DEFAULT;
import static net.twentyonesolutions.m2pg.Config.COLUMN_DEFINITION;
import static net.twentyonesolutions.m2pg.Config.COLUMN_NAME;
import static net.twentyonesolutions.m2pg.Config.DATA_TYPE;
import static net.twentyonesolutions.m2pg.Config.IS_COMPUTED;
import static net.twentyonesolutions.m2pg.Config.IS_IDENTITY;
import static net.twentyonesolutions.m2pg.Config.IS_NULLABLE;
import static net.twentyonesolutions.m2pg.Config.NUMERIC_PRECISION;
import static net.twentyonesolutions.m2pg.Config.ORDINAL_POSITION;
import static net.twentyonesolutions.m2pg.Config.TABLE_NAME;
import static net.twentyonesolutions.m2pg.Config.TABLE_SCHEMA;

public class Schema {

    Config config;
    Map<String, Table> schema;

    private HashSet<String> explicitConversionSqlTypes;
    private HashSet<String> skipTables;
    private Map<String, HashSet<String>> skipColumns;
    private boolean isTestInsertMode;

    public Schema(Config config) throws Exception {
        this.config = config;

        this.explicitConversionSqlTypes = getConfigSetValues("explicit_conversion_types");
        this.skipTables = getConfigSetValues("skip_tables");
        this.skipColumns = getConfigSkippedColumns();
        this.isTestInsertMode = (boolean) this.config.config.getOrDefault("test_insert", false);

        String informationSchemaSql = (String) config.config.get("information_schema.query");
        if (informationSchemaSql.isEmpty()) {
            throw new IllegalArgumentException("information_schema.query is missing");
        }

        Connection conSrc = config.connect(config.source);
        Statement statement = conSrc.createStatement();
        ResultSet resultSet = statement.executeQuery(informationSchemaSql);

        if (!resultSet.isBeforeFirst()) {
            System.out.println("information_schema.query returned no results: \n" + informationSchemaSql);
            throw new IllegalArgumentException("information_schema.query returned no results");
        }

        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();
        int[] columnTypes = new int[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            columnTypes[i - 1] = metaData.getColumnType(i);
        }

        schema = new TreeMap(String.CASE_INSENSITIVE_ORDER);

        while (resultSet.next()) {

            String schemaName = resultSet.getString(TABLE_SCHEMA);
            String tableName = resultSet.getString(TABLE_NAME);

            if (needSkipTable(tableName)) {
                continue;
            }

            String fullTableName = schemaName + "." + tableName;
            Table table = schema.computeIfAbsent(fullTableName, Table::new);
            String columnName = resultSet.getString(COLUMN_NAME);

            if (needSkipColumn(tableName, columnName)) {
                continue;
            }

            boolean isIdentity = Util.isSqlTrue(resultSet.getString(IS_IDENTITY));
            boolean isComputed = Util.isSqlTrue(resultSet.getString(IS_COMPUTED));
            String definition = isComputed ? resultSet.getString(COLUMN_DEFINITION) : resultSet.getString(COLUMN_DEFAULT);

            Column col = new Column(
                    columnName
                    , resultSet.getString(DATA_TYPE)
                    , resultSet.getInt(ORDINAL_POSITION)
                    , Util.isSqlTrue(resultSet.getObject(IS_NULLABLE))
                    , resultSet.getInt(CHARACTER_MAXIMUM_LENGTH)
                    , resultSet.getInt(NUMERIC_PRECISION)
                    , isIdentity
                    , isComputed
                    , definition    // in MSSQL user must have GRANT VIEW DEFINITION ON database to see default values
            );

            table.columns.add(col);

            if (isIdentity)
                table.setIdentity(col);
        }
    }

    public String copyTable(String tableName, IProgress progress) {

        if (needSkipTable(tableName)) {
            return String.format("/** Skipped table %s */", tableName);
        }

        System.out.printf("\n%s%n", tableName);
        StringBuilder log = new StringBuilder(1024);

        String qSelect, qInsert;
        int copied = 0;
        long rowCount = 0;

        Config config = this.config;
        Table table = this.getTable(tableName);
        TableName targetTableName = config.getTargetTableName(table);
        String tgtTable = targetTableName.getTableName();

        log.append(String.format("/** copy table %s to %s */\n", tableName, tgtTable));

        long tc = System.currentTimeMillis();
        try {
            Connection conSrc = config.connect(config.source);
            Connection conTgt = config.connect(config.target);

            Statement statTgt = conTgt.createStatement();
            statTgt.execute("BEGIN TRANSACTION;");
            statTgt.execute("TRUNCATE TABLE " + tgtTable + " CASCADE;");

            qSelect = "SELECT COUNT(*) AS row_count" + "\nFROM " + table.toString();
            Statement statSrc = conSrc.createStatement();
            ResultSet rs;
            rs = statSrc.executeQuery(qSelect);

            long longValue = 0;
            if (rs.next()) {
                rowCount = rs.getLong("row_count");
                log.append(String.format(" /* %,d rows */\n", rowCount));
            } else {
                throw new RuntimeException("No results found for " + qSelect);
            }

            qSelect = "SELECT " + table.getColumnListSrc(config) + "\nFROM " + table.toString();

            qInsert = "INSERT INTO " + tgtTable + " (" + table.getColumnListTgt(config) + ")";

            // add VALUES( ?, ... ) to INSERT query
            int insertColumnCount = qInsert.substring(qInsert.indexOf('(')).split(",").length;
            qInsert += "\nVALUES(" + String.join(", ", Collections.nCopies(insertColumnCount, "?")) + ")";

            statSrc = conSrc.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            PreparedStatement statInsert = conTgt.prepareStatement(qInsert);

            statSrc.setFetchSize(1000);

            rs = statSrc.executeQuery(qSelect);

            ResultSetMetaData rsMetaData = rs.getMetaData();

            Map<String, String> jdbcTypeMapping = (Map<String, String>) config.dml.get("jdbc_type_mapping");

            int columnCount = rsMetaData.getColumnCount();

            String[] sqlColumnTypes = new String[columnCount];
            int[] columnTypes = new int[columnCount];

            boolean hasErrors = false;

            for (int i = 1; i <= columnCount; i++) {

                int srcType = 0, tgtType;
                String srcTypeName;
                int index = i - 1;

                try {
                    String sqlColumnType = rsMetaData.getColumnTypeName(i).toLowerCase();
                    srcType = rsMetaData.getColumnType(i);
                    tgtType = srcType;

                    if (explicitConversionSqlTypes.contains(sqlColumnType)) {
                        sqlColumnTypes[index] = sqlColumnType;
                    } else {
                        sqlColumnTypes[index] = null;

                        // translate unsupported types, e.g. nvarchar to varchar, dml jdbcTypeMapping is based on JDBC types, while ddl jdbcTypeMapping is based on SQL types
                        //            if (jdbcTypeMapping.containsKey(String.valueOf(srcType)))
                        //                tgtType = Integer.parseInt(jdbcTypeMapping.getOrDefault(String.valueOf(srcType), String.valueOf(tgtType)));
                        srcTypeName = JDBCType.valueOf(srcType).getName();   // WARN: rsMetaData.getColumnTypeName(i) returns the vendor's name instead of JDBC name, e.g. ntext instead of longnvarchar for MSSQL
                        if (jdbcTypeMapping.containsKey(srcTypeName)) {
                            String tgtTypeName = jdbcTypeMapping.get(srcTypeName);
                            tgtType = JDBCType.valueOf(tgtTypeName).getVendorTypeNumber();
                        }

                        columnTypes[index] = tgtType;
                    }
                } catch (Throwable t) {
                    String colName = rsMetaData.getColumnName(i);
                    log.append(String.format(" /* Error: Failed to get JDBC column type (%s) for %s.%s */\n", srcType, tableName, colName));
                    log.append(" /* No rows copied */");
                    return log.toString();
                }
            }

            if (this.isTestInsertMode) {
                rowCount = Math.min(1, rowCount);
            }

            int row = 0;
            while (rs.next()) {

                if (this.isTestInsertMode && row > 0) {
                    String logentry = String.format("/** Test insert row into %s", tableName);
                    System.out.println("\n" + logentry);
                    log.append(logentry);
                    break;
                }

                row++;

                for (int i = 1; i <= columnCount; i++) {

                    Object value = rs.getObject(i);
                    int index = i - 1;
                    String sqlColumnType = sqlColumnTypes[index];

                    if (sqlColumnType != null && !sqlColumnType.isEmpty()) {

                        if (sqlColumnType.equals("uniqueidentifier")) {
                            UUID uuid = value == null ? null : UUID.fromString(value.toString());
                            statInsert.setObject(i, uuid);
                        }
                    } else {
                        statInsert.setObject(i, value, columnTypes[index]);
                    }
                }

                try {
                    int executeResult = statInsert.executeUpdate();
                    copied += executeResult;

                } catch (SQLException ex) {

                    String failedSql = statInsert.toString().replace("\n", "\n\t") + ";";

                    System.err.println("\n\nInsert Failed. " + ex.toString());
                    System.err.println("\n\t" + failedSql);

                    log.append("\n/** Error: Insert Failed. ")
                            .append(ex.toString())
                            .append("\n\t")
                            .append(failedSql)
                            .append("\n");

                    hasErrors = true;
                    break;  // exit the loop and stop processing. TODO: modify for other exception handling strategies
                }

                if (progress != null) {
                    progress.progress(new IProgress.Status(tableName, row, rowCount));
                }
            }

            if (rowCount == 0) {
                if (progress != null)   // report progress in case the table was empty
                    progress.progress(new IProgress.Status(tableName, 0, 0));
            } else {
                if (table.hasIdentity()) {

                    Column identity = table.getIdentity();
                    qSelect = "SELECT MAX(" + identity.name + ") AS max_value" + "\nFROM " + table.toString();
                    rs = statSrc.executeQuery(qSelect);
                    if (rs.next()) {

                        longValue = rs.getLong("max_value");

                        double recommendFactor = 1_000.0;

                        if (longValue > 1_000_000)
                            recommendFactor = 10_000.0;

                        long recommendValue = (long) (Math.ceil((longValue + recommendFactor) / recommendFactor) * recommendFactor);
                        String idColumnName = config.getTargetColumnName(identity.name).replace("\"", "");
                        String seqTableName = tgtTable.replace("\"", "").replace(targetTableName.getSchemaName(), "").replace(".", "");
                        String seqName = String.format("%s.\"%s_%s_seq\"", targetTableName.getSchemaName(), seqTableName, idColumnName);
                        String sqlRecommended = String.format("SELECT setval('%s', %d);", seqName, longValue);

                        log.append(" -- Identity column ")
                                .append(identity.name)
                                .append(" has max value of ")
                                .append(longValue)
                                .append(". ");

                        if (config.dml.get("execute.recommended").toString().toLowerCase().equals("all")) {
                            Util.executeQueries(Arrays.asList(sqlRecommended), log, conTgt);
                        } else {
                            log.append("Recommended:\n\t")
                                    .append(sqlRecommended)
                                    .append("\n");
                        }
                    } else {
                        throw new RuntimeException("No results found for " + qSelect);
                    }
                }
            }

            if (hasErrors) {

                if (config.dml.getOrDefault("on_error", "rollback").equals("rollback")) {
                    log.append("  rolling back transaction **/\n");
                    statTgt.execute("ROLLBACK;");

                    if (progress != null)
                        progress.progress(new IProgress.Status(tableName, 0, 0));
                }
            } else {

                statTgt.execute("COMMIT;");
            }

            statSrc.cancel();  // if the statement did not complete then we should cancel it or else we have to wait for a timeout
            statSrc.close();
            rs.close();

            conSrc.close();
            conTgt.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        tc = System.currentTimeMillis() - tc;

        if (rowCount > 0) {
            log.append(String.format(" /* copied %,d / %,d records in %.3f seconds **/\n", copied, rowCount, tc / 1000.0));
        }

        return log.toString();
    }


    public Collection<Table> getTables() {

        return schema.values();
    }


    public Table getTable(String fullTableName) {

        return schema.get(fullTableName);
    }


    public String generateDdl() {

        boolean doTransaction = true;
        boolean doDropSchema = (boolean) config.ddl.get("drop_schema");

        StringBuilder sb = new StringBuilder(4096);

        sb.append(PgMigrator.getBanner());

        if (doTransaction) {
            sb.append("BEGIN TRANSACTION;\n\n");
        }

        Map<String, String> schemaMapping = (Map<String, String>) config.config.getOrDefault("schema_mapping", Collections.EMPTY_MAP);

        List<String> distinctSchemas = schema
                .values()
                .stream()
                .map(t -> schemaMapping.getOrDefault(t.schemaName, t.schemaName).toLowerCase())
                .distinct()
                .collect(Collectors.toList());

//        distinctSchemas = ((Map<String, String>)config.config.get("schema_mapping")).values()
//                .stream()
//                .distinct()
//                .collect(Collectors.toList());

        for (String tgtSchema : distinctSchemas) {

            if (doDropSchema) {
                sb.append("DROP SCHEMA IF EXISTS " + tgtSchema + " CASCADE;\n");
            }

            sb.append("CREATE SCHEMA IF NOT EXISTS " + tgtSchema + ";\n\n");
        }
        sb.append("\n");

        for (Table table : schema.values()) {

            if (!doDropSchema) {
                // script DROP TABLE if we do not drop the schema
                sb.append("DROP TABLE IF EXISTS " + config.getTargetTableName(table) + ";\n");
            }

            sb.append(
                            table.getDdl(config)
                    )
                    .append("\n\n");
        }

//        sb.append("\n\n")
//            .append(config.ddl.get("after_all"));

        if (doTransaction) {
            sb.append("\n-- ROLLBACK;\n");
            sb.append("\nCOMMIT;\n");
        }

        return sb.toString();
    }

    private HashSet<String> getConfigSetValues(String configKey) {
        Object configValue = config.dml.get(configKey);

        if (!(configValue instanceof ArrayList)) {
            return new HashSet<>();
        }

        ArrayList<String> configValues = (ArrayList<String>) configValue;

        return configValues
                .stream()
                .map(String::toLowerCase)
                .collect(Collectors.toCollection(HashSet::new));
    }

    private Map<String, HashSet<String>> getConfigSkippedColumns() {
        Object configValue = config.dml.get("skip_columns");

        if (!(configValue instanceof LinkedTreeMap)) {
            return new HashMap<>();
        }

        LinkedTreeMap<String, ArrayList<String>> configValues = (LinkedTreeMap<String, ArrayList<String>>) configValue;
        LinkedHashMap<String, HashSet<String>> result = new LinkedHashMap<>(configValues.size());

        for (Map.Entry<String, ArrayList<String>> pair : configValues.entrySet()) {
            HashSet<String> values = pair.getValue()
                    .stream()
                    .map(String::toLowerCase)
                    .collect(Collectors.toCollection(HashSet::new));
            result.put(pair.getKey().toLowerCase(), values);
        }

        return result;
    }

    private boolean needSkipColumn(String tableName, String columnName) {

        tableName = tableName.toLowerCase();
        columnName = columnName.toLowerCase();

        for (Map.Entry<String, HashSet<String>> pair : skipColumns.entrySet()) {

            if (tableName.contains(pair.getKey()) && pair.getValue().contains(columnName)) {
                return true;
            }
        }

        return false;
    }

    private boolean needSkipTable(String tableName) {

        tableName = tableName.toLowerCase();

        for (String skipTableName : skipTables) {
            if (tableName.contains(skipTableName)) {
                return true;
            }
        }

        return false;
    }

    private int batchInsert(Statement statInsert, StringBuilder log) {
        int copied = 0;

        try {
            int[] executeResult = statInsert.executeBatch();

            for (int j : executeResult) {
                copied += j;
            }

        } catch (SQLException ex) {

            String failedSql = statInsert.toString().replace("\n", "\n\t") + ";";

            System.err.println("\n\nInsert Failed. " + ex.toString());
            System.err.println("\n\t" + failedSql);

            log.append("\n/** Error: Insert Failed. ")
                    .append(ex.toString())
                    .append("\n\t")
                    .append(failedSql)
                    .append("\n");

            return -1;
        }

        return copied;
    }
}
