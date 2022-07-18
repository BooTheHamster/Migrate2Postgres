package net.twentyonesolutions.m2pg;

public class TableName {
    private final String tableName;
    private final String schemaName;

    public TableName(String tableName, String schemaName) {

        this.tableName = tableName;
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public String getSchemaName() {
        return this.schemaName;
    }
}
