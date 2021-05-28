package org.meta;

public enum LoadMode {
    FULL("full"), INCR("incr");
    private String dbMode;
    private LoadMode(String dbMode) {
        this.dbMode = dbMode;
    }
    public String getDbMode() {
        return dbMode;
    }
}
