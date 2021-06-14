package org.meta;

public enum MetaLayer {
    STAGE("stage"), REPL("repl"), DDS("dds"), DATA_MART("data_mart");
    private String dbLayer;
    private MetaLayer(String dbLayer) {
        this.dbLayer = dbLayer;
    }
    public String getDbLayer() {
        return dbLayer;
    }
}
