package org.meta;

public enum MetaLayer {
    STAGE("stage"), ODS("ods"), DDS("dds"), DATA_MART("data_mart");
    private String dbLayer;
    private MetaLayer(String dbLayer) {
        this.dbLayer = dbLayer;
    }
    public String getDbLayer() {
        return dbLayer;
    }
}
