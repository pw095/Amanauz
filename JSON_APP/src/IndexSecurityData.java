public class IndexSecurityData {

    private String indexName;
    private String tradeDate;
    private String securityId;
    private Double weight;

    public IndexSecurityData(String indexName, String tradeDate, String securityId, Double weight) {
        this.indexName = indexName;
        this.tradeDate = tradeDate;
        this.securityId = securityId;
        this.weight = weight;
    }

    public String getIndexName() {
        return this.indexName;
    }

    public String getTradeDate() {
        return this.tradeDate;
    }

    public String getSecurityId() {
        return this.securityId;
    }

    public Double getWeight() {
        return this.weight;
    }
}
