public enum LoadStatus {
    RUNNING("running"), SUCCEEDED("succeeded"), FAILED("failed"), ENQUEUED("enqueued"), NOT_STARTED("not_started");
    private String dbStatus;
    private LoadStatus(String dbStatus) {
        this.dbStatus = dbStatus;
    }
    public String getDbStatus() {
        return dbStatus;
    }
}
