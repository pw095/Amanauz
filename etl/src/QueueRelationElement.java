class QueueRelationElement extends QueueElement {

    private String entityLoadMode = "";
    private String entityEffFromDt = "";
    private String entityEffToDt = "";

    QueueRelationElement(int flowId, int elmId, int pendingParentCount, int iterationNumber, LoadStatus loadStatus) {
        super(flowId, elmId, pendingParentCount, iterationNumber, loadStatus);
    }

    QueueRelationElement(QueueElement queueElement) {
        super(queueElement);
    }

    QueueRelationElement(QueueRelationElement queueRelationElement) {
        super(queueRelationElement);
        setEntityLoadMode(queueRelationElement.getEntityLoadMode());
        setEntityEffFromDt(queueRelationElement.getEntityEffFromDt());
        setEntityEffToDt(queueRelationElement.getEntityEffToDt());
    }

    void setEntityLoadMode(String entityLoadMode) {
        this.entityLoadMode = entityLoadMode;
    }
    String getEntityLoadMode() {
        return this.entityLoadMode;
    }

    void setEntityEffFromDt(String entityEffFromDt) {
        this.entityEffFromDt = entityEffFromDt;
    }
    String getEntityEffFromDt() {
        return this.entityEffFromDt;
    }

    void setEntityEffToDt(String entityEffToDt) {
        this.entityEffToDt = entityEffToDt;
    }
    String getEntityEffToDt() {
        return this.entityEffToDt;
    }

}