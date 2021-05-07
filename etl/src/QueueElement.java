import org.meta.LoadStatus;

import java.time.LocalDateTime;

class QueueElement {
    private int flowId;
    private int elmId;
    private int pendingParentCount;
    private int iterationNumber;
    private String layerCode;
    private String entityCode;
    private String entityEnabled;
    private LoadStatus loadStatus;

    private LocalDateTime startDttm;
    private LocalDateTime finishDttm;

    QueueElement(int flowId, int elmId, int pendingParentCount, int iterationNumber, LoadStatus loadStatus) {
        this.flowId = flowId;
        this.elmId = elmId;
        this.pendingParentCount = pendingParentCount;
        this.loadStatus = loadStatus;
    }

    QueueElement(QueueElement queueElement) {
        setFlowId(queueElement.getFlowId());
        setElmId(queueElement.getElmId());
        setPendingParentCount(queueElement.getPendingParentCount());
        setIterationNumber(queueElement.getIterationNumber());
        setLayerCode(queueElement.getLayerCode());
        setEntityCode(queueElement.getEntityCode());
        setEntityEnabled(queueElement.getEntityEnabled());
        setLoadStatus(queueElement.getLoadStatus());
        setStartDttm(queueElement.getStartDttm());
        setFinishDttm(queueElement.getFinishDttm());
    }

    void setFlowId(int flowId) {
        this.flowId = flowId;
    }
    int getFlowId() {
        return this.flowId;
    }

    void setElmId(int elmId) {
        this.elmId = elmId;
    }
    int getElmId() {
        return this.elmId;
    }

    void setPendingParentCount(int pendingParentCount) {
        this.pendingParentCount = pendingParentCount;
    }
    int getPendingParentCount() {
        return this.pendingParentCount;
    }

    void setIterationNumber(int iterationNumber) {
        this.iterationNumber = iterationNumber;
    }
    int getIterationNumber() {
        return this.iterationNumber;
    }

    void setLayerCode(String layerCode) {
        this.layerCode = layerCode;
    }
    String getLayerCode() {
        return this.layerCode;
    }

    void setEntityCode(String entityCode) {
        this.entityCode = entityCode;
    }
    String getEntityCode() {
        return this.entityCode;
    }

    void setEntityEnabled(String entityEnabled) {
        this.entityEnabled = entityEnabled;
    }
    String getEntityEnabled() {
        return this.entityEnabled;
    }

    void setLoadStatus(LoadStatus loadStatus) {
        this.loadStatus = loadStatus;
    }
    LoadStatus getLoadStatus() {
        return this.loadStatus;
    }

    void setStartDttm(LocalDateTime startDttm) {
        this.startDttm = startDttm;
    }
    LocalDateTime getStartDttm() {
        return this.startDttm;
    }

    void setFinishDttm(LocalDateTime finishDttm) {
        this.finishDttm = finishDttm;
    }
    LocalDateTime getFinishDttm() {
        return this.finishDttm;
    }

    void setEntityLoadMode(String entityLoadMode) {
        throw new RuntimeException();
    }
    String getEntityLoadMode() {
        throw new RuntimeException();
    }

    void setEntityEffFromDt(String entityEffFromDt) {
        throw new RuntimeException();
    }
    String getEntityEffFromDt() {
        throw new RuntimeException();
    }

    void setEntityEffToDt(String entityEffToDt) {
        throw new RuntimeException();
    }
    String getEntityEffToDt() {
        throw new RuntimeException();
    }
}