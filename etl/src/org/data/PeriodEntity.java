package org.data;

import org.flow.Flow;
import org.meta.LoadMode;
import org.meta.Meta;
import org.meta.MetaLayer;

import static org.util.AuxUtil.dateFormat;

public abstract class PeriodEntity extends AbstractEntity {

    private String effectiveFromDt;
    private String effectiveToDt;

    public String getEffectiveFromDt() {
        return this.effectiveFromDt;
    }
    public void setEffectiveFromDt(String effectiveFromDt) {
        this.effectiveFromDt = effectiveFromDt;
    }

    public String getEffectiveToDt() {
        return this.effectiveToDt;
    }
    public void setEffectiveToDt(String effectiveToDt) {
        this.effectiveToDt = effectiveToDt;
    }

    public PeriodEntity(Flow flow, String entityCode) {
        super(flow, MetaLayer.STAGE, entityCode);
        if (getEntityLoadMode() == LoadMode.INCR) {
            Meta.getPreviousEffectiveToDt(this);
        } else {
            setEffectiveFromDt("2001-01-01");
        }
        this.setEffectiveToDt(flow.getFlowLogStartTimestamp().format(dateFormat));
    }

}
