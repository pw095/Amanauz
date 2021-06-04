package org.data;

import org.flow.Flow;
import org.meta.MetaLayer;

public abstract class SnapshotEntity extends AbstractEntity {
    public SnapshotEntity(Flow flow, String entityCode) {
        super(flow, MetaLayer.STAGE, entityCode);
    }
}
