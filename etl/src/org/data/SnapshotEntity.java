package org.data;

import org.flow.Flow;
import org.meta.MetaLayer;

public abstract class SnapshotEntity extends AbstractEntity {
    public SnapshotEntity(Flow flow, MetaLayer metaLayer, String entityCode) {
        super(flow, metaLayer, entityCode);
    }
}
