package org.data;

import org.meta.MetaLayer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;

import static org.meta.Properties.getInstance;

public abstract class FileEntity /*extends AbstractEntity */{

    static private final String dataSourceFile = "$$entity_name.txt";
    static private String dataSourceDir;
    final Path path = null;
/*
    FileEntity(long flowLoadId, String entityCode) {
        super(flowLoadId, MetaLayer.STAGE, entityCode);
        try {
            dataSourceDir = getInstance().getProperty("dataSourceDirectory").concat("source_files/");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        path = Paths.get(dataSourceDir.concat(dataSourceFile).replace("$$entity_name", this.entityCode));
    }

    protected abstract void detailLoad(PreparedStatement stmtUpdate, Path path);


    @Override
    protected void detailLoad(PreparedStatement stmtUpdate) {
        detailLoad(stmtUpdate, path);
    }
*/

}
