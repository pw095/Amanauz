package org.data;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.data.AbstractEntity;
import org.data.file.ExcelEntity;
import org.flow.Flow;
import org.meta.Meta;
import org.meta.MetaLayer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.util.Map;

import static org.meta.Properties.getInstance;

public abstract class FileEntity extends AbstractEntity implements ExcelEntity {

    private final Path path;
    private String fileName;
    private String fileHash;
    private long fileSize;

    public String getFileName() {
        return this.fileName;
    }
    public String getFileHash() {
        return this.fileHash;
    }
    public long getFileSize() {
        return this.fileSize;
    }

    @Override
    public boolean ifCancelLoad() {
        return getFileHash().equals(Meta.getPreviousFileHash(this).get(getFileName()));
    }

    protected FileEntity(Flow flow, String entityCode, String fileName) {
        super(flow, MetaLayer.STAGE, entityCode);
        this.fileName = getInstance().getProperty("fileDirectory") + fileName;
        path = Paths.get(this.fileName);
        try (InputStream inputStream = new FileInputStream(this.fileName)) {
            fileSize = Files.size(path);
            fileHash = DigestUtils.sha1Hex(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
