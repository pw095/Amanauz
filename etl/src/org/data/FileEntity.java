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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.meta.Properties.getInstance;

public abstract class FileEntity extends AbstractEntity implements ExcelEntity {

    public static class FileInfo {
        Path filePath;
        String fileName;
        String fileHash;
        long fileSize;

        FileInfo(Path filePath, String fileName, String fileHash, long fileSize) {
            this.filePath = filePath;
            this.fileName = fileName;
            this.fileHash = fileHash;
            this.fileSize = fileSize;
        }
        public String getFilePathString() { return filePath.toString(); }
        public String getFileName() { return fileName; }
        public String getFileHash() { return fileHash; }
        public long getFileSize() { return fileSize; }
    }
//    private final Path path;
    private Path directoryPath;
    private String currentFileName;
    private String currentFileHash;
//    private String fileName;
    List<FileInfo> fileInfoList = new ArrayList<>();
//    private String fileHash;
//    private long fileSize;

    public String getFileName() {
        return this.currentFileName;
    }
    public void setFileName(String fileName) {
        int index = fileName.indexOf(".xlsx");
        this.currentFileName = fileName.substring(0, index);
//        this.currentFileName = fileName;
    }
/*
    public String getFileHash() {
        return this.currentFileHash;
    }
    public void setFileHash(String fileHash) {
        this.currentFileHash = fileHash;
    }*/

    public List<FileInfo> getFileInfo() { return this.fileInfoList; }
//    public List<String> getFileNameList() {
//        return this.fileNameList;
//    }
//    public String getFileHash() {
//        return this.fileHash;
//    }
//    public long getFileSize() {
//        return this.fileSize;
//    }

    @Override
    public boolean ifCancelLoad(FileEntity.FileInfo fileInfo) {
        return false;
//        return fileInfo.getFileHash().equals(Meta.getPreviousFileHash(this).get(fileInfo.getFileName()));
    }

    private static FileInfo readFileInfo(Path path) {

        FileInfo fileInfo;
        try (InputStream inputStream = new FileInputStream(path.toString())) {
            fileInfo = new FileInfo(path, path.getFileName().toString(), DigestUtils.sha1Hex(inputStream), Files.size(path));
//            fileSize = Files.size(path);
//            fileHash = DigestUtils.sha1Hex(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return fileInfo;

    }

    public Path getDirectoryPath() {
        return Paths.get(getInstance().getProperty("masterFileDirectory")).normalize();
    }

    protected FileEntity(Flow flow, String entityCode, String fileNameMask) {
        super(flow, MetaLayer.STAGE, entityCode);
//        this.fileName = getInstance().getProperty("fileDirectory") + fileName;
//        path = Paths.get(this.fileName);

//        String directoryName = getInstance().getProperty("fileDirectory");
//        directoryPath = Paths.get(getInstance().getProperty("fileDirectory")).normalize();
        directoryPath = getDirectoryPath();
        try {
            Files.walk(directoryPath)
                    .filter(p -> p.toString().endsWith(".xlsx"))
                    .filter(p -> p.getFileName().toString().contains(fileNameMask) || fileNameMask.isEmpty())
                    .map(FileEntity::readFileInfo)
//                    .map(p -> p.toString())
                    .forEach(fileInfoList::add);
            fileInfoList.forEach(p -> System.out.println(p.getFilePathString()));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

/*        for (FileInfo fileInfo : getFileInfo()) {
            try (InputStream inputStream = new FileInputStream(fileInfo.fileName)) {
                fileSize = Files.size(path);
                fileHash = DigestUtils.sha1Hex(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }*/
    }
}
