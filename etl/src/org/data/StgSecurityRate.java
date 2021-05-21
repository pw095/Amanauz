package org.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StgSecurityRate extends FileEntity {
/*

    public StgSecurityRate(long flowLoadId) {
        super(flowLoadId);

        this.flowLoadId = flowLoadId;
        this.entityLayer = "stage";
        this.entityCode = "security_rate";
    }

    public void detailLoad(PreparedStatement stmtUpdate, Path path) {
//        Path path = Paths.get("C:\\Users\\pw095\\Documents\\Git\\Amanauz\\source_files\\security_rate.txt");
        try (BufferedReader reader = Files.newBufferedReader(path, Charset.forName("UTF-8"))) {

            String str;
            int insertCount = 0;

            while((str = reader.readLine()) != null) {
                String[] stringArray = reader.readLine().split(" ");

                stmtUpdate.setString(1, stringArray[0]);
                stmtUpdate.setString(2, stringArray[1]);
                stmtUpdate.setString(3, stringArray[2]);
                stmtUpdate.setLong(4, this.flowLoadId);

                stmtUpdate.addBatch();
                if (++insertCount % 1_000 == 0) {
                    stmtUpdate.executeBatch();
                    stmtUpdate.clearBatch();
                }
            }
            stmtUpdate.executeBatch();
            stmtUpdate.clearBatch();

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
*/

}
