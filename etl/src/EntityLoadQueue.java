import org.meta.LoadStatus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EntityLoadQueue {

    class LayerEntityInfo {
        int elmId;
        String layerCode;
        String entityCode;
        String entityLoadMode;
        String entityEnabled;
        int iterationNumber;

        LayerEntityInfo(int elmId, String layerCode, String entityCode, String entityLoadMode, String entityEnabled, int iterationNumber) {
            this.elmId = elmId;
            this.layerCode = layerCode;
            this.entityCode = entityCode;
            this.entityLoadMode = entityLoadMode;
            this.entityEnabled = entityEnabled;
            this.iterationNumber = iterationNumber;
        }
    }

    class TreeElement {
        int elmId;
        int childElmId;

        TreeElement(int elmId, int childElmId) {
            this.elmId = elmId;
            this.childElmId = childElmId;
        }
    }

    List<QueueElement> queueElementList;
    List<TreeElement> treeElementList;

    public EntityLoadQueue(int flowId) throws IOException {

        Path path1 = Paths.get("../db/script/meta/auxiliary/s_entity_load_queue.sql");
        Path path2 = Paths.get("../db/script/meta/auxiliary/s_entity_load_tree.sql");
        Path path3 = Paths.get("../db/script/meta/auxiliary/s_layer_entity_info.sql");
        Path path4 = Paths.get("../db/script/meta/auxiliary/s_entity_relation_info.sql");
        String sqlSelect1String = Files.lines(path1).collect(Collectors.joining(System.getProperty("line.separator")));
        String sqlSelect2String = Files.lines(path2).collect(Collectors.joining(System.getProperty("line.separator")));
        String sqlSelect3String = Files.lines(path3).collect(Collectors.joining(System.getProperty("line.separator")));
        String sqlSelect4String = Files.lines(path4).collect(Collectors.joining(System.getProperty("line.separator")));
        String sqlDeleteString = "DELETE FROM tbl_entity_load_queue";
        String sqlInsertString = "INSERT INTO tbl_entity_load_queue(elq_flow_id, elq_elm_id, elq_pending_parent_cnt, elq_iteration_number, elq_status) VALUES(?, ?, ?, ?, ?)";

        queueElementList = new ArrayList<>();
        treeElementList = new ArrayList<>();
        LoadStatus loadStatus = LoadStatus.ENQUEUED;

        try(Connection conn = DriverManager.getConnection(Flow.connString);
            Statement stmt = conn.createStatement();
            Statement stmtDelete = conn.createStatement();
            PreparedStatement preparedStatement = conn.prepareStatement(sqlInsertString)) {

//            System.out.println("PreParentCnt");
//            System.out.println(sqlSelect1String);
            try(ResultSet rs = stmt.executeQuery(sqlSelect1String)) {
                while (rs.next()) {
                    queueElementList.add(new QueueRelationElement(flowId, rs.getInt("elm_id"), rs.getInt("parent_cnt"), 0, loadStatus));
                }
            }
//            System.out.println("PostParentCnt");
            try(ResultSet rs = stmt.executeQuery(sqlSelect2String)) {
                while (rs.next()) {
                    treeElementList.add(new TreeElement(rs.getInt("elm_id"), rs.getInt("child_elm_id")));
                }
            }

            try(ResultSet rs = stmt.executeQuery(sqlSelect3String)) {
                while (rs.next()) {
                    LayerEntityInfo layerEntityInfo = new LayerEntityInfo(rs.getInt("elm_id"), rs.getString("layer_code"), rs.getString("ent_code"), rs.getString("elm_mode"), rs.getString("elm_enabled"), rs.getInt("ell_iteration_number"));
//                    System.out.println(rs.getString("ent_code") + " " + layerEntityInfo.entityCode + " " + layerEntityInfo.layerCode + " " + layerEntityInfo.entityLoadMode);
//                    System.out.println(layerEntityInfo.elmId);
                    queueElementList.stream()
                            .filter(p -> p.getElmId() == layerEntityInfo.elmId)
                            .forEach(p -> {
                                p.setEntityCode(layerEntityInfo.entityCode);
                                p.setLayerCode(layerEntityInfo.layerCode);
                                p.setEntityEnabled(layerEntityInfo.entityEnabled);
                                p.setIterationNumber(layerEntityInfo.iterationNumber);
                                p.setEntityLoadMode(layerEntityInfo.entityLoadMode);
                            });
                }
            }

            try(ResultSet rs = stmt.executeQuery(sqlSelect4String)) {
                while (rs.next()) {
                    int elmId = rs.getInt(1);
                    String entityEffToDt = rs.getString("rll_effective_to_dt");
                    queueElementList.stream()
                            .filter(p -> p.getElmId() == elmId)
                            .forEach(p -> {
                                p.setEntityEffFromDt(entityEffToDt);
                            });
                }
            }

//            System.out.println(queueElementList.size());
//            queueElementList.forEach(p -> System.out.println(p.getElmId() + " " + p.getFlowId() + " " + p.getEntityCode()));
            stmtDelete.executeUpdate(sqlDeleteString);

            for(QueueElement queueElement : queueElementList) {
                preparedStatement.setInt(1, queueElement.getFlowId());
                preparedStatement.setInt(2, queueElement.getElmId());
                preparedStatement.setInt(3, queueElement.getPendingParentCount());
                preparedStatement.setInt(4, queueElement.getIterationNumber());
                preparedStatement.setString(5, queueElement.getLoadStatus().getDbStatus());
                preparedStatement.addBatch();
                preparedStatement.executeBatch();
            }

        } catch(SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
