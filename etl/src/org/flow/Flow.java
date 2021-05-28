package org.flow;

import org.data.*;
import org.meta.LoadMode;
import org.meta.LoadStatus;
import org.meta.Meta;
import org.meta.Properties;

import java.io.IOException;

import static org.util.AuxUtil.getProperties;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class Flow {
/*
    public static final String connString = "jdbc:sqlite:../db/file/meta.db";
    public static final DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private int flowId;
    private LoadStatus loadStatus;
    private LocalDateTime startDttm;
    private LocalDateTime finishDttm;

    public int getFlowId() {
        return this.flowId;
    }

    public void startFlow() {

        String sqlSelectString = "SELECT MAX(flow_id) AS flow_id FROM tbl_flow_log";
        String sqlInsertString = "INSERT INTO tbl_flow_log(flow_id, flow_status, flow_start_dttm) VALUES(?, ?, ?)";
        try(Connection conn = DriverManager.getConnection(connString);
            Statement stmt = conn.createStatement();
            PreparedStatement preparedStatement = conn.prepareStatement(sqlInsertString);
            ResultSet rs = stmt.executeQuery(sqlSelectString)) {
              if (rs.next()) {
                  flowId = rs.getInt("flow_id");
              } else {
                  flowId = 0;
              }

              flowId++;
              loadStatus = LoadStatus.RUNNING;
              startDttm = LocalDateTime.now();

              preparedStatement.setInt(1, flowId);
              preparedStatement.setString(2, loadStatus.getDbStatus());
              preparedStatement.setString(3, startDttm.format(dateFormat));

              preparedStatement.executeUpdate();

        } catch(SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    public void finishFlow() {

        String sqlUpdateString = "UPDATE tbl_flow_log SET flow_status = ?, flow_finish_dttm = ? WHERE flow_status = 'running' AND flow_id = ?";
        try(Connection conn = DriverManager.getConnection(connString);
            PreparedStatement preparedStatement = conn.prepareStatement(sqlUpdateString)) {

            loadStatus = LoadStatus.SUCCEEDED;
            finishDttm = LocalDateTime.now();

            preparedStatement.setString(1, loadStatus.getDbStatus());
            preparedStatement.setString(2, finishDttm.format(dateFormat));
            preparedStatement.setInt(3, flowId);

            preparedStatement.executeUpdate();

        } catch(SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void terminateFlow() {

        String sqlUpdateString = "UPDATE tbl_flow_log SET flow_status = ?, flow_finish_dttm = ? WHERE flow_status = 'running' AND flow_id = ?";
        try(Connection conn = DriverManager.getConnection(connString);
            PreparedStatement preparedStatement = conn.prepareStatement(sqlUpdateString)) {

            loadStatus = LoadStatus.FAILED;
            finishDttm = LocalDateTime.now();

            preparedStatement.setString(1, loadStatus.getDbStatus());
            preparedStatement.setString(2, finishDttm.format(dateFormat));
            preparedStatement.setInt(3, flowId);

            preparedStatement.executeUpdate();

        } catch(SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }*/

    public static void main(String[] args) throws InterruptedException, IOException {


        Properties.getInstance().setProperties(getProperties(args[0]));
        System.out.println(Properties.getInstance().getProperties().size());
        System.out.println(Properties.getInstance().getProperty("metaSqlDirectory"));
        long flowId = Meta.setFlowLogStart();
        List<AbstractEntity> entityList = new ArrayList<>();
//        AbstractEntity ent = new StageIndexSecurityWeight(flowId);

        // Данные за несколько дней, листов много
//        entityList.add(new StageIndexSecurityWeight(flowId));
//        entityList.add(new StageSecuritySharesRate(flowId));

        // Данные за 1 день, но много листов
        entityList.add(new StageSecurityEmitentMap(flowId));

        // Данные за 1 день, но лист 1
/*        entityList.add(new StageSecurityDailyMarketdata(flowId));
        entityList.add(new StageSecurityDailyInfo(flowId));
        entityList.add(new StageSecurityEngines(flowId));
        entityList.add(new StageSecurityMarkets(flowId));
        entityList.add(new StageSecurityBoards(flowId));
        entityList.add(new StageSecurityBoardGroups(flowId));
        entityList.add(new StageSecurityTypes(flowId));
        entityList.add(new StageSecurityGroups(flowId));
        entityList.add(new StageSecurityCollections(flowId));*/
        for (AbstractEntity entity : entityList) {
            entity.call();
        }
/*        AbstractEntity ent = new StageSecurityDailyMarketdata(flowId);
        AbstractEntity ent = new StageSecurityEngines(flowId);
        AbstractEntity ent = new StageSecurityMarkets(flowId);
        AbstractEntity ent = new StageSecurityBoards(flowId);
        AbstractEntity ent = new StageSecurityBoardGroups(flowId);

        AbstractEntity ent = new StageSecurityTypes(flowId);
        AbstractEntity ent1 = new StageSecurityGroups(flowId);
        AbstractEntity ent2 = new StageSecurityCollections(flowId);
        System.out.println("ent1: " + ent1.getEntityCode());
        System.out.println("ent2: " + ent2.getEntityCode());
        ent1.call();
        ent2.call();*/

        Meta.setFlowLogFinish(flowId, LoadStatus.SUCCEEDED);
//        Flow fl = new Flow();
//        fl.startFlow();
/*
        Consumer<QueueElement> cons = queueElement -> {
            AbstractEntity entity = null;
            switch (queueElement.getLayerCode()) {
                case "stage" :
                    switch (queueElement.getEntityCode()) {
                        case "index_security_weight" :
                            entity = new StageIndexSecurityWeight();
                            break;
                        case "security_rate" :
                            break;
                    }
                    break;
                case "ods" :
                    switch (queueElement.getEntityCode()) {
                        case "index_security_weight" :
                            break;
                        case "security_rate" :
                            break;
                    }
                    break;
                case "dds" :
                    switch (queueElement.getEntityCode()) {
                        case "index_hub" :
                            break;
                        case "security_hub" :
                            break;
                        case "index_sat" :
                            break;
                        case "security_sat" :
                            break;
                        case "index_security_link" :
                            break;
                        case "index_security_sat" :
                            break;
                    }
                    break;
            }
            if (entity != null) {
                System.out.println(queueElement instanceof QueueRelationElement);
                queueElement = entity.preLoad(queueElement);
                System.out.println(queueElement instanceof QueueRelationElement);
                queueElement = entity.load(queueElement);
                System.out.println(queueElement instanceof QueueRelationElement);
                queueElement = entity.postLoad(queueElement);
            }

        };
        EntityLoadQueue entityLoadQueue = new EntityLoadQueue(fl.getFlowId());
        entityLoadQueue.queueElementList.stream().filter(p -> p.getPendingParentCount() == 0).forEach(cons);
        new Thread().sleep(1_000);
        fl.finishFlow();*/
    }
}
