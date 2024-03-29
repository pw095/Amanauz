package org.flow;

import org.data.*;
import org.meta.LoadStatus;
import org.meta.Meta;
import org.meta.Properties;

import java.io.IOException;

import static org.util.AuxUtil.getProperties;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class Flow {

    private long flowLoadId;
    private LocalDateTime flowLogStartTimestamp;
    private LocalDateTime flowLogFinishTimestamp;

    public void setFlowLoadId(long flowLoadId) {
        this.flowLoadId = flowLoadId;
    }

    public long getFlowLoadId() {
        return flowLoadId;
    }

    public void setFlowLogStartTimestamp(LocalDateTime flowLogStartTimestamp) {
        this.flowLogStartTimestamp = flowLogStartTimestamp;
    }
    public LocalDateTime getFlowLogStartTimestamp() {
        return flowLogStartTimestamp;
    }

    public void setFlowLogFinishTimestamp(LocalDateTime flowLogFinishTimestamp) {
        this.flowLogFinishTimestamp = flowLogFinishTimestamp;
    }
    public LocalDateTime getFlowLogFinishTimestamp() {
        return flowLogFinishTimestamp;
    }

    public Flow() {
        Meta.setFlowLogStart(this);
    }
    public static void main(String[] args) throws InterruptedException, IOException {


        Properties.getInstance().setProperties(getProperties(args[0]));

        Flow flow = new Flow();
        List<AbstractEntity> entityList = new ArrayList<>();

//        Данные за 1 день, но лист 1
        entityList.add(new StageSecurityDailyInfoShares(flow)); // OK
        entityList.add(new StageSecurityDailyInfoBonds(flow)); // OK
        entityList.add(new StageSecurityEngines(flow)); // OK

        entityList.add(new StageSecurityMarkets(flow)); // OK
        entityList.add(new StageSecurityBoardGroups(flow)); // OK

        entityList.add(new StageSecurityBoards(flow)); // OK
        entityList.add(new StageSecurityCollections(flow)); // OK
        entityList.add(new StageSecurityDailyMarketdataBonds(flow)); // OK
        entityList.add(new StageSecurityDailyMarketdataShares(flow)); // OK

        entityList.add(new StageSecurityTypes(flow)); // OK
        entityList.add(new StageSecurityGroups(flow)); // OK

//        Данные справочника с cbr.ru, лист 1
        entityList.add(new StageForeignCurrencyDictionary(flow)); // OK
//        Данные не из справочника, но лист тоже всегда 1.
        entityList.add(new StageForeignCurrencyRate(flow)); // OK

//        Данные за 1 день, но много листов
        entityList.add(new StageSecurityEmitentMap(flow)); // OK
//        Данные за несколько дней, листов много
        entityList.add(new StageIndexSecurityWeight(flow)); // OK
        entityList.add(new StageSecurityRateShares(flow)); // OK
        entityList.add(new StageSecurityRateBonds(flow)); // OK

        for (AbstractEntity entity : entityList) {
            entity.call();
        }
        entityList.clear();

        entityList.add(new ReplicationEntity(flow, "foreign_currency_dictionary"));
        entityList.add(new ReplicationEntity(flow, "foreign_currency_rate"));
        entityList.add(new ReplicationEntity(flow, "index_security_weight"));
        entityList.add(new ReplicationEntity(flow, "security_board_groups"));
        entityList.add(new ReplicationEntity(flow, "security_boards"));
        entityList.add(new ReplicationEntity(flow, "security_collections"));
        entityList.add(new ReplicationEntity(flow, "security_daily_info_bonds"));
        entityList.add(new ReplicationEntity(flow, "security_daily_info_shares"));
        entityList.add(new ReplicationEntity(flow, "security_daily_marketdata_bonds"));
        entityList.add(new ReplicationEntity(flow, "security_daily_marketdata_shares"));
        entityList.add(new ReplicationEntity(flow, "security_emitent_map"));
        entityList.add(new ReplicationEntity(flow, "security_engines"));
        entityList.add(new ReplicationEntity(flow, "security_groups"));
        entityList.add(new ReplicationEntity(flow, "security_markets"));
        entityList.add(new ReplicationEntity(flow, "security_rate_bonds"));
        entityList.add(new ReplicationEntity(flow, "security_rate_shares"));
        entityList.add(new ReplicationEntity(flow, "security_types"));

        for (AbstractEntity entity : entityList) {
            entity.call();
        }
        entityList.clear();

        System.out.println(flow.getFlowLoadId());
        Meta.setFlowLogFinish(flow.getFlowLoadId(), LoadStatus.SUCCEEDED);

    }
}
