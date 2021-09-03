package org.data;

import org.flow.Flow;
import org.meta.MetaLayer;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.util.AuxUtil.dateFormat;
import static org.util.AuxUtil.dateTimeFormat;

public class StageForeignCurrencyRate extends PeriodEntity implements CbrSourceEntity {

    static class StageForeignCurrencyRateData extends ExternalData {
        String tradeDate;
        String id;
        long   nominal;
        double value;

        public StageForeignCurrencyRateData(String tradeDate,
                                            String id,
                                            long   nominal,
                                            double value) {
            this.tradeDate = tradeDate;
            this.id = id;
            this.nominal = nominal;
            this.value = value;
        }

    }

    public StageForeignCurrencyRate(Flow flow) {
        super(flow, MetaLayer.STAGE, "foreign_currency_rate");
    }

    @Override
    public void callLoad(Connection conn) {
        concreteLoad(conn);
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        String urlStringRaw = "http://www.cbr.ru/scripts/XML_dynamic.asp?VAL_NM_RQ=";
        String urlStringData;
        Document document;
        ArrayList<String> currencyList = new ArrayList<>();
        currencyList.add("R01239"); // Евро
        currencyList.add("R01235"); // Доллар США
        currencyList.add("R01035"); // Фунт стерлингов Соединенного королевства
        currencyList.add("R01010"); // Австралийский доллар
        currencyList.add("R01265"); // Израильский новый шекель
        currencyList.add("R01265C"); // Новый израильский шекель

        for (String lString : currencyList) {
            urlStringData = urlStringRaw.concat(lString)
                    .concat("&date_req1=")
                    .concat(LocalDate.parse(this.getEffectiveFromDt(), dateFormat).format(cbrPutDateFormat))
                    .concat("&date_req2=")
                    .concat(LocalDate.parse(this.getEffectiveToDt(), dateFormat).format(cbrPutDateFormat));
            document = load(urlStringData);
            saveData(stmtUpdate, accumulateData(document));
        }
    }

    @Override
    public List<? extends ExternalData> accumulateData(Document document) {

        List<StageForeignCurrencyRateData> dataList = new ArrayList<>();
        NodeList nodeList = document.getDocumentElement().getChildNodes();

        long   nominal = -1;
        double value = -1;

        for (int ii=0; ii < nodeList.getLength(); ii++) {
            Node nodeItem = nodeList.item(ii);
            NodeList itemList = nodeItem.getChildNodes();
            for (int jj=0; jj < itemList.getLength(); jj++) {
                Node itemInfo = itemList.item(jj);
                if (itemInfo.getNodeType() != Node.TEXT_NODE) {
                    Node item = itemInfo.getChildNodes().item(0);
                    String currentItem = item == null ? null : item.getTextContent();
                    switch(jj) {
                        case 0:
                            nominal = Long.valueOf(currentItem);
                            break;
                        case 1:
                            value = Double.valueOf(currentItem.replace(",", "."));
                            break;
                    }
                }
            }
            dataList.add
                    (new StageForeignCurrencyRateData
                            (LocalDate.parse(nodeItem.getAttributes().getNamedItem("Date").getTextContent(), cbrGetDateFormat).format(dateFormat),
                             nodeItem.getAttributes().getNamedItem("Id").getTextContent(),
                             nominal,
                             value));
        }
        return dataList;
    }

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageForeignCurrencyRateData jter = (StageForeignCurrencyRateData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.tradeDate);
                stmtUpdate.setString(4, jter.id);
                stmtUpdate.setLong(5, jter.nominal);
                stmtUpdate.setDouble(6, jter.value);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
