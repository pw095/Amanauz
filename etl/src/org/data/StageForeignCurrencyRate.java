package org.data;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import static org.data.ImoexWebSiteEntity.ImoexData;
import static org.util.AuxUtil.dateFormat;
import static org.util.AuxUtil.dateTimeFormat;

public class StageForeignCurrencyRate extends CbrWebSiteEntity {

    static class StageForeignCurrencyRateData extends ImoexData {
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

    public StageForeignCurrencyRate(long flowLoadId) {
        super(flowLoadId, "foreign_currency_rate");
    }

    public void detailLoad(PreparedStatement stmtUpdate) {
        //http://www.cbr.ru/scripts/XML_dynamic.asp?date_req1=01/05/2021&date_req2=28/05/2021&VAL_NM_RQ=R01010
        String urlStringRaw = "http://www.cbr.ru/scripts/XML_dynamic.asp?VAL_NM_RQ=";
        String urlStringData;

        this.setEffectiveFromDt("2021-05-01");
        this.setEffectiveToDt("2021-05-31");
        urlStringData = urlStringRaw.concat("R01010");

        try {
            historyCbrLoad(stmtUpdate, urlStringData);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    List<? extends ImoexData> parseXML(Document document) {

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
                    System.out.println(itemInfo.getNodeName() + " " + currentItem);
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



    void saveData(PreparedStatement stmtUpdate, List<? extends ImoexData> dataArray) {
        try {
            System.out.println("iter.count = " + dataArray.size());
            for(ImoexData iter : dataArray) {
                StageForeignCurrencyRateData jter = (StageForeignCurrencyRateData) iter;

                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getEntityStartLoadTimestamp().format(dateTimeFormat));
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
