package org.data;

import org.flow.Flow;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.util.AuxUtil.dateTimeFormat;

public class StageForeignCurrencyDictionary extends SnapshotEntity implements CbrSourceEntity {

    static class StageForeignCurrencyDictionaryData extends ExternalData {
        String id;
        String name;
        String engName;
        long   nominal;
        String parentCode;
        String isoNumCode;
        String isoCharCode;

        public StageForeignCurrencyDictionaryData(String id,
                                                  String name,
                                                  String engName,
                                                  long   nominal,
                                                  String parentCode,
                                                  String isoNumCode,
                                                  String isoCharCode) {
            this.id = id;
            this.name = name;
            this.engName = engName;
            this.nominal = nominal;
            this.parentCode = parentCode;
            this.isoNumCode = isoNumCode;
            this.isoCharCode = isoCharCode;
        }

    }

    public StageForeignCurrencyDictionary(Flow flow) {
        super(flow, "foreign_currency_dictionary");
    }

    @Override
    public void detailLoad(PreparedStatement stmtUpdate) {
        String urlStringRaw = "http://www.cbr.ru/scripts/XML_valFull.asp?d=";
        String urlStringData;
        Document document;

        urlStringData = urlStringRaw.concat("0");
        document = load(urlStringData);
        saveData(stmtUpdate, accumulateData(document));

        urlStringData = urlStringRaw.concat("1");
        document = load(urlStringData);
        saveData(stmtUpdate, accumulateData(document));
    }

    @Override
    public List<? extends ExternalData> accumulateData(Document document) {

        List<StageForeignCurrencyDictionaryData> dataList = new ArrayList<>();
        NodeList nodeList = document.getDocumentElement().getChildNodes();

        String name = null;
        String engName = null;
        long   nominal = -1;
        String parentCode = null;
        String isoNumCode = null;
        String isoCharCode = null;

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
                            name = currentItem;
                            break;
                        case 1:
                            engName = currentItem;
                            break;
                        case 2:
                            nominal = Long.valueOf(currentItem);
                            break;
                        case 3:
                            parentCode = currentItem;
                            break;
                        case 4:
                            isoNumCode = currentItem;
                            break;
                        case 5:
                            isoCharCode = currentItem;
                            break;
                    }
                }
            }
            dataList.add(new StageForeignCurrencyDictionaryData(nodeItem.getAttributes().getNamedItem("ID").getTextContent(),
                                                                name,
                                                                engName,
                                                                nominal,
                                                                parentCode,
                                                                isoNumCode,
                                                                isoCharCode));
        }
        return dataList;
    }

    @Override
    public void saveData(PreparedStatement stmtUpdate, List<? extends ExternalData> dataArray) {
        try {
            for(ExternalData iter : dataArray) {
                StageForeignCurrencyDictionaryData jter = (StageForeignCurrencyDictionaryData) iter;
                stmtUpdate.setLong(1, this.getFlowLoadId());
                stmtUpdate.setString(2, this.getFlowLogStartTimestamp().format(dateTimeFormat));
                stmtUpdate.setString(3, jter.id);
                stmtUpdate.setString(4, jter.name);
                stmtUpdate.setString(5, jter.engName);
                stmtUpdate.setLong(6, jter.nominal);
                stmtUpdate.setString(7, jter.parentCode);
                stmtUpdate.setString(8, jter.isoNumCode);
                stmtUpdate.setString(9, jter.isoCharCode);
                stmtUpdate.addBatch();
            }
            setInsertCount(getInsertCount() + stmtUpdate.executeBatch().length);
            stmtUpdate.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
