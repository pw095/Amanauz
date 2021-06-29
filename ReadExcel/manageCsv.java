import java.io.*;
import java.util.Iterator;
import java.sql.*;
import java.math.*;


class InsertApp {

  private Connection conn;
  private PreparedStatement pstmt;
  
  private Connection connect() {
    String url = "jdbc:sqlite:/media/sf_shared_folder/chinook.db";
    Connection conn = null;
    try {
      conn = DriverManager.getConnection(url);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
    return conn;
  }

  InsertApp() {
    conn = connect();
  }

  public void declare() {
  
    String sql = "INSERT INTO stg_moex_rates(load_date,secid,shortname,name,typename,isin,regnumber,listlevel,facevalue,faceunit,issuesize,is_collateral,is_external,primary_boardid,primary_board_title,is_rii,issuedate,eveningsession,is_qualified_investors,high_risk,registryclosedate,dividendvalue,dividendyield,securitycapitalization,emitentcapitalization,emitentname,inn,lotsize,price,price_rub,rtl1,rth1,rtl2,rth2,rtl3,rth3,discount1,limit1,discount2,limit2,discount3,discountl0,discounth0) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
//    String sql = "INSERT INTO stg_moex_rates_1(load_date, secid,shortname,name,typename,isin,regnumber,listlevel,facevalue,faceunit,issuesize,issuedate) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

//    String sql = "INSERT INTO stg_moex_rates_3(load_date,secid,shortname,name,typename,isin,regnumber) VALUES(?, ?, ?, ?, ?, ?, ?)";

//    String sql = "INSERT INTO stg_moex_rates_2(load_date) VALUES(?)";
    
    try {
      pstmt = conn.prepareStatement(sql);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }
  

  
  public void bindString(int Index, String arr) {

    try {
      if (arr.isEmpty()) {
        pstmt.setNull(Index, Types.CHAR);
      } else {
        pstmt.setString(Index, arr);
      }
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }
  
  public void bindDate(int Index, String arr) {
  
    String str_tmp;

    if (arr.isEmpty()) {
      str_tmp = arr;
    } else {
      str_tmp = arr.substring(6, 10) + '-' + arr.substring(3, 5) + '-' + arr.substring(0, 2);
    }
    
    bindString(Index, arr);
    
  }
  
  public void bindInt(int Index, String arr) {

    try {
      if (arr.isEmpty()) {
        pstmt.setNull(Index, Types.INTEGER);
      } else {
        try {
          pstmt.setInt(Index, Integer.valueOf(arr));
        } catch (NumberFormatException e) {
          pstmt.setLong(Index, Long.valueOf(arr));
        }
      }
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  
  }

  public void bindBigDecimal(int Index, String arr) {
  
    try {
      if (arr.isEmpty()) {
        pstmt.setNull(Index, Types.REAL);
      } else {
        pstmt.setBigDecimal(Index, BigDecimal.valueOf(Double.valueOf(arr.replace(',', '.'))));
      }
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }

  public void bindBatch() {

    try {
      pstmt.addBatch();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }

  }

  public void commit() {
  
    try {
      pstmt.executeBatch();
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }
}



class manageCsv {

  private static void readCSV(File inputFile) throws IOException{
  
    String str = new String("b");
    String fileName = inputFile.getName();
    int StartIndex, EndIndex, EndIndexTmp, ValueIndex, RowNumber = 0;
    
    InsertApp app = new InsertApp();
//    System.out.println(fileName.substring(0, fileName.indexOf(".csv")));
    
    try(BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), "windows-1251"))) {
    
      while ((str = br.readLine()) != null) {
      
        app.declare();      
        
        if(RowNumber++ < 3 || str.isEmpty()) {
          continue;
        } else {
        
          StartIndex=EndIndex=0;
          ValueIndex=1;
          
          app.bindString(ValueIndex, fileName.substring(0, fileName.indexOf(".csv")));
          do {
            EndIndexTmp = str.indexOf(';', StartIndex);
            EndIndex = EndIndexTmp != -1 ? EndIndexTmp : str.length();
            
            String str_tmp = str.substring(StartIndex, EndIndex);
//            System.out.println(str_tmp);
            switch (++ValueIndex) {
              case 2: // secid
                app.bindString(ValueIndex, str_tmp);
                break;
              case 3: // shortname
                app.bindString(ValueIndex, str_tmp);
                break;
              case 4: // name
                app.bindString(ValueIndex, str_tmp);
                break;
              case 5: // typename
                app.bindString(ValueIndex, str_tmp);
                break;
              case 6: // isin
                app.bindString(ValueIndex, str_tmp);
                break;
              case 7: // regnumber
                app.bindString(ValueIndex, str_tmp);
                break;
              case 8: // listlevel
                app.bindInt(ValueIndex, str_tmp);
                break;
              case 9: // facevalue
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 10: // faceunit
                app.bindString(ValueIndex, str_tmp);
                break;
              case 11: // issuesize
                app.bindInt(ValueIndex, str_tmp);
                break;
              case 12: // is_collateral
                app.bindString(ValueIndex, str_tmp);
                break;
              case 13: // is_external
                app.bindString(ValueIndex, str_tmp);
                break;
              case 14: // primary_boardid
                app.bindString(ValueIndex, str_tmp);
                break;
              case 15: // primary_board_title
                app.bindString(ValueIndex, str_tmp);
                break;
              case 16: // is_rii
                app.bindString(ValueIndex, str_tmp);
                break;
              case 17: // issuedate
                app.bindDate(ValueIndex, str_tmp);
                break;
              case 18: // eveningsession
                app.bindString(ValueIndex, str_tmp);
                break;
              case 19: // is_qualified_investors
                app.bindString(ValueIndex, str_tmp);
                break;
              case 20: // high_risk
                app.bindString(ValueIndex, str_tmp);
                break;
              case 21: // registryclosedate
                app.bindDate(ValueIndex, str_tmp);
                break;
              case 22: // dividendvalue
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 23: // dividendyield
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 24: // securitycapitalization
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 25: // emitentcapitalization
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 26: // emitentname
                app.bindString(ValueIndex, str_tmp);
                break;
              case 27: // inn
                app.bindString(ValueIndex, str_tmp);
                break;
              case 28: // lotsize
                app.bindInt(ValueIndex, str_tmp);
                break;
              case 29: // price
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 30: // price_rub
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 31: // rtl1
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 32: // rth1
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 33: // rtl2
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 34: // rth2
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 35: // rtl3
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 36: // rth3
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 37: // discount1
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 38: // limit1
                app.bindInt(ValueIndex, str_tmp);
                break;
              case 39: // discount2
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 40: // limit2
                app.bindInt(ValueIndex, str_tmp);
                break;
              case 41: // discount3
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 42: // discountl0
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
              case 43: // discounth0
                app.bindBigDecimal(ValueIndex, str_tmp);
                break;
            }
            
            StartIndex = EndIndex + 1;
          } while (EndIndexTmp != -1);
          app.bindBatch();
        }
        app.commit();
      }

    } catch(FileNotFoundException e) {
      System.out.println("File " + inputFile.getName() + " cannot be found.");
    }
    
  }
  
  public static void main(String[] args) throws IOException {
  /*
    InsertApp app = new InsertApp();
    app.declare();
    app.bindString(1, "2020-09-24");
    app.bindBatch();
    app.commit();
  */

    File csvDirectory = new File("/media/sf_shared_folder/moex_csv");
    for(File CurrFile : csvDirectory.listFiles()) {
      readCSV(CurrFile);
    }

  }
  
}

