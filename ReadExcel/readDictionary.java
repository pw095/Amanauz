import java.io.*;
import java.util.Iterator;
import java.math.*;

import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;

import java.sql.*;

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
  
    String sql = "INSERT INTO dictionary_stg(level, leaf_code, code, parent_leaf_code, parent_code, name) VALUES (?, ?, ?, ?, ?, ?);";

    
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
  
  public void bindDate(int Index, Date arr) {
  
    java.text.DateFormat df = new java.text.SimpleDateFormat("yyyy-MM-dd");
    bindString(Index, df.format(arr));
    
  }

  public void bindNumber(int Index, double arr) {

    try {
      pstmt.setLong(Index, (long) arr);
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



public class readDictionary {
  public static void main(String[] args) {
  
    int RowIndex = 0;
    try (FileInputStream inputStream = new FileInputStream(new File("/media/sf_shared_folder/Dictionary.xlsx"))) {
    
      XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
      XSSFSheet sheet = workbook.getSheetAt(0);
      
      String CellValueString = new String();
      long CellValueLong = -1;
      
      int ValueIndex;
      InsertApp app = new InsertApp();
      
      app.declare();

      
      Iterator<Row> rowIterator = sheet.iterator();
      while (rowIterator.hasNext()) {
        Row row = rowIterator.next();
        if(RowIndex++ == 0) {
          continue;
        }
        
        ValueIndex = 0;
        Iterator<Cell> cellIterator = row.cellIterator();

        while (cellIterator.hasNext()) {

          Cell cell = cellIterator.next();
          if (cell == null) {
            return;
          }
          CellType cellType = cell.getCellType();

          switch (cellType) {
/*
            case NUMERIC:
                CellValueLong = (long) cell.getNumericCellValue();
                break;
            case STRING:
                CellValueString = cell.getStringCellValue();
                break;
*/
            case ERROR:
                System.out.println("Error");
                break;
            case _NONE:
                System.out.println("None");
                break;
            case BLANK:
                System.out.println("Blank");
                System.out.println("RowIndex = " + RowIndex);
                return;
/*
            case BOOLEAN:
                System.out.println("Bool");
                break;

            case FORMULA:
                System.out.println("Formula");
                break;
            default:
                System.out.println("!!!<>!!!");
                break;
                */
          }
/*
          System.out.println("ValueIndex = " + ValueIndex);
          System.out.println(CellValueLong + " " + CellValueString);
*/
          switch(ValueIndex++) {
            case 0: // level
              app.bindNumber(ValueIndex, cell.getNumericCellValue());
              break;
            case 1: // leaf_code
              app.bindString(ValueIndex, cell.getStringCellValue());
              break;
            case 2: // code
              app.bindString(ValueIndex, cell.getStringCellValue());
              break;
            case 3: // parent_leaf_code
              app.bindString(ValueIndex, cell.getStringCellValue());
              break;
            case 4: // parent_code
              app.bindString(ValueIndex, cell.getStringCellValue());
              break;
            case 5: // name
              app.bindString(ValueIndex, cell.getStringCellValue());
              break;
          }
        }
        app.bindBatch();
      }
      app.commit();

    } catch (IOException e) {
      System.out.println(e.getMessage());
    } catch (NullPointerException e) {
      System.out.println("RowIndex = " + RowIndex);
      System.out.println(e.getMessage());
    }
  }
}

