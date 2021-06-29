import java.io.*;
import java.util.Iterator;

import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.util.CellAddress;

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
    String sql = "INSERT INTO prod_prog(prod_code, prod_name, prog_code, prog_name, prod_group, prog_group_type) VALUES(?, ?, ?, ?, ?, ?)";
    
    try {
      pstmt = conn.prepareStatement(sql);
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }
  
  
  public void bind(String[] arr) {

    for(int i=0; i<arr.length; i++) {
      try {
        pstmt.setString(i+1, arr[i]);
      } catch (SQLException e) {
        System.out.println(e.getMessage());
      }
    }
    
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
/*
  public void insert(String[] arr) {

    String sql = "INSERT INTO prod_prog(prod_code, prod_name, prog_code, prog_name, prod_group, prog_group_type) VALUES(?, ?, ?, ?, ?, ?)";
    
    try(Connection conn = this.connect();
        PreparedStatement pstmt = conn.prepareStatement(sql)) {
        
      pstmt.setString(1, arr[0]);
      pstmt.setString(2, arr[1]);
      pstmt.setString(3, arr[2]);
      pstmt.setString(4, arr[3]);
      pstmt.setString(5, arr[4]);
      pstmt.setString(6, arr[5]);
      
      pstmt.executeUpdate();
      
    } catch (SQLException e) {
      System.out.println(e.getMessage());
    }
  }
  */
}

public class manageExcel {

  public static void main(String[] args) throws IOException {
  
    InsertApp app = new InsertApp();
    String stringValue = null;
    int i=0;
    String[] arr = new String[6];
    if (args.length != 1) {
      System.out.println("Incorrect number of arguments");
    }
    
    try(FileInputStream inputStream = new FileInputStream(args[0])) {
      XSSFWorkbook workbook = new XSSFWorkbook(inputStream);
      Iterator<Sheet> sheetIterator = workbook.sheetIterator();
      
        while (sheetIterator.hasNext()) {
          Sheet sheet = sheetIterator.next();
          Iterator<Row> rowIterator = sheet.rowIterator();
          
          app.declare();
          while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            Iterator<Cell> cellIterator = row.cellIterator();
            
            i=0;
            while (cellIterator.hasNext()) {
              Cell cell = cellIterator.next();
              CellType cellType = cell.getCellType();
              
              switch (cellType) {
                case STRING:
                  stringValue = cell.getStringCellValue();
//                  System.out.println("arr[" + (i-1) + "] = " + arr[i-1]);
                  break;
                case NUMERIC:
                  stringValue = Integer.valueOf((int) cell.getNumericCellValue()).toString();
                  break;
                default:
                  CellAddress cellAddress = cell.getAddress();
                  System.out.println("Sheet " + sheet.getSheetName() + " Cell at (" + (1 + cellAddress.getRow()) + ", " + (1 + cellAddress.getColumn()) + ") has non-String or non-Numeric type!");
//                  return;
              }
              arr[i++] = stringValue;
            }
            System.out.println("arr_length = " + arr.length);
            app.bind(arr);
//            app.insert(arr);
//            return;
          }
          app.commit();
          return;
        }
    } catch(FileNotFoundException e) {
      System.out.println("File " + args[0] + " cannot be found.");
    }
  }
}

