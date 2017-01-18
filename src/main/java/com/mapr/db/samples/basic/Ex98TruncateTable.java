package com.mapr.db.samples.basic;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.types.ODate;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;

public class Ex98TruncateTable {

  public static final String TABLE_PATH = "/apps/user_profiles";
  private Table table;


  public static void main(String[] args) throws Exception {

    Ex98TruncateTable app = new Ex98TruncateTable();
    app.run();

  }

  private void run() throws Exception {

    System.out.println("=== START ===");
    this.table = this.getTable(TABLE_PATH);

    System.out.println("\t ResetTable");
    this.resetTable();



    this.table.close();

    System.out.println("=== END ===");

  }

  private void resetTable() {

    // all recordss in the table
    DocumentStream rs = table.find();
    Iterator<Document> itrs = rs.iterator();
    Document readRecord;
    while (itrs.hasNext()) {
      readRecord = itrs.next();
      table.delete(readRecord.getId());
    }
    rs.close();
    table.flush();

  }

  /**
   * Get the table, create it if not present
   *
   * @throws IOException
   */
  private Table getTable(String tableName) throws IOException {
    Table table;

    if (!MapRDB.tableExists(tableName)) {
      table = MapRDB.createTable(tableName); // Create the table if not already present
    } else {
      table = MapRDB.getTable(tableName); // get the table
    }
    return table;
  }

  private void deleteTable(String tableName) throws IOException {
    if (MapRDB.tableExists(tableName)) {
      MapRDB.deleteTable(tableName);
    }

  }

}
