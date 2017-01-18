package com.mapr.db.samples.basic;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.QueryCondition;

import java.io.IOException;

public class Ex04MultipleDocumentsQuery {

  public static final String TABLE_PATH = "/apps/user_profiles";
  private Table table;


  public static void main(String[] args) throws Exception {

    Ex04MultipleDocumentsQuery app = new Ex04MultipleDocumentsQuery();
    app.run();

  }

  private void run() throws Exception {

    System.out.println("=== START ===");
    this.table = this.getTable(TABLE_PATH);


    System.out.println("\t Query documents");
    this.queryByAge();


    this.table.close();

    System.out.println("\n\n=== END ===");

  }

  private void queryByAge() {

    QueryCondition condition = MapRDB.newCondition()
            .is("age", QueryCondition.Op.GREATER , 50)
            .build();

    int counter = 0;
    try(DocumentStream documentStream = table.find(condition)) {
      for (Document doc : documentStream ) {
        System.out.print(".");
        counter++;
      }
    }
    System.out.println("\n\nTotal documents : "+ counter + " for "+ condition);

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
