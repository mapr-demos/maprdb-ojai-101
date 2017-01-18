package com.mapr.db.samples.basic;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;
import org.ojai.types.ODate;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class Ex03MultipleDocuments {

  public static final String TABLE_PATH = "/apps/user_profiles";
  private Table table;

  Random randomGenerator = new Random();
  List<String> interestsList =  Arrays.asList("sports", "movies", "electronics", "development", "java", "sailing");


  public static void main(String[] args) throws Exception {

    Ex03MultipleDocuments app = new Ex03MultipleDocuments();
    app.run();

  }

  private void run() throws Exception {

    System.out.println("=== START ===");
    this.table = this.getTable(TABLE_PATH);

    long start = System.currentTimeMillis();

    System.out.println("\t Inserting documents");
    this.insertManyDocuments();

    long diff = System.currentTimeMillis() - start;


    System.out.println("Total execution time: " + (diff) );


    this.table.close();

    System.out.println("=== END ===");

  }

  private void insertManyDocuments() {

    Random random = new Random();
    for (int i=0 ; i < 100000; i++) {

      String id = String.format("%05d", i);
      int age = random.nextInt(100 - 10) + 10;


      Document document = MapRDB.newDocument()
              .set("_id", "user-"+ id  )
              .set("first_name", "First-"+ id)
              .set("last_name", "Last-"+ id)
              .set("age", age)
              .set("interests", this.generateInterests())
              .set("created_at", new ODate( new Date()) );

      // save document into the table
      table.insertOrReplace(document);

    }

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

  private List<String> generateInterests() {

    int index1 = randomGenerator.nextInt(interestsList.size());
    int index2 = randomGenerator.nextInt(interestsList.size());
    int index3 = randomGenerator.nextInt(interestsList.size());



    return Arrays.asList( interestsList.get(index1) , interestsList.get(index2) , interestsList.get(index3)  );


  }

}
