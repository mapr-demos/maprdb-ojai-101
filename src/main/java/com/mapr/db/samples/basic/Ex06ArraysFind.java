package com.mapr.db.samples.basic;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.DocumentMutation;
import org.ojai.store.QueryCondition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Ex06ArraysFind {

  public static final String TABLE_PATH = "/apps/sample_array";

  private Table table;


  public static void main(String[] args) throws IOException {

    System.out.println(" ==== Ex06ArraysFind started ===");
    Ex06ArraysFind app = new Ex06ArraysFind();
    app.run();

    System.out.println(" ==== Ex06ArraysFind finished ===");
  }

  public void run() throws IOException {

    Table table = getTable(TABLE_PATH);


    insertDocuments(table);

    // all recordss in the table
    System.out.println("\n\nAll records");
    DocumentStream rs = table.find();
    Iterator<Document> itrs = rs.iterator();
    Document readRecord;
    while (itrs.hasNext()) {
      readRecord = itrs.next();
      System.out.println("\t" + readRecord);
    }
    rs.close();



    System.out.println("\n\n Find document with int_list containing 3  ");
    QueryCondition condition = MapRDB.newCondition();
    condition.is("int_list[]", QueryCondition.Op.EQUAL, 3);
    rs = table.find(condition);
    itrs = rs.iterator();
    while (itrs.hasNext()) {
      readRecord = itrs.next();
      System.out.println("\t" + readRecord);
    }
    rs.close();


    System.out.println("\n\n Find document with string_list containing three  ");
    condition = MapRDB.newCondition();
    condition.is("string_list[]", QueryCondition.Op.EQUAL, "three");
    rs = table.find(condition);
    itrs = rs.iterator();
    while (itrs.hasNext()) {
      readRecord = itrs.next();
      System.out.println("\t" + readRecord);
    }
    rs.close();



    System.out.println("\n\n Find document with string_list containing three or trois  ");
    condition = MapRDB.newCondition();
    condition.in("string_list[]", Arrays.asList( new String[]{"three","trois"} ));
    rs = table.find(condition);
    itrs = rs.iterator();
    while (itrs.hasNext()) {
      readRecord = itrs.next();
      System.out.println("\t" + readRecord);
    }
    rs.close();

    
  }

  private void insertDocuments(Table table) {

    String doc1 = "{\"_id\":\"test001\",\"string_list\":[\"zero\",\"one\",\"two\",\"three\",\"four\"],\"int_list\":[0,1,2,3,4,5],\"doc_list\":[{\"name\":\"subdoc0\",\"value\":\"ZERO\"},{\"name\":\"subdoc1\",\"value\":\"one\"},{\"name\":\"subdoc2\",\"value\":\"two\"},{\"name\":\"subdoc3\",\"value\":\"three\"}]}";
    String doc2 ="{\"_id\":\"test002\",\"string_list\":[\"zero\",\"un\",\"deux\",\"trois\",\"quatre\"],\"int_list\":[10,20,30,40,50],\"doc_list\":[{\"name\":\"mood\",\"value\":\"good\"},{\"name\":\"time\",\"value\":\"bad\"},{\"name\":\"temperature\",\"value\":\"hot\"}]}";
    String doc3 = "{\"_id\":\"test003\",\"string_list\":[\"0\",\"1\",\"deux\",\"trois\",\"cinq\"],\"int_list\":[0,10,20,3,40,5],\"doc_list\":[{\"name\":\"mood\",\"value\":\"bad\"},{\"name\":\"mood\",\"value\":\"good\"},{\"name\":\"temperature\",\"value\":\"cold\"}]}";
    table.insertOrReplace( MapRDB.newDocument(doc1) );
    table.insertOrReplace( MapRDB.newDocument(doc2) );
    table.insertOrReplace( MapRDB.newDocument(doc3) );
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

}
