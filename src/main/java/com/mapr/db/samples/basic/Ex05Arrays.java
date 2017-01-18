package com.mapr.db.samples.basic;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;
import org.ojai.store.DocumentMutation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Ex05Arrays {

  public static final String TABLE_PATH = "/apps/sample_array";

  private Table table;


  public static void main(String[] args) throws IOException {

    System.out.println(" ==== Ex05Arrays started ===");
    Ex05Arrays app = new Ex05Arrays();
    app.run();

    System.out.println(" ==== Ex05Arrays finished ===");
  }

  public void run() throws IOException {

    Table table = getTable(TABLE_PATH);

    org.ojai.Document document = MapRDB.newDocument();

    String id = "test001";

    // Scalar list
    List<String> strings =  Arrays.asList(new String[]{"Zero", "one", "two" , "three"});

    List<Integer> ints = new ArrayList<Integer>();
    ints.add(0);
    ints.add(1);
    ints.add(2);
    ints.add(3);


    List<Float> floats = new ArrayList<Float>();
    floats.add(0.0f);
    floats.add(11.11f);
    floats.add(22.22f);
    floats.add(33.33f);


    // Array/List of Objects
    List<Document> documentList = new ArrayList<Document>();
    documentList.add( MapRDB.newDocument().set("name","subdoc0").set("value" , "zero") );
    documentList.add( MapRDB.newDocument().set("name","subdoc1").set("value" , "one") );
    documentList.add( MapRDB.newDocument().set("name","subdoc2").set("value" , "two") );



    document.set("string_list", strings);
    document.set("int_list", ints);
    document.set("float_list", floats);
    document.set("doc_list", documentList);

    // save document in the DB
    System.out.println("\t saving in the db");
    table.insertOrReplace(id, document);
    table.flush();

    //get document and print it
    System.out.println( table.findById(id)  );


    // Add new entry to the lists
    DocumentMutation mutation = MapRDB.newMutation()
            .append("string_list", Arrays.asList( new String[]{ "four"}) )
            .append("int_list", Arrays.asList( new Integer[]{ 4}) );

    table.update(id, mutation);
    table.flush();

    //get document and print it
    System.out.println("\n\t== After append ===");
    System.out.println( table.findById(id)  );

    // Increment some elements
    DocumentMutation mutationIncrement = MapRDB.newMutation()
            .increment("int_list[1]", 1 )
            .increment("int_list[10]", 100 ); // add an element at the end or increment the 10th entry

    table.update(id, mutationIncrement);
    table.flush();

    //get document and print it
    System.out.println("\n\t== After increment ===");
    System.out.println( table.findById(id)  );


    // update specific elements in list
    // Increment some elements
    DocumentMutation mutationUpdateElement = MapRDB.newMutation()
            .set("int_list[3]", 100 )
            .set("int_list[30]", 300 )
            .set("doc_list[4]", MapRDB.newDocument().set("name","subdoc4").set("value" , "four") )
            .set("doc_list[3]", MapRDB.newDocument().set("name","subdoc3").set("value" , "THREE") )
            .set("doc_list[0].value", "ZERO" )
            .set("doc_list[2].value", "TWO" );



    table.update(id, mutationUpdateElement);
    table.flush();

    //get document and print it
    System.out.println("\n\t== After element update ===");
    System.out.println( table.findById(id)  );


    // update specific elements in list
    // Increment some elements
    DocumentMutation mutationUpdateRemove = MapRDB.newMutation()
            .delete("int_list[0]")
            .delete("int_list[5]")
            .delete("int_list[50]") // nothing to do
            .delete("doc_list[4]");

    table.update(id, mutationUpdateRemove);
    table.flush();

    //get document and print it
    System.out.println("\n\t== After element update ===");
    System.out.println( table.findById(id)  );



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
