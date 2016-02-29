/*
 *  Copyright 2009-2016 MapR Technologies
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mapr.db.samples.basic;

import com.mapr.db.Admin;
import com.mapr.db.FamilyDescriptor;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.TableDescriptor;
import org.ojai.Document;
import org.ojai.json.Json;
import org.ojai.store.DocumentMutation;
import org.ojai.types.ODate;
import org.ojai.types.OTimestamp;

import java.io.IOException;
import java.util.Arrays;

/**
 * This class shows the basic operations of Argonaut
 */
public class Ex02MapRdbAdmin {

  public static final String TABLE_PATH = "/apps/web_analytics";
  private Table table;


  public Ex02MapRdbAdmin() {
  }

  public static void main(String[] args) throws Exception {

    Ex02MapRdbAdmin app = new Ex02MapRdbAdmin();
    app.run();

  }

  private void run() throws IOException, InterruptedException {
    // See how to create a table with many options
    // various CF with different configuration
    createTable();

    // Create multiple documents
    // use multiple CF
    createAndReadDocuments();

    // close table
    table.close();

  }



  private void createTable() throws IOException {
    // delete table
    if ( MapRDB.tableExists(TABLE_PATH)) {
      MapRDB.deleteTable(TABLE_PATH);
    }


    // Admin Tool
    Admin admin = MapRDB.newAdmin();

    // Create a table descriptor
    TableDescriptor tableDescriptor = MapRDB.newTableDescriptor()
            .setPath(TABLE_PATH)  // set the Path of the table in MapR-FS
            .setSplitSize(512)    // Size in mebibyte (Mega Binary Bytes)
            .setBulkLoad(false);   // Created with Bulk mode by default

    // Configuration of the default Column Family, used to store JSON element by default
    FamilyDescriptor familyDesc = MapRDB.newDefaultFamilyDescriptor()
            .setCompression(FamilyDescriptor.Compression.None)
            .setInMemory(true); // To tell the DB to keep these value in RAM as much as possible
    tableDescriptor.addFamily(familyDesc);


    // Create a new colmn family to store specific JSON attributes
    familyDesc = MapRDB.newFamilyDescriptor()
            .setName("clicks")
            .setJsonFieldPath("clicks")
            .setCompression(FamilyDescriptor.Compression.ZLIB)  // compression for this CF
            .setInMemory(false);

    tableDescriptor.addFamily(familyDesc);

    Table table = admin.createTable(tableDescriptor);
  }

  private void createAndReadDocuments() throws InterruptedException {
    table = MapRDB.getTable(TABLE_PATH);
    table.setOption(Table.TableOption.BUFFERWRITE, true);

    Document meta = Json.newDocument()
            .set("title", "Home Page")
            .set("created_at", ODate.parse("2015-08-15"));

    Document doc = MapRDB.newDocument()
            .set("_id", "index.html")
            .set("meta", meta);

    table.insertOrReplace( doc );

    // read the document, only the default column family
    printClickInfos("index.html", false);


    addNewClickToPage("index.html");
    printClickInfos("index.html", false);


    addNewClickToPage("index.html");
    printClickInfos("index.html", false);

    // add multiple clicks
    addNewClickToPage("index.html");
    Thread.sleep(100);
    addNewClickToPage("index.html");
    Thread.sleep(100);
    addNewClickToPage("index.html");
    Thread.sleep(100);
    addNewClickToPage("index.html");
    Thread.sleep(100);
    addNewClickToPage("index.html");

    // read the document, only the default column family
    printClickInfos("index.html", false);




    // read the full document, all column families
    System.out.println("\n Print all clicks");
    printClickInfos("index.html", true);


  }

  private void addNewClickToPage(String s) {
    // add clicks to the document
    Document clickDetail = Json.newDocument()
            .set("ip", "54.54.54.54")
            .set("at", new OTimestamp(new java.util.Date()));

    DocumentMutation mutation = MapRDB.newMutation()
            .increment("nb_of_clicks", 1)
            .append("X", "X")
            .append("clicks", Arrays.asList(clickDetail));

    table.update("index.html", mutation);
  }

  /**
   *
   * @param page to show
   * @param complete if true print all the informations and individual clicks
   */
  private void printClickInfos(String page, boolean complete) {
    Document document = null;
    if (!complete) {
      document = table.findById(page, "_id", "meta", "nb_of_clicks");
    } else {
      document = table.findById(page);
    }
    System.out.println( document );
  }


  /**
   * Print table information such as Name, Path and Tablets information (sharding)
   *
   * @param tableName
   * @throws IOException
   */
  private void printTableInformation(String tableName) throws IOException {
    Table table = MapRDB.getTable(tableName);
    System.out.println("\n=============== TABLE INFO ===============");
    System.out.println(" Table Name : " + table.getName());
    System.out.println(" Table Path : " + table.getPath());
    System.out.println(" Table Infos : " + Arrays.toString(table.getTabletInfos()));

    System.out.println("Table Descriptor : " + table.getTableDescriptor() );

    System.out.println("==========================================\n");
  }

}
