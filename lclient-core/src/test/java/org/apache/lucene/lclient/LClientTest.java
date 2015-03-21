package org.apache.lucene.lclient;


import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.List;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.LockObtainFailedException;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.Lists;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LClientTest {

  private static final String sep = StandardSystemProperty.FILE_SEPARATOR.value();
  private String dataPath;

  private LSchema schema;

  @Before
  public void setUp() {
    String root = getClass().getResource("/").getFile();
    this.dataPath = root + sep + "LClientTest" + sep + "data";

    this.schema = TestUtils.getLSchema();
  }

  @Test
  public void test001() throws IOException {
    try (LConnection conn = new LConnection(dataPath + sep + "mydb")) {
      LCommand cmd1 = new LCommand(conn, "test1", schema);
      assertThat(cmd1.count("*:*"), is(0));
      LCommand cmd2 = new LCommand(conn, "test2", schema);
      assertThat(cmd2.count("*:*"), is(0));
      assertThat(new File(dataPath, "mydb").exists(), is(true));
    }
  }

  @Test
  public void test002() throws IOException {
    LConnection conn = new LConnection(dataPath + sep + "mydb");
    LCommand cmd = new LCommand(conn, "test1", schema);
    assertThat(cmd.count("*:*"), is(0));
    conn.close();
  }

  @Test
  public void test003() throws IOException, ParseException {
    try (LConnection conn = new LConnection(dataPath + sep + "db")) {
      LCommand cmd = new LCommand(conn, "coll", schema);

      LDocument doc1 =  new LDocument(cmd.schema())
        .append("id", "1")
        .append("price", 10.25)
        .append("star", 2.5)
        .append("count", 12345)
        .append("date", DateTools.stringToTime("20150228123010000"))
        .append("datenow", Calendar.getInstance().getTime().getTime())
        .append("tag", "aaa")
        .append("text", "aaa bbb cccc");
      cmd.update(doc1);

      LDocument doc2 = new LDocument(cmd.schema()).append("id", "remove1");
      cmd.update(doc2);

      cmd.refresh();
      assertThat(cmd.count("*:*"), is(2));

      cmd.remove("remove1");
      cmd.refresh();
      assertThat(cmd.count("*:*"), is(1));
      assertThat(cmd.count("id:1"), is(1));
    }
  }

  @Test
  public void test004() throws IOException, ParseException {
    try (LConnection conn = new LConnection(dataPath + sep + "db")) {
      LCommand cmd = new LCommand(conn, "coll", schema);

      List<List<Document>> queryAndFilter = Lists.newArrayList();

      List<Document> Qdocs = cmd.find("id:1", null, null, null, null);
      assertThat(Qdocs.size(), is(1));
      queryAndFilter.add(Qdocs);

      List<Document> Fdocs = cmd.find(null, "id:1", null, null, null);
      assertThat(Fdocs.size(), is(1));
      queryAndFilter.add(Fdocs);

      System.out.println("---------- same values ----------");
      for (List<Document> entry : queryAndFilter) {
        for (Document doc : entry) {
          for (IndexableField field : doc.getFields()) {
            System.out.println("name:"+field.name()+" "+"value:"+field.stringValue());
            if (field.name().startsWith("date")){
              System.out.println("dateValue:"
              +DateTools.timeToString(field.numericValue().longValue(), Resolution.MILLISECOND));
            }
          }
        }
      }

    }
  }

  @Test
  public void test005() throws IOException, ParseException {
    try (LConnection conn = new LConnection(dataPath + sep + "db")) {
      LCommand cmd = new LCommand(conn, "coll", schema);

      assertThat(cmd.count("*:*"), is(1));
      assertThat(cmd.count("price:[1 TO 2]"), is(0));
      assertThat(cmd.count("price:[10 TO 20]"), is(1));
      assertThat(cmd.count("star:[1 TO 2]"), is(0));
      assertThat(cmd.count("star:[2 TO 3]"), is(1));
      assertThat(cmd.count("count:[1000 TO 2000]"), is(0));
      assertThat(cmd.count("count:[10000 TO 20000]"), is(1));
      long from = DateTools.stringToTime("2014");
      long to = DateTools.stringToTime("2015");
      assertThat(cmd.count("date:["+from+" TO "+to+"]"), is(0));
      from = DateTools.stringToTime("2015");
      to = DateTools.stringToTime("2016");
      assertThat(cmd.count("date:["+from+" TO "+to+"]"), is(1));
      assertThat(cmd.count("tag:aaaaa"), is(0));
      assertThat(cmd.count("tag:aaa"), is(1));
      assertThat(cmd.count("text:xxx"), is(0));
      assertThat(cmd.count("text:bbb"), is(1));
    }
  }

  @Test
  public void test006() throws IOException {
    try (LConnection conn = new LConnection(dataPath + sep + "db006")) {
      LCommand cmd1 = new LCommand(conn, "table1", schema);
      LCommand cmd2 = new LCommand(conn, "table2", schema);
      cmd1.update(new LDocument(cmd1.schema()).append("id", "xxx"));
      cmd1.refresh();
      assertThat(cmd1.count("id:xxx"), is(1));
      cmd2.update(new LDocument(cmd2.schema()).append("id", "yyy"));
      cmd2.update(new LDocument(cmd2.schema()).append("id", "zzz"));
      cmd2.refresh();
      assertThat(cmd2.count("*:*"), is(2));
    }
  }

  @Test
  public void test007() throws IOException, ParseException {
    try (LConnection conn = new LConnection(dataPath + sep + "db")) {
      LCommand cmd = new LCommand(conn, "coll", schema);
      List<Document> find = cmd.find("count:[1000 TO 2000]");
      List<Document> filter = cmd.filter("count:[1000 TO 2000]");
      assertThat(find.size(), is(filter.size()));
      find = cmd.find("count:[10000 TO 20000]");
      filter = cmd.filter("count:[10000 TO 20000]");
      assertThat(find.size(), is(filter.size()));
    }
  }

  @Test
  public void test008() throws IOException {
    String dbName = "sameDB";
    String collName = "sameCollection";

    try (LConnection conn = new LConnection(dataPath + sep + dbName)) {
      LCommand cmd1 = new LCommand(conn, collName, schema);
      LCommand cmd2 = new LCommand(conn, collName, schema);

      cmd1.removeByQuery("*:*"); cmd1.refresh();
      cmd2.removeByQuery("*:*"); cmd2.refresh();

      cmd1.update(new LDocument(cmd1.schema()).append("id", "xxx"));
      // skipping -- cmd1.refresh();
      assertThat(cmd1.count("*:*"), is(0));

      cmd2.update(new LDocument(cmd2.schema()).append("id", "yyy"));
      cmd2.update(new LDocument(cmd2.schema()).append("id", "zzz"));
      cmd2.refresh();
      assertThat(cmd2.count("*:*"), is(3));

      assertThat(cmd1.count("*:*"), is(0));
      cmd1.refresh();
      assertThat(cmd1.count("*:*"), is(3));

      try (LConnection connA = new LConnection(dataPath + sep + dbName)) {
        try {
          @SuppressWarnings("unused")
          LCommand cmdA = new LCommand(connA, collName, schema);
          fail("Should not get here");
        } catch (Exception e) {
          assert e instanceof LockObtainFailedException;
        }
      }
      cmd1.refresh();

      assertThat(cmd1.count("*:*"), is(3));
    }
  }

}
