package org.apache.lucene.lclient;

import java.io.IOException;
import java.util.stream.IntStream;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.common.base.StandardSystemProperty;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class QueryTest {

  private static final String sep = StandardSystemProperty.FILE_SEPARATOR.value();
  private String dataPath;

  private static final int DOC_SIZE = 100;

  private LSchema schema;

  @Before
  public void setUp() {
    String root = getClass().getResource("/").getFile();
    this.dataPath = root + sep + "QueryTest" + sep + "data";

    this.schema = TestUtils.getLSchema();
  }

  @Test
  public void test001() throws IOException {
    try (LConnection conn = new LConnection(dataPath + sep + "db")) {
      LCommand cmd = new LCommand(conn, "coll", schema);
      IntStream.range(0, DOC_SIZE).parallel().forEach(TestUtils.getIndexConsumer(cmd));
      cmd.refresh();
    }
  }

  @Test
  public void test002() throws IOException {
    try (LConnection conn = new LConnection(dataPath + sep + "db")) {

    LCommand cmd = new LCommand(conn, "coll", schema);
      System.out.println("--price desc--");
        new LQuery(cmd)
       .find("*:*")
       .limit(5)
       .sort("price desc")
       .fields("id,price")
       .toStream()
       .forEach(doc -> print(doc));

        System.out.println("--price asc--");
        new LQuery(cmd)
       .find("*:*")
       .limit(5)
       .sort("price asc")
       .fields("id,price")
       .toStream()
       .forEach(doc -> print(doc));

        System.out.println("--star desc--");
        new LQuery(cmd)
       .find("*:*")
       .limit(5)
       .sort("star desc")
       .fields("id,star")
       .toStream()
       .forEach(doc -> print(doc));

        System.out.println("--star asc--");
        new LQuery(cmd)
       .find("*:*")
       .limit(5)
       .sort("star asc")
       .fields("id,star")
       .toStream()
       .forEach(doc -> print(doc));

        System.out.println("--count desc--");
        new LQuery(cmd)
       .find("*:*")
       .limit(5)
       .sort("count desc")
       .fields("id,count")
       .toStream()
       .forEach(doc -> print(doc));

        System.out.println("--count asc--");
        new LQuery(cmd)
       .find("*:*")
       .limit(5)
       .sort("count asc")
       .fields("id,count")
       .toStream()
       .forEach(doc -> print(doc));

        System.out.println("--long desc--");
        new LQuery(cmd)
       .find("*:*")
       .limit(5)
       .sort("date desc")
       .fields("id,date")
       .toStream()
       .forEach(doc -> print(doc));

        System.out.println("--long asc--");
        new LQuery(cmd)
       .find("*:*")
       .limit(5)
       .sort("date asc")
       .fields("id,date")
       .toStream()
       .forEach(doc -> print(doc));

        System.out.println("--tag desc--");
        new LQuery(cmd)
       .find("*:*")
       .limit(5)
       .sort("tag desc")
       .fields("id,tag")
       .toStream()
       .forEach(doc -> print(doc));

        System.out.println("--tag asc--");
        new LQuery(cmd)
       .find("*:*")
       .limit(5)
       .sort("tag asc")
       .fields("id,tag")
       .toStream()
       .forEach(doc -> print(doc));

        System.out.println("--text desc--");
        new LQuery(cmd)
       .find("*:*")
       .limit(5)
       .sort("text desc")
       .fields("id,text")
       .toStream()
       .forEach(doc -> print(doc));

        System.out.println("--text asc--");
        new LQuery(cmd)
       .find("*:*")
       .limit(5)
       .sort("text asc,id asc")
       .fields("id,text")
       .toStream()
       .forEach(doc -> print(doc));
    }
  }

  @Test
  public void test003() throws IOException {
    try (LConnection conn = new LConnection(dataPath + sep + "db")) {

    LCommand cmd = new LCommand(conn, "coll", schema);
      System.out.println("--text asc #2--");
      cmd.stream("*:*", null, 5, "text asc, id asc", "id, text")
      .forEach(doc -> print(doc));
    }
  }

  private void print(Document doc) {
    for (IndexableField field : doc.getFields()) {
      System.out.println(field.name()+":"+field.stringValue());
    }
  }

  @Test
  public void test004() throws IOException {
    try (LConnection conn = new LConnection(dataPath + sep + "db")) {
      LCommand cmd = new LCommand(conn, "perfieldanalyzer", schema);

      LDocument doc = new LDocument(schema)
        .append("id", "A01")
        .append("text", "Lucene Solr")
        .append("text_ws", "Lucene Solr")
        .append("text_kw", "Lucene Solr")
        .append("text_1g", "Lucene Solr")
        .append("text_2g", "Lucene Solr");
      cmd.update(doc);
      cmd.refresh();

      assertThat(cmd.count("text:lucene"), is(1));
      assertThat(cmd.count("text:Lucene"), is(1));
      assertThat(cmd.count("text:solr"), is(1));
      assertThat(cmd.count("text:(lucene solr)"), is(1));
      assertThat(cmd.count("text:\"lucene solr\""), is(1));
      assertThat(cmd.count("text:Elasticsearch"), is(0));

      assertThat(cmd.count("text_ws:lucene"), is(0));
      assertThat(cmd.count("text_ws:Lucene"), is(1));
      assertThat(cmd.count("text_ws:Solr"), is(1));
      assertThat(cmd.count("text_ws:(Lucene Solr)"), is(1));
      assertThat(cmd.count("text_ws:\"Lucene Solr\""), is(1));
      assertThat(cmd.count("text_ws:Elasticsearch"), is(0));

      assertThat(cmd.count("text_ws:lucene"), is(0));
      assertThat(cmd.count("text_kw:Lucene"), is(0));
      assertThat(cmd.count("text_kw:Solr"), is(0));
      assertThat(cmd.count("text_kw:(Lucene Solr)"), is(0));
      assertThat(cmd.count("text_kw:\"Lucene Solr\""), is(1));
      assertThat(cmd.count("text_kw:Elasticsearch"), is(0));

      assertThat(cmd.count("text_1g:lucene"), is(1));
      assertThat(cmd.count("text_1g:Lucene"), is(1));
      assertThat(cmd.count("text_1g:a"), is(0));
      assertThat(cmd.count("text_1g:ab"), is(0));
      assertThat(cmd.count("text_1g:L"), is(1));
      assertThat(cmd.count("text_1g:LS"), is(1));
      assertThat(cmd.count("text_1g:n"), is(1));
      assertThat(cmd.count("text_1g:no"), is(1));
      assertThat(cmd.count("text_1g:\"no\""), is(0));

      assertThat(cmd.count("text_2g:lucene"), is(1));
      assertThat(cmd.count("text_2g:Lucene"), is(1));
      assertThat(cmd.count("text_2g:a"), is(0));
      assertThat(cmd.count("text_2g:ab"), is(0));
      assertThat(cmd.count("text_2g:L"), is(0));
      assertThat(cmd.count("text_2g:LS"), is(0));
      assertThat(cmd.count("text_2g:ne"), is(1));
      assertThat(cmd.count("text_2g:nelr"), is(1));
      assertThat(cmd.count("text_2g:\"nelr\""), is(0));
    }
  }
}
