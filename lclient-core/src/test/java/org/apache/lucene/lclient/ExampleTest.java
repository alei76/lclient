package org.apache.lucene.lclient;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.lclient.util.Documents;
import org.apache.lucene.uninverting.UninvertingReader;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.common.base.Joiner;
import com.google.common.base.StandardSystemProperty;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ExampleTest {

  private static final String sep = StandardSystemProperty.FILE_SEPARATOR.value();

  @Test
  public void test001() throws IOException {
    // get DB
    String root = getClass().getResource("/").getFile();
    String dataPath = root + sep + "ExampleTest" + sep + "data";
    try (LConnection conn = new LConnection(dataPath + sep + "db")) {

      // schema
      LSchema schema = LSchema.Builder()
        .setUniqueKey("id")
        .addField("id", LDataType.STRING)
        .addField("name", LDataType.TEXT, new StandardAnalyzer(), new StandardAnalyzer())
        .build();

      // get command
      LCommand cmd = new LCommand(conn, "coll", schema);

      // create LDocument, insert them 
      Arrays.asList(
        new LDocument(cmd.schema()).append("id", "A01").append("name", "Apache Lucene"),
        new LDocument(cmd.schema()).append("id", "A02").append("name", "Apache Solr"),
        new LDocument(cmd.schema()).append("id", "A03").append("name", "Apache ManifoldCF")
      )
      .stream()
      .forEach(doc -> {
        try {
          cmd.update(doc);
        } catch (IOException e) { }
      });
      cmd.refresh();

      assertThat(cmd.count("*:*"), is(3));
      assertThat(cmd.count("name:db"), is(0));

      // read all documents
      System.out.println("before:");
      print(cmd);

      // select a document, update it
      new LQuery(cmd)
        .find("name:lucene")
        .sort("id asc")
        .toStream()
        .map(doc -> Documents.toMap(cmd.schema(), doc))
        .peek(map -> {
          System.out.println("get a document:");
          System.out.println(Joiner.on(",").withKeyValueSeparator("=").join(map)); 
        })
        .forEach(doc -> {
          String newName = doc.get("name") + " " + "DB";
          doc.put("name", newName);
          try {
            LDocument updateDoc =  new LDocument(cmd.schema(), doc);
            cmd.update(updateDoc);
          } catch (IOException e) { }
        });
      cmd.refresh();
      // cache of uniqueKey.
      System.out.println(Joiner.on(",").join(UninvertingReader.getUninvertedStats()));

      assertThat(cmd.count("*:*"), is(3));
      assertThat(cmd.count("name:db"), is(1));

      // read all documents
      System.out.println("after:");
      print(cmd);

    } // close connection
  }

  private void print(LCommand cmd) throws IOException {
    new LQuery(cmd).find("*:*").toStream()
      .map(doc -> Documents.toMap(cmd.schema(), doc))
      .forEach(map -> {
        System.out.println(Joiner.on(",").withKeyValueSeparator("=").join(map));
      });
  }

}
