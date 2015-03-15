package org.apache.lucene.lclient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.common.base.Joiner;
import com.google.common.base.StandardSystemProperty;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JoinTest {

  private static final String sep = StandardSystemProperty.FILE_SEPARATOR.value();
  private String dataPath;

  private LSchema customerHSchema;
  private LSchema salesOrderDetailSchema;

  @Before
  public void setUp() {
    String root = getClass().getResource("/").getFile();
    this.dataPath = root + sep + "JoinTest" + sep + "data";

    this.customerHSchema = LSchema.Builder()
          .setUniqueKey("id")
          .addField("id", LDataType.STRING)
          .addField("customer", LDataType.TEXT, new StandardAnalyzer(), new StandardAnalyzer()).build();
    this.salesOrderDetailSchema = LSchema.Builder()
           .setUniqueKey("Did")
           .addField("Did", LDataType.STRING)
           .addField("Hid", LDataType.STRING)
           .addField("item", LDataType.TEXT, new StandardAnalyzer(), new StandardAnalyzer()).build();
  }

  @Test
  public void test001() throws IOException {
    try (LConnection conn = new LConnection(dataPath + sep + "db")) {
      LCommand cmdH = new LCommand(conn, "CustomerH", customerHSchema);
      LCommand cmdD = new LCommand(conn, "SalesOrderD", salesOrderDetailSchema);

      Arrays.asList(
        new LDocument(cmdH.schema()).append("id", "C01").append("customer", "Apache"),
        new LDocument(cmdH.schema()).append("id", "C02").append("customer", "Google"),
        new LDocument(cmdH.schema()).append("id", "C03").append("customer", "Java")
      )
      .stream()
      .forEach(doc -> { try { cmdH.update(doc); } catch (IOException e) { } });
      cmdH.refresh();
      assertThat(cmdH.count("*:*"), is(3));

      Arrays.asList(
        new LDocument(cmdD.schema()).append("Did", "SD011").append("item", "Laptop").append("Hid", "C01"),
        new LDocument(cmdD.schema()).append("Did", "SD012").append("item", "Book").append("Hid", "C01"),
        new LDocument(cmdD.schema()).append("Did", "SD013").append("item", "Monitor").append("Hid", "C01"),
        new LDocument(cmdD.schema()).append("Did", "SD021").append("item", "Monitor").append("Hid", "C02"),
        new LDocument(cmdD.schema()).append("Did", "SD022").append("item", "Laptop").append("Hid", "C02"),
        new LDocument(cmdD.schema()).append("Did", "SD031").append("item", "Book").append("Hid", "C03")
      )
      .stream()
      .forEach(doc -> { try { cmdD.update(doc); } catch (IOException e) { } });
      cmdD.refresh();
      assertThat(cmdD.count("*:*"), is(6));

      // select H.* from H inner join D on D.Hid = H.id where D.item = book
      List<String> resultList =
      new LQuery(cmdH)
      .join(cmdD).fromField("Hid").toField("id").fromFilter("item:book")
      .toStream().map(doc -> doc.getField("customer").stringValue())
      .collect(Collectors.toList());
      assertThat(Joiner.on(",").skipNulls().join(resultList), is("Apache,Java"));

      // select D.* from D inner join H on H.id = D.Hid where H.customer = google
      long result = 
      new LQuery(cmdD)
      .join(cmdH).fromField("id").toField("Hid").fromFilter("customer:google")
      .toStream()
      .count();
      assertThat(result, is(2L));

    }
  }

}
