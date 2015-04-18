package org.apache.lucene.lclient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.common.base.Joiner;
import com.google.common.base.StandardSystemProperty;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class GroupingTest {

  private static final String sep = StandardSystemProperty.FILE_SEPARATOR.value();
  private String dataPath;
  private LSchema schema;
  private LConnection conn;
  private LCommand cmd;

  @Before
  public void setUp() throws IOException {
    String root = getClass().getResource("/").getFile();
    this.dataPath = root + sep + "GroupingTest" + sep + "data";
    this.schema = LSchema.Builder()
                    .setUniqueKey("id")
                    .addField("id", LDataType.STRING)
                    .addField("price", LDataType.DOUBLE)
                    .addField("code", LDataType.STRING)
                    .build();
    this.conn = new LConnection(dataPath + sep + "db");
    this.cmd = new LCommand(conn, "test", schema);
  }

  @After
  public void tearDown() throws IOException {
    conn.close();
  }

  @Test
  public void test001() throws IOException {
    cmd.update(new LDocument(schema).append("id", "04").append("price", 0.40).append("code", "A02"));
    cmd.update(new LDocument(schema).append("id", "03").append("price", 0.30).append("code", "A01"));
    cmd.update(new LDocument(schema).append("id", "01").append("price", 0.10).append("code", "A01"));
    cmd.update(new LDocument(schema).append("id", "05").append("price", 0.50).append("code", "A01"));
    cmd.update(new LDocument(schema).append("id", "07").append("price", 0.70).append("code", "A01"));
    cmd.update(new LDocument(schema).append("id", "02").append("price", 0.20).append("code", "A02"));
    cmd.update(new LDocument(schema).append("id", "08").append("price", 0.80).append("code", "A02"));
    cmd.update(new LDocument(schema).append("id", "10").append("price", 1.00).append("code", "A02"));
    cmd.update(new LDocument(schema).append("id", "09").append("price", 0.90).append("code", "A01"));
    cmd.update(new LDocument(schema).append("id", "06").append("price", 0.60).append("code", "A01"));
    cmd.refresh();

    List<String> result1 = cmd.groupingField("code", null, "*:*", null);
    assertThat(Joiner.on(",").skipNulls().join(result1), is("A01,A02"));

    List<String> result2 = cmd.groupingField("code", "code desc", null, null);
    assertThat(Joiner.on(",").skipNulls().join(result2), is("A02,A01"));

    List<String> result3 = cmd.groupingField("id", "id desc", "id:[05 TO 10]", null);
    assertThat(Joiner.on(",").skipNulls().join(result3), is("10,09,08,07,06,05"));

    List<String> result4 = cmd.groupingField("id", null, null, "price:[0.10 TO 0.30]");
    assertThat(Joiner.on(",").skipNulls().join(result4), is("01,02,03"));

    try {
      List<String> resultNG = cmd.groupingField("price", null, null, null);
      assertThat(Joiner.on(",").skipNulls().join(resultNG), is("0.10,0.20"));
      fail("Should not get here");
    } catch (Exception e) {
      assert e instanceof IllegalStateException;
      assertThat(e.getMessage(), is("unexpected docvalues type NUMERIC for field 'price' (expected=SORTED). Use UninvertingReader or index with docvalues."));
      // at org.apache.lucene.search.grouping.term.TermFirstPassGroupingCollector.doSetNextReader(TermFirstPassGroupingCollector.java:92)
    }

    List<ImmutablePair<String, Integer>> r1
      = cmd.groupingPairStream("code", null, "*:*", null)
           .collect(Collectors.toCollection(() -> new ArrayList<>()));
    assertThat(r1.get(0).left, is("A01"));
    assertThat(r1.get(0).right, is(6));
    assertThat(r1.get(1).left, is("A02"));
    assertThat(r1.get(1).right, is(4));

    List<ImmutablePair<String, Integer>> r2
      = cmd.groupingPairStream("code", "code desc", null, null)
           .collect(Collectors.toCollection(() -> new ArrayList<>()));
    assertThat(r2.get(0).left, is("A02"));
    assertThat(r2.get(0).right, is(4));
    assertThat(r2.get(1).left, is("A01"));
    assertThat(r2.get(1).right, is(6));
  }

}
