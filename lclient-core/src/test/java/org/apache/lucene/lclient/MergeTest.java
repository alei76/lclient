package org.apache.lucene.lclient;
import java.io.IOException;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.common.base.StandardSystemProperty;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MergeTest {

  private static final String sep = StandardSystemProperty.FILE_SEPARATOR.value();
  private String dataPath;
  private LSchema schema;

  @Before
  public void setUp() {
    String root = getClass().getResource("/").getFile();
    this.dataPath = root + sep + "MergeTest" + sep + "data";

    this.schema = TestUtils.getLSchema();

  }

  @Test
  public void test001() throws IOException {
    insertDocs("n9commit", 9, false); // 0-8 -> not merged
  }

  @Test
  public void test002() throws IOException {
    insertDocs("n9refresh", 9, true); // 0-8 -> not merged
  }

  @Test
  public void test003() throws IOException {
    insertDocs("n10commit", 10, false); // segment > 10 -> merged
  }

  @Test
  public void test004() throws IOException {
    insertDocs("n10refresh", 10, true); // segment > 10 -> *not* merged
  }

  @Test
  public void test005() throws IOException {
    insertDocs("n10refreshcommit", 10, true); // segment > 10 -> *not* merged
    LConnection conn = new LConnection(dataPath + sep + "db");
    LCommand cmd = new LCommand(conn, "n10refreshcommit", schema);
    assertThat(cmd.count("*:*"), is(10));
    conn.close(); // by CommitOnClose, segment > 10 -> merged
  }

  private void insertDocs(String collName, int n, boolean refresh) throws IOException {
    LConnection conn = new LConnection(dataPath + sep + "db");
    LCommand cmd = new LCommand(conn, collName, schema);
    IntStream.range(0, n).forEach(id -> {
      try {
        cmd.update(new LDocument(schema).append("id", id));
        if (refresh) {
          cmd.refresh();
        } else {
          cmd.commit();
        }
      } catch (IOException e) { }
    }
    );
    conn.close();
  }

}
