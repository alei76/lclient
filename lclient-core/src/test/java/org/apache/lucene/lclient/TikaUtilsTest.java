package org.apache.lucene.lclient;

import java.io.File;
import java.io.IOException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.lucene.lclient.util.TikaUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TikaUtilsTest {

  @Before
  public void setUp() {

  }

  @Test
  public void test001() throws IOException, TikaException {
    String path = getClass().getResource("/test-documents/testEXCEL.xlsx").getPath();
    File file = new File(path);
    ImmutablePair<String, Metadata> result = TikaUtils.parseToString(file);
    assertThat(result.left, containsString("123"));
    assertThat(result.left, containsString("abc"));
    assertThat(result.right.toString(), containsString("Content-Type"));
    assertThat(result.right.toString(), containsString("Content-Length"));
    assertThat(result.right.toString(), containsString("resourceName"));
    String mimetype = TikaUtils.detect(file);
    assertThat(mimetype, is("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"));
  }

  @Test
  public void test002() throws IOException, TikaException {
    String path = getClass().getResource("/test-documents/testPDF.pdf").getPath();
    File file = new File(path);
    ImmutablePair<String, Metadata> result = TikaUtils.parseToString(file);
    assertThat(result.left, containsString("pdf-test"));
    assertThat(result.right.toString(), containsString("Content-Type"));
    assertThat(result.right.toString(), containsString("Content-Length"));
    assertThat(result.right.toString(), containsString("resourceName"));
    assertThat(result.right.toString(), containsString("Last-Modified"));
    assertThat(result.right.toString(), containsString("title"));
    String mimetype = TikaUtils.detect(file);
    assertThat(mimetype, is("application/pdf"));
  }

}
