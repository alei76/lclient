package org.apache.lucene.lclient;

import java.io.IOException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.lclient.util.QueryPredicate;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MeasureTest {

  private static final String sep = StandardSystemProperty.FILE_SEPARATOR.value();
  private String dataPath;

  private static final int DOC_SIZE = 1_0000;

  private LSchema schema;

  @Before
  public void setUp() {
    String root = getClass().getResource("/").getFile();
    this.dataPath = root + sep + "MeasureTest" + sep + "data";

    this.schema = TestUtils.getLSchema();
  }

  @Test
  public void test001() throws IOException {
    System.out.println("---------- measure indexing ----------");
        Stopwatch stopwatch = Stopwatch.createStarted();
    LConnection conn = new LConnection(dataPath + sep + "db");
        System.out.println("conn:"+stopwatch);
        stopwatch = Stopwatch.createStarted();
    LCommand cmd = new LCommand(conn, "coll", schema);
        System.out.println("cmd:"+stopwatch);

        stopwatch = Stopwatch.createStarted();
    IntStream.range(0, DOC_SIZE).parallel().forEach(TestUtils.getIndexConsumer(cmd));
        System.out.println("indexing:"+stopwatch);
        stopwatch = Stopwatch.createStarted();
    cmd.refresh();
        System.out.println("refreshing:"+stopwatch);
        stopwatch = Stopwatch.createStarted();
    cmd.commit();
        System.out.println("committing:"+stopwatch);

        stopwatch = Stopwatch.createStarted();
    int count = cmd.count("*:*");
        System.out.println("count():"+stopwatch);
    assertThat(count, is(DOC_SIZE));
        stopwatch = Stopwatch.createStarted();
    conn.close();
        System.out.println("closing:"+stopwatch);
  }

  @Test
  public void test002() throws IOException {
    System.out.println("---------- measure quering ----------");
    LConnection conn = new LConnection(dataPath + sep + "db");
    LCommand cmd = new LCommand(conn, "coll", schema);

        Stopwatch stopwatch = Stopwatch.createStarted();
    Iterable<Document> docsA = cmd.find("*:*");
        System.out.println("find():"+stopwatch);
    assertThat(Iterables.size(docsA), is(DOC_SIZE));

        stopwatch = Stopwatch.createStarted();
    Iterable<Document> docsAA = cmd.find("*:*");
        System.out.println("find() two:"+stopwatch);
    assertThat(Iterables.size(docsAA), is(DOC_SIZE));

        stopwatch = Stopwatch.createStarted();
    Document docs1 = cmd.findOne("*:*");
        System.out.println("findOne():"+stopwatch);
    assertThat(docs1, is(notNullValue()));
    for (IndexableField field : docs1.getFields()) {
      System.out.println("name:"+field.name()+" "+"value:"+field.stringValue());
    }

        stopwatch = Stopwatch.createStarted();
    Iterable<Document> docsB = cmd.filter("*:*");
        System.out.println("filter():"+stopwatch);
    assertThat(Iterables.size(docsB), is(DOC_SIZE));

        stopwatch = Stopwatch.createStarted();
    Iterable<Document> docsBB = cmd.filter("*:*");
        System.out.println("filter() two:"+stopwatch);
    assertThat(Iterables.size(docsBB), is(DOC_SIZE));

        stopwatch = Stopwatch.createStarted();
    Iterable<Document> docsC =
      new LQuery(cmd).find("*:*").filter("*:*").limit(null).toIterable();
        System.out.println("LQuery():"+stopwatch);

        stopwatch = Stopwatch.createStarted();
    assertThat(Iterables.size(docsC), is(DOC_SIZE));
        System.out.println("Iterables.size():"+stopwatch);

        stopwatch = Stopwatch.createStarted();
    cmd.forceMerge();
        System.out.println("forceMerging:"+stopwatch);

        stopwatch = Stopwatch.createStarted();
    conn.close();
        System.out.println("closing:"+stopwatch);
  }

  @Test
  public void test003() throws IOException {
    System.out.println("---------- LQuery test ----------");
    Stopwatch stopwatch = Stopwatch.createStarted();

    LConnection conn = new LConnection(dataPath + sep + "db");
    LCommand cmd = new LCommand(conn, "coll", schema);

    Iterable<Document> docs = new LQuery(cmd)
      .find("*:*")
      .filter("id:345")
      .limit(1)
      .fields("id,count,tag")
      .toIterable();

    for (IndexableField field : Iterables.get(docs, 0).getFields()) {
      System.out.println("name:"+field.name()+" "+"value:"+field.stringValue());
    }

    Document docsF = new LQuery(cmd)
      .find("*:*")
      .filter("id:345")
      .limit(1)
      .fields("id,count,tag")
      .toFluentIterable()
      .filter(Predicates.notNull())
      .get(0);

    for (IndexableField field : docsF.getFields()) {
      System.out.println("name:"+field.name()+" "+"value:"+field.stringValue());
    }

    String id1 = new LQuery(cmd)
      .find("*:*")
      .filter("id:(345 OR 346)")
      .limit(2)
      .fields("id")
      .toFluentIterable()
      .filter(Predicates.notNull())
      .transform(new Function<Document,String>(){
         @Override public String apply(Document doc) {
           return doc.getField("id").stringValue();
         }
      })
      .filter(new QueryPredicate("345"))
      .get(0);

    assertThat(id1, is("345"));

    String id2 = new LQuery(cmd)
      .find("*:*")
      .filter("id:(345 OR 346)")
      .limit(2)
      .fields("id")
      .toStream()
      .parallel()
      .map(doc -> doc.getField("id").stringValue())
      .filter(new QueryPredicate("345"))
      .collect(Collectors.toList())
      .get(0);

    assertThat(id2, is("345"));

    conn.close();

    System.out.println("elapsed:"+stopwatch);
  }

  @Test
  public void test004() throws IOException {
    System.out.println("---------- LQuery Stream test ----------");
        Stopwatch stopwatch = Stopwatch.createStarted();

    LConnection conn = new LConnection(dataPath + sep + "db");
    LCommand cmd = new LCommand(conn, "coll", schema);

        Stopwatch stopwatch1 = Stopwatch.createStarted();
    Iterable<Document> first = new LQuery(cmd)
      .find("*:*")
      .filter("count:[1 TO 800]")
      .fields("price").toIterable();
    double firstSum = 0d;
    for (Document e : first) {
      firstSum = firstSum + e.getField("price").numericValue().doubleValue();
    }
        System.out.println("firstSum1:"+firstSum+" time:"+stopwatch1);

        Stopwatch stopwatch2 = Stopwatch.createStarted();
    Iterable<Double> doubles = new LQuery(cmd)
      .find("*:*")
      .filter("count:[1 TO 800]")
      .fields("price").toFluentIterable()
      .transform(new Function<Document,Double>(){
        @Override public Double apply(Document doc) {
          return doc.getField("price").numericValue().doubleValue();
        }
      });
    double dSum = 0d;
    for (Double d : doubles) {
      dSum = dSum + d;
    }
        System.out.println("firstSumF:"+dSum+" time:"+stopwatch2);

        Stopwatch stopwatch3 = Stopwatch.createStarted();
    double secondSum1 = StreamSupport.stream(
      /*--spliterator--*/
      Spliterators.spliterator(
        /*iterator*/new LQuery(cmd) 
                      .find("*:*")
                      .filter("count:[1 TO 800]")
                      .fields("price").toIterable().iterator(),
        /*size*/ DOC_SIZE,
        /*characteristics*/ Spliterator.SIZED),
      /*--parallel--*/ true)
      .mapToDouble(doc -> doc.getField("price").numericValue().doubleValue())
      .sum();
        System.out.println("secondSum1:"+secondSum1+" time:"+stopwatch3);

        Stopwatch stopwatch4 = Stopwatch.createStarted();
    double secondSum2 = StreamSupport.stream(
      /*--spliterator--*/
      new LQuery(cmd) 
        .find("*:*")
        .filter("count:[1 TO 800]")
        .fields("price").toIterable().spliterator(),
      /*--parallel--*/ true)
      .mapToDouble(doc -> doc.getField("price").numericValue().doubleValue())
      .sum();
        System.out.println("secondSum2:"+secondSum2+" time:"+stopwatch4);

        Stopwatch stopwatch5 = Stopwatch.createStarted();
    double secondSum3 =
      new LQuery(cmd) 
        .find("*:*")
        .filter("count:[1 TO 800]")
        .fields("price")
        .toStream()
        .parallel()
        .mapToDouble(doc -> doc.getField("price").numericValue().doubleValue())
        .sum();
        System.out.println("secondSum3:"+secondSum3+" time:"+stopwatch5);

    conn.close();

    System.out.println("elapsed:"+stopwatch);
  }

}
