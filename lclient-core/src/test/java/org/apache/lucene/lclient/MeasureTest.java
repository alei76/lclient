package org.apache.lucene.lclient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.lclient.util.QueryPredicate;
import org.apache.lucene.search.ScoreDoc;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Stopwatch;
import com.google.common.collect.FluentIterable;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MeasureTest {

  private static final String sep = StandardSystemProperty.FILE_SEPARATOR.value();
  private String dataPath;

  private static final int DOC_SIZE = 1_000;

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
    List<Document> docsA = cmd.find("*:*");
        System.out.println("find():"+stopwatch);
    assertThat(docsA.size(), is(DOC_SIZE));

        stopwatch = Stopwatch.createStarted();
    List<Document> docsAA = cmd.find("*:*");
        System.out.println("find() two:"+stopwatch);
    assertThat(docsAA.size(), is(DOC_SIZE));

        stopwatch = Stopwatch.createStarted();
    Document docs1 = cmd.findOne("*:*");
        System.out.println("findOne():"+stopwatch);
    assertThat(docs1, is(notNullValue()));
    for (IndexableField field : docs1.getFields()) {
      System.out.println("name:"+field.name()+" "+"value:"+field.stringValue());
    }

        stopwatch = Stopwatch.createStarted();
    List<Document> docsB = cmd.filter("*:*");
        System.out.println("filter():"+stopwatch);
    assertThat(docsB.size(), is(DOC_SIZE));

        stopwatch = Stopwatch.createStarted();
    List<Document> docsBB = cmd.filter("*:*");
        System.out.println("filter() two:"+stopwatch);
    assertThat(docsBB.size(), is(DOC_SIZE));

        stopwatch = Stopwatch.createStarted();
    List<Document> docsC =
      new LQuery(cmd).find("*:*").filter("*:*").limit(null).toList();
        System.out.println("LQuery():"+stopwatch);

        stopwatch = Stopwatch.createStarted();
    assertThat(docsC.size(), is(DOC_SIZE));
        System.out.println("List.size():"+stopwatch);

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

    List<Document> docs = new LQuery(cmd)
      .find("*:*")
      .filter("id:345")
      .limit(1)
      .fields("id,count,tag")
      .toList();

    for (IndexableField field : docs.get(0).getFields()) {
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
      .toDocumentStream()
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
    List<Document> first = new LQuery(cmd)
      .find("*:*")
      .filter("count:[1 TO 800]")
      .fields("price").toList();
    double firstSum = 0d;
    for (Document e : first) {
      firstSum = firstSum + e.getField("price").numericValue().doubleValue();
    }
        System.out.println("firstSum1:"+firstSum+" time:"+stopwatch1);

        Stopwatch stopwatch2 = Stopwatch.createStarted();
    FluentIterable<Double> doubles = new LQuery(cmd)
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
                      .fields("price").toList().iterator(),
        /*size*/ DOC_SIZE,
        /*characteristics*/ Spliterator.SIZED),
      /*--parallel--*/ false)
      .mapToDouble(doc -> doc.getField("price").numericValue().doubleValue())
      .reduce(0, Double::sum);
        System.out.println("secondSum1:"+secondSum1+" time:"+stopwatch3);

        Stopwatch stopwatch4 = Stopwatch.createStarted();
    double secondSum2 = StreamSupport.stream(
      /*--spliterator--*/
      new LQuery(cmd) 
        .find("*:*")
        .filter("count:[1 TO 800]")
        .fields("price").toList().spliterator(),
      /*--parallel--*/ false)
      .mapToDouble(doc -> doc.getField("price").numericValue().doubleValue())
      .reduce(0, Double::sum);
        System.out.println("secondSum2:"+secondSum2+" time:"+stopwatch4);

        Stopwatch stopwatch5 = Stopwatch.createStarted();
    double secondSum3 =
      new LQuery(cmd) 
        .find("*:*")
        .filter("count:[1 TO 800]")
        .fields("price")
        .toList()
        .stream()
        //.parallel()
        .mapToDouble(doc -> doc.getField("price").numericValue().doubleValue())
        .reduce(0, Double::sum);
        System.out.println("secondSum3:"+secondSum3+" time:"+stopwatch5);
 
        Stopwatch stopwatch6 = Stopwatch.createStarted();
    double secondSum4 =
      new LQuery(cmd) 
        .find("*:*")
        .filter("count:[1 TO 800]")
        .fields("price")
        .toDocumentStream()
        //.parallel()
        .mapToDouble(doc -> doc.getField("price").numericValue().doubleValue())
        .reduce(0, Double::sum);
        System.out.println("secondSum4:"+secondSum4+" time:"+stopwatch6);

    conn.close();

    System.out.println("elapsed:"+stopwatch);
  }

  @Test
  public void test005() throws IOException {
    System.out.println("---------- Grouping test ----------");
        Stopwatch stopwatch = Stopwatch.createStarted();

    LConnection conn = new LConnection(dataPath + sep + "db");
    LCommand cmd = new LCommand(conn, "coll", schema);

       Stopwatch stopwatch1 = Stopwatch.createStarted();
    cmd.groupingStream("id", "id asc", "*:*", "*:*");
       System.out.println("group by id:"+stopwatch1);

       Stopwatch stopwatch2 = Stopwatch.createStarted();
    cmd.groupingStream("tag", "tag asc", "*:*", "*:*");
       System.out.println("group by tag:"+stopwatch2);

    conn.close();

    System.out.println("elapsed:"+stopwatch);
  }

  @Test
  public void test006() throws IOException {
    System.out.println("---------- Scan test(standard) ----------");

    LConnection conn = new LConnection(dataPath + sep + "db");
    LCommand cmd = new LCommand(conn, "coll", schema);

       Stopwatch stopwatch1 = Stopwatch.createStarted();
    List<ImmutablePair<ScoreDoc,Document>> docs =
      cmd.documentPairStream("*:*", "count:[1 TO 9000]", DOC_SIZE, "id asc", "id")
         .collect(Collectors.toCollection(() -> new ArrayList<>()));
    int hits = docs.size();
       System.out.println("elapsed:"+stopwatch1);
       System.out.println("hits:"+hits);

    System.out.println("---------- Scan test(deep paging) ----------");
       Stopwatch stopwatch2 = Stopwatch.createStarted();
    int resultSize = 0;
    ScoreDoc bottom = null;
    while (true) {
      List<ImmutableTriple<ScoreDoc,Document,ScoreDoc>> list =
        cmd.documentTripleStream(bottom, "*:*", "count:[1 TO 9000]", 100, "id asc", "id")
           .collect(Collectors.toCollection(() -> new ArrayList<>()));
      int listSize = list.size();
      resultSize = resultSize + listSize;
      if (listSize == 0 || list.get(0).right == null) break;
      bottom = list.get(0).right;
    }
       System.out.println("elapsed:"+stopwatch2);
    assertThat(resultSize, is(hits));

    conn.close();
  }
}
