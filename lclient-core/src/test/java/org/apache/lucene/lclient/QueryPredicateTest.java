package org.apache.lucene.lclient;

import java.util.List;

import org.apache.lucene.lclient.util.QueryPredicate;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class QueryPredicateTest {

  private QueryPredicate queryPredicate;

  @Before
  public void setUp() {
    queryPredicate = new QueryPredicate();
  }

  @Test
  public void test001() {
    List<String> list = Lists.newArrayList("1", "2", "3", "10");
    FluentIterable<String> filtered
      = FluentIterable.from(list).filter(queryPredicate.withQuery("1 OR 3"));

    assertThat(Joiner.on(",").skipNulls().join(filtered), is("1,3"));
  }

  @Test
  public void test002() {
    List<String> list = Lists.newArrayList("1", "2", "3", "10");
    FluentIterable<String> filtered
      = FluentIterable.from(list).filter(queryPredicate);

    assertThat(Joiner.on(",").skipNulls().join(filtered), is("1,2,3,10"));
  }

  @Test
  public void test003() {
    List<String> list = Lists.newArrayList("Aaa", "Bbb", "ccC", "ddD");
    FluentIterable<String> filtered
      = FluentIterable.from(list).filter(queryPredicate.withQuery("aaa ddd"));

    assertThat(Joiner.on(",").skipNulls().join(filtered), is("Aaa,ddD"));
  }

  @Test
  public void test004() {
    List<String> list = Lists.newArrayList("/home/u1", "/home/u1/a", "/home/u2", "/home/u2/b");
    FluentIterable<String> filtered
      = FluentIterable.from(list).filter(queryPredicate.withQuery("\"/home/u2\""));

    assertThat(Joiner.on(",").skipNulls().join(filtered), is("/home/u2,/home/u2/b"));
  }

  @Test
  public void test005() {
    List<String> list = Lists.newArrayList("userA1", "userA2", "userB1", "userB2");
    FluentIterable<String> filtered
      = FluentIterable.from(list).filter(new QueryPredicate("userA*"));

    assertThat(Joiner.on(",").skipNulls().join(filtered), is("userA1,userA2"));
  }

  @Test
  public void test006() {
    List<String> list = Lists.newArrayList("300", "1000", "800", "200");
    FluentIterable<String> filtered
      = FluentIterable.from(list).filter(new QueryPredicate("[200 TO 300]"));

    assertThat(Joiner.on(",").skipNulls().join(filtered), is("300,200"));
  }

}
