package org.apache.lucene.lclient.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

public class QueryPredicate implements com.google.common.base.Predicate<CharSequence>, java.util.function.Predicate<CharSequence>{

  private static final String DF = "text";
  private Analyzer analyzer = new StandardAnalyzer();
  private Query query = new MatchAllDocsQuery();

  public QueryPredicate() { }

  public QueryPredicate(String query) {
    withQuery(query);
  }

  public QueryPredicate(Analyzer analyzer, String query) {
    withAnalyzer(analyzer);
    withQuery(query);
  }

  public QueryPredicate withAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
    return this;
  }

  public QueryPredicate withQuery(String query) {
    StandardQueryParser parser = new StandardQueryParser();
    parser.setAnalyzer(analyzer);
    // TODO: NumericConfig
    try {
      this.query = parser.parse(query, DF);
    } catch (QueryNodeException e) { /* ignore */ }
    return this;
  }

  @Override
  public boolean apply(CharSequence input) {
    return isMatchingWithQuery(input);
  }

  @Override
  public boolean test(CharSequence t) {
    return isMatchingWithQuery(t);
  }

  private boolean isMatchingWithQuery(CharSequence charSequence) {
    MemoryIndex index = new MemoryIndex();
    index.addField(DF, charSequence.toString(), analyzer);
    index.freeze();
    return (index.search(query) > 0.0f);
  }

}
