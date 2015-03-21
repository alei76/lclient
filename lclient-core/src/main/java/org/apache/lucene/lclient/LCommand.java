package org.apache.lucene.lclient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.grouping.GroupingSearch;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.lucene.util.BytesRef;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class LCommand {

  private LConnection connection;
  private String name;
  private LSchema schema;

  private Directory directory;
  private IndexWriter writer; 
  private DirectoryReader reader;
  private IndexSearcher searcher;

  private Map<String,UninvertingReader.Type> mapping = Maps.newHashMap();

  private static final int MAX_LIMIT = 100_0000;

  public LCommand(LConnection connection, String collectionName, LSchema schema) throws IOException {
    this.connection = Preconditions.checkNotNull(connection);
    this.name = Preconditions.checkNotNull(collectionName);
    this.schema = Preconditions.checkNotNull(schema);

    directory = connection.getDirectory(name);
    writer = connection.getIndexWriter(name, schema);

    mapping.put(schema.getUniqueKey(), UninvertingReader.Type.SORTED);

    fresh();

    BooleanQuery.setMaxClauseCount(MAX_LIMIT);
  }

  public void commit() throws IOException {
    writer.commit();
    fresh();
  }

  public void forceMerge() throws IOException {
    writer.forceMerge(1);
    fresh();
  }

  private void fresh() throws IOException {
    DirectoryReader directoryReader = DirectoryReader.open(directory);
    reader = UninvertingReader.wrap(directoryReader, mapping);
    searcher = new IndexSearcher(reader);
    connection.setIndexReader(name, reader);
  }

  public void refresh() throws IOException {
    DirectoryReader directoryReader = DirectoryReader.openIfChanged(reader, writer, true);
    if (directoryReader != null) {
      reader = UninvertingReader.wrap(directoryReader, mapping);
      searcher = new IndexSearcher(reader);
      connection.setIndexReader(name, reader);
    }
  }

  public void update(LDocument document) throws IOException {
    writer = connection.getIndexWriter(name, schema);
    writer.updateDocument(document.uniqueKey(), document.document());
  }

  public void remove(String id) throws IOException {
    writer = connection.getIndexWriter(name, schema);
    writer.deleteDocuments(new Term(schema.getUniqueKey(), id));
  }

  public void removeByQuery(String query) throws IOException {
    writer = connection.getIndexWriter(name, schema);
    writer.deleteDocuments(query(query));
  }

  public int count(String query) throws IOException {
    TopFieldDocs results = search(filteredQuery(query, null), null, null);
    return results.totalHits;
  }

  public List<Document> find(String query) throws IOException {
    return find(query, null, null, null, null);
  }

  public Document findOne(String query) throws IOException {
    List<Document> results = find(query, null, 1, null, null);
    return results.get(0);
  }

  public List<Document> filter(String filterQuery) throws IOException {
    return find(null, filterQuery, null, null, null);
  }

  public List<Document> find(String query, String filterQuery, Integer limit, String sort, String fields) throws IOException {
    return stream(query, filterQuery, limit, sort, fields)
           .collect(Collectors.toCollection(() -> new ArrayList<>()));
  }

  public Stream<Document> stream(String query, String filterQuery, Integer limit, String sort, String fields) throws IOException {
    TopFieldDocs results = search(filteredQuery(query, filterQuery), limit, sort);
    return Arrays.stream(results.scoreDocs).map(scoreDoc -> getDoc(scoreDoc, fields));
  }

  public List<Document> JoinFrom(String query, String filterQuery, Integer limit, String sort, String fields, LCommand fromCommand, String fromField, String toField, String fromQuery, String fromFilterQuery) throws IOException {
    return join(query, filterQuery, limit, sort, fields, fromCommand, fromField, toField, fromQuery, fromFilterQuery)
           .collect(Collectors.toCollection(() -> new ArrayList<>()));
  }

  public Stream<Document> join(String query, String filterQuery, Integer limit, String sort, String fields, LCommand fromCommand, String fromField, String toField, String fromQuery, String fromFilterQuery) throws IOException {
    Query joinQuery = joinQuery(fromCommand, fromField, toField, fromQuery, fromFilterQuery);
    Filter joinFilter = new QueryWrapperFilter(joinQuery);
    TopFieldDocs results = search(filteredQuery(query, filterQuery, joinFilter), limit, sort);
    return Arrays.stream(results.scoreDocs).map(scoreDoc -> getDoc(scoreDoc, fields));
  }

  public List<String> groupingField(String groupField, String groupFieldSort, String query, String filterQuery) throws IOException {
    return grouping(groupField, groupFieldSort, query, filterQuery)
           .collect(Collectors.toCollection(() -> new ArrayList<>()));
  }

  public Stream<String> grouping(String groupField, String groupFieldSort, String query, String filterQuery) throws IOException {
    TopGroups<BytesRef> result = groupingSearch(groupField, groupFieldSort, query, filterQuery);
    return Arrays.stream(result.groups).map(group -> group.groupValue.utf8ToString());
  }

  private TopGroups<BytesRef> groupingSearch(String groupField, String groupFieldSort, String query, String filterQuery) throws IOException {
    GroupingSearch groupingSearch = new GroupingSearch(groupField);
    groupingSearch.setAllGroupHeads(false);
    groupingSearch.setAllGroups(false);
    groupingSearch.setCaching(MAX_LIMIT, /* cacheScores */ false);
    groupingSearch.setFillSortFields(false);
    groupingSearch.setGroupDocsLimit(1);
    groupingSearch.setGroupDocsOffset(0);
    String gSort = MoreObjects.firstNonNull(groupFieldSort, groupField + " asc");
    groupingSearch.setGroupSort(sort(gSort));
    groupingSearch.setIncludeMaxScore(false);
    groupingSearch.setIncludeScores(false);
    String sortWithinGroup = null;
    String sortWG = MoreObjects.firstNonNull(sortWithinGroup, schema.getUniqueKey() + " asc");
    groupingSearch.setSortWithinGroup(sort(sortWG));
    return groupingSearch.search(searcher, filteredQuery(query, filterQuery), 0, MAX_LIMIT);
  }

  IndexSearcher searcher() {
    return searcher;
  }

  public LSchema schema() {
    return schema;
  }

  private Document getDoc(ScoreDoc scoreDoc, String fields) {
    Document doc = null;
    try {
      if (fields == null) {
        doc = searcher.doc(scoreDoc.doc);
      } else {
        doc = searcher.doc(scoreDoc.doc, fieldsToLoad(fields));
      }
    } catch (IOException e) { /* ignore */ }
    return doc;
  }

  private Set<String> fieldsToLoad(String fields) {
    return Sets.newHashSet(Splitter.on(",").trimResults().omitEmptyStrings().split(fields));
  }

  private TopFieldDocs search(Query filteredQuery, Integer limit, String sort) throws IOException {
    TopFieldDocs results =
      searcher.search(/*query */                     filteredQuery,
                      /*filter*/                              null, // LUCENE-6286
                      /*n     */                      limit(limit),
                      /*sort  */                        sort(sort),
                      /*scores*/                              true,
                      /*maxscr*/                             false);
    return results;
  }

  private Query filteredQuery(String query, String filterQuery) {
    return filteredQuery(query, filterQuery, null);
  }

  private Query filteredQuery(String query, String filterQuery, Filter filter) {
    BooleanFilter boolFilter = new BooleanFilter();
    boolFilter.add(filterQuery(filterQuery), BooleanClause.Occur.MUST);
    if (filter != null) {
      boolFilter.add(filter, BooleanClause.Occur.MUST);
    }
    return new FilteredQuery(query(query), boolFilter);
  }

  private Query query(String queryString) {
    String q = MoreObjects.firstNonNull(queryString, "*:*");
    StandardQueryParser parser = new StandardQueryParser();
    parser.setAnalyzer(schema.getQueryAnalyzer());
    parser.setNumericConfigMap(schema.getNumericConfigMap());
    Query query = null;
    try {
      query = parser.parse(q, schema.getDefaultField());
    } catch (QueryNodeException e) { /* ignore */ }
    return query;
  }

  private Filter filterQuery(String filterQuery) {
    Query query = query(filterQuery);
    return new CachingWrapperFilter(new QueryWrapperFilter(query)); // LUCENE-6303
  }

  private Query joinQuery(LCommand fromCommand, String fromField, String toField, String fromQuery, String fromFilterQuery) throws IOException {
    return JoinUtil.createJoinQuery(/*fromField    */MoreObjects.firstNonNull(fromField, fromCommand.schema().getUniqueKey()),
                                    /*multipleValuesPerDocument*/false,
                                    /*toField      */MoreObjects.firstNonNull(toField, schema.getUniqueKey()),
                                    /*fromQuery    */filteredQuery(fromQuery, fromFilterQuery),
                                    /*fromSearcher */fromCommand.searcher(),
                                    /*scoreMode    */ScoreMode.None);
  }

  private Integer limit(Integer limit) {
    return MoreObjects.firstNonNull(limit, MAX_LIMIT);
  }

  private Sort sort(String sortFieldOrders) {
    Sort sort = new Sort();
    if (sortFieldOrders == null) {
      return sort;
    }
    List<SortField> sortFieldList = Lists.newArrayList();
    boolean reverse = false;
    List<String> fieldOrders = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(sortFieldOrders);
    for (String fieldOrder : fieldOrders) {
      List<String> fieldOrderList = Splitter.on(" ").trimResults().omitEmptyStrings().splitToList(fieldOrder);
      String field = fieldOrderList.get(0);
      String order = fieldOrderList.get(1);
      if (order.equals("desc")) reverse = true;
      SortField sField = null;
      if (schema.getFieldType(field).equals(LDataType.TEXT)) {
        sField = new SortField(field, SortField.Type.STRING_VAL, reverse);
        sortFieldList.add(sField); continue;
      }
      if (schema.getFieldType(field).equals(LDataType.DOUBLE)) {
        sField = new SortField(field, SortField.Type.DOUBLE, reverse);
        sortFieldList.add(sField); continue;
      }
      if (schema.getFieldType(field).equals(LDataType.FLOAT)) {
        sField = new SortField(field, SortField.Type.FLOAT, reverse);
        sortFieldList.add(sField); continue;
      }
      if (schema.getFieldType(field).equals(LDataType.INT)) {
        sField = new SortField(field, SortField.Type.INT, reverse);
        sortFieldList.add(sField); continue;
      }
      if (schema.getFieldType(field).equals(LDataType.LONG)) {
        sField = new SortField(field, SortField.Type.LONG, reverse);
        sortFieldList.add(sField); continue;
      }
      if (schema.getFieldType(field).equals(LDataType.STRING)) {
        sField = new SortField(field, SortField.Type.STRING, reverse);
        sortFieldList.add(sField); continue;
      }
      Preconditions.checkNotNull(sField);
    }
    SortField [] sortFieldArray = sortFieldList.toArray(new SortField[sortFieldList.size()]);
    sort.setSort(sortFieldArray);
    return sort;
  }

}
