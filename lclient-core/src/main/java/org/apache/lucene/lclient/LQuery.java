package org.apache.lucene.lclient;

import java.io.IOException;
import java.util.stream.Stream;

import org.apache.lucene.document.Document;

import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;

public class LQuery {

  private LCommand command;

  private String query;
  private String filterQuery;
  private Integer limit;
  private String sort;
  private String fields;

  private LCommand fromCommand;
  private String fromField;
  private String toField;
  private String fromQuery;
  private String fromFilterQuery;

  public LQuery(LCommand command) {
    this.command = command;
  }

  public LQuery find(String query) {
    this.query = query;
    return this;
  }

  public LQuery filter(String filterQuery) {
    this.filterQuery = filterQuery;
    return this;
  }

  public LQuery limit(Integer limit) {
    this.limit = limit;
    return this;
  }

  public LQuery sort(String sort) {
    this.sort = sort;
    return this;
  }

  public LQuery fields(String fields) {
    this.fields = fields;
    return this;
  }

  public LQuery join(LCommand fromCommand, String fromField) {
    this.fromCommand = fromCommand;
    this.fromField = fromField;
    return this;
  }

  public LQuery toField(String toField) {
    this.toField = toField;
    return this;
  }

  public LQuery fromQuery(String fromQuery) {
    this.fromQuery = fromQuery;
    return this;
  }

  public LQuery fromFilter(String fromFilterQuery) {
    this.fromFilterQuery = fromFilterQuery;
    return this;
  }

  public Iterable<Document> toIterable() throws IOException {
    Preconditions.checkNotNull(command);
    if ((fromCommand != null) && (fromField != null)) {
      return command.JoinFrom(query, filterQuery, limit, sort, fields, fromCommand, fromField, toField, fromQuery, fromFilterQuery);
    } else {
      return command.find(query, filterQuery, limit, sort, fields);
    }
  }

  public FluentIterable<Document> toFluentIterable() throws IOException {
    return FluentIterable.from(toIterable());
  }

  public Stream<Document> toStream() throws IOException {
    Preconditions.checkNotNull(command);
    if ((fromCommand != null) && (fromField != null)) {
      return command.join(query, filterQuery, limit, sort, fields, fromCommand, fromField, toField, fromQuery, fromFilterQuery);
    } else {
      return command.stream(query, filterQuery, limit, sort, fields);
    }
  }

}
