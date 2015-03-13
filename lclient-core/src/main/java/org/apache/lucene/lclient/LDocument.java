package org.apache.lucene.lclient;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class LDocument {

  private LSchema schema;
  private Term uniqueKey;
  private Document doc;

  public LDocument(LSchema schema) throws IOException {
    Map<String, Object> emptyDocument = Maps.newHashMap();
    init(schema, emptyDocument);
  }

  public LDocument(LSchema schema, Map<String, Object> document) throws IOException {
    init(schema, document);
  }

  private void init(LSchema schema, Map<String, Object> document) throws IOException {
    this.schema = schema;
    this.doc = new Document();

    for (Map.Entry<String, Object> entry : document.entrySet()) {
      append(entry.getKey(), entry.getValue());
    }
  }

  public LDocument append(String name, Object value) throws IOException {
    append(name, value, schema.getFieldType(name));
    return this;
  }

  private LDocument append(String name, Object value, FieldType dataType) throws IOException {
    if (name.equals(schema.getUniqueKey())) {
      this.uniqueKey = new Term(name, value.toString());
    }
    if (dataType.equals(LDataType.TEXT)) {
      appendText(name, value.toString());
      return this;
    }
    Field f = null;
    Field fdv = null;
    if (dataType.equals(LDataType.DOUBLE)) {
      f = new DoubleField(name, Double.parseDouble(value.toString()), dataType);
      fdv = new NumericDocValuesField(name, Double.doubleToLongBits(f.numericValue().doubleValue()));
    }
    if (dataType.equals(LDataType.FLOAT)) {
      f = new FloatField(name, Float.parseFloat(value.toString()), dataType);
      fdv = new NumericDocValuesField(name, Float.floatToIntBits(f.numericValue().floatValue()));
    }
    if (dataType.equals(LDataType.INT)) {
      f = new IntField(name, Integer.parseInt(value.toString()), dataType);
      fdv = new NumericDocValuesField(name, f.numericValue().longValue());
    }
    if (dataType.equals(LDataType.LONG)) {
      f = new LongField(name, Long.parseLong(value.toString()), dataType);
      fdv = new NumericDocValuesField(name, f.numericValue().longValue());
    }
    if (dataType.equals(LDataType.STRING)) {
      f = new Field(name, value.toString(), dataType);
      fdv = new SortedDocValuesField(name, new BytesRef(value.toString()));
    }
    doc.add(Preconditions.checkNotNull(f));
    if (!name.equals(schema.getUniqueKey())) {
      doc.add(Preconditions.checkNotNull(fdv));
    }
    return this;
  }

  private LDocument appendText(String name, String text) throws IOException {
    Field f = new Field(name, text, LDataType.TEXT_STORED);
    Field fdv = new BinaryDocValuesField(name, new BytesRef(text));
    doc.add(f);
    doc.add(fdv);
    Field textf = new Field(name, text, LDataType.TEXT_NOT_STORED);
    doc.add(textf);
    return this;
  }

  Term uniqueKey() {
    return uniqueKey;
  }

  Document document() {
    return doc;
  }

}
