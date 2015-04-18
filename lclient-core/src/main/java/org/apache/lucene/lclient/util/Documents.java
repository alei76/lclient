package org.apache.lucene.lclient.util;

import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.lclient.LDataType;
import org.apache.lucene.lclient.LSchema;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class Documents {

  private Documents() { }

  public static Map<String,Object> toMap(LSchema schema, Document document) {
    Map<String,Object> result = Maps.newHashMap();
    for (IndexableField field : document) {
      String key = field.name();
      Object value = null;
      if (schema.getFieldType(key).equals(LDataType.TEXT)) {
        value = field.stringValue();
      }
      if (schema.getFieldType(key).equals(LDataType.DOUBLE)) {
        value = field.numericValue().doubleValue();
      }
      if (schema.getFieldType(key).equals(LDataType.FLOAT)) {
        value = field.numericValue().floatValue();
      }
      if (schema.getFieldType(key).equals(LDataType.INT)) {
        value = field.numericValue().intValue();
      }
      if (schema.getFieldType(key).equals(LDataType.LONG)) {
        value = field.numericValue().longValue();
      }
      if (schema.getFieldType(key).equals(LDataType.STRING)) {
        value = field.stringValue();
      }
      Preconditions.checkNotNull(value);
      result.put(key, value);
    }
    return result;
  }

}
