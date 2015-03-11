package org.apache.lucene.lclient;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

public class LDataType {

  public static final FieldType DOUBLE = new FieldType();
  static {
    DOUBLE.setTokenized(true);
    DOUBLE.setOmitNorms(true);
    DOUBLE.setIndexOptions(IndexOptions.DOCS);
    DOUBLE.setNumericType(FieldType.NumericType.DOUBLE);
    DOUBLE.setNumericPrecisionStep(4);
    DOUBLE.setStored(true);
    DOUBLE.freeze();
  }

  public static final FieldType FLOAT = new FieldType();
  static {
    FLOAT.setTokenized(true);
    FLOAT.setOmitNorms(true);
    FLOAT.setIndexOptions(IndexOptions.DOCS);
    FLOAT.setNumericType(FieldType.NumericType.FLOAT);
    FLOAT.setNumericPrecisionStep(4);
    FLOAT.setStored(true);
    FLOAT.freeze();
  }

  public static final FieldType INT = new FieldType();
  static {
    INT.setTokenized(true);
    INT.setOmitNorms(true);
    INT.setIndexOptions(IndexOptions.DOCS);
    INT.setNumericType(FieldType.NumericType.INT);
    INT.setNumericPrecisionStep(4);
    INT.setStored(true);
    INT.freeze();
  }

  public static final FieldType LONG = new FieldType();
  static {
    LONG.setTokenized(true);
    LONG.setOmitNorms(true);
    LONG.setIndexOptions(IndexOptions.DOCS);
    LONG.setNumericType(FieldType.NumericType.LONG);
    LONG.setNumericPrecisionStep(4);
    LONG.setStored(true);
    LONG.freeze();
  }

  public static final FieldType STRING = new FieldType();
  static {
    STRING.setTokenized(false);
    STRING.setOmitNorms(true);
    STRING.setIndexOptions(IndexOptions.DOCS);
    STRING.setStored(true);
    STRING.freeze();
  }

  public static final FieldType TEXT = new FieldType();

//------------------------------------------------------------< text >--

  static final FieldType TEXT_STORED = new FieldType();
  static {
    TEXT_STORED.setStored(true);
    TEXT_STORED.freeze();
  }

  static final FieldType TEXT_NOT_STORED = new FieldType();
  static {
    TEXT_NOT_STORED.setTokenized(true);
    TEXT_NOT_STORED.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    TEXT_NOT_STORED.setStored(false);
    TEXT_NOT_STORED.setStoreTermVectors(true);
    TEXT_NOT_STORED.setStoreTermVectorPositions(true);
    TEXT_NOT_STORED.setStoreTermVectorOffsets(true);
    TEXT_NOT_STORED.freeze();
  }

}
