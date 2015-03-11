package org.apache.lucene.lclient;

import java.text.NumberFormat;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.queryparser.flexible.standard.config.NumericConfig;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public final class LSchema {

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getNumberInstance(Locale.ROOT);

  private String defaultField;
  private String uniqueKey;
  private Map<String,FieldType> fieldMap = Maps.newHashMap();
  private Analyzer indexAnalyzer;
  private Analyzer queryAnalyzer;
  private Map<String,NumericConfig> numericConfigMap = Maps.newHashMap();

              LSchema(String defaultField,
                     String uniqueKey,
                     Map<String,FieldType> fieldMap,
                     Analyzer indexAnalyzer,
                     Analyzer queryAnalyzer,
                     Map<String,NumericConfig> numericConfigMap) {
    this.defaultField = defaultField;
    this.uniqueKey = uniqueKey;
    this.fieldMap = fieldMap;
    this.indexAnalyzer = indexAnalyzer;
    this.queryAnalyzer = queryAnalyzer;
    this.numericConfigMap = numericConfigMap;
  }

  public static Builder Builder() {
    return new Builder();
  }

  public String getDefaultField() {
    return defaultField;
  }

  public String getUniqueKey() {
    return Preconditions.checkNotNull(uniqueKey);
  }

  public FieldType getFieldType(String name) {
    return Preconditions.checkNotNull(fieldMap.get(name));
  }

  public Analyzer getIndexAnalyzer() {
    return indexAnalyzer;
  }

  public Analyzer getQueryAnalyzer() {
    return queryAnalyzer;
  }

  public Map<String,NumericConfig> getNumericConfigMap() {
    return numericConfigMap;
  }

  public static final class Builder {

    private Analyzer defaultIndexAnalyzer = new KeywordAnalyzer();
    private Analyzer defaultQueryAnalyzer = new KeywordAnalyzer();
    private String defaultField = "text";
    private String uniqueKey;
    private Map<String,Analyzer> fieldIndexAnalyzers = Maps.newHashMap();
    private Map<String,Analyzer> fieldQueryAnalyzers = Maps.newHashMap();
    private Map<String,FieldType> fieldMap = Maps.newHashMap();
    private Map<String,NumericConfig> numericConfigMap = Maps.newHashMap();

    public Builder() { }

    public Builder setDefaultIndexAnalyzer(Analyzer analyzer) {
      defaultIndexAnalyzer = analyzer;
      return this;
    }

    public Builder setDefaultQueryAnalyzer(Analyzer analyzer) {
      defaultQueryAnalyzer = analyzer;
      return this;
    }

    public Builder setDefaultField(String defaultField) {
      this.defaultField = defaultField;
      return this;
    }

    public Builder setUniqueKey(String uniqueKey) {
      this.uniqueKey = uniqueKey;
      return this;
    }

    public Builder addField(String name, FieldType dataType) {
      fieldMap.put(name, dataType);
      if (dataType.numericType() != null) {
        NumericConfig nConfig
          = new NumericConfig(dataType.numericPrecisionStep(), NUMBER_FORMAT, dataType.numericType());
        numericConfigMap.put(name, nConfig);
      }
      return this;
    }

    public Builder addField(String name, FieldType dataType, Analyzer indexAnalyzer, Analyzer queryAnalyzer) {
      if (dataType.equals(LDataType.TEXT)) {
        fieldIndexAnalyzers.put(name, indexAnalyzer);
        fieldQueryAnalyzers.put(name, queryAnalyzer);
        addField(name, dataType);
      }
      return this;
    }

    public LSchema build() {
      return new LSchema(
        defaultField,
        uniqueKey,
        fieldMap,
        new PerFieldAnalyzerWrapper(defaultIndexAnalyzer, fieldIndexAnalyzers),
        new PerFieldAnalyzerWrapper(defaultQueryAnalyzer, fieldQueryAnalyzers),
        numericConfigMap
      );
    }

  }

}
