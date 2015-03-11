package org.apache.lucene.analysis;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.ngram.NGramTokenizer;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;

public class NGramAnalyzer extends Analyzer {

  private Analyzer analyzer;

  public NGramAnalyzer(int minGram, int maxGram) {
    Map<String,String> params = Maps.newHashMap();
    int minGramSize = MoreObjects.firstNonNull(minGram, NGramTokenizer.DEFAULT_MIN_NGRAM_SIZE);
    int maxGramSize = MoreObjects.firstNonNull(maxGram, NGramTokenizer.DEFAULT_MAX_NGRAM_SIZE);
    params.put("minGramSize", String.valueOf(minGramSize));
    params.put("maxGramSize", String.valueOf(maxGramSize));
    try {
      analyzer = CustomAnalyzer.builder()
        .withTokenizer("ngram", params)
        .addTokenFilter("lowercase")
        .build();
    } catch (IOException e) { }
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    return analyzer.createComponents(fieldName);
  }

}
