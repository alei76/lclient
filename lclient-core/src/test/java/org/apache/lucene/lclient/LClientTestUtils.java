package org.apache.lucene.lclient;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntConsumer;

import org.apache.lucene.analysis.NGramAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.English;
import org.apache.lucene.util.TestUtil;

import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;

public class LClientTestUtils {

   private LClientTestUtils() { }

   public static LSchema getLSchema() {
     LSchema schema = LSchema.Builder()
       .setUniqueKey("id")
       .addField("id", LDataType.STRING)
       .addField("price", LDataType.DOUBLE)
       .addField("star", LDataType.FLOAT)
       .addField("count", LDataType.INT)
       .addField("date", LDataType.LONG)
       .addField("datenow", LDataType.LONG)
       .addField("tag", LDataType.STRING)
       .addField("text", LDataType.TEXT, new StandardAnalyzer(), new StandardAnalyzer())
       .addField("text_ws", LDataType.TEXT, new WhitespaceAnalyzer(), new WhitespaceAnalyzer())
       .addField("text_kw", LDataType.TEXT, new KeywordAnalyzer(), new KeywordAnalyzer())
       .addField("text_1g", LDataType.TEXT, new NGramAnalyzer(1, 1), new NGramAnalyzer(1, 1))
       .addField("text_2g", LDataType.TEXT, new NGramAnalyzer(2, 2), new NGramAnalyzer(2, 2))
       .build();
     return schema;
   }

   public static IndexConsumer getIndexConsumer(LCommand cmd) {
     return new IndexConsumer(cmd);
   }

   private static class IndexConsumer implements IntConsumer {

    private LCommand cmd;
    private Stopwatch stopwatch;

    public IndexConsumer(LCommand cmd) {
      this.cmd = cmd;
      this.stopwatch = Stopwatch.createStarted();
    }

    @Override  public void accept(int value) {
      LDocument doc;
      try {
        doc = new LDocument(cmd.schema())
          .append("id", String.valueOf(value))
          .append("price", ThreadLocalRandom.current().nextDouble(100d))
          .append("star", ThreadLocalRandom.current().nextFloat())
          .append("count", ThreadLocalRandom.current().nextInt(10000))
          .append("date", ThreadLocalRandom.current().nextLong(1425169899651L))
          .append("tag", Splitter.on(" ").trimResults().omitEmptyStrings().splitToList(English.intToEnglish(value)).get(0))
          .append("text", TestUtil.randomSimpleString(ThreadLocalRandom.current(), 10)
                          +" "+TestUtil.randomSimpleString(ThreadLocalRandom.current()));
        cmd.update(doc);
        if (value % 1000 == 0) {
           cmd.refresh();
           System.out.println("refreshing at "+value+":"+stopwatch);
           stopwatch = Stopwatch.createStarted();
        }
      } catch (IOException e) { }
    }

  }

}
