package org.apache.lucene.demo;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.NGramAnalyzer;
import org.apache.lucene.lclient.LCommand;
import org.apache.lucene.lclient.LConnection;
import org.apache.lucene.lclient.LDataType;
import org.apache.lucene.lclient.LDocument;
import org.apache.lucene.lclient.LQuery;
import org.apache.lucene.lclient.LSchema;
import org.apache.lucene.lclient.util.Documents;

import com.google.common.base.Function;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.FluentIterable;
import com.google.common.io.Files;

public class FindFiles {

  private static final String dataPath = StandardSystemProperty.JAVA_IO_TMPDIR.value();
  private static final String sep = StandardSystemProperty.FILE_SEPARATOR.value();
  private static final String DB_NAME = "findfiles";
  private static final String databasePath = dataPath + sep + DB_NAME;

  private LSchema schema = LSchema.Builder()
    .setUniqueKey("id")
    .setDefaultField("text")
    .addField("id", LDataType.STRING)
    .addField("text", LDataType.TEXT, new NGramAnalyzer(1, 1), new NGramAnalyzer(1, 1))
    .build();

  private String seed;

  public FindFiles(String seed) {
    this.seed = seed;
  }

  public String getDatabasePath() {
    return databasePath;
  }

  public String getSeed() {
    return seed;
  }

  public void fetch() throws IOException {
    String collectionName = Files.getNameWithoutExtension(new File(seed).getName());
    try (LConnection conn = new LConnection(databasePath)) {
      LCommand cmd = new LCommand(conn, collectionName, schema);

      cmd.removeByQuery("*:*");

      FluentIterable<String> files = 
        Files.fileTreeTraverser().preOrderTraversal(new File(seed)).filter(Files.isFile())
          .transform(new Function<File,String>(){
            @Override public String apply(File file) {
            return file.getAbsolutePath();
          }
        });
      for (String file : files) {
        LDocument doc = new LDocument(schema).append("id", file).append("text", file);
        cmd.update(doc); 
      }

      cmd.commit();
    }
  }

  public void find(String query) throws IOException {
    String collectionName = Files.getNameWithoutExtension(new File(seed).getName());
    try (LConnection conn = new LConnection(databasePath)) {
      LCommand cmd = new LCommand(conn, collectionName, schema);

      String q = query.replace("'", "\"");
      new LQuery(cmd).filter(q).sort("id asc").toStream()
        .map(doc -> Documents.toMap(schema, doc))
        .map(doc -> doc.get("id"))
        .forEach(id -> { System.out.println(id); });
    }
  }

}
