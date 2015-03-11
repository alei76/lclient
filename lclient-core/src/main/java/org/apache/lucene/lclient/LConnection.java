package org.apache.lucene.lclient;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

public class LConnection implements Closeable {

  private String databasePath;

  public LConnection(String databasePath) throws IOException {
    this.databasePath = new File(databasePath).getAbsolutePath();
    open();
  }

  String getDatabasePath() {
    return databasePath;
  }

  private void open() throws IOException {
    File dir = new File(databasePath);
    if (!dir.exists()) {
      Files.createParentDirs(dir);
      dir.mkdir();
    }
  }

  @Override
  public void close() throws IOException {
    for (Map.Entry<String, IndexWriter> entry : indexWriters.entrySet())
      entry.getValue().close();
    for (Map.Entry<String, DirectoryReader> entry : indexReaders.entrySet())
      entry.getValue().close();
    for (Map.Entry<String, Directory> entry : directories.entrySet())
      entry.getValue().close();
  }

  private Map<String,Directory> directories = Maps.newHashMap();
  private Map<String,IndexWriter> indexWriters = Maps.newHashMap();

  Directory getDirectory(String name) throws IOException {
    createDirectory(name);
    return directories.get(name);
  }

  IndexWriter getIndexWriter(String name, LSchema schema) throws IOException {
    IndexWriter writer = indexWriters.get(name);
    if (writer == null) {
      createDirectory(name);
      createIndexWriter(name, schema);
    }
    return indexWriters.get(name);
  }

  private void createDirectory(String name) throws IOException {
    Directory fsDir = FSDirectory.open(new File(databasePath, name).toPath());
    NRTCachingDirectory cachedFSDir = new NRTCachingDirectory(fsDir, 5.0, 60.0);
    directories.put(name, cachedFSDir);
  }

  private void createIndexWriter(String name, LSchema schema) throws IOException {
    IndexWriterConfig config = new IndexWriterConfig(schema.getIndexAnalyzer())
      .setOpenMode(OpenMode.CREATE_OR_APPEND)
      .setUseCompoundFile(true)
      .setCommitOnClose(true)
      .setRAMBufferSizeMB(100.0);
    IndexWriter writer = new IndexWriter(directories.get(name), config);
    writer.commit();
    indexWriters.put(name, writer);
  }

  private Map<String,DirectoryReader> indexReaders = Maps.newHashMap();

  void setIndexReader(String name, DirectoryReader reader) {
    indexReaders.put(name, reader);
  }


}
