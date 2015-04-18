package org.apache.lucene.lclient.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;

public class TikaUtils {

  private static final int MAX_LENGTH = -1;

  private static Tika tika = new Tika();
  private static Metadata metadata = new Metadata();

  private TikaUtils() { }

  public static ImmutablePair<String,Metadata> parseToString(File file) throws IOException, TikaException {
    metadata.add(TikaMetadataKeys.RESOURCE_NAME_KEY, file.getName());
    InputStream stream = TikaInputStream.get(file.toURI().toURL(), metadata);
    metadata.add(HttpHeaders.CONTENT_TYPE, detect(stream, metadata));
    return parseToString(stream, metadata, MAX_LENGTH);
  }

  public static String detect(File file) throws IOException {
    metadata.add(TikaMetadataKeys.RESOURCE_NAME_KEY, file.getName());
    InputStream stream = TikaInputStream.get(file.toURI().toURL(), metadata);
    return detect(stream, metadata);
  }

  public static ImmutablePair<String,Metadata> parseToString(InputStream stream, Metadata metadata, int maxLength) throws IOException, TikaException {
    String s = tika.parseToString(stream, metadata, maxLength);
    return ImmutablePair.of(s, metadata);
  }

  public static String detect(InputStream stream, Metadata metadata) throws IOException {
    return tika.detect(stream, metadata);
  }

}
