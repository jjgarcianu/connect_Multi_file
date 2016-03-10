/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.datio.connect.multifile;

import io.datio.connect.multifile.metadatafilestore.FileRegEx;
import io.datio.connect.multifile.metadatafilestore.FileRegexResultset;
import io.datio.connect.multifile.metadatafilestore.FilesysAdaptor;
import io.datio.connect.multifile.metadatafilestore.FilesysException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;



/**
 * FilesysystemQuerier obtains data from a specific file. Implementations handle different types
 * of file extraction: periodic bulk loading, incremental loads or using auto incrementing IDs, incremental
 * loads using timestamps, etc.
 */
abstract class FilesystemQuerier implements Comparable<FilesystemQuerier> {
  public enum QueryMode {
    TABLE, // Copying whole files, with queries constructed automatically
    QUERY // User-specified query
  }

  protected final QueryMode mode;
  protected final String name;
  protected final String query;
  protected final String topicPrefix;
  protected long lastUpdate;
  protected FileRegEx stmt;
  protected FileRegexResultset resultSet;
  protected Schema schema;

  public FilesystemQuerier(QueryMode mode, String nameOrQuery, String topicPrefix) {
    this.mode = mode;
    this.name = mode.equals(QueryMode.TABLE) ? nameOrQuery : null;
    this.query = mode.equals(QueryMode.QUERY) ? nameOrQuery : null;
    this.topicPrefix = topicPrefix;
    this.lastUpdate = 0;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public FileRegEx getOrCreateFileRegex(FilesysAdaptor db) throws FilesysException {
    if (stmt != null) {
      return stmt;
    }
    createFileRegex(db);
    return stmt;
  }

  protected abstract void createFileRegex(FilesysAdaptor db) throws FilesysException;

  public boolean querying() {
    return resultSet != null;
  }

  public void maybeStartQuery(FilesysAdaptor db) throws FilesysException {
    if (resultSet == null) {
      stmt = getOrCreateFileRegex(db);
      resultSet = executeQuery();
      schema = DataConverter.convertSchema(name, resultSet.getMetaData());
    }
  }

  protected abstract FileRegexResultset executeQuery() throws FilesysException;

  public boolean next() throws FilesysException {
    return resultSet.next();
  }

  public abstract SourceRecord extractRecord() throws FilesysException;

  public void close(long now) throws FilesysException {
    resultSet.close();
    resultSet = null;
    // TODO: Can we cache this and quickly check that it's identical for the next query
    // instead of constructing from scratch since it's almost always the same
    schema = null;

    lastUpdate = now;
  }

  @Override
  public int compareTo(FilesystemQuerier other) {
    if (this.lastUpdate < other.lastUpdate) {
      return -1;
    } else if (this.lastUpdate > other.lastUpdate) {
      return 1;
    } else {
      return this.name.compareTo(other.name);
    }
  }
}
