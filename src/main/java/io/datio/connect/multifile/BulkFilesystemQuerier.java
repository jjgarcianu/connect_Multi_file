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

import io.datio.connect.multifile.metadatafilestore.FileRegexResultset;
import io.datio.connect.multifile.metadatafilestore.FilesysAdaptor;
import io.datio.connect.multifile.metadatafilestore.FilesysException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Collections;
import java.util.Map;

/**
 * BulkTableQuerier always returns the entire file.
 */
public class BulkFilesystemQuerier extends FilesystemQuerier {
  private static final Logger log = LoggerFactory.getLogger(BulkFilesystemQuerier.class);

  public BulkFilesystemQuerier(QueryMode mode, String name, String topicPrefix) {
    super(mode, name, topicPrefix);
  }

  @Override
  protected void createFileRegex(FilesysAdaptor db) throws FilesysException {
    switch (mode) {
      case TABLE:
        String quoteString = FilesystemUtils.getIdentifierQuoteString(db);
        String queryString = "SELECT * FROM " + FilesystemUtils.quoteString(name, quoteString);
        log.debug("{} prepared SQL query: {}", this, queryString);
        stmt = db.prepareRegEx(queryString);
        break;
      case QUERY:
        log.debug("{} prepared SQL query: {}", this, query);
        stmt = db.prepareRegEx(query);
        break;
    }
  }

  @Override
  protected FileRegexResultset executeQuery() throws FilesysException {
    return stmt.getFileData();
  }

  @Override
  public SourceRecord extractRecord() throws FilesysException {
    Struct record = DataConverter.convertRecord(schema, resultSet);
    // TODO: key from primary key? partition?
    final String topic;
    final Map<String, String> partition;
    switch (mode) {
      case TABLE:
        partition = Collections.singletonMap(FilesystemSourceConnectorConstants.TABLE_NAME_KEY, name);
        topic = topicPrefix + name;
        break;
      case QUERY:
        partition = Collections.singletonMap(FilesystemSourceConnectorConstants.QUERY_NAME_KEY,
                                             FilesystemSourceConnectorConstants.QUERY_NAME_VALUE);
        topic = topicPrefix;
        break;
      default:
        throw new ConnectException("Unexpected query mode: " + mode);
    }
    return new SourceRecord(partition, null, topic, record.schema(), record);
  }

  @Override
  public String toString() {
    return "BulkTableQuerier{" +
           "name='" + name + '\'' +
           ", query='" + query + '\'' +
           ", topicPrefix='" + topicPrefix + '\'' +
           '}';
  }

}
