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

package io.datio.connect.file;

import io.datio.connect.file.filesys.FilesysAdaptor;
import io.datio.connect.file.filesys.FilesysException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.common.config.ConfigException;
import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;
import io.datio.connect.file.util.Version;

/**
 * JdbcSourceTask is a Kafka Connect SourceTask implementation that reads from JDBC databases and
 * generates Kafka Connect records.
 */
public class FilesystemSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(FilesystemSourceTask.class);

  static final String INCREMENTING_FIELD = "incrementing";
  static final String TIMESTAMP_FIELD = "timestamp";

  private Time time;
  private FilesystemSourceTaskConfig config;
  private FilesysAdaptor db;
  private PriorityQueue<FilesystemQuerier> tableQueue = new PriorityQueue<FilesystemQuerier>();
  private AtomicBoolean stop;

  public FilesystemSourceTask() {
    this.time = new SystemTime();
  }

  public FilesystemSourceTask(Time time) {
    this.time = time;
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    try {
      config = new FilesystemSourceTaskConfig(properties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start JdbcSourceTask due to configuration error", e);
    }

    List<String> tables = config.getList(FilesystemSourceTaskConfig.FILES_CONFIG);
    String query = config.getString(FilesystemSourceTaskConfig.QUERY_CONFIG);
    if ((tables.isEmpty() && query.isEmpty()) || (!tables.isEmpty() && !query.isEmpty())) {
      throw new ConnectException("Invalid configuration: each JdbcSourceTask must have at "
                                        + "least one table assigned to it or one query specified");
    }
    FilesystemQuerier.QueryMode queryMode = !query.isEmpty() ? FilesystemQuerier.QueryMode.QUERY :
                                       FilesystemQuerier.QueryMode.TABLE;
    List<String> tablesOrQuery = queryMode == FilesystemQuerier.QueryMode.QUERY ?
                                 Collections.singletonList(query) : tables;

    String mode = config.getString(FilesystemSourceTaskConfig.MODE_CONFIG);
    Map<Map<String, String>, Map<String, Object>> offsets = null;
    if (mode.equals(FilesystemSourceTaskConfig.MODE_INCREMENTING) ||
        mode.equals(FilesystemSourceTaskConfig.MODE_TIMESTAMP) ||
        mode.equals(FilesystemSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
      List<Map<String, String>> partitions = new ArrayList<>(tables.size());
      switch (queryMode) {
        case TABLE:
          for (String table : tables) {
            Map<String, String> partition =
                Collections.singletonMap(FilesystemSourceConnectorConstants.TABLE_NAME_KEY, table);
            partitions.add(partition);
          }
          break;
        case QUERY:
          partitions.add(Collections.singletonMap(FilesystemSourceConnectorConstants.QUERY_NAME_KEY,
                                                  FilesystemSourceConnectorConstants.QUERY_NAME_VALUE));
          break;
      }
      offsets = context.offsetStorageReader().offsets(partitions);
    }

    // Must setup the connection now to validate NOT NULL columns. At this point we've already
    // caught any easy-to-find errors so deferring the connection creation won't save any effort
    String dbUrl = config.getString(FilesystemSourceTaskConfig.CONNECTION_URL_CONFIG);
    log.debug("Trying to connect to {}", dbUrl);
    try {
      db = FilesysAdaptor.getFilesys(dbUrl);
    } catch (FilesysException e) {
      log.error("Couldn't open connection to {}: {}", dbUrl, e);
      throw new ConnectException(e);
    }

    String incrementingColumn
        = config.getString(FilesystemSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG);
    String timestampColumn
        = config.getString(FilesystemSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG);

    for (String tableOrQuery : tablesOrQuery) {
      final Map<String, String> partition;
      switch (queryMode) {
        case TABLE:
          validateNonNullable(mode, tableOrQuery, incrementingColumn, timestampColumn);
          partition = Collections.singletonMap(
              FilesystemSourceConnectorConstants.TABLE_NAME_KEY, tableOrQuery);
          break;
        case QUERY:
          partition = Collections.singletonMap(FilesystemSourceConnectorConstants.QUERY_NAME_KEY,
                                               FilesystemSourceConnectorConstants.QUERY_NAME_VALUE);
          break;
        default:
          throw new ConnectException("Unexpected query mode: " + queryMode);
      }
      Map<String, Object> offset = offsets == null ? null : offsets.get(partition);
      Long incrementingOffset = offset == null ? null :
                              (Long)offset.get(INCREMENTING_FIELD);
      Long timestampOffset = offset == null ? null :
                             (Long)offset.get(TIMESTAMP_FIELD);

      String topicPrefix = config.getString(FilesystemSourceTaskConfig.TOPIC_PREFIX_CONFIG);

      if (mode.equals(FilesystemSourceTaskConfig.MODE_BULK)) {
        tableQueue.add(new BulkFilesystemQuerier(queryMode, tableOrQuery, topicPrefix));
      } else if (mode.equals(FilesystemSourceTaskConfig.MODE_INCREMENTING)) {
        tableQueue.add(new TimestampIncrementingFilesystemQuerier(
            queryMode, tableOrQuery, topicPrefix, null, null, incrementingColumn, incrementingOffset));
      } else if (mode.equals(FilesystemSourceTaskConfig.MODE_TIMESTAMP)) {
        tableQueue.add(new TimestampIncrementingFilesystemQuerier(
            queryMode, tableOrQuery, topicPrefix, timestampColumn, timestampOffset, null, null));
      } else if (mode.endsWith(FilesystemSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
        tableQueue.add(new TimestampIncrementingFilesystemQuerier(
            queryMode, tableOrQuery, topicPrefix, timestampColumn, timestampOffset,
            incrementingColumn, incrementingOffset));
      }
    }

    stop = new AtomicBoolean(false);
  }

  @Override
  public void stop() throws ConnectException {
    if (stop != null) {
      stop.set(true);
    }
    if (db != null) {
      log.debug("Trying to close database connection");
      try {
        db.close();
      } catch (FilesysException e) {
        log.error("Failed to close database connection: ", e);
      }
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    long now = time.milliseconds();
    log.trace("{} Polling for new data");
    while (!stop.get()) {
      // If not in the middle of an update, wait for next update time
      FilesystemQuerier querier = tableQueue.peek();
      if (!querier.querying()) {
        long nextUpdate = querier.getLastUpdate() +
                          config.getInt(FilesystemSourceTaskConfig.POLL_INTERVAL_MS_CONFIG);
        long untilNext = nextUpdate - now;
        log.trace("Waiting {} ms to poll {} next", untilNext, querier.toString());
        if (untilNext > 0) {
          time.sleep(untilNext);
          now = time.milliseconds();
          // Handle spurious wakeups
          continue;
        }
      }

      List<SourceRecord> results = new ArrayList<>();
      try {
        log.trace("Checking for next block of results from {}", querier.toString());
        querier.maybeStartQuery(db);

        int batchMaxRows = config.getInt(FilesystemSourceTaskConfig.BATCH_MAX_ROWS_CONFIG);
        boolean hadNext = true;
        while (results.size() < batchMaxRows && (hadNext = querier.next())) {
          results.add(querier.extractRecord());
        }

        // If we finished processing the results from this query, we can clear it out
        if (!hadNext) {
          log.trace("Closing this query for {}", querier.toString());
          FilesystemQuerier removedQuerier = tableQueue.poll();
          assert removedQuerier == querier;
          now = time.milliseconds();
          querier.close(now);
          tableQueue.add(querier);
        }

        if (results.isEmpty()) {
          log.trace("No updates for {}", querier.toString());
          continue;
        }

        log.trace("Returning {} records for {}", results.size(), querier.toString());
        return results;
      } catch (FilesysException e) {
        log.error("Failed to run query for table {}: {}", querier.toString(), e);
        return null;
      }
    }

    // Only in case of shutdown
    return null;
  }

  private void validateNonNullable(String incrementalMode, String table, String incrementingColumn,
                                   String timestampColumn) {
    try {
      // Validate that requested columns for offsets are NOT NULL. Currently this is only performed
      // for table-based copying because custom query mode doesn't allow this to be looked up
      // without a query or parsing the query since we don't have a table name.
      if ((incrementalMode.equals(FilesystemSourceConnectorConfig.MODE_INCREMENTING) ||
           incrementalMode.equals(FilesystemSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING)) &&
          FilesystemUtils.isColumnNullable(db, table, incrementingColumn)) {
        throw new ConnectException("Cannot make incremental queries using incrementing column " +
                                   incrementingColumn + " on " + table + " because this column is "
                                   + "nullable.");
      }
      if ((incrementalMode.equals(FilesystemSourceConnectorConfig.MODE_TIMESTAMP) ||
           incrementalMode.equals(FilesystemSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING)) &&
          FilesystemUtils.isColumnNullable(db, table, timestampColumn)) {
        throw new ConnectException("Cannot make incremental queries using timestamp column " +
                                   timestampColumn + " on " + table + " because this column is "
                                   + "nullable.");
      }
    } catch (FilesysException e) {
      throw new ConnectException("Failed trying to validate that columns used for offsets are NOT"
                                 + " NULL", e);
    }
  }
}
