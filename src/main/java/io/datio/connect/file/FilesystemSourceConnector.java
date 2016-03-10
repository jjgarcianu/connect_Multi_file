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
import io.datio.connect.file.util.StringUtils;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.common.config.ConfigException;
import io.datio.connect.file.util.Version;

/**
 * JdbcConnector is a Kafka Connect Connector implementation that watches a JDBC database and
 * generates tasks to ingest database contents.
 */
public class FilesystemSourceConnector extends SourceConnector {

  private static final Logger log = LoggerFactory.getLogger(FilesystemSourceConnector.class);

  private static final long MAX_TIMEOUT = 10000L;

  private Map<String, String> configProperties;
  private FilesystemSourceConnectorConfig config;
  private FilesysAdaptor db;
  private FilesystemMonitorThread filesystemMonitorThread;

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) throws ConnectException {
    try {
      configProperties = properties;
      config = new FilesystemSourceConnectorConfig(configProperties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start Flat File SourceConnector due to configuration "
                                 + "error", e);
    }

    String dbUrl = config.getString(FilesystemSourceConnectorConfig.CONNECTION_URL_CONFIG);
    log.debug("Trying to connect to {}", dbUrl);
    try {
      db = FilesysAdaptor.getFilesys(dbUrl);
    } catch (FilesysException e) {
      log.error("Couldn't open connection to {}: {}", dbUrl, e);
      throw new ConnectException(e);
    }

    long filesysPollMs = config.getLong(FilesystemSourceConnectorConfig.TABLE_POLL_INTERVAL_MS_CONFIG);
    List<String> whitelist = config.getList(FilesystemSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
    Set<String> whitelistSet = whitelist.isEmpty() ? null : new HashSet<>(whitelist);
    List<String> blacklist = config.getList(FilesystemSourceConnectorConfig.TABLE_BLACKLIST_CONFIG);
    Set<String> blacklistSet = blacklist.isEmpty() ? null : new HashSet<>(blacklist);
    if (whitelistSet != null && blacklistSet != null)
      throw new ConnectException(FilesystemSourceConnectorConfig.TABLE_WHITELIST_CONFIG + " and "
                                 + FilesystemSourceConnectorConfig.TABLE_BLACKLIST_CONFIG+ " are "
                                 + "exclusive.");
    String query = config.getString(FilesystemSourceConnectorConfig.QUERY_CONFIG);
    if (!query.isEmpty()) {
      if (whitelistSet != null || blacklistSet != null)
        throw new ConnectException(FilesystemSourceConnectorConfig.QUERY_CONFIG + " may not be combined"
                                   + " with whole-table copying settings.");
      // Force filtering out the entire set of files since the one task we'll generate is for the
      // query.
      whitelistSet = Collections.emptySet();
    }
    filesystemMonitorThread = new FilesystemMonitorThread(db, context, filesysPollMs, whitelistSet,
                                                blacklistSet);
    filesystemMonitorThread.start();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return FilesystemSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    String query = config.getString(FilesystemSourceConnectorConfig.QUERY_CONFIG);
    if (!query.isEmpty()) {
      List<Map<String, String>> taskConfigs = new ArrayList<>(1);
      Map<String, String> taskProps = new HashMap<>(configProperties);
      taskProps.put(FilesystemSourceTaskConfig.FILES_CONFIG, "");
      taskConfigs.add(taskProps);
      return taskConfigs;
    } else {
      List<String> currentFiles = filesystemMonitorThread.files();
      int numGroups = Math.min(currentFiles.size(), maxTasks);
      List<List<String>> filesGrouped = ConnectorUtils.groupPartitions(currentFiles, numGroups);
      List<Map<String, String>> taskConfigs = new ArrayList<>(filesGrouped.size());
      for (List<String> taskFiles : filesGrouped) {
        Map<String, String> taskProps = new HashMap<>(configProperties);
        taskProps.put(FilesystemSourceTaskConfig.FILES_CONFIG,
                      StringUtils.join(taskFiles, ","));
        taskConfigs.add(taskProps);
      }
      return taskConfigs;
    }
  }

  @Override
  public void stop() throws ConnectException {
    log.info("Stopping table monitoring thread");
    filesystemMonitorThread.shutdown();
    try {
      filesystemMonitorThread.join(MAX_TIMEOUT);
    } catch (InterruptedException e) {
      // Ignore, shouldn't be interrupted
    }

    log.debug("Trying to close database connection");
    try {
      db.close();
    } catch (FilesysException e) {
      log.error("Failed to close database connection: ", e);
    }
  }
}
