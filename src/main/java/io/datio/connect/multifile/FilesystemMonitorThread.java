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

import io.datio.connect.multifile.metadatafilestore.FilesysAdaptor;
import io.datio.connect.multifile.metadatafilestore.FilesysException;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Thread that monitors the filesystem for changes to the set of files to be published in the filesystem that this
 * connector should load data from.
 */
public class FilesystemMonitorThread extends Thread {
  private static final Logger log = LoggerFactory.getLogger(FilesystemMonitorThread.class);

  private final FilesysAdaptor fs;
  private final ConnectorContext context;
  private final CountDownLatch shutdownLatch;
  private final long pollMs;
  private Set<String> whitelist;
  private Set<String> blacklist;
  private List<String> files;

  public FilesystemMonitorThread(FilesysAdaptor filesys, ConnectorContext context, long pollMs,
                                 Set<String> whitelist, Set<String> blacklist) {
    this.fs = filesys;
    this.context = context;
    this.shutdownLatch = new CountDownLatch(1);
    this.pollMs = pollMs;
    this.whitelist = whitelist;
    this.blacklist = blacklist;
    this.files = null;
  }

  @Override
  public void run() {
    while (shutdownLatch.getCount() > 0) {
      if (updateFiles()) {
        context.requestTaskReconfiguration();
      }

      try {
        boolean shuttingDown = shutdownLatch.await(pollMs, TimeUnit.MILLISECONDS);
        if (shuttingDown) {
          return;
        }
      } catch (InterruptedException e) {
        log.error("Unexpected InterruptedException, ignoring: ", e);
      }
    }
  }

  public List<String> files() {
    final long TIMEOUT = 10000L;
    synchronized (fs) {
      long started = System.currentTimeMillis();
      long now = started;
      while (files == null && now - started < TIMEOUT) {
        try {
          fs.wait(TIMEOUT - (now - started));
        } catch (InterruptedException e) {
          // Ignore
        }
        now = System.currentTimeMillis();
      }
      if (files == null) {
        throw new ConnectException("Tables could not be updated quickly enough.");
      }
      return files;
    }
  }

  public void shutdown() {
    shutdownLatch.countDown();
  }

  // Update files and return true if the
  private boolean updateFiles() {
    synchronized (fs) {
      final List<String> files;
      try {
        files = FilesystemUtils.getFiles(fs);
      } catch (FilesysException e) {
        log.error("Error while trying to get updated file list, ignoring and waiting for next "
                  + "file poll interval", e);
        return false;
      }

      final List<String> filteredFiles;
      if (whitelist != null) {
        filteredFiles = new ArrayList<>(files.size());
        for (String table : files) {
          if (whitelist.contains(table)) {
            filteredFiles.add(table);
          }
        }
      } else if (blacklist != null) {
        filteredFiles = new ArrayList<>(files.size());
        for (String file : files) {
          if (!blacklist.contains(file)) {
            filteredFiles.add(file);
          }
        }
      } else {
        filteredFiles = files;
      }

      if (!filteredFiles.equals(this.files)) {
        List<String> previousFiles = this.files;
        this.files = filteredFiles;
        fs.notifyAll();
        // Only return true if the file list wasn't previously null, i.e. if this was not the
        // first file lookup
        return previousFiles != null;
      }

      return false;
    }
  }
}
