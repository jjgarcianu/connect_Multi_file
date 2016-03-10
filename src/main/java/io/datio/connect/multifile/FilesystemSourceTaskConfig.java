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

import java.util.Map;

import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;

/**
 * Configuration options for a single FilesystemSourceTask. These are processed after all
 * Connector-level configs have been parsed.
 */
public class FilesystemSourceTaskConfig extends FilesystemSourceConnectorConfig {

  public static final String FILES_CONFIG = "files";
  private static final String TABLES_DOC = "List of files for this task to watch for changes.";

  static ConfigDef config = baseConfigDef()
      .define(FILES_CONFIG, Type.LIST, Importance.HIGH, TABLES_DOC);

  public FilesystemSourceTaskConfig(Map<String, String> props) {
    super(config, props);
  }
}
