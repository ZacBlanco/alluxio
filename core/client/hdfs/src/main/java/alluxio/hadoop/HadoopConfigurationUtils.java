/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.hadoop;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;

import alluxio.util.ConfigurationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility class for merging {@link AlluxioConfiguration} with Hadoop's Configuration class.
 */
@ThreadSafe
public final class HadoopConfigurationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopConfigurationUtils.class);

  private HadoopConfigurationUtils() {} // Prevent instantiation.

  /**
   * Merges Hadoop {@link org.apache.hadoop.conf.Configuration} into the Alluxio configuration.
   *
   * @param source the {@link org.apache.hadoop.conf.Configuration} to merge
   * @param alluxioConfiguration the Alluxio configuration to merge to
   */
  public static AlluxioConfiguration mergeHadoopConfiguration(org.apache.hadoop.conf.Configuration source,
      AlluxioConfiguration alluxioConfiguration) {
    // Load Alluxio configuration if any and merge to the one in Alluxio file system
    // Push Alluxio configuration to the Job configuration
    Properties alluxioConfProperties = new Properties();
    // Load any Alluxio configuration parameters existing in the Hadoop configuration.
    for (Map.Entry<String, String> entry : source) {
      String propertyName = entry.getKey();
      if (PropertyKey.isValid(propertyName)) {
        alluxioConfProperties.put(propertyName, entry.getValue());
      }
    }
    LOG.info("Loading Alluxio properties from Hadoop configuration: {}", alluxioConfProperties);
    // Merge the relevant Hadoop configuration into Alluxio's configuration.
    // TODO(jiri): support multiple client configurations (ALLUXIO-2034)
    AlluxioConfiguration conf = ConfigurationUtils.merge(alluxioConfiguration,
        alluxioConfProperties, Source.RUNTIME);
    conf.validate();
    return conf;
  }
}
