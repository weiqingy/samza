/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.config;

public class KubeConfig {

  public static final String APP_IMAGE = "app.image";
  public static final String LOG_DIR = "kube.samza.log.dir";
  public static final String K8S_API_NAMESPACE = "kube.api.namespace";
  public static final String K8S_POD_LABELS = "kube.pod.labels";
  // The amount of time for container to remain after it exits.
  public static final String DEBUG_DELAY = "kube.container.debug.delay";
  public static final String STREAM_PROCESSOR_CONTAINER_NAME = "stream-processor";
  public static final String SAMZA_AM_CONTAINER_NAME_PREFIX = "jc";
  public static final String POD_RESTART_POLICY = "Always";
  public static final String MY_POD_NAME = "MY_POD_NAME";
  public static final String POD_NAME_FORMAT = "%s-%s-%s";

  private Config config;

  public KubeConfig(Config config) {
    this.config = config;
  }

  public static KubeConfig validate(Config config) throws ConfigException {
    KubeConfig kc = new KubeConfig(config);
    kc.validate();
    return kc;
  }

  private void validate() throws ConfigException {
    // TODO
  }

}
