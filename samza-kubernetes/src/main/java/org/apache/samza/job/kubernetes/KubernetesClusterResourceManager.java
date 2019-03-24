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

package org.apache.samza.job.kubernetes;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.samza.clustermanager.ClusterResourceManager;
import org.apache.samza.clustermanager.SamzaApplicationState;
import org.apache.samza.clustermanager.SamzaResource;
import org.apache.samza.clustermanager.SamzaResourceRequest;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.job.CommandBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesClusterResourceManager extends ClusterResourceManager {
  private static final Logger log = LoggerFactory.getLogger(KubernetesClusterResourceManager.class);

  KubernetesClient client = null;

  KubernetesClusterResourceManager(Config config, JobModelManager jobModelManager,
      ClusterResourceManager.Callback callback, SamzaApplicationState samzaAppState ) {
    super(null);
    ConfigBuilder builder = new ConfigBuilder();
    io.fabric8.kubernetes.client.Config kubeConfig = builder.build();

    client = new DefaultKubernetesClient(kubeConfig);

  }

  @Override
  public void start() {

  }

  @Override
  public void requestResources(SamzaResourceRequest resourceRequest) {
    log.info("Requesting resources on " + resourceRequest.getPreferredHost() + " for container " + resourceRequest.getContainerID());

    int memoryMb = resourceRequest.getMemoryMB();
    int cpuCores = resourceRequest.getNumCores();
    String preferredHost = resourceRequest.getPreferredHost();

    if (preferredHost.equals("ANY_HOST")) {
      log.info("Making a request for ANY_HOST ");

      // Create a pod with only one container in anywhere
      client.pods().inNamespace("namespace<TODO>").createNew()
          .withNewMetadata()
          .withName("TODO" + resourceRequest.getContainerID()).endMetadata()
          .withNewSpec().addNewContainer().withName(resourceRequest.getContainerID()).withImage("imagename-todo")
          .withCommand("command-todo").endContainer().endSpec().done();
    } else {

    }

  }

  @Override
  public void cancelResourceRequest(SamzaResourceRequest request) {

  }

  @Override
  public void releaseResources(SamzaResource resource) {

  }

  @Override
  public void launchStreamProcessor(SamzaResource resource, CommandBuilder builder) {

  }

  @Override
  public void stopStreamProcessor(SamzaResource resource) {

  }

  @Override
  public void stop(SamzaApplicationState.SamzaAppStatus status) {

  }
}