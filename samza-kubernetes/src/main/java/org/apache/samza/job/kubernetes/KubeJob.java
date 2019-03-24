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

import com.google.common.collect.ImmutableList;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.Collection;
import org.apache.samza.clustermanager.SamzaResourceRequest;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KubeJob implements StreamJob {
  private static final Logger log = LoggerFactory.getLogger(KubeJob.class);
  private Config config;
  private KubernetesClient kubernetesClient;
  private String podName;
  private ApplicationStatus currentStatus;

  public KubeJob(Config config) {
    this.kubernetesClient = KubernetesClientFactory.create();
    this.config = config;
    this.podName = config.get("job.id");
    this.currentStatus = ApplicationStatus.New;
  }

  /**
   * {@inheritDoc}
   */
  public KubeJob submit() {
    // create SamzaResourceRequest
    int numCores = 1;
    int memoryMB = 500;
    String preferredHost = "localhost";
    String expectedContainerID = "SamzaOperator";
    SamzaResourceRequest request = new SamzaResourceRequest(numCores, memoryMB, preferredHost, expectedContainerID);

    // create Container
    String containerId  = "SamzaOperator";
    String image = "xx";
    Container container = KubernetesUtils.createContainer(containerId, image ,  request);

    // create Pod
    OwnerReference ownerReference = null;
    String restartPolicy = "Never";
    Pod pod = KubernetesUtils.createPod(podName, ownerReference, restartPolicy, container);
    kubernetesClient.pods().create(pod);

    return this;
  }

  /**
   * {@inheritDoc}
   */
  public KubeJob kill() {
    kubernetesClient.pods().withName(podName).delete();
    return this;
  }

  /**
   * {@inheritDoc}
   */
  public ApplicationStatus waitForFinish(long timeoutMs) {
    return waitForStatus(
        ImmutableList.of(ApplicationStatus.SuccessfulFinish, ApplicationStatus.UnsuccessfulFinish),
        timeoutMs);
  }

  /**
   * {@inheritDoc}
   */
  public ApplicationStatus waitForStatus(ApplicationStatus status, long timeoutMs) {
    return waitForStatus(ImmutableList.of(status), timeoutMs);
  }

  /**
   * {@inheritDoc}
   */
  public ApplicationStatus getStatus() {
    return currentStatus;
  }

  private ApplicationStatus waitForStatus(Collection<ApplicationStatus> status, long timeoutMs) {
    final long startTimeMs = System.currentTimeMillis();

    while (System.currentTimeMillis() - startTimeMs < timeoutMs) {
      if (status.contains(currentStatus)) {
        return currentStatus;
      }

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        return currentStatus;
      }
    }
    return currentStatus;
  }
}
