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

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.concurrent.TimeUnit;
import org.apache.samza.SamzaException;
import org.apache.samza.clustermanager.ResourceRequestState;
import org.apache.samza.clustermanager.SamzaResourceRequest;
import org.apache.samza.config.Config;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.mutable.StringBuilder;

import static org.apache.samza.job.ApplicationStatus.*;
import static org.apache.samza.job.kubernetes.KubeUtils.POD_NAME_FORMAT;
import static org.apache.samza.job.kubernetes.KubeUtils.SAMZA_AM_CONTAINER_NAME;


public class KubeJob implements StreamJob {
  private static final Logger log = LoggerFactory.getLogger(KubeJob.class);
  private Config config;
  private KubernetesClient kubernetesClient;
  private String podName;
  private ApplicationStatus currentStatus;
  private String nameSpace = "";
  private KubePodStatusWatcher watcher;

  public KubeJob(Config config) {
    this.kubernetesClient = KubeClientFactory.create();
    this.config = config;
    this.podName = String.format(POD_NAME_FORMAT, SAMZA_AM_CONTAINER_NAME, config.get("job.id"));
    this.currentStatus = ApplicationStatus.New;
    this.watcher = new KubePodStatusWatcher(podName);
  }

  /**
   * {@inheritDoc}
   */
  public KubeJob submit() {
    // create SamzaResourceRequest
    int numCores = 1;
    int memoryMB = 500;
    String preferredHost = ResourceRequestState.ANY_HOST;
    SamzaResourceRequest request = new SamzaResourceRequest(numCores, memoryMB, preferredHost, podName);

    // create Container
    String image = "xx";
    String fwkPath = config.get("samza.fwk.path", "");
    String fwkVersion = config.get("samza.fwk.version");
    String cmd = buildOperatorCmd(fwkPath, fwkVersion);
    log.info(String.format("samza.fwk.path: %s. samza.fwk.version: %s. Command: %s", fwkPath, fwkVersion, cmd));
    Container container = KubeUtils.createContainer(SAMZA_AM_CONTAINER_NAME, image , request, cmd);

    // create Pod
    String restartPolicy = "OnFailure";
    Pod pod = KubeUtils.createPod(podName, restartPolicy, container);
    kubernetesClient.pods().create(pod);
    // add watcher
    kubernetesClient.pods().withName(podName).watch(watcher);
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
    watcher.waitForCompleted(timeoutMs, TimeUnit.MILLISECONDS);
    return getStatus();
  }

  /**
   * {@inheritDoc}
   */
  public ApplicationStatus waitForStatus(ApplicationStatus status, long timeoutMs) {
    switch (status.getStatusCode()) {
      case New:
        watcher.waitForPending(timeoutMs, TimeUnit.MILLISECONDS);
        return New;
      case Running:
        watcher.waitForRunning(timeoutMs, TimeUnit.MILLISECONDS);
        return Running;
      case SuccessfulFinish:
        watcher.waitForSucceeded(timeoutMs, TimeUnit.MILLISECONDS);
        return SuccessfulFinish;
      case UnsuccessfulFinish:
        watcher.waitForFailed(timeoutMs, TimeUnit.MILLISECONDS);
        return UnsuccessfulFinish;
      default:
        throw new SamzaException("Unsupported application status type: " + status);
    }
  }

  /**
   * {@inheritDoc}
   */
  public ApplicationStatus getStatus() {
    Pod operatorPod = kubernetesClient.pods().inNamespace(nameSpace).withName(podName).get();
    PodStatus podStatus = operatorPod.getStatus();
    // TODO
    switch (podStatus.getPhase()) {
      case "Pending":
        currentStatus = ApplicationStatus.New;
        break;
      case "Running":
        currentStatus = Running;
        break;
      case "Completed":
      case "Succeeded":
        currentStatus = ApplicationStatus.SuccessfulFinish;
        break;
      case "Failed":
        String err = new StringBuilder().append("Reason: ").append(podStatus.getReason())
            .append("Conditions: ").append(podStatus.getConditions().toString()).toString();
        currentStatus = ApplicationStatus.unsuccessfulFinish(new SamzaException(err));
        break;
      case "CrashLoopBackOff":
      case "Unknown":
      default:
        currentStatus = ApplicationStatus.New;
    }
    return currentStatus;
  }

  private String buildOperatorCmd(String fwkPath, String fwkVersion) {
    // figure out if we have framework is deployed into a separate location
    if (fwkVersion == null || fwkVersion.isEmpty()) {
      fwkVersion = "STABLE";
    }
    log.info(String.format("Inside KubeJob: fwk_path is %s, ver is %s use it directly ", fwkPath, fwkVersion));

    // default location
    String cmdExec = "./__package/bin/run-jc.sh";
    if (!fwkPath.isEmpty()) {
      // if we have framework installed as a separate package - use it
      cmdExec = fwkPath + "/" + fwkVersion + "/bin/run-jc.sh";
    }
    log.info("Inside KubeJob: cmdExec is: " + cmdExec);
    return cmdExec;
  }
}
