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

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.samza.SamzaException;
import org.apache.samza.clustermanager.ResourceRequestState;
import org.apache.samza.clustermanager.SamzaResourceRequest;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.apache.samza.util.Util;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.mutable.StringBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.samza.config.ApplicationConfig.APP_ID;
import static org.apache.samza.config.ApplicationConfig.APP_NAME;
import static org.apache.samza.config.KubeConfig.*;
import static org.apache.samza.job.ApplicationStatus.*;
import static org.apache.samza.serializers.model.SamzaObjectMapper.getObjectMapper;

/**
 * The client to start a Kubernetes job coordinator
 */
public class KubeJob implements StreamJob {
  private static final Logger LOG = LoggerFactory.getLogger(KubeJob.class);
  private Config config;
  private KubernetesClient kubernetesClient;
  private String podName;
  private ApplicationStatus currentStatus;
  private String nameSpace;
  private KubePodStatusWatcher watcher;
  private String image;

  public KubeJob(Config config) {
    this.kubernetesClient = KubeClientFactory.create();
    this.config = config;
    this.podName = String.format(OPERATOR_POD_NAME_FORMAT, SAMZA_OPERATOR_CONTAINER_NAME_PREFIX,
            config.get(APP_NAME, "samza"), config.get(APP_ID, "1"));
    this.currentStatus = ApplicationStatus.New;
    this.watcher = new KubePodStatusWatcher(podName);
    this.nameSpace = config.get(K8S_API_NAMESPACE, "default");
    this.image = config.get(APP_IMAGE, "weiqingyang/hello-samza-new:v0");
  }

  /**
   * submit the kubernetes job coordinator
   */
  public KubeJob submit() {
    // create SamzaResourceRequest
    int memoryMB = config.getInt("cluster-manager.container.memory.mb", 1024);  // TODO
    int numCores = config.getInt("cluster-manager.container.cpu.cores", 1);  // TODO
    String preferredHost = ResourceRequestState.ANY_HOST;
    SamzaResourceRequest request = new SamzaResourceRequest(numCores, memoryMB, preferredHost, podName);

    // create Container
    String fwkPath = config.get("samza.fwk.path", "");
    String fwkVersion = config.get("samza.fwk.version");
    String cmd = buildJobCoordinatorCmd(fwkPath, fwkVersion);
    LOG.info(String.format("samza.fwk.path: %s. samza.fwk.version: %s. Command: %s", fwkPath, fwkVersion, cmd));
    Container container = KubeUtils.createContainer(SAMZA_OPERATOR_CONTAINER_NAME_PREFIX, image, request, cmd);
    container.setEnv(getEnvs());

    AzureFileVolumeSource azureFileVolumeSource = new AzureFileVolumeSource(false, "azure-secret", "aksshare");
    Volume volume = new Volume();
    volume.setAzureFile(azureFileVolumeSource);
    volume.setName("azure");
    //     volumeMounts:
    //    - mountPath: /etc/jmx-zookeeper
    //      name: jmx-config
    VolumeMount volumeMount = new VolumeMount();
    volumeMount.setMountPath(config.get(SAMZA_MOUNT_DIR, "/tmp/mnt"));
    volumeMount.setName("azure");
    volumeMount.setSubPath(podName);
    container.setVolumeMounts(Collections.singletonList(volumeMount));

    // create Pod
    String restartPolicy = "OnFailure";
    PodBuilder podBuilder = new PodBuilder()
            .editOrNewMetadata()
              .withNamespace(nameSpace)
              .withName(podName)
            .endMetadata()
            .editOrNewSpec()
              .withRestartPolicy(restartPolicy)
              .withContainers(container)
              .withVolumes(volume)
            .endSpec();

    Pod pod = podBuilder.build();
    kubernetesClient.pods().create(pod);
    // TODO: adding watcher here makes Client waiting .. Need to fix.
    // kubernetesClient.pods().withName(podName).watch(watcher);
    return this;
  }

  /**
   * Kill the job coordinator pod
   */
  public KubeJob kill() {
    LOG.info("Killing application: {}, operator pod: {}, namespace: {}", config.get(APP_NAME), podName, nameSpace);
    System.out.println("Killing application: " + config.get(APP_NAME) +
        "; Operator pod: " + podName + "; namespace: " + nameSpace);
    kubernetesClient.pods().inNamespace(nameSpace).withName(podName).delete();
    return this;
  }

  /**
   * Wait for finish without timeout
   */
  public ApplicationStatus waitForFinish(long timeoutMs) {
    watcher.waitForCompleted(timeoutMs, TimeUnit.MILLISECONDS);
    return getStatus();
  }

  /**
   * Wait for the application to reach a status
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
   * Get teh Status of the job coordinator pod
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

  // Build the job coordinator command
  private String buildJobCoordinatorCmd(String fwkPath, String fwkVersion) {
    // figure out if we have framework is deployed into a separate location
    if (fwkVersion == null || fwkVersion.isEmpty()) {
      fwkVersion = "STABLE";
    }
    LOG.info(String.format("KubeJob: fwk_path is %s, ver is %s use it directly ", fwkPath, fwkVersion));

    // default location
    String cmdExec = "/opt/hello-samza/bin/run-jc.sh"; // TODO
    if (!fwkPath.isEmpty()) {
      // if we have framework installed as a separate package - use it
      cmdExec = fwkPath + "/" + fwkVersion + "/bin/run-jc.sh";
    }

    /*cmdExec1 = "/bin/bash -c \"" + cmdExec1 + "\"";
    String cmdExec2 = "";
    if (config.containsKey(DEBUG_DELAY)) {
      long sleepTime = config.getInt(DEBUG_DELAY);
      cmdExec2 = "; sleep " + sleepTime;
    }
    String commands = "/bin/bash -c '" + cmdExec1 + cmdExec2 + "'";
    LOG.info("KubeJob: cmdExec is: " + commands);*/

    return cmdExec;
  }

  // Construct the envs for the job coordinator pod
  private List<EnvVar> getEnvs() {
    MapConfig coordinatorSystemConfig = CoordinatorStreamUtil.buildCoordinatorStreamConfig(config);
    List<EnvVar> envList = new ArrayList<>();
    ObjectMapper objectMapper = getObjectMapper();
    String coordinatorSysConfig;
    try  {
      coordinatorSysConfig = objectMapper.writeValueAsString(coordinatorSystemConfig);
    } catch (IOException ex) {
      LOG.warn("No coordinator system configs!", ex);
      coordinatorSysConfig = "";
    }
    envList.add(new EnvVar("SAMZA_COORDINATOR_SYSTEM_CONFIG", Util.envVarEscape(coordinatorSysConfig), null));
    envList.add(new EnvVar("SAMZA_LOG_DIR", config.get(SAMZA_MOUNT_DIR), null));
    envList.add(new EnvVar(OPERATOR_POD_NAME, podName, null));
    LOG.info("======================================");
    LOG.info(Util.envVarEscape(coordinatorSysConfig));
    LOG.info("======================================");
    // TODO: "JAVA_OPTS" and "JAVA_HOME" are optional, but may need to set them later
    // "JAVA_OPTS"
    envList.add(new EnvVar("JAVA_OPTS", "", null));
    // envMap.put("JAVA_OPTS", Util.envVarEscape(yarnConfig.getAmOpts));
    // "JAVA_HOME"
    // envMap.put("JAVA_HOME", yarnConfig.getAMJavaHome);

    return envList;
  }
}