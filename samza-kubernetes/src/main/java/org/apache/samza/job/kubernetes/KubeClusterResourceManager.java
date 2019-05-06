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
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import org.apache.samza.clustermanager.*;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.job.CommandBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.samza.config.ApplicationConfig.*;
import static org.apache.samza.config.KubeConfig.*;

public class KubeClusterResourceManager extends ClusterResourceManager {
  private static final Logger log = LoggerFactory.getLogger(KubeClusterResourceManager.class);

  KubernetesClient client = null;
  private final Object lock = new Object();
  private String appId;
  private String image;
  private String namespace;
  private OwnerReference ownerReference;
  private JobModelManager jobModelManager;
  private final Map<String, String> podLabels = new HashMap<>();
  private boolean hostAffinityEnabled = false;

  KubeClusterResourceManager(Config config, JobModelManager jobModelManager,
                             ClusterResourceManager.Callback callback, SamzaApplicationState samzaAppState ) {
    super(callback);
    client = KubeClientFactory.create();
    createOwnerReferences();
    this.jobModelManager = jobModelManager;
    this.image = config.get(APP_IMAGE, "weiqingyang/hello-samza:v6");
    this.namespace = config.get(K8S_API_NAMESPACE, "default");
    this.appId = config.get(APP_ID, "001");
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    this.hostAffinityEnabled = clusterManagerConfig.getHostAffinityEnabled();

  }

  @Override
  public void start() {
    log.info("Kubernetes Cluster ResourceManager started, starting watcher");
    startPodWatcher();
  }

  // Create the owner reference of the samaza-operator pod
  private void createOwnerReferences() {
    // The operator pod yaml needs to pass in MY_POD_NAME env
    String thisPodName = System.getenv(MY_POD_NAME);
    Pod pod = client.pods().inNamespace(namespace).withName(thisPodName).get();
    ownerReference = new OwnerReferenceBuilder()
          .withName(pod.getMetadata().getName())
          .withApiVersion(pod.getApiVersion())
          .withUid(pod.getMetadata().getUid()).withKind(pod.getKind())
          .withController(true).build();
  }

  public void startPodWatcher() {
    Watcher watcher = new Watcher<Pod>() {
      @Override
      public void eventReceived(Action action, Pod pod) {
        log.info("Pod watcher received action " + action + " for pod " + pod.getMetadata().getName());
        switch (action) {
          case ADDED:
            log.info("Pod " + pod.getMetadata().getName() + " is added.");
            break;
          case MODIFIED:
            log.info("Pod " + pod.getMetadata().getName() + " is modified.");
            if (isPodFailed(pod)) {
              deletePod(pod);
              createNewStreamProcessor(pod);
            }
            break;
          case ERROR:
            log.info("Pod " + pod.getMetadata().getName() + " received error.");
            if (isPodFailed(pod)) {
              deletePod(pod);
              createNewStreamProcessor(pod);
            }
            break;
          case DELETED:
            log.info("Pod " + pod.getMetadata().getName() + " is deleted.");
            createNewStreamProcessor(pod);
            break;
        }
      }
      @Override
      public void onClose(KubernetesClientException e) {
        log.error("Pod watcher closed", e);
      }
    };

    client.pods().withLabels(podLabels).watch(watcher);
  }

  private boolean isPodFailed(Pod pod) {
    return pod.getStatus() != null && pod.getStatus().getPhase().equals("Failed");
  }

  private void deletePod(Pod pod) {
    boolean deleted = client.pods().delete(pod);
    if (deleted) {
      log.info("Deleted pod " + pod.getMetadata().getName());
    } else {
      log.info("Failed to deleted pod " + pod.getMetadata().getName());
    }
  }
  private void createNewStreamProcessor(Pod pod) {
    int memory = Integer.valueOf(pod.getSpec().getContainers().get(0).getResources().getRequests().get("memory").getAmount());
    int cpu = Integer.valueOf(pod.getSpec().getContainers().get(0).getResources().getRequests().get("cpu").getAmount());

    String containerId = KubeUtils.getSamzaContainerNameFromPodName(pod.getMetadata().getName());
    // Find out previously running container location

    String lastSeenOn = jobModelManager.jobModel().getContainerToHostValue(containerId, SetContainerHostMapping.HOST_KEY);
    if (!hostAffinityEnabled || lastSeenOn == null) {
      lastSeenOn = ResourceRequestState.ANY_HOST;
    }
    SamzaResourceRequest request = new SamzaResourceRequest(cpu, memory, lastSeenOn, containerId);
    requestResources(request);
  }

  @Override
  public void requestResources(SamzaResourceRequest resourceRequest) {
    log.info("Requesting resources on " + resourceRequest.getPreferredHost() + " for container " + resourceRequest.getContainerID());
    Container container = KubeUtils.createContainer(STREAM_PROCESSOR_CONTAINER_NAME, image, resourceRequest);
    PodBuilder podBuilder = new PodBuilder().editOrNewMetadata()
            .withName(String.format(POD_NAME_FORMAT, appId, resourceRequest.getContainerID()))
            .withOwnerReferences(ownerReference)
            .addToLabels(podLabels).endMetadata()
            .editOrNewSpec()
            .withRestartPolicy(POD_RESTART_POLICY).addToContainers(container).endSpec();

    String preferredHost = resourceRequest.getPreferredHost();
    Pod pod;
    if (preferredHost.equals("ANY_HOST")) {
      // Create a pod with only one container in anywhere
      pod = podBuilder.build();
    } else {
      log.info("Making a preferred host request on " + preferredHost);
      pod = podBuilder.editOrNewSpec().editOrNewAffinity().editOrNewNodeAffinity()
              .addNewPreferredDuringSchedulingIgnoredDuringExecution().withNewPreference()
              .addNewMatchExpression()
                .withKey("kubernetes.io/hostname")
                .withOperator("Equal")
                .withValues(preferredHost).endMatchExpression()
              .endPreference().endPreferredDuringSchedulingIgnoredDuringExecution().endNodeAffinity().endAffinity().endSpec().build();
    }
    client.pods().inNamespace(namespace).create(pod);
    log.info("Created a pod " + pod.getMetadata().getName() + " on " + preferredHost);
  }

  @Override
  public void cancelResourceRequest(SamzaResourceRequest request) {
    // no need to implement
  }

  @Override
  public void releaseResources(SamzaResource resource) {
    // no need to implement
  }

  @Override
  public void launchStreamProcessor(SamzaResource resource, CommandBuilder builder) {
    // no need to implement
  }

  @Override
  public void stopStreamProcessor(SamzaResource resource) {
    client.pods().withName(resource.getResourceID()).delete();
  }

  @Override
  public void stop(SamzaApplicationState.SamzaAppStatus status) {
    log.info("Kubernetes Cluster ResourceManager stopped");
  }
}
