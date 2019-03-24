package org.apache.samza.job.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import org.apache.samza.clustermanager.ClusterResourceManager;
import org.apache.samza.clustermanager.SamzaApplicationState;
import org.apache.samza.clustermanager.SamzaResource;
import org.apache.samza.clustermanager.SamzaResourceRequest;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.job.CommandBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.samza.job.kubernetes.KubeUtils.CONTAINER_NAME;
import static org.apache.samza.job.kubernetes.KubeUtils.POD_NAME_PREFIX;
import static org.apache.samza.job.kubernetes.KubeUtils.POD_RESTART_POLICY;

public class KubeClusterResourceManager extends ClusterResourceManager {
  private static final Logger log = LoggerFactory.getLogger(KubeClusterResourceManager.class);

  KubernetesClient client = null;
  private final Object lock = new Object();
  //TODO
  private String jobId = "";
  private String image = "";
  private final Map<String, String> podLabels = new HashMap<>();

  KubeClusterResourceManager(Config config, JobModelManager jobModelManager,
                             ClusterResourceManager.Callback callback, SamzaApplicationState samzaAppState ) {
    super(callback);
    client = KubeClientFactory.create();
  }

  @Override
  public void start() {
    log.info("Kubernetes Cluster ResourceManager started, starting watcher");
    startPodWatcher();
  }

  private void createOwnerReferences() {

  }

  public void startPodWatcher() {
    Watcher watcher = new Watcher<Pod>() {
      @Override
      public void eventReceived(Action action, Pod pod) {
        log.info("Pod watcher received action " + action + " for pod " + pod.getMetadata().getName());
      }
      @Override
      public void onClose(KubernetesClientException e) {
        log.error("Pod watcher closed", e);
      }
    };

    client.pods().withLabels(podLabels).watch(watcher);
  }

  @Override
  public void requestResources(SamzaResourceRequest resourceRequest) {
    log.info("Requesting resources on " + resourceRequest.getPreferredHost() + " for container " + resourceRequest.getContainerID());
    Container container = KubeUtils.createContainer(CONTAINER_NAME, image, resourceRequest);
    PodBuilder podBuilder = new PodBuilder().editOrNewMetadata()
            .withName(jobId + "-" + resourceRequest.getContainerID())
            .withOwnerReferences(new ArrayList<>())
            .addToLabels(podLabels).endMetadata()
            .editOrNewSpec()
            .withRestartPolicy(POD_RESTART_POLICY).addToContainers(container).endSpec();

    String preferredHost = resourceRequest.getPreferredHost();
    if (preferredHost.equals("ANY_HOST")) {
      log.info("Making a request for ANY_HOST ");
      Pod pod = podBuilder.build();
      // Create a pod with only one container in anywhere
      client.pods().create(pod);
    } else {
      log.info("Making a preferred host request on " + preferredHost);
      //TODO make affinity request
      podBuilder.editOrNewSpec().editOrNewAffinity().editOrNewNodeAffinity();
    }
    client.pods().inNamespace("namespace<TODO>").createNew();
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
