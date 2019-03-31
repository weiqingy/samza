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

import static org.apache.samza.job.kubernetes.KubeUtils.*;

public class KubeClusterResourceManager extends ClusterResourceManager {
  private static final Logger log = LoggerFactory.getLogger(KubeClusterResourceManager.class);

  KubernetesClient client = null;
  private final Object lock = new Object();
  //TODO
  private String jobId = "";
  private String image = "";
  private String namespace = "";
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
    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    this.hostAffinityEnabled = clusterManagerConfig.getHostAffinityEnabled();

  }

  @Override
  public void start() {
    log.info("Kubernetes Cluster ResourceManager started, starting watcher");
    startPodWatcher();
  }

  private void createOwnerReferences() {
    // The pod needs to pass in MY_POD_NAME env
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
              createNewStreamProcessor(pod);
            }
            break;
          case ERROR:
            log.info("Pod " + pod.getMetadata().getName() + " received error.");
            if (isPodFailed(pod)) {
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
    Container container = KubeUtils.createContainer(CONTAINER_NAME, image, resourceRequest);
    PodBuilder podBuilder = new PodBuilder().editOrNewMetadata()
            .withName(String.format(POD_NAME_FORMAT, jobId, resourceRequest.getContainerID()))
            .withOwnerReferences(ownerReference)
            .addToLabels(podLabels).endMetadata()
            .editOrNewSpec()
            .withRestartPolicy(POD_RESTART_POLICY).addToContainers(container).endSpec();

    String preferredHost = resourceRequest.getPreferredHost();
    Pod pod = null;
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
