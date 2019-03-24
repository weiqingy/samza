package org.apache.samza.job.kubernetes;

import io.fabric8.kubernetes.api.model.*;
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

import java.util.ArrayList;

import static org.apache.samza.job.kubernetes.KubernetesUtils.POD_NAME_PREFIX;
import static org.apache.samza.job.kubernetes.KubernetesUtils.POD_RESTART_POLICY;

public class KubernetesClusterResourceManager extends ClusterResourceManager {
  private static final Logger log = LoggerFactory.getLogger(KubernetesClusterResourceManager.class);

    KubernetesClient client = null;
    private final Object lock = new Object();

    KubernetesClusterResourceManager(Config config, JobModelManager jobModelManager,
                                     ClusterResourceManager.Callback callback, SamzaApplicationState samzaAppState ) {
      //TODO pass in callback
      super(null);
      client = KubernetesClientFactory.create();
    }

    @Override
    public void start() {

    }

    @Override
    public void requestResources(SamzaResourceRequest resourceRequest) {
      log.info("Requesting resources on " + resourceRequest.getPreferredHost() + " for container " + resourceRequest.getContainerID());
      String preferredHost = resourceRequest.getPreferredHost();
      Container container = KubernetesUtils.createContainer(resourceRequest.getContainerID(), "imageName", resourceRequest);
      PodBuilder podBuilder = new PodBuilder().editOrNewMetadata().withName(POD_NAME_PREFIX + resourceRequest.getContainerID())
              .withOwnerReferences(new ArrayList<>()).endMetadata()
              .editOrNewSpec().withRestartPolicy(POD_RESTART_POLICY).addToContainers(container).endSpec();

      if (preferredHost.equals("ANY_HOST")) {
        log.info("Making a request for ANY_HOST ");
        //TODO setup owner references
        Pod pod = podBuilder.build();
        // Create a pod with only one container in anywhere
        client.pods().create(pod);
      } else {
        log.info("Making a preferred host request on " + preferredHost);
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
      client.pods().withName(resource.getResourceID()).delete();
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
