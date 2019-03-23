package org.apache.samza.job.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import org.apache.samza.clustermanager.SamzaResourceRequest;

public class KubernetesUtils {

  public static final String POD_NAME_PREFIX = "Pod-";

  public static final String POD_RESTART_POLICY = "Always";

  public static Pod createPod(String name, OwnerReference ownerReference, String restartPolicy
          , Container container) {
    return new PodBuilder().editOrNewMetadata().withName(name).withOwnerReferences(ownerReference).endMetadata()
            .editOrNewSpec().withRestartPolicy(restartPolicy).addToContainers(container).endSpec().build();
  }

  public static Container createContainer(String containerId, String image, SamzaResourceRequest resourceRequest) {
    Quantity memQuantity = new QuantityBuilder(false)
            .withAmount(String.valueOf(resourceRequest.getMemoryMB())).withFormat("Mi").build();
    Quantity cpuQuantity = new QuantityBuilder(false)
            .withAmount(String.valueOf(resourceRequest.getNumCores())).build();
    return new ContainerBuilder().withName(containerId).withImage(image).editOrNewResources()
            .addToRequests("memory", memQuantity).addToRequests("cpu", cpuQuantity).endResources().build();
  }
}
