package org.apache.samza.job.kubernetes;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

public class KubernetesClientFactory {

  public static KubernetesClient create() {
    ConfigBuilder builder = new ConfigBuilder();
    Config config = builder.build();

    KubernetesClient client = new DefaultKubernetesClient(config);
    return client;
  }
}
