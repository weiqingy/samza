package org.apache.samza.job.kubernetes;

import org.apache.samza.clustermanager.ClusterResourceManager;
import org.apache.samza.clustermanager.ResourceManagerFactory;
import org.apache.samza.clustermanager.SamzaApplicationState;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.JobModelManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubeResourceManagerFactory implements ResourceManagerFactory {
  private static Logger log = LoggerFactory.getLogger(KubeResourceManagerFactory.class);


  @Override
  public ClusterResourceManager getClusterResourceManager(ClusterResourceManager.Callback callback, SamzaApplicationState state) {
    log.info("Creating an instance of a cluster resource manager for Kubernetes. ");
    JobModelManager jobModelManager = state.jobModelManager;
    Config config = jobModelManager.jobModel().getConfig();
    KubeClusterResourceManager manager = new KubeClusterResourceManager(config, jobModelManager, callback, state);
    return manager;
  }
}
