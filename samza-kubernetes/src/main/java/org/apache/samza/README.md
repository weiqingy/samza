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

# Configurations
- app.image
- kube.api.namespace 
- kube.samza.log.container.path
- kube.samza.log.host.path
- cluster-manager.container.memory.mb
- cluster-manager.container.cpu.cores

# How to set the log folder for samza job
By default, the log is written inside the container. If the container exits, the log will be lost. If we want to persist
the log after container exits, one way to achieve that is to mount a host path into the container so that the logs will
be written to the local host folder. Below are the config properties

- `kube.samza.log.container.path`: the path inside the container where the log will be written to, by default it's '/tmp'
- `kube.samza.log.host.path`: the path on the local host that will be mounted to the container.

For example, if set `kube.samza.log.container.path=/var/log` and `kube.samza.log.host.path=/tmp/log`. The logs will be 
written into "/var/log" inside the container, and "/tmp/log" on the local host. After the container exits, the logs will
still remain on local host "/tmp/log". Note that, extra mechanism will be required to clean the left over logs.
