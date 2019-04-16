#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


FROM openjdk:8-alpine

ARG samza_uid=888

# Before building the docker image, first build and make a Samza distribution following
# the instructions in https://github.com/apache/samza (./gradlew clean releaseSqlShellTarGz)
# If this docker file is being used in the context of building your images from a Samza distribution
# the docker build command should be invoked from the top level directory
# of the Samza distribution. E.g.:
# docker build -t samza:latest -f kubernetes/dockerfiles/Dockerfile .

COPY samza-sql-shell_*.jar /opt/samza/jars

ENV SAMZA_HOME /opt/samza

WORKDIR /opt/samza/work-dir
RUN chmod g+w /opt/samza/work-dir

# Specify the User that the actual main process will run as
USER ${samza_uid}
