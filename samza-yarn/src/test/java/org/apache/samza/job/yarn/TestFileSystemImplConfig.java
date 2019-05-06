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
package org.apache.samza.job.yarn;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class TestFileSystemImplConfig {
  private static final Logger log = LoggerFactory.getLogger(TestFileSystemImplConfig.class);
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testFileSystemImplConfigSuccess() {
    String envlist = "{\"systems.kafka.consumer.zookeeper.connect\":\"undercooked-ladybird-cp-zookeeper-0.undercooked-ladybird-cp-zookeeper-headless:2181\",\"systems.kafka.samza.factory\":\"org.apache.samza.system.kafka.KafkaSystemFactory\",\"job.coordinator.monitor-partition-change.frequency.ms\":\"300000\",\"job.coordinator.system\":\"kafka\",\"job.id\":\"3\",\"job.name\":\"wikipedia-application\",\"systems.kafka.default.stream.replication.factor\":\"1\",\"systems.kafka.producer.bootstrap.servers\":\"undercooked-ladybird-cp-kafka-headless:9092\"}";
    try {
      //Read and parse the coordinator system config.
      log.info("Parsing coordinator system config {}", envlist);
      System.out.println("Parsing coordinator system config: " + envlist);

      System.out.println(envlist);
      MapConfig coordinatorSystemConfig =
          new MapConfig(SamzaObjectMapper.getObjectMapper().readValue(envlist, Config.class));
    } catch (IOException e) {
      log.error("Exception while reading coordinator stream config {}", e);
      System.out.println("Exception while reading coordinator stream config: " + e);
      throw new SamzaException(e);
    }
    /*Map<String, String> configMap = new HashMap<>();

    configMap.put("fs.http.impl", "org.apache.samza.HttpFileSystem");
    configMap.put("fs.myscheme.impl", "org.apache.samza.MySchemeFileSystem");

    Config conf = new MapConfig(configMap);

    FileSystemImplConfig manager = new FileSystemImplConfig(conf);
    assertEquals(2, manager.getSchemes().size());
    assertEquals("http", manager.getSchemes().get(0));
    assertEquals("myscheme", manager.getSchemes().get(1));*/
  }

  @Test
  public void testNullConfig() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("config cannot be null");
    FileSystemImplConfig manager = new FileSystemImplConfig(null);
  }

  @Test
  public void testSchemeWithSubkeys() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("fs.http.impl", "org.apache.samza.HttpFileSystem");
    configMap.put("fs.myscheme.impl", "org.apache.samza.MySchemeFileSystem");
    configMap.put("fs.http.impl.key1", "val1");
    configMap.put("fs.http.impl.key2", "val2");
    Config conf = new MapConfig(configMap);

    FileSystemImplConfig manager = new FileSystemImplConfig(conf);

    Map<String, String> expectedFsHttpImplConfs = ImmutableMap.of( //Scheme with additional subkeys
        "fs.http.impl", "org.apache.samza.HttpFileSystem",
        "fs.http.impl.key1", "val1",
        "fs.http.impl.key2", "val2"
    );

    Map<String, String> expectedFsMyschemeImplConfs = ImmutableMap.of( // Scheme without subkeys
        "fs.myscheme.impl", "org.apache.samza.MySchemeFileSystem"
    );

    assertEquals(Arrays.asList("http", "myscheme"), manager.getSchemes());
    assertEquals(expectedFsHttpImplConfs, manager.getSchemeConfig("http"));
    assertEquals(expectedFsMyschemeImplConfs, manager.getSchemeConfig("myscheme"));
  }
}
