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

package org.apache.samza.sql.client.cli;

import org.apache.samza.sql.client.interfaces.EnvironmentVariableHandlerImpl;

import java.io.OutputStream;
import java.io.PrintStream;


public class CliShellEnvironmentVariableHandler extends EnvironmentVariableHandlerImpl {
  private static final String SHELL_DEBUG = "shell.debug";

  public CliShellEnvironmentVariableHandler() {
    specs.put(SHELL_DEBUG, new String[] {"true", "false"}, "false");
  }

  protected boolean processEnvironmentVariable(String envName, String value) {
    switch (envName) {
      case SHELL_DEBUG:
        value = value.toLowerCase();
        if (value.equals("true")) {
          enableJavaSystemOutAndErr();
        } else if (value.equals("false")) {
          disableJavaSystemOutAndErr();
        }
        return true;
      default:
        return false;
    }
  }

  // We control terminal directly; Forbid any Java System.out and System.err stuff so
  // any underlying output will not mess up the console
  private void disableJavaSystemOutAndErr() {
    PrintStream ps = new PrintStream(new NullOutputStream());
    System.setOut(ps);
    System.setErr(ps);
  }

  private void enableJavaSystemOutAndErr() {
    System.setOut(stdout);
    System.setErr(stderr);
  }

  private static PrintStream stdout = System.out;
  private static PrintStream stderr = System.err;

  private class NullOutputStream extends OutputStream {
    public void close() {
    }

    public void flush() {
    }

    public void write(byte[] b) {
    }

    public void write(byte[] b, int off, int len) {
    }

    public void write(int b) {
    }
  }
}
