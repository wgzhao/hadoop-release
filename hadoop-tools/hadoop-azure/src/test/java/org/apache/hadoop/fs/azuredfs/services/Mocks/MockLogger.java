/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azuredfs.services.Mocks;

import org.slf4j.Logger;
import org.slf4j.Marker;

import org.apache.hadoop.fs.azuredfs.contracts.log.LogLevel;

public class MockLogger implements Logger {
  public String lastMessage;
  public LogLevel lastLogLevel;
  public boolean debugEnabled = false;
  public boolean tracingEnabled = false;
  public boolean infoEnabled = false;
  public boolean warningEnabled = false;
  public boolean errorEnabled = false;

  public MockLogger() {
    this(true);
  }

  public MockLogger(boolean allLevelsEnabled) {
    this.debugEnabled = allLevelsEnabled;
    this.tracingEnabled = allLevelsEnabled;
    this.infoEnabled = allLevelsEnabled;
    this.warningEnabled = allLevelsEnabled;
    this.errorEnabled = allLevelsEnabled;
  }

  public MockLogger(boolean debugEnabled, boolean tracingEnabled, boolean infoEnabled, boolean warningEnabled,
      boolean errorEnabled) {
    this.debugEnabled = debugEnabled;
    this.tracingEnabled = tracingEnabled;
    this.infoEnabled = infoEnabled;
    this.warningEnabled = warningEnabled;
    this.errorEnabled = errorEnabled;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public boolean isTraceEnabled() {
    return tracingEnabled;
  }

  @Override
  public void trace(String s) {
    lastMessage = s;
    lastLogLevel = LogLevel.Trace;
  }

  @Override
  public void trace(String s, Object o) {

  }

  @Override
  public void trace(String s, Object o, Object o1) {

  }

  @Override
  public void trace(String s, Object... objects) {

  }

  @Override
  public void trace(String s, Throwable throwable) {

  }

  @Override
  public boolean isTraceEnabled(Marker marker) {
    return tracingEnabled;
  }

  @Override
  public void trace(Marker marker, String s) {

  }

  @Override
  public void trace(Marker marker, String s, Object o) {

  }

  @Override
  public void trace(Marker marker, String s, Object o, Object o1) {

  }

  @Override
  public void trace(Marker marker, String s, Object... objects) {

  }

  @Override
  public void trace(Marker marker, String s, Throwable throwable) {

  }

  @Override
  public boolean isDebugEnabled() {
    return debugEnabled;
  }

  @Override
  public void debug(String s) {
    lastMessage = s;
    lastLogLevel = LogLevel.Debug;
  }

  @Override
  public void debug(String s, Object o) {

  }

  @Override
  public void debug(String s, Object o, Object o1) {

  }

  @Override
  public void debug(String s, Object... objects) {

  }

  @Override
  public void debug(String s, Throwable throwable) {

  }

  @Override
  public boolean isDebugEnabled(Marker marker) {
    return false;
  }

  @Override
  public void debug(Marker marker, String s) {

  }

  @Override
  public void debug(Marker marker, String s, Object o) {

  }

  @Override
  public void debug(Marker marker, String s, Object o, Object o1) {

  }

  @Override
  public void debug(Marker marker, String s, Object... objects) {

  }

  @Override
  public void debug(Marker marker, String s, Throwable throwable) {

  }

  @Override
  public boolean isInfoEnabled() {
    return infoEnabled;
  }

  @Override
  public void info(String s) {
    lastMessage = s;
    lastLogLevel = LogLevel.Info;
  }

  @Override
  public void info(String s, Object o) {

  }

  @Override
  public void info(String s, Object o, Object o1) {

  }

  @Override
  public void info(String s, Object... objects) {

  }

  @Override
  public void info(String s, Throwable throwable) {

  }

  @Override
  public boolean isInfoEnabled(Marker marker) {
    return infoEnabled;
  }

  @Override
  public void info(Marker marker, String s) {

  }

  @Override
  public void info(Marker marker, String s, Object o) {

  }

  @Override
  public void info(Marker marker, String s, Object o, Object o1) {

  }

  @Override
  public void info(Marker marker, String s, Object... objects) {

  }

  @Override
  public void info(Marker marker, String s, Throwable throwable) {

  }

  @Override
  public boolean isWarnEnabled() {
    return warningEnabled;
  }

  @Override
  public void warn(String s) {
    lastMessage = s;
    lastLogLevel = LogLevel.Warning;
  }

  @Override
  public void warn(String s, Object o) {

  }

  @Override
  public void warn(String s, Object... objects) {

  }

  @Override
  public void warn(String s, Object o, Object o1) {

  }

  @Override
  public void warn(String s, Throwable throwable) {

  }

  @Override
  public boolean isWarnEnabled(Marker marker) {
    return warningEnabled;
  }

  @Override
  public void warn(Marker marker, String s) {

  }

  @Override
  public void warn(Marker marker, String s, Object o) {

  }

  @Override
  public void warn(Marker marker, String s, Object o, Object o1) {

  }

  @Override
  public void warn(Marker marker, String s, Object... objects) {

  }

  @Override
  public void warn(Marker marker, String s, Throwable throwable) {

  }

  @Override
  public boolean isErrorEnabled() {
    return errorEnabled;
  }

  @Override
  public void error(String s) {
    lastMessage = s;
    lastLogLevel = LogLevel.Error;
  }

  @Override
  public void error(String s, Object o) {

  }

  @Override
  public void error(String s, Object o, Object o1) {

  }

  @Override
  public void error(String s, Object... objects) {

  }

  @Override
  public void error(String s, Throwable throwable) {

  }

  @Override
  public boolean isErrorEnabled(Marker marker) {
    return errorEnabled;
  }

  @Override
  public void error(Marker marker, String s) {

  }

  @Override
  public void error(Marker marker, String s, Object o) {

  }

  @Override
  public void error(Marker marker, String s, Object o, Object o1) {

  }

  @Override
  public void error(Marker marker, String s, Object... objects) {

  }

  @Override
  public void error(Marker marker, String s, Throwable throwable) {

  }
}
