/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.checkpoint;

import java.io.File;
import java.util.EnumSet;
import java.util.Set;

/**
 * Tracks the state of a single checkpoint installation session.
 * This class replaces multiple boolean flags with a more structured
 * state tracking mechanism using enums.
 */
public class CheckpointInstallSession {
  private final long startTime;
  private File oldDBLocation;
  private File dbBackup;

  // State flags - using enum instead of multiple booleans
  private final Set<InstallationPhase> completedPhases =
      EnumSet.noneOf(InstallationPhase.class);

  private long term;
  private long lastAppliedIndex;

  /**
   * Enum representing different phases of checkpoint installation.
   */
  public enum InstallationPhase {
    BACKGROUND_SERVICES_STOPPED,
    STATE_MACHINE_PAUSED,
    RPC_SERVER_STOPPED,
    METADATA_MANAGER_STOPPED,
    SNAPSHOT_MANAGER_CLOSED,
    DATABASE_REPLACED,
    STATE_RELOADED,
    RPC_SERVER_RESTARTED
  }

  public CheckpointInstallSession() {
    this.startTime = System.nanoTime();
  }

  public long getStartTime() {
    return startTime;
  }

  public File getOldDBLocation() {
    return oldDBLocation;
  }

  public void setOldDBLocation(File oldDBLocation) {
    this.oldDBLocation = oldDBLocation;
  }

  public File getDbBackup() {
    return dbBackup;
  }

  public void setDbBackup(File dbBackup) {
    this.dbBackup = dbBackup;
  }

  public boolean hasCompleted(InstallationPhase phase) {
    return completedPhases.contains(phase);
  }

  public void markCompleted(InstallationPhase phase) {
    completedPhases.add(phase);
  }

  public long getTerm() {
    return term;
  }

  public void setTerm(long term) {
    this.term = term;
  }

  public long getLastAppliedIndex() {
    return lastAppliedIndex;
  }

  public void setLastAppliedIndex(long lastAppliedIndex) {
    this.lastAppliedIndex = lastAppliedIndex;
  }

  public boolean isMetadataManagerStopped() {
    return hasCompleted(InstallationPhase.METADATA_MANAGER_STOPPED);
  }

  public boolean isRpcServerStopped() {
    return hasCompleted(InstallationPhase.RPC_SERVER_STOPPED);
  }

  public boolean isStateReloaded() {
    return hasCompleted(InstallationPhase.STATE_RELOADED);
  }
}


