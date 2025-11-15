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
import java.nio.file.Path;

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.util.Time;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the installCheckpoint logic that used to live in
 * {@link org.apache.hadoop.ozone.om.OzoneManager}.
 */
public class OMCheckpointInstaller {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMCheckpointInstaller.class);

  private final ServiceLifecycleManager serviceManager;

  public OMCheckpointInstaller(ServiceLifecycleManager serviceManager) {
    this.serviceManager = serviceManager;
  }

  /**
   * Install the provided checkpoint if it is newer than the current OM state.
   * The behaviour matches the original {@code OzoneManager.installCheckpoint}.
   *
   * @return TermIndex for the installed checkpoint or {@code null} if no state
   *     change occurred.
   */
  public TermIndex install(String leaderId, Path checkpointLocation,
      TransactionInfo checkpointTrxnInfo) throws Exception {
    long startTime = Time.monotonicNow();
    File oldDBLocation = serviceManager.getOldDBLocation();
    stopServicesAndPause();

    OzoneManagerRatisServer ratisServer = serviceManager.getRatisServer();
    TermIndex currentTermIndex = ratisServer.getLastAppliedTermIndex();
    long term = currentTermIndex.getTerm();
    long lastAppliedIndex = currentTermIndex.getIndex();

    boolean canProceed = OzoneManagerRatisUtils.verifyTransactionInfo(
        checkpointTrxnInfo, lastAppliedIndex, leaderId, checkpointLocation);

    InstallResult installResult = installCheckpointIfPossible(
        canProceed, leaderId, checkpointLocation, checkpointTrxnInfo,
        oldDBLocation, currentTermIndex, term, lastAppliedIndex);

    term = installResult.getTerm();
    lastAppliedIndex = installResult.getLastAppliedIndex();
    boolean omRpcServerStopped = installResult.isOmRpcServerStopped();
    boolean oldOmMetadataManagerStopped =
        installResult.isOldOmMetadataManagerStopped();

    if (oldOmMetadataManagerStopped) {
      serviceManager.closeSnapshotManager();
    }

    boolean newMetadataManagerStarted = reloadServices(
        oldOmMetadataManagerStopped, lastAppliedIndex, term, currentTermIndex);

    if (omRpcServerStopped && newMetadataManagerStarted) {
      startRpcServer();
    }

    serviceManager.buildDBCheckpointInstallAuditLog(
        leaderId, term, lastAppliedIndex);
    deleteBackup(installResult.getDbBackup());

    TermIndex result = finalizeResult(checkpointTrxnInfo, term,
        lastAppliedIndex);
    if (result != null) {
      serviceManager.logInstallCheckpointCompletion(result,
          Time.monotonicNow() - startTime);
    }
    return result;
  }

  private void stopServicesAndPause() throws Exception {
    try {
      serviceManager.stopKeyManager();
      serviceManager.stopSecretManager();
      serviceManager.stopTrashEmptier();
      serviceManager.invalidateSnapshotCache();
      serviceManager.getRatisServer().getOmStateMachine().pause();
    } catch (Exception ex) {
      LOG.error("Failed to stop/ pause the services. Cannot proceed with " +
          "installing the new checkpoint.");
      restartFrontEndServices();
      throw ex;
    }
  }

  private InstallResult installCheckpointIfPossible(boolean canProceed,
      String leaderId, Path checkpointLocation,
      TransactionInfo checkpointTrxnInfo, File oldDBLocation,
      TermIndex currentTermIndex, long term, long lastAppliedIndex)
      throws Exception {
    File dbBackup = null;
    boolean oldOmMetadataManagerStopped = false;
    boolean omRpcServerStopped = false;
    long time = Time.monotonicNow();

    if (!canProceed) {
      serviceManager.logCheckpointCannotProceedWarning(
          currentTermIndex, checkpointTrxnInfo.getTermIndex());
    } else {
      serviceManager.stopRpcServer();
      serviceManager.setOmRpcServerRunning(false);
      omRpcServerStopped = true;
      serviceManager.logRpcServerStopped(Time.monotonicNow() - time);
      try {
        time = Time.monotonicNow();
        serviceManager.stopMetadataManager();
        oldOmMetadataManagerStopped = true;
        serviceManager.logMetadataManagerStopped(Time.monotonicNow() - time);
      } catch (Exception e) {
        String errorMsg = "Failed to stop metadataManager. Cannot proceed " +
            "with installing the new checkpoint.";
        LOG.error(errorMsg, e);
        serviceManager.failCheckpointInstall(errorMsg, e);
        throw new RuntimeException(errorMsg, e);
      }
      try {
        time = Time.monotonicNow();
        dbBackup = serviceManager.replaceOMDBWithCheckpoint(lastAppliedIndex,
            oldDBLocation, checkpointLocation);
        term = checkpointTrxnInfo.getTerm();
        lastAppliedIndex = checkpointTrxnInfo.getTransactionIndex();
        LOG.info("Replaced DB with checkpoint from OM: {}, term: {}, index: " +
            "{}, time: {} ms", leaderId, term, lastAppliedIndex,
            Time.monotonicNow() - time);
      } catch (Exception e) {
        LOG.error("Failed to install Snapshot from {} as OM failed to replace" +
            " DB with downloaded checkpoint. Reloading old OM state.", leaderId,
            e);
      }
    }

    return new InstallResult(dbBackup, term, lastAppliedIndex,
        oldOmMetadataManagerStopped, omRpcServerStopped);
  }

  private boolean reloadServices(boolean oldOmMetadataManagerStopped,
      long lastAppliedIndex, long term, TermIndex currentTermIndex) {
    try {
      if (oldOmMetadataManagerStopped) {
        long time = Time.monotonicNow();
        serviceManager.reloadOMState();
        serviceManager.setTransactionInfo(TransactionInfo
            .valueOf(currentTermIndex));
        serviceManager.getRatisServer().getOmStateMachine()
            .unpause(lastAppliedIndex, term);
        serviceManager.logReloadedOmState(term, lastAppliedIndex,
            Time.monotonicNow() - time);
        return true;
      }
      restartFrontEndServices();
      serviceManager.getRatisServer().getOmStateMachine()
          .unpause(lastAppliedIndex, term);
      serviceManager.logOmDbNotStopped(term, lastAppliedIndex);
      return false;
    } catch (Exception ex) {
      String errorMsg = "Failed to reload OM state and instantiate services.";
      LOG.error(errorMsg, ex);
      serviceManager.failCheckpointInstall(errorMsg, ex);
      throw new RuntimeException(errorMsg, ex);
    }
  }

  private void restartFrontEndServices() throws Exception {
    serviceManager.startKeyManager();
    serviceManager.startSecretManagerIfNecessary();
    serviceManager.startTrashEmptier();
  }

  private void startRpcServer() {
    try {
      long time = Time.monotonicNow();
      serviceManager.startRpcServer();
      serviceManager.setOmRpcServerRunning(true);
      serviceManager.logRpcServerRestarted(Time.monotonicNow() - time);
    } catch (Exception e) {
      String errorMsg = "Failed to start RPC Server.";
      LOG.error(errorMsg, e);
      serviceManager.failCheckpointInstall(errorMsg, e);
      throw new RuntimeException(errorMsg, e);
    }
  }

  private void deleteBackup(File dbBackup) {
    try {
      if (dbBackup != null) {
        FileUtils.deleteFully(dbBackup);
      }
    } catch (Exception e) {
      LOG.error("Failed to delete the backup of the original DB {}", dbBackup,
          e);
    }
  }

  private TermIndex finalizeResult(TransactionInfo checkpointTrxnInfo,
      long term, long lastAppliedIndex) {
    if (lastAppliedIndex != checkpointTrxnInfo.getTransactionIndex()) {
      return null;
    }
    return TermIndex.valueOf(term, lastAppliedIndex);
  }

  private static final class InstallResult {
    private final File dbBackup;
    private final long term;
    private final long lastAppliedIndex;
    private final boolean oldOmMetadataManagerStopped;
    private final boolean omRpcServerStopped;

    InstallResult(File dbBackup, long term, long lastAppliedIndex,
        boolean oldOmMetadataManagerStopped, boolean omRpcServerStopped) {
      this.dbBackup = dbBackup;
      this.term = term;
      this.lastAppliedIndex = lastAppliedIndex;
      this.oldOmMetadataManagerStopped = oldOmMetadataManagerStopped;
      this.omRpcServerStopped = omRpcServerStopped;
    }

    File getDbBackup() {
      return dbBackup;
    }

    long getTerm() {
      return term;
    }

    long getLastAppliedIndex() {
      return lastAppliedIndex;
    }

    boolean isOldOmMetadataManagerStopped() {
      return oldOmMetadataManagerStopped;
    }

    boolean isOmRpcServerStopped() {
      return omRpcServerStopped;
    }
  }
}

