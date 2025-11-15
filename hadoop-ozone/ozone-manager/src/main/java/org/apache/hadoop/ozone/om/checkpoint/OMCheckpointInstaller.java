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
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.util.Time;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class responsible for installing checkpoints in OzoneManager.
 * This class encapsulates the checkpoint installation logic that was previously
 * embedded in the OzoneManager class, improving code organization and maintainability.
 */
public class OMCheckpointInstaller {

  private static final Logger LOG = LoggerFactory.getLogger(OMCheckpointInstaller.class);

  private final ServiceLifecycleManager serviceManager;

  /**
   * Constructor for OMCheckpointInstaller.
   * @param serviceManager the service lifecycle manager to use for service operations
   */
  public OMCheckpointInstaller(ServiceLifecycleManager serviceManager) {
    this.serviceManager = serviceManager;
  }

  /**
   * Install checkpoint. If the checkpoint snapshot index is greater than
   * OM's last applied transaction index, then re-initialize the OM
   * state via this checkpoint. Before re-initializing OM state, the OM Ratis
   * server should be stopped so that no new transactions can be applied.
   *
   * @param leaderId leader OM node ID
   * @param checkpointLocation path to the checkpoint
   * @param checkpointTrxnInfo transaction info from the checkpoint
   * @return TermIndex if successful, null otherwise
   * @throws Exception if installation fails
   */
  public TermIndex install(String leaderId,
      Path checkpointLocation,
      TransactionInfo checkpointTrxnInfo) throws Exception {
    long startTime = Time.monotonicNow();

    // Get current state before making any changes
    File oldDBLocation = serviceManager.getOldDBLocation();

    stopServicesAndPause();

    TermIndex termIndex = serviceManager.getRatisServer().getLastAppliedTermIndex();
    long term = termIndex.getTerm();
    long lastAppliedIndex = termIndex.getIndex();

    // Check if current applied log index is smaller than the downloaded
    // checkpoint transaction index. If yes, proceed by stopping the ratis
    // server so that the OM state can be re-initialized. If no then do not
    // proceed with installSnapshot.
    boolean canProceed = OzoneManagerRatisUtils.verifyTransactionInfo(
        checkpointTrxnInfo, lastAppliedIndex, leaderId, checkpointLocation);

    CheckpointInstallParams params = new CheckpointInstallParams(
        canProceed, leaderId, oldDBLocation, checkpointLocation,
        checkpointTrxnInfo, termIndex);
    CheckpointInstallResult result = installCheckpointIfPossible(params);
    File dbBackup = result.getDbBackup();
    term = result.getTerm();
    lastAppliedIndex = result.getLastAppliedIndex();
    boolean oldOmMetadataManagerStopped =
        result.isOldOmMetadataManagerStopped();
    boolean omRpcServerStopped = result.isOmRpcServerStopped();

    if (oldOmMetadataManagerStopped) {
      // Close snapDiff's rocksDB instance only if metadataManager gets closed.
      serviceManager.closeSnapshotManager();
    }

    // Reload the OM DB store with the new checkpoint.
    // Restart (unpause) the state machine and update its last applied index
    // to the installed checkpoint's snapshot index.
    boolean newMetadataManagerStarted = reloadServices(
        oldOmMetadataManagerStopped, termIndex, term, lastAppliedIndex);

    if (omRpcServerStopped && newMetadataManagerStarted) {
      startRpcServerIfNeeded();
    }
    serviceManager.logCheckpointInstallAudit(leaderId, term, lastAppliedIndex);

    // Delete the backup DB
    try {
      if (dbBackup != null) {
        FileUtils.deleteFully(dbBackup);
      }
    } catch (Exception e) {
      LOG.error("Failed to delete the backup of the original DB {}",
          dbBackup, e);
    }

    if (lastAppliedIndex != checkpointTrxnInfo.getTransactionIndex()) {
      // Install Snapshot failed and old state was reloaded. Return null to
      // Ratis to indicate that installation failed.
      return null;
    }

    // TODO: We should only return the snpashotIndex to the leader.
    //  Should be fixed after RATIS-586
    TermIndex newTermIndex = TermIndex.valueOf(term, lastAppliedIndex);
    LOG.info("Install Checkpoint is finished with Term: {} and Index: {}. " +
        "Spend {} ms.", newTermIndex.getTerm(), newTermIndex.getIndex(),
        (Time.monotonicNow() - startTime));
    return newTermIndex;
  }

  /**
   * Stop background services and pause the state machine.
   * @throws Exception if stopping services fails
   */
  private void stopServicesAndPause() throws Exception {
    try {
      // Stop Background services
      serviceManager.stopKeyManager();
      serviceManager.stopSecretManager();
      serviceManager.stopTrashEmptier();
      // Note: omSnapshotManager.invalidateCache() should be handled by
      // serviceManager

      // Pause the State Machine so that no new transactions can be applied.
      // This action also clears the OM Double Buffer so that if there are any
      // pending transactions in the buffer, they are discarded.
      serviceManager.getRatisServer().getOmStateMachine().pause();
    } catch (Exception e) {
      LOG.error("Failed to stop/ pause the services. Cannot proceed with " +
          "installing the new checkpoint.");
      // Stop the checkpoint install process and restart the services.
      try {
        serviceManager.startKeyManager(serviceManager.getConfiguration());
        serviceManager.startSecretManagerIfNecessary();
        serviceManager.startTrashEmptier(serviceManager.getConfiguration());
      } catch (Exception restartEx) {
        LOG.error("Failed to restart services after initial failure",
            restartEx);
      }
      throw e;
    }
  }

  /**
   * Parameters for checkpoint installation operation.
   */
  private static class CheckpointInstallParams {
    private final boolean canProceed;
    private final String leaderId;
    private final File oldDBLocation;
    private final Path checkpointLocation;
    private final TransactionInfo checkpointTrxnInfo;
    private final TermIndex termIndex;

    CheckpointInstallParams(boolean canProceed, String leaderId,
        File oldDBLocation, Path checkpointLocation,
        TransactionInfo checkpointTrxnInfo, TermIndex termIndex) {
      this.canProceed = canProceed;
      this.leaderId = leaderId;
      this.oldDBLocation = oldDBLocation;
      this.checkpointLocation = checkpointLocation;
      this.checkpointTrxnInfo = checkpointTrxnInfo;
      this.termIndex = termIndex;
    }

    boolean canProceed() {
      return canProceed;
    }

    String getLeaderId() {
      return leaderId;
    }

    File getOldDBLocation() {
      return oldDBLocation;
    }

    Path getCheckpointLocation() {
      return checkpointLocation;
    }

    TransactionInfo getCheckpointTrxnInfo() {
      return checkpointTrxnInfo;
    }

    TermIndex getTermIndex() {
      return termIndex;
    }

    long getTerm() {
      return termIndex.getTerm();
    }

    long getLastAppliedIndex() {
      return termIndex.getIndex();
    }
  }

  /**
   * Result class for checkpoint installation operation.
   */
  private static class CheckpointInstallResult {
    private File dbBackup;
    private long term;
    private long lastAppliedIndex;
    private boolean oldOmMetadataManagerStopped;
    private boolean omRpcServerStopped;

    CheckpointInstallResult(File dbBackup, long term, long lastAppliedIndex,
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

  /**
   * Install checkpoint if possible.
   * @param params installation parameters
   * @return installation result
   */
  private CheckpointInstallResult installCheckpointIfPossible(
      CheckpointInstallParams params) {
    File dbBackup = null;
    boolean oldOmMetadataManagerStopped = false;
    boolean omRpcServerStopped = false;
    long term = params.getTerm();
    long lastAppliedIndex = params.getLastAppliedIndex();
    long time = Time.monotonicNow();

    if (params.canProceed()) {
      // Stop RPC server before stop metadataManager
      serviceManager.stopRpcServer();
      omRpcServerStopped = true;
      LOG.info("RPC server is stopped. Spend " +
          (Time.monotonicNow() - time) + " ms.");
      try {
        // Stop old metadataManager before replacing DB Dir
        time = Time.monotonicNow();
        serviceManager.stopMetadataManager();
        oldOmMetadataManagerStopped = true;
        LOG.info("metadataManager is stopped. Spend " +
            (Time.monotonicNow() - time) + " ms.");
      } catch (Exception e) {
        String errorMsg = "Failed to stop metadataManager. Cannot proceed " +
            "with installing the new checkpoint.";
        LOG.error(errorMsg);
        throw new RuntimeException(errorMsg, e);
      }
      try {
        time = Time.monotonicNow();
        dbBackup = serviceManager.replaceOMDBWithCheckpoint(
            lastAppliedIndex, params.getOldDBLocation(),
            params.getCheckpointLocation());
        term = params.getCheckpointTrxnInfo().getTerm();
        lastAppliedIndex =
            params.getCheckpointTrxnInfo().getTransactionIndex();
        LOG.info("Replaced DB with checkpoint from OM: {}, term: {}, " +
            "index: {}, time: {} ms", params.getLeaderId(), term,
            lastAppliedIndex, Time.monotonicNow() - time);
      } catch (Exception e) {
        LOG.error("Failed to install Snapshot from {} as OM failed to " +
            "replace DB with downloaded checkpoint. Reloading old OM state.",
            params.getLeaderId(), e);
      }
    } else {
      LOG.warn("Cannot proceed with InstallSnapshot as OM is at TermIndex {} " +
          "and checkpoint has lower TermIndex {}. Reloading old state of OM.",
          params.getTermIndex(),
          params.getCheckpointTrxnInfo().getTermIndex());
    }

    return new CheckpointInstallResult(dbBackup, term, lastAppliedIndex,
        oldOmMetadataManagerStopped, omRpcServerStopped);
  }

  /**
   * Reload services after checkpoint installation.
   * @param oldOmMetadataManagerStopped whether metadata manager was stopped
   * @param termIndex current term index
   * @param term current term
   * @param lastAppliedIndex last applied index
   * @return whether new metadata manager was started
   * @throws RuntimeException if reloading fails
   */
  private boolean reloadServices(boolean oldOmMetadataManagerStopped,
      TermIndex termIndex, long term, long lastAppliedIndex) {
    try {
      if (oldOmMetadataManagerStopped) {
        long time = Time.monotonicNow();
        serviceManager.reloadOMState();
        serviceManager.setTransactionInfo(TransactionInfo.valueOf(termIndex));
        serviceManager.getRatisServer().getOmStateMachine()
            .unpause(lastAppliedIndex, term);
        LOG.info("Reloaded OM state with Term: {} and Index: {}. Spend {} ms",
            term, lastAppliedIndex, Time.monotonicNow() - time);
        return true;
      } else {
        // OM DB is not stopped. Start the services.
        serviceManager.startKeyManager(serviceManager.getConfiguration());
        serviceManager.startSecretManagerIfNecessary();
        serviceManager.startTrashEmptier(serviceManager.getConfiguration());
        serviceManager.getRatisServer().getOmStateMachine()
            .unpause(lastAppliedIndex, term);
        LOG.info("OM DB is not stopped. Started services with Term: {} and " +
            "Index: {}", term, lastAppliedIndex);
        return false;
      }
    } catch (Exception ex) {
      String errorMsg = "Failed to reload OM state and instantiate services.";
      throw new RuntimeException(errorMsg, ex);
    }
  }

  /**
   * Start RPC server if needed.
   * @throws RuntimeException if starting RPC server fails
   */
  private void startRpcServerIfNeeded() {
    try {
      long time = Time.monotonicNow();
      serviceManager.startRpcServer();
      LOG.info("RPC server is re-started. Spend " +
          (Time.monotonicNow() - time) + " ms.");
    } catch (Exception e) {
      String errorMsg = "Failed to start RPC Server.";
      throw new RuntimeException(errorMsg, e);
    }
  }

}
