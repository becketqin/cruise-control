/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import com.linkedin.kafka.cruisecontrol.common.BalancingAction;
import com.linkedin.kafka.cruisecontrol.common.KafkaCruiseControlThreadFactory;
import com.linkedin.kafka.cruisecontrol.common.MetadataClient;
import com.linkedin.kafka.cruisecontrol.monitor.LoadMonitor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

import static com.linkedin.kafka.cruisecontrol.executor.ExecutionTask.State.*;


/**
 * Executor for Kafka GoalOptimizer.
 * <p>
 * The executor class is responsible for talking to the Kafka cluster to execute the rebalance proposals.
 *
 * The executor is not thread safe.
 */
public class Executor {
  private static final Logger LOG = LoggerFactory.getLogger(Executor.class);
  // The execution progress is controlled by the ExecutionTaskManager.
  private final ExecutionTaskManager _executionTaskManager;
  private final MetadataClient _metadataClient;
  private final long _statusCheckingIntervalMs;
  private final ExecutorService _proposalExecutor;
  private final Pattern _excludedTopics;
  private final String _zkConnect;
  private volatile ZkUtils _zkUtils;

  // Some state for external service to query
  private AtomicReference<ExecutorState.State> _state;
  private volatile boolean _stopRequested;
  private volatile int _numFinishedPartitionMovements;
  private volatile long _finishedDataMovementInMB;

  /**
   * The executor class that execute the proposals generated by optimizer.
   *
   * @param config The configurations for Cruise Control.
   */
  public Executor(KafkaCruiseControlConfig config, Time time, MetricRegistry dropwizardMetricRegistry) {
    _executionTaskManager =
        new ExecutionTaskManager(config.getInt(KafkaCruiseControlConfig.NUM_CONCURRENT_PARTITION_MOVEMENTS_PER_BROKER_CONFIG),
                                 config.getInt(KafkaCruiseControlConfig.NUM_CONCURRENT_LEADER_MOVEMENTS_CONFIG),
                                 dropwizardMetricRegistry);
    _zkConnect = config.getString(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG);
    _metadataClient = new MetadataClient(config, new Metadata(), -1L, time);
    _statusCheckingIntervalMs = config.getLong(KafkaCruiseControlConfig.EXECUTION_PROGRESS_CHECK_INTERVAL_MS_CONFIG);
    _excludedTopics = Pattern.compile(config.getString(KafkaCruiseControlConfig.TOPICS_EXCLUDED_FROM_PARTITION_MOVEMENT_CONFIG));
    _proposalExecutor =
        Executors.newSingleThreadExecutor(new KafkaCruiseControlThreadFactory("ProposalExecutor", false, LOG));
    _state = new AtomicReference<>(ExecutorState.State.NO_TASK_IN_PROGRESS);
    _stopRequested = false;
  }

  /**
   * Check whether the executor is executing a set of proposals.
   */
  public ExecutorState state() {
    switch (_state.get()) {
      case NO_TASK_IN_PROGRESS:
        return ExecutorState.noTaskInProgress();
      case EXECUTION_STARTED:
        return ExecutorState.executionStarted();
      case LEADER_MOVEMENT_TASK_IN_PROGRESS:
        return ExecutorState.leaderMovementInProgress();
      case REPLICA_MOVEMENT_TASK_IN_PROGRESS:
        ExecutorState currState =
            ExecutorState.replicaMovementInProgress(_numFinishedPartitionMovements,
                                                    _executionTaskManager.remainingPartitionMovements(),
                                                    _executionTaskManager.inProgressTasks(),
                                                    _executionTaskManager.abortingTasks(),
                                                    _executionTaskManager.abortedTasks(),
                                                    _executionTaskManager.deadTasks(),
                                                    _executionTaskManager.remainingDataToMoveInMB(),
                                                    _finishedDataMovementInMB);
        if (_state.get() == ExecutorState.State.REPLICA_MOVEMENT_TASK_IN_PROGRESS) {
          return currState;
        } else {
          return state();
        }
      case STOPPING_EXECUTION:
        return ExecutorState.stopping(_numFinishedPartitionMovements,
                                      _executionTaskManager.remainingPartitionMovements(),
                                      _executionTaskManager.inProgressTasks(),
                                      _executionTaskManager.abortingTasks(),
                                      _executionTaskManager.abortedTasks(),
                                      _executionTaskManager.deadTasks(),
                                      _executionTaskManager.remainingDataToMoveInMB(),
                                      _finishedDataMovementInMB);
      default:
        throw new IllegalStateException("Should never be here!");
    }
  }

  /**
   * Kick off the execution.
   */
  public void startExecution(LoadMonitor loadMonitor) {
    _zkUtils = ZkUtils.apply(_zkConnect, 30000, 30000, false);
    try {
      if (!ExecutorUtils.partitionsBeingReassigned(_zkUtils).isEmpty()) {
        throw new IllegalStateException("There are ongoing partition reassignments.");
      }
      if (_state.compareAndSet(ExecutorState.State.NO_TASK_IN_PROGRESS, ExecutorState.State.EXECUTION_STARTED)) {
        _proposalExecutor.submit(new ProposalExecution(loadMonitor));
      } else {
        throw new IllegalStateException("Cannot execute proposals because the executor is in " + _state + " state.");
      }
    } finally {
      _zkUtils.close();
    }
  }

  public void stopExecution() {
    if (_state.get() != ExecutorState.State.NO_TASK_IN_PROGRESS) {
      _state.set(ExecutorState.State.STOPPING_EXECUTION);
      _stopRequested = true;
    }
  }

  /**
   * Shutdown the executor.
   */
  public void shutdown() {
    LOG.info("Shutting down executor.");
    if (_state.get() != ExecutorState.State.NO_TASK_IN_PROGRESS) {
      LOG.warn("Shutdown executor may take long because execution is still in progress.");
    }
    _proposalExecutor.shutdown();

    try {
      _proposalExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for anomaly detector to shutdown.");
    }
    LOG.info("Executor shutdown completed.");
  }

  /**
   * Add the given balancing proposals for execution.
   */
  public void addBalancingProposals(Collection<BalancingProposal> proposals,
                                    Collection<Integer> unthrottledBrokers) {
    if (_state.get() != ExecutorState.State.NO_TASK_IN_PROGRESS) {
      throw new IllegalStateException("Cannot add new proposals while the execution is in progress.");
    }
    // Remove any proposal that involves an excluded topic. This should not happen but if it happens we want to
    // detect this and avoid executing the proposals for those topics.
    Iterator<BalancingProposal> iter = proposals.iterator();
    while (iter.hasNext()) {
      BalancingProposal proposal = iter.next();
      if (_excludedTopics.matcher(proposal.topic()).matches()
          && proposal.balancingAction() == BalancingAction.REPLICA_MOVEMENT) {
        LOG.warn("Ignoring balancing proposal {} because the topics is in the excluded topic set {}",
                 proposal, _excludedTopics);
        iter.remove();
      }
    }
    _executionTaskManager.addBalancingProposals(proposals, unthrottledBrokers);
  }

  private class ProposalExecution implements Runnable {
    private final LoadMonitor _loadMonitor;
    ProposalExecution(LoadMonitor loadMonitor) {
      _loadMonitor = loadMonitor;
    }

    public void run() {
      LOG.info("Starting executing balancing proposals.");
      execute();
      LOG.info("Execution finished.");
    }

    /**
     * Start the actual execution of the proposals.
     */
    private void execute() {
      _zkUtils = ZkUtils.apply(_zkConnect, 30000, 30000, false);
      try {
        if (_state.compareAndSet(ExecutorState.State.EXECUTION_STARTED,
                                 ExecutorState.State.REPLICA_MOVEMENT_TASK_IN_PROGRESS)) {
          moveReplicas();
        }
        // Start leader movements.
        if (_state.compareAndSet(ExecutorState.State.REPLICA_MOVEMENT_TASK_IN_PROGRESS,
                                 ExecutorState.State.LEADER_MOVEMENT_TASK_IN_PROGRESS)) {
          moveLeaders();
        }
      } catch (Throwable t) {
        LOG.error("Executor got exception during execution", t);
      } finally {
        // Add the null pointer check for unit test.
        if (_loadMonitor != null) {
          _loadMonitor.resumeMetricSampling();
        }
        _stopRequested = false;
        _executionTaskManager.clear();
        KafkaCruiseControlUtils.closeZkUtilsWithTimeout(_zkUtils, 10000);
        _state.set(ExecutorState.State.NO_TASK_IN_PROGRESS);
        _zkUtils.close();
      }
    }

    private void moveReplicas() {
      int numTotalPartitionMovements = _executionTaskManager.remainingPartitionMovements().size();
      long totalDataToMoveInMB = _executionTaskManager.remainingDataToMoveInMB();
      int partitionsToMove = numTotalPartitionMovements;
      LOG.info("Starting {} partition movements.", numTotalPartitionMovements);
      // Exhaust all the pending partition movements.
      while ((partitionsToMove > 0 || _executionTaskManager.hasTaskInProgress()) && !_stopRequested) {
        // Get tasks to execute.
        List<ExecutionTask> tasksToExecute = _executionTaskManager.getReplicaMovementTasks();
        LOG.info("Executor will execute " + tasksToExecute.size() + " task(s)");

        if (!tasksToExecute.isEmpty()) {
          // Execute the tasks.
          _executionTaskManager.markTasksInProgress(tasksToExecute);
          ExecutorUtils.executeReplicaReassignmentTasks(_zkUtils, tasksToExecute);
        }
        // Wait for some partition movements to finish
        waitForExecutionTaskToFinish();
        partitionsToMove = _executionTaskManager.remainingPartitionMovements().size();
        long dataToMove = _executionTaskManager.remainingDataToMoveInMB();
        _numFinishedPartitionMovements =
            numTotalPartitionMovements - partitionsToMove - _executionTaskManager.inExecutionTasks().size();
        _finishedDataMovementInMB = totalDataToMoveInMB - dataToMove;
        LOG.info("{}/{} ({}%) partition movements completed. {}/{} ({}%) MB have been moved.",
                 _numFinishedPartitionMovements, numTotalPartitionMovements,
                 _numFinishedPartitionMovements * 100 / numTotalPartitionMovements,
                 _finishedDataMovementInMB, totalDataToMoveInMB,
                 totalDataToMoveInMB == 0 ? 100 : (_finishedDataMovementInMB * 100) / totalDataToMoveInMB);
      }
      // After the partition movement finishes, wait for the controller to clean the reassignment zkPath. This also
      // ensures a clean stop when the execution is stopped in the middle.
      while (!_executionTaskManager.inExecutionTasks().isEmpty()) {
        LOG.info("Waiting for {} tasks to finish: {}", _executionTaskManager.inExecutionTasks().size(),
                 _executionTaskManager.inExecutionTasks());
        waitForExecutionTaskToFinish();
      }
      if (_executionTaskManager.inProgressTasks().isEmpty()) {
        LOG.info("Partition movements finished.");
      } else if (_stopRequested) {
        LOG.info("Partition movements stopped. {} in progress, {} pending, {} aborted, {} dead.",
                 _executionTaskManager.inProgressTasks().size(),
                 _executionTaskManager.remainingPartitionMovements().size(),
                 _executionTaskManager.abortedTasks().size(),
                 _executionTaskManager.deadTasks().size());
      }
    }

    private void moveLeaders() {
      int numTotalLeaderMovements = _executionTaskManager.remainingLeaderMovements().size();
      LOG.info("Starting {} leader movements.", numTotalLeaderMovements);
      int leaderMoved = 0;
      while (!_executionTaskManager.remainingLeaderMovements().isEmpty() && !_stopRequested) {
        leaderMoved += moveLeadersInBatch();
        LOG.info("{}/{} ({}%) leader movements completed.", leaderMoved, numTotalLeaderMovements,
                 leaderMoved * 100 / numTotalLeaderMovements);
      }
      LOG.info("Leader movements finished.");
    }

    private int moveLeadersInBatch() {
      List<ExecutionTask> leaderMovementTasks = _executionTaskManager.getLeaderMovementTasks();
      int numLeadersToMove = leaderMovementTasks.size();
      LOG.debug("Executing {} leader movements in a batch.", numLeadersToMove);
      // Execute the leader movements.
      if (!leaderMovementTasks.isEmpty() && !_stopRequested) {
        // Mark leader movements in progress.
        _executionTaskManager.markTasksInProgress(leaderMovementTasks);
        // Execute leader movement tasks
        // Ideally we should avoid adjust replica order if not needed, but due to a bug in open source Kafka
        // metadata cache on the broker side, the returned replica list may not match the list in zookeeper.
        // Because reading the replica list from zookeeper would be too expensive, we simply write all the
        // orders to zookeeper and let the controller discard the ones that do not need an update.
        LOG.trace("Adjusting replica orders");
        ExecutorUtils.adjustReplicaOrderBeforeLeaderMovements(_zkUtils, leaderMovementTasks);
        waitForReplicaOrderAdjustmentFinish();

        // Run preferred leader election.
        ExecutorUtils.executePreferredLeaderElection(_zkUtils, leaderMovementTasks);
        LOG.trace("Waiting for leader movement batch to finish.");
        while (!_executionTaskManager.inProgressTasks().isEmpty() && !_stopRequested) {
          waitForExecutionTaskToFinish();
        }
      }
      return numLeadersToMove;
    }

    /**
     * Periodically check to see if the replica order adjustment has finished.
     */
    private void waitForReplicaOrderAdjustmentFinish() {
      while (!ExecutorUtils.partitionsBeingReassigned(_zkUtils).isEmpty() && !_stopRequested) {
        try {
          Thread.sleep(_statusCheckingIntervalMs);
        } catch (InterruptedException e) {
          // let it go
        }
      }
    }

    /**
     * This method periodically check zookeeper to see if the partition reassignment has finished or not.
     */
    private void waitForExecutionTaskToFinish() {
      List<ExecutionTask> finishedTasks = new ArrayList<>();
      do {
        Cluster cluster = _metadataClient.refreshMetadata().cluster();
        LOG.debug("Tasks in execution: {}", _executionTaskManager.inExecutionTasks());
        List<ExecutionTask> deadOrAbortingTasks = new ArrayList<>();
        for (ExecutionTask task : _executionTaskManager.inExecutionTasks()) {
          TopicPartition tp = task.proposal.topicPartition();
          if (cluster.partition(tp) == null) {
            // Handle topic deletion during the execution.
            LOG.debug("Task {} is marked as finished because the topic has been deleted", task);
            finishedTasks.add(task);
            _executionTaskManager.markTaskAborting(task);
            _executionTaskManager.markTaskDone(task);
          } else if (isTaskDone(cluster, tp, task)) {
            // Check to see if the task is done.
            finishedTasks.add(task);
            _executionTaskManager.markTaskDone(task);
          } else if (maybeMarkTaskAsDeadOrAborting(cluster, task)) {
            // Only add the dead or aborted tasks to execute if it is not a leadership movement.
            if (task.proposal.balancingAction() != BalancingAction.LEADERSHIP_MOVEMENT) {
              deadOrAbortingTasks.add(task);
            }
            // A dead or aborted task is considered as finished.
            if (task.state() == DEAD || task.state() == ABORTED) {
              finishedTasks.add(task);
            }
          }
        }
        // TODO: Execute the dead or aborted tasks.
        if (!deadOrAbortingTasks.isEmpty()) {
          // TODO: re-enable this rollback action when KAFKA-6304 is available.
          // ExecutorUtils.executeReplicaReassignmentTasks(_zkUtils, deadOrAbortingTasks);
          if (!_stopRequested) {
            // If there is task aborted or dead, we stop the execution.
            stopExecution();
          }
        }

        // If there is no finished tasks, we need to check if anything is blocked.
        if (finishedTasks.isEmpty()) {
          maybeReexecuteTasks();
          try {
            Thread.sleep(_statusCheckingIntervalMs);
          } catch (InterruptedException e) {
            // let it go
          }
        }
      } while (!_executionTaskManager.inExecutionTasks().isEmpty() && finishedTasks.size() == 0);
      // Some tasks have finished, remove them from in progress task map.
      LOG.info("Completed tasks: {}", finishedTasks);
    }

    /**
     * Check if a task is done.
     */
    private boolean isTaskDone(Cluster cluster, TopicPartition tp, ExecutionTask task) {
      switch (task.proposal.balancingAction()) {
        case REPLICA_MOVEMENT:
          return isReplicaMovementDone(cluster, tp, task);
        case REPLICA_DELETION:
          return isReplicaDeletionDone(cluster, tp, task);
        case REPLICA_ADDITION:
          return isReplicaAdditionDone(cluster, tp, task);
        case LEADERSHIP_MOVEMENT:
          return isLeadershipMovementDone(cluster, tp, task);
        default:
          throw new IllegalStateException("Should never be here.");
      }
    }

    /**
     * For a replica movement, the completion depends on the task state:
     * IN_PROGRESS: done when source is not in the replica list while the destination is there.
     * ABORTING: done when destination is not in the replica list but the source is there. Due to race condition,
     *           we also accept the completion condition for IN_PROGRESS tasks.
     * DEAD: always considered as done because we neither move forward or rollback.
     *
     * There should be no other task state seen here.
     */
    private boolean isReplicaMovementDone(Cluster cluster, TopicPartition tp, ExecutionTask task) {
      boolean destinationExists = false;
      boolean sourceExists = false;
      for (Node node : cluster.partition(tp).replicas()) {
        destinationExists = destinationExists || (node.id() == task.destinationBrokerId());
        sourceExists = sourceExists || (node.id() == task.sourceBrokerId());
      }
      switch (task.state()) {
        case IN_PROGRESS:
          return destinationExists && !sourceExists;
        case ABORTING:
          // There could be a race condition that when we abort a task, it is already completed.
          // in that case, we treat it as aborted as well.
          return (!destinationExists && sourceExists) || (destinationExists && !sourceExists);
        case DEAD:
          return true;
        default:
          throw new IllegalStateException("Should never be here. State " + task.state());
      }
    }

    /**
     * A replica deletion is done when the the source node is no longer in the replica list.
     */
    private boolean isReplicaDeletionDone(Cluster cluster, TopicPartition tp, ExecutionTask task) {
      boolean sourceExists = false;
      for (Node node : cluster.partition(tp).replicas()) {
        sourceExists = sourceExists || (node.id() == task.sourceBrokerId());
      }
      return !sourceExists;
    }

    /**
     * The completeness of replica addition depends on the task state:
     * IN_PROGRESS: done when the destination shows in the replica list.
     * ABORTING or DEAD: always considered as done because there is nothing that can be added.
     *
     * There should be no other task state seen here.
     */
    private boolean isReplicaAdditionDone(Cluster cluster, TopicPartition tp, ExecutionTask task) {
      boolean destinationExists = false;
      for (Node node : cluster.partition(tp).replicas()) {
        destinationExists = destinationExists || (node.id() == task.destinationBrokerId());
      }
      switch (task.state()) {
        case IN_PROGRESS:
          return destinationExists;
        case ABORTING:
        case DEAD:
          return true;
        default:
          throw new IllegalStateException("Should never be here.");
      }
    }

    /**
     * The completeness of leadership movement depends on the task state:
     * IN_PROGRESS: done when the leader becomes the destination.
     * ABORTING or DEAD: always considered as done the destination cannot become leader anymore.
     *
     * There should be no other task state seen here.
     */
    private boolean isLeadershipMovementDone(Cluster cluster, TopicPartition tp, ExecutionTask task) {
      Node leader = cluster.leaderFor(tp);
      switch (task.state()) {
        case IN_PROGRESS:
          return leader != null && leader.id() == task.destinationBrokerId();
        case ABORTING:
        case DEAD:
          return true;
        default:
          throw new IllegalStateException("Should never be here.");
      }

    }

    /**
     * Mark the task as aborting or dead if needed.
     *
     * Ideally, the task should be marked as:
     * 1. ABORTING: when the execution is stopped by the users.
     * 2. ABORTING: When the destination broker is dead so the task cannot make progress, but the source broker is
     *              still alive.
     * 3. DEAD: when both the source and destination brokers are dead.
     *
     * Currently KafkaController does not support updates on the partitions that is being reassigned. (KAFKA-6034)
     * Therefore once a proposals is written to ZK, we cannot revoke it. So the actual behavior we are using is to
     * set the task state to:
     * 1. IN_PROGRESS: when the execution is stopped by the users. i.e. do nothing but let the task finish normally.
     * 2. DEAD: when the destination broker is dead. i.e. do not block on the execution.
     *
     * @param cluster the kafka cluster
     * @param task the task to check
     * @return true if the task is marked as dead or aborting, false otherwise.
     */
    private boolean maybeMarkTaskAsDeadOrAborting(Cluster cluster, ExecutionTask task) {
      // Only check tasks with IN_PROGRESS or ABORTING state.
      if (task.state() == IN_PROGRESS || task.state() == ABORTING) {
        boolean destinationAlive =
            task.destinationBrokerId() == null || cluster.nodeById(task.destinationBrokerId()) != null;
        if (!destinationAlive) {
          _executionTaskManager.markTaskDead(task);
          LOG.warn("Killing execution for task {} because both source and destination broker are down.", task);
          return true;
        }
      }
      return false;
    }

    /**
     * Due to the race condition between the controller and Cruise Control, some of the submitted tasks may be
     * deleted by controller without being executed. We will resubmit those tasks in that case.
     */
    private void maybeReexecuteTasks() {
      boolean shouldReexecuteTasks = !_executionTaskManager.inExecutionTasks().isEmpty() &&
          ExecutorUtils.partitionsBeingReassigned(_zkUtils).isEmpty();
      if (shouldReexecuteTasks) {
        LOG.info("Reexecuting tasks {}", _executionTaskManager.inExecutionTasks());
        List<ExecutionTask> tasksToReexecute = new ArrayList<>();
        for (ExecutionTask executionTask : _executionTaskManager.inExecutionTasks()) {
          if (executionTask.proposal.balancingAction() != BalancingAction.LEADERSHIP_MOVEMENT) {
            tasksToReexecute.add(executionTask);
          }
        }
        ExecutorUtils.executeReplicaReassignmentTasks(_zkUtils, tasksToReexecute);
      }
    }
  }
}
