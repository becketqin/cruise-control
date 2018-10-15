/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl;


import com.linkedin.kafka.cruisecontrol.model.ReplicaWrapper;

/**
 * This class helps keep the context throughout the entire replica search.
 */
public class ReplicaSearchResult {

  /**
   * A enum that tracks the replica searching state.
   *
   * IN_PROGRESS: the searching is still on going.
   * SUCCEEDED: the replica search has found a replica.
   * FAILED: The replica search has failed. This state is also used by the {@link ReplicaFinder} to indicate
   *         that the search should stop immediately.
   */
  enum Progress {
    IN_PROGRESS, SUCCEEDED, FAILED
  }

  /**
   * The searching algorithm uses the {@link ReplicaFinder} to check whether a replica is a valid candidate.
   * The enum listed the possible checking result.
   *
   * ACCEPTED: The replica is a valid candidate.
   * REJECTED: The replica is rejected, but the replica may be in the valid range. This may happen when the replica is
   *           rejected by one of the topology goal (e.g. RackAwareGoal), but the metric distribution goal is OK with
   *           the candidate replica.
   * CANDIDATE_OUT_OF_BOUNDARY: The replica fall out of the utilization boundary. This is useful to allow early
   *                            termination of the search when the search algorithm is running
   *                            {@link ReplicaSearchAlgorithm#searchForClosestLegitReplica(ReplicaFinder, ReplicaWrapper)}.
   */
  public enum ReplicaCheckResult {
    ACCEPTED, REJECTED, CANDIDATE_OUT_OF_BOUNDARY
  }

  /**
   * The search progress used to control the search behavior.
   */
  private Progress _progress;
  /**
   * The replica check result. This should be set to null by the {@link ReplicaSearchAlgorithm} before every check.
   */
  private ReplicaCheckResult _replicaCheckResult;
  /**
   * The result of the search, it is always null unless a valid replica is found.
   */
  private ReplicaWrapper _result;
  /**
   * The total number of replicas that have been searched.
   */
  private int _numReplicasSearched;

  public ReplicaSearchResult() {
    _progress = Progress.IN_PROGRESS;
    _result = null;
    _numReplicasSearched = 0;
  }

  public ReplicaSearchResult fail() {
    if (_progress != Progress.IN_PROGRESS) {
      throw new IllegalStateException("Cannot fail a search that is not in progress.");
    }
    _progress = Progress.FAILED;
    return this;
  }

  public void updateReplicaCheckResult(ReplicaCheckResult replicaCheckResult) {
    _replicaCheckResult = replicaCheckResult;
  }

  ReplicaSearchResult found(ReplicaWrapper result) {
    if (_progress != Progress.IN_PROGRESS) {
      throw new IllegalStateException("Cannot update the result of a search that is not in progress.");
    }
    if (result == null) {
      throw new IllegalArgumentException("The result replica found cannot be null.");
    }
    _result = result;
    _progress = Progress.SUCCEEDED;
    return this;
  }

  void incNumReplicasSearched() {
    _numReplicasSearched++;
  }

  void addNumReplicasSearched(int count) {
    _numReplicasSearched += count;
  }

  void resetReplicaCheckResult() {
    _replicaCheckResult = null;
  }

  ReplicaCheckResult replicaCheckResult() {
    return _replicaCheckResult;
  }

  public Progress progress() {
    return _progress;
  }

  public ReplicaWrapper replicaWrapper() {
    return _result;
  }

  public int numReplicasSearched() {
    return _numReplicasSearched;
  }

}
