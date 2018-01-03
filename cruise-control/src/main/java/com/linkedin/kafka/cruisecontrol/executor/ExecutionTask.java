/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingProposal;
import java.util.HashMap;
import java.util.Map;

/**
 * A class that wraps the execution information of a balancing proposal
 */
public class ExecutionTask implements Comparable<ExecutionTask> {
  // The execution id of the proposal so we can keep track of the task when execute it.
  public final long executionId;
  // The corresponding balancing proposal of this task.
  public final BalancingProposal proposal;
  private volatile State _state;

  public ExecutionTask(long executionId, BalancingProposal proposal) {
    this.executionId = executionId;
    this.proposal = proposal;
    this._state = State.PENDING;
  }

  /**
   * @return The source broker of this execution task.
   */
  public Integer sourceBrokerId() {
    return proposal.sourceBrokerId();
  }

  /**
   * @return The destination broker of this execution task.
   */
  public Integer destinationBrokerId() {
    return proposal.destinationBrokerId();
  }

  /**
   * @return the state of the task.
   */
  public State state() {
    return this._state;
  }

  /**
   * Mark task in progress.
   */
  public void inProgress() {
    this._state = State.IN_PROGRESS;
  }

  /**
   * Kill the task.
   */
  public void kill() {
    this._state = State.DEAD;
  }

  /**
   * Abort the task.
   */
  public void abort() {
    this._state = State.ABORTING;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof ExecutionTask && this.executionId == ((ExecutionTask) o).executionId;
  }

  @Override
  public int hashCode() {
    return (int) executionId;
  }

  /*
   * Return an object that can be further used
   * to encode into JSON
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> executionStatsMap = new HashMap<>();
    executionStatsMap.put("executionId", executionId);
    executionStatsMap.put("proposal", proposal.getJsonStructure());
    return executionStatsMap;
  }

  public enum State {
    PENDING, IN_PROGRESS, ABORTING, ABORTED, DEAD, COMPLETED
  }

  @Override
  public String toString() {
    return String.format("{EXE_ID: %d, %s, %s}", executionId, proposal, _state);
  }

  @Override
  public int compareTo(ExecutionTask o) {
    if (this.executionId > o.executionId) {
      return 1;
    } else if (this.executionId == o.executionId) {
      return 0;
    } else {
      return -1;
    }
  }
}
