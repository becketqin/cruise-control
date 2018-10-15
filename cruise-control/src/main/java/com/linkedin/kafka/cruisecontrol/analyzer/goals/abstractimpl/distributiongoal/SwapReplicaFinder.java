/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.distributiongoal;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.ReplicaSearchResult;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaWrapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;


/**
 * A class that helps find a {@link com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction} to apply in the
 * cluster model which makes the <tt>brokerToOptimize</tt> and <tt>brokerToBalanceWith</tt> become more balanced
 * overall.
 */
class SwapReplicaFinder extends DistributionGoalReplicaFinder {
  private final Iterator<ReplicaWrapper> _replicaIterator;
  private final Broker _brokerToSearch;
  private final int _maxIterations;
  private boolean _done;
  private ReplicaWrapper _replicaToSwap;
  private int _iterations;

  /**
   * Construct the SwapActionFinder.
   *
   * @param brokerToOptimize the broker to optimize.
   * @param brokerToBalanceWith the broker to provide replicas to swap with for optimization.
   * @param targetValueForBrokerToOptimize the target metric value for the broker to optimize.
   * @param currentValueForBrokerToOptimize the current metric value for the broker to optimize.
   * @param currentValueForBrokerToSwapWith the current value for the broker to swap with.
   * @param distributionGoalHelper the goal helper needed by this balancing action finder.
   * @param maxIterations the maximum iterations to make to find the replica.
   * @param selfHealing whether the cluster is performing self-healing.
   */
  public SwapReplicaFinder(Broker brokerToOptimize,
                           Broker brokerToBalanceWith,
                           String sortName,
                           double targetValueForBrokerToOptimize,
                           double currentValueForBrokerToOptimize,
                           double currentValueForBrokerToSwapWith,
                           DistributionGoalHelper distributionGoalHelper,
                           int maxIterations,
                           boolean selfHealing) {
    super(brokerToOptimize,
          brokerToBalanceWith,
          sortName,
          targetValueForBrokerToOptimize,
          currentValueForBrokerToOptimize,
          currentValueForBrokerToSwapWith, distributionGoalHelper);
    double valueToChange = targetValueForBrokerToOptimize - currentValueForBrokerToOptimize;
    // We always iterate from the smaller replicas to larger replicas to minimize bytes movement.
    if (valueToChange > 0) {
      // The broker to optimize needs more load.
      _brokerToSearch = brokerToBalanceWith;
      _replicaIterator = shortcutReplicas(brokerToOptimize, selfHealing);
    } else {
      // The broker to optimize needs less load.
      _brokerToSearch = brokerToOptimize;
      _replicaIterator = shortcutReplicas(brokerToBalanceWith, selfHealing);
    }
    _maxIterations = maxIterations;
    _iterations = 0;
    _done = false;
  }

  @Override
  public void checkReplica(Replica replica, ReplicaSearchResult result) {
    // Get the balancing action.
    BalancingAction action = getBalancingActionForReplica(replica);
    ActionAcceptance actionAcceptance = _distributionGoalHelper.checkAcceptance(action);
    // When the replica to swap cannot be moved to the broker,
    if (actionAcceptance == ActionAcceptance.ACCEPT) {
      result.updateReplicaCheckResult(ReplicaSearchResult.ReplicaCheckResult.ACCEPTED);
    } else if (_replicaToSwap.replica().broker().id() == action.sourceBrokerId()
        && actionAcceptance == ActionAcceptance.DEST_BROKER_REJECT) {
      result.updateReplicaCheckResult(ReplicaSearchResult.ReplicaCheckResult.REJECTED);
      result.fail();
    } else if (_replicaToSwap.replica().broker().id() == action.destinationBrokerId()
        && actionAcceptance == ActionAcceptance.SRC_BROKER_REJECT) {
      result.updateReplicaCheckResult(ReplicaSearchResult.ReplicaCheckResult.REJECTED);
      result.fail();
    } else {
      result.updateReplicaCheckResult(ReplicaSearchResult.ReplicaCheckResult.REJECTED);
    }
  }

  @Override
  BalancingAction getBalancingActionForReplica(Replica replica) {
    return new BalancingAction(_replicaToSwap.replica().topicPartition(),
                               _replicaToSwap.replica().broker().id(),
                               replica.broker().id(),
                               ActionType.REPLICA_SWAP,
                               replica.topicPartition());
  }

  @Override
  Broker brokerToSearch() {
    return _brokerToSearch;
  }

  @Override
  boolean hasNextAttempt() {
    if (!_done && _replicaIterator.hasNext() && _iterations++ < _maxIterations) {
      _replicaToSwap = _replicaIterator.next();
      return true;
    } else {
      _replicaToSwap = null;
      return false;
    }
  }

  @Override
  DistributionGoalReplicaFinder nextAttempt() {
    return this;
  }

  /**
   * We shortcut the replica iteration when either broker to optimize or broker to balance with is new.
   *
   * This is to avoid iterating over the replicas unnecessarily.
   * @return an iterator of replica wrapper.
   */
  private Iterator<ReplicaWrapper> shortcutReplicas(Broker brokerToIterate,
                                                    boolean selfHealing) {
    if (!selfHealing && brokerToIterate.isNew() && !_brokerToSearch.isNew()) {
      List<ReplicaWrapper> shortcutReplicas = new ArrayList<>();
      brokerToIterate.trackedSortedReplicas(_sortName).sortedReplicas().forEach(rw -> {
        if (rw.replica().originalBroker().id() == _brokerToSearch.id()) {
          shortcutReplicas.add(rw);
        }
      });
      return shortcutReplicas.iterator();
    } else {
      return brokerToIterate.trackedSortedReplicas(_sortName).sortedReplicas().iterator();
    }
  }
}
