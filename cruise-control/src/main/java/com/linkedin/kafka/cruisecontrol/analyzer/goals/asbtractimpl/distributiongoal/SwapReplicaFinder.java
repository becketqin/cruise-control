/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.distributiongoal;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.BrokerAndSortedReplicas;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.ReplicaWrapper;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.Iterator;
import java.util.NavigableSet;


/**
 * A class that helps find a {@link com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction} to apply in the
 * cluster model which makes the <tt>brokerToOptimize</tt> and <tt>brokerToBalanceWith</tt> become more balanced
 * overall.
 */
class SwapReplicaFinder extends DistributionGoalReplicaFinder {
  private final Iterator<ReplicaWrapper> _replicaIterator;
  private final BrokerAndSortedReplicas _brokerToSearch;
  private ReplicaWrapper _replicaToSwapWith;

  /**
   * Construct the SwapActionFinder.
   *
   * @param brokerToOptimize the broker to optimize.
   * @param brokerToBalanceWith the broker to provide replicas to swap with for optimization.
   * @param targetValueForBrokerToOptimize the target metric value for the broker to optimize.
   * @param currentValueForBrokerToOptimize the current metric value for the broker to optimize.
   * @param currentValueForBrokerToSwapWith the current value for the broker to swap with.
   * @param distributionGoalHelper the goal helper needed by this balancing action finder.
   */
  public SwapReplicaFinder(BrokerAndSortedReplicas brokerToOptimize,
                           BrokerAndSortedReplicas brokerToBalanceWith,
                           double targetValueForBrokerToOptimize,
                           double currentValueForBrokerToOptimize,
                           double currentValueForBrokerToSwapWith,
                           DistributionGoalHelper distributionGoalHelper) {
    super(brokerToOptimize,
          brokerToBalanceWith,
          targetValueForBrokerToOptimize,
          currentValueForBrokerToOptimize,
          currentValueForBrokerToSwapWith, distributionGoalHelper);
    double valueToChange = targetValueForBrokerToOptimize - currentValueForBrokerToOptimize;
    // We always iterate from the smaller replicas to larger replicas to minimize bytes movement.
    if (valueToChange > 0) {
      // The broker to optimize needs more load.
      _brokerToSearch = brokerToBalanceWith;
      _replicaIterator = brokerToOptimize.sortedReplicas().iterator();
    } else {
      // The broker to optimize needs less load.
      _brokerToSearch = brokerToOptimize;
      _replicaIterator = brokerToBalanceWith.sortedReplicas().iterator();
    }
  }

  @Override
  public NavigableSet<ReplicaWrapper> replicasToSearch() {
    return super.replicasToSearch();
  }

  @Override
  BalancingAction getBalancingActionForReplica(Replica replica) {
    return new BalancingAction(replica.topicPartition(),
                               replica.broker().id(),
                               _replicaToSwapWith.replica().broker().id(),
                               ActionType.REPLICA_SWAP,
                               _replicaToSwapWith.replica().topicPartition());
  }

  @Override
  BrokerAndSortedReplicas brokerAndReplicasToSearch() {
    return _brokerToSearch;
  }

  @Override
  boolean hasNextAttempt() {
    while (_replicaIterator.hasNext()) {
      _replicaToSwapWith = _replicaIterator.next();
      ActionAcceptance acceptance =
          _distributionGoalHelper.checkAcceptance(getBalancingActionForReplica(_replicaToSwapWith.replica()));

      if (acceptance == ActionAcceptance.ACCEPT) {
        return true;
      } else if (acceptance == ActionAcceptance.BROKER_REJECT) {
        break;
      }
    }
    _replicaToSwapWith = null;
    return false;
  }

  @Override
  DistributionGoalReplicaFinder nextAttempt() {
    return this;
  }
}
