/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.distributiongoal;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.BrokerAndSortedReplicas;
import com.linkedin.kafka.cruisecontrol.model.Replica;


public class LeaderMovementReplicaFinder extends DistributionGoalReplicaFinder {
  private final BrokerAndSortedReplicas _brokerAndReplicasToSearch;
  private boolean _done;

  LeaderMovementReplicaFinder(BrokerAndSortedReplicas brokerToOptimize,
                              BrokerAndSortedReplicas brokerToBalanceWith,
                              double targetValueForBrokerToOptimize,
                              double currentValueForBrokerToOptimize,
                              double currentValueForBrokerToBalanceWith,
                              DistributionGoalHelper distributionGoalHelper) {
    super(brokerToOptimize,
          brokerToBalanceWith,
          targetValueForBrokerToOptimize,
          currentValueForBrokerToOptimize,
          currentValueForBrokerToBalanceWith, distributionGoalHelper);
    if (targetValueForBrokerToOptimize > currentValueForBrokerToOptimize) {
      _brokerAndReplicasToSearch = brokerToOptimize;
    } else {
      _brokerAndReplicasToSearch = brokerToBalanceWith;
    }
    _done = false;
  }

  @Override
  boolean hasNextAttempt() {
    return !_done;
  }

  @Override
  DistributionGoalReplicaFinder nextAttempt() {
    _done = true;
    return this;
  }

  @Override
  BalancingAction getBalancingActionForReplica(Replica replica) {
    int destBrokerId = replica.broker().id() == _brokerToOptimize.broker().id() ?
        _brokerToBalanceWith.broker().id() : _brokerToOptimize.broker().id();

    return new BalancingAction(replica.topicPartition(),
                               replica.broker().id(),
                               destBrokerId,
                               ActionType.LEADERSHIP_MOVEMENT);
  }

  @Override
  BrokerAndSortedReplicas brokerAndReplicasToSearch() {
    return _brokerAndReplicasToSearch;
  }
}
