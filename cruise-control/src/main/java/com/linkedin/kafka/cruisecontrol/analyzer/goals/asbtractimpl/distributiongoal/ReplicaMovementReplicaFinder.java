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


public class ReplicaMovementReplicaFinder extends DistributionGoalReplicaFinder {
  private final Broker _brokerToSearch;
  private boolean _done;

  ReplicaMovementReplicaFinder(Broker brokerToOptimize,
                               Broker brokerToBalanceWith,
                               String sortName,
                               double targetValueForBrokerToOptimize,
                               double currentValueForBrokerToOptimize,
                               double currentValueForBrokerToBalanceWith,
                               DistributionGoalHelper distributionGoalHelper) {
    super(brokerToOptimize,
          brokerToBalanceWith,
          sortName,
          targetValueForBrokerToOptimize,
          currentValueForBrokerToOptimize,
          currentValueForBrokerToBalanceWith, distributionGoalHelper);
    if (targetValueForBrokerToOptimize > currentValueForBrokerToOptimize) {
      _brokerToSearch = brokerToBalanceWith;
    } else {
      _brokerToSearch = brokerToOptimize;
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
    int destBrokerId = replica.broker().id() == _brokerToOptimize.id() ?
        _brokerToBalanceWith.id() : _brokerToOptimize.id();

    return new BalancingAction(replica.topicPartition(),
                               replica.broker().id(),
                               destBrokerId,
                               ActionType.REPLICA_MOVEMENT);
  }

  @Override
  Broker brokerToSearch() {
    return _brokerToSearch;
  }

  @Override
  public void checkReplica(Replica replica, ReplicaSearchResult result) {
    ActionAcceptance actionAcceptance = _distributionGoalHelper.checkAcceptance(getBalancingActionForReplica(replica));
    if (actionAcceptance == ActionAcceptance.ACCEPT) {
      result.updateReplicaCheckResult(ReplicaSearchResult.ReplicaCheckResult.ACCEPTED);
    } else {
      result.updateReplicaCheckResult(ReplicaSearchResult.ReplicaCheckResult.REJECTED);
    }
  }
}
