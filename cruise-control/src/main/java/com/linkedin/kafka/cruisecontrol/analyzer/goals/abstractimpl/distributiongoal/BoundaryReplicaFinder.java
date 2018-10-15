/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.distributiongoal;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.ReplicaSearchResult;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaWrapper;
import org.apache.kafka.common.TopicPartition;

/**
 * A replica finder that is used to binary search for replicas in the {@link MetricDistributionGoal}.
 *
 * A balancing action consists of a few information.
 * 1. Source broker
 * 2. Destination broker
 * 3. Source partition
 * 4. Destination partition
 * 5. Action type.
 *
 * This class takes an <I>incomplete balancing action</I> which has provided the source broker, destination broker,
 * and action type but have the source partition and/or destination partition as a place holder. The replica that
 * is going to replace the place holder must ensure that the its broker's metric value after the action will be
 * between the minimum and maximum allowed values. Therefore there is a suitable range of replicas that can serve
 * the purpose. This class helps find the replica at the boundary of that suitable replica range.
 *
 * For the action type of
 * {@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#REPLICA_MOVEMENT REPLICA_MOVEMENT},
 * {@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#LEADERSHIP_MOVEMENT LEADERSHIP_MOVEMENT},
 * partition and destination partition are the same.
 *
 * For
 * {@link com.linkedin.kafka.cruisecontrol.analyzer.ActionType#REPLICA_SWAP REPLICA_SWAP}, source partition and
 * destination partition are different.
 *
 *
 */
public class BoundaryReplicaFinder extends DistributionGoalReplicaFinder {
  private final double _targetValue;
  private final BoundaryType _boundaryType;
  private final Broker _brokerToSearch;
  private final BalancingAction _balancingAction;

  /**
   * Constructor of the {@link BoundaryReplicaFinder}.
   *
   * @param srcBroker the source broker
   * @param destBroker the destination broker
   * @param sortName the sort name of the metric goal
   * @param balancingAction the balance action with replica place holder
   * @param boundaryType the boundary to find (lower or upper)
   * @param currentValueForSrcBroker the current metric value of the source broker.
   * @param currentValueForDestBroker the current metric value of the destination broker.
   * @param distributionGoalHelper the distribution goal helper to aid the search.
   */
  BoundaryReplicaFinder(Broker srcBroker,
                        Broker destBroker,
                        String sortName,
                        BalancingAction balancingAction,
                        BoundaryType boundaryType,
                        double currentValueForSrcBroker,
                        double currentValueForDestBroker,
                        DistributionGoalHelper distributionGoalHelper) {
    super(srcBroker, // brokerToOptimize
          destBroker, // brokerToBalanceWith
          sortName,
          0L,
          currentValueForSrcBroker,
          currentValueForDestBroker,
          distributionGoalHelper);
    _balancingAction = balancingAction;
    _boundaryType = boundaryType;

    // The broker to search is the broker with place holder partition.
    _brokerToSearch = _balancingAction.topicPartition() == PLACE_HOLDER_PARTITION ? srcBroker : destBroker;

    if (boundaryType == BoundaryType.UPPER) {
      // For the broker with place holder partition, to get the upper bound of the replicas the target broker
      // value should be the minimum. i.e. the broker should give as big a replica away as possible.
      _targetValue = _brokerToSearch == srcBroker ? _minValueForBrokerToOptimize : _minValueForBrokerToBalanceWith;
    } else {
      // For the broker with place holder partition, to get the lower bound of the replicas the target broker
      // value should be the minimum. i.e. the broker should give as small a replica away as possible.
      _targetValue = _brokerToSearch == srcBroker ? _maxValueForBrokerToOptimize : _maxValueForBrokerToBalanceWith;
    }
  }

  @Override
  boolean hasNextAttempt() {
    return false;
  }

  @Override
  DistributionGoalReplicaFinder nextAttempt() {
    return null;
  }

  @Override
  BalancingAction getBalancingActionForReplica(Replica replica) {
    return new BalancingAction(replacePlaceHolder(_balancingAction.topicPartition(), replica),
                               _balancingAction.sourceBrokerId(),
                               _balancingAction.destinationBrokerId(),
                               _balancingAction.actionType(),
                               replacePlaceHolder(_balancingAction.destinationTopicPartition(), replica));
  }

  @Override
  Broker brokerToSearch() {
    return _brokerToSearch;
  }

  @Override
  protected double targetValueForBrokerToOptimize() {
    return _targetValue;
  }

  @Override
  public void checkReplica(Replica replica, ReplicaSearchResult replicaSearchResult) {
    replicaSearchResult.updateReplicaCheckResult(ReplicaSearchResult.ReplicaCheckResult.ACCEPTED);
  }

  @Override
  public ReplicaWrapper choose(ReplicaWrapper r1, ReplicaWrapper r2) {
    // When searching for the upper bound of replicas, take the smaller replica to give away,
    // otherwise, take the bigger replica;
    return _boundaryType == BoundaryType.UPPER ? r1 : r2;
  }

  private TopicPartition replacePlaceHolder(TopicPartition tp, Replica replica) {
    return tp == PLACE_HOLDER_PARTITION ? replica.topicPartition() : tp;
  }

  enum BoundaryType {
    UPPER, LOWER
  }
}
