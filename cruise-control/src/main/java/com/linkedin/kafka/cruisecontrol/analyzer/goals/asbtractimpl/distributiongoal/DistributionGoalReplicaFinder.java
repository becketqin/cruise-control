/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.distributiongoal;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.ReplicaFinder;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaWrapper;
import org.apache.kafka.common.TopicPartition;

import java.util.NavigableSet;


/**
 * An abstract class to find replica in the {@link MetricDistributionGoal}.
 *
 * The abstract goal provides necessary context about the optimization target and boundaries.
 */
public abstract class DistributionGoalReplicaFinder implements ReplicaFinder {
  // A place holder partition used to indicate the replicas to search in a balancing action.
  static final TopicPartition PLACE_HOLDER_PARTITION = new TopicPartition("PLACE_HOLDER", 0);
  protected final Broker _brokerToOptimize;
  protected final Broker _brokerToBalanceWith;
  protected final String _sortName;
  protected final double _targetValueForBrokerToOptimize;
  protected final double _currentValueForBrokerToOptimize;
  protected final double _currentValueForBrokerToBalanceWith;
  protected final double _currentMetricValueDiff;
  protected final double _minValueForBrokerToOptimize;
  protected final double _maxValueForBrokerToOptimize;
  protected final double _minValueForBrokerToBalanceWith;
  protected final double _maxValueForBrokerToBalanceWith;
  protected final DistributionGoalHelper _distributionGoalHelper;

  DistributionGoalReplicaFinder(Broker brokerToOptimize,
                                Broker brokerToBalanceWith,
                                String sortName,
                                double targetValueForBrokerToOptimize,
                                double currentValueForBrokerToOptimize,
                                double currentValueForBrokerToBalanceWith,
                                DistributionGoalHelper distributionGoalHelper) {
    _brokerToOptimize = brokerToOptimize;
    _brokerToBalanceWith = brokerToBalanceWith;
    _sortName = sortName;
    _distributionGoalHelper = distributionGoalHelper;
    _targetValueForBrokerToOptimize = targetValueForBrokerToOptimize;
    _currentValueForBrokerToOptimize = currentValueForBrokerToOptimize;
    _currentValueForBrokerToBalanceWith = currentValueForBrokerToBalanceWith;
    _currentMetricValueDiff = Math.abs(currentValueForBrokerToOptimize - currentValueForBrokerToBalanceWith);
    double valueToChange = targetValueForBrokerToOptimize - currentValueForBrokerToOptimize;
    // when valueToChange > 0, the broker brokerToOptimize needs a larger metric value, the action should meet the
    // following requirements:
    // 1. After the action, the value of brokerToOptimize is larger, and the value of brokerToBalanceWith is lower.
    // 2. After the action, the metric value of broker brokerToOptimize should not be more than the metric value of
    //    broker brokerToBalanceWith before the action.
    // 3. After the action, the metric value of broker brokerToOptimize should not be less than the metric value of
    //    broker brokerToBalanceWith before the action.
    //
    // When valueToChange < 0, the broker brokerToOptimize needs a smaller metric value, the action should meet the
    // following requirements:
    // 4. After the action, the value of brokerToOptimize is smaller, and the value of brokerToBalanceWith is larger.
    // 5. After the action, the metric value of broker brokerToOptimize should not be less than the metric value of
    //    broker brokerToBalanceWith before the action.
    // 6. After the action, the metric value of broker brokerToBalanceWith should not be more than the metric value
    //    of broker brokerToOptimize before the action.
    //
    // We do not require the swap to be under the balance upper limit or lower limit. Instead, we just ensure
    // that after the swap, the two replicas are closer to the mean usage.
    if (valueToChange > 0) {
      // broker toSwap needs more load.
      // Condition 1
      _minValueForBrokerToOptimize = currentValueForBrokerToOptimize;
      _maxValueForBrokerToBalanceWith = currentValueForBrokerToBalanceWith;
      // Condition 2
      _maxValueForBrokerToOptimize = currentValueForBrokerToBalanceWith;
      // Condition 3
      _minValueForBrokerToBalanceWith = currentValueForBrokerToOptimize;
    } else {
      // broker toSwap needs less load.
      // Condition 4
      _minValueForBrokerToBalanceWith = currentValueForBrokerToBalanceWith;
      _maxValueForBrokerToOptimize = currentValueForBrokerToOptimize;
      // Condition 5
      _minValueForBrokerToOptimize = currentValueForBrokerToBalanceWith;
      // Condition 6
      _maxValueForBrokerToBalanceWith = currentValueForBrokerToOptimize;
    }
  }

  /**
   * @return whether the finder has more attempts to perform.
   */
  abstract boolean hasNextAttempt();

  /**
   * @return the balancing action finder for next attempt.
   */
  abstract DistributionGoalReplicaFinder nextAttempt();

  /**
   * Get the balancing action for the replica.
   *
   * @param replica the replica to get the balancing action.
   * @return a balancing action for the replica.
   */
  abstract BalancingAction getBalancingActionForReplica(Replica replica);

  /**
   * Get the broker and the replicas to search. This method allows the abstract method know how to evaluate
   * the candidate replicas in {@link #evaluate(Replica)}, so that the subclasses do not need to implement
   * the evaluate functions respectively.
   *
   * @return The {@link Broker} to indicate in which broker should the search be performed.
   */
  abstract Broker brokerToSearch();

  @Override
  public NavigableSet<ReplicaWrapper> replicasToSearch() {
    int brokerId = brokerToSearch().id();
    if (brokerId != _brokerToOptimize.id() && brokerId != _brokerToBalanceWith.id()) {
      throw new IllegalArgumentException(String.format("Invalid broker id %d returned from brokerToSearch()."
                                                           + " The broker id must be either %d(brokerToOptimize) or"
                                                           + " %d(brokerToBalanceWith).",
                                                       brokerId, _brokerToOptimize.id(),
                                                       _brokerToBalanceWith.id()));
    }
    return brokerToSearch().trackedSortedReplicas(_sortName).sortedReplicas();
  }

  @Override
  public int evaluate(Replica replica) {
    BalancingAction action = getBalancingActionForReplica(replica);

    if ((action.sourceBrokerId() != _brokerToOptimize.id()
        && action.sourceBrokerId() != _brokerToBalanceWith.id())
        || (action.destinationBrokerId() != _brokerToOptimize.id()
        && action.destinationBrokerId() != _brokerToBalanceWith.id())) {
      throw new IllegalArgumentException(String.format("The balancing action %s returned by getBalancingActionForReplica() "
                                                           + "gives an action with invalid participant brokers. "
                                                           + "The valid brokers are %d(toOptimize) and %d(toBalanceWith)",
                                                       action, _brokerToOptimize.id(),
                                                       _brokerToBalanceWith.id()));
    }

    double brokerToOptimizeValueAfterAction = _distributionGoalHelper.brokerMetricValueAfterAction(_brokerToOptimize, action);
    double brokerToBalanceWithValueAfterAction = _distributionGoalHelper.brokerMetricValueAfterAction(_brokerToBalanceWith, action);
    boolean searchingInBrokerToOptimize = brokerToSearch().id() == _brokerToOptimize.id();

    if (brokerToOptimizeValueAfterAction >= _maxValueForBrokerToOptimize
        || brokerToBalanceWithValueAfterAction <= _minValueForBrokerToBalanceWith) {
      // The broker to optimize needs less metric value, or the broker to balance with needs more metric value.
      // If we are searching in the broker to optimize, a larger replica is needed.
      // Otherwise if we are searching in the broker to balance with, a smaller replica is needed.
      return searchingInBrokerToOptimize ? 1 : -1;
    } else if (brokerToOptimizeValueAfterAction <= _minValueForBrokerToOptimize
        || brokerToBalanceWithValueAfterAction >= _maxValueForBrokerToBalanceWith) {
      // The broker to optimize needs more metric value, or the broker to balance with needs less metric value.
      // If we are searching in the broker to optimize, a smaller replica is needed.
      // Otherwise if we are searching in the broker to balance with, a larger replica is needed.
      return searchingInBrokerToOptimize ? -1 : 1;
    } else {
      // The replica is acceptable to both broker to optimize and broker to balance with. Looking for a more suitable one.
      if (searchingInBrokerToOptimize) {
        // Searching in the broker to optimize, just compare the estimation to the target broker value.
        return Double.compare(brokerToOptimizeValueAfterAction, _targetValueForBrokerToOptimize);
      } else {
        // Searching in the broker to balance with, compare the target broker value to the estimation.
        return Double.compare(_targetValueForBrokerToOptimize, brokerToOptimizeValueAfterAction);
      }
    }
  }

  @Override
  public ReplicaWrapper choose(ReplicaWrapper r1, ReplicaWrapper r2) {
    Double value1 = null;
    Double value2 = null;
    if (r1 != null) {
      BalancingAction action = getBalancingActionForReplica(r1.replica());
      value1 = _distributionGoalHelper.brokerMetricValueAfterAction(_brokerToOptimize, action);
    }
    if (r2 != null) {
      BalancingAction action = getBalancingActionForReplica(r2.replica());
      value2 = _distributionGoalHelper.brokerMetricValueAfterAction(_brokerToOptimize, action);
    }
    if (value1 == null && value2 == null) {
      return null;
    } else if (value1 == null) {
      return r2;
    } else if (value2 == null) {
      return r1;
    } else {
      return Math.abs(value1 - _targetValueForBrokerToOptimize) < Math.abs(value2 - _targetValueForBrokerToOptimize) ?
          r1 : r2;
    }
  }
}
