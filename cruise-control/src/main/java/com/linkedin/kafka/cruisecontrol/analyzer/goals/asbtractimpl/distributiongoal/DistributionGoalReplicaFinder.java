/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.distributiongoal;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.ReplicaFinder;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.BrokerAndSortedReplicas;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.ReplicaWrapper;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.NavigableSet;


/**
 * An abstract class to find replica in the {@link MetricDistributionGoal}.
 *
 * The abstract goal provides necessary context about the optimization target and boundaries.
 */
public abstract class DistributionGoalReplicaFinder implements ReplicaFinder {
  protected final BrokerAndSortedReplicas _brokerToOptimize;
  protected final BrokerAndSortedReplicas _brokerToBalanceWith;
  protected final double _targetValueForBrokerToOptimize;
  protected final double _currentValueForBrokerToOptimize;
  protected final double _currentValueForBrokerToBalanceWith;
  protected final double _currentMetricValueDiff;
  protected final double _minValueForBrokerToOptimize;
  protected final double _maxValueForBrokerToOptimize;
  protected final double _minValueForBrokerToBalanceWith;
  protected final double _maxValueForBrokerToBalanceWith;
  protected final DistributionGoalHelper _distributionGoalHelper;

  DistributionGoalReplicaFinder(BrokerAndSortedReplicas brokerToOptimize,
                                BrokerAndSortedReplicas brokerToBalanceWith,
                                double targetValueForBrokerToOptimize,
                                double currentValueForBrokerToOptimize,
                                double currentValueForBrokerToBalanceWith,
                                DistributionGoalHelper distributionGoalHelper) {
    _brokerToOptimize = brokerToOptimize;
    _brokerToBalanceWith = brokerToBalanceWith;
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
   * @return The {@link BrokerAndSortedReplicas} to indicate in which broker should the search be performed.
   */
  abstract BrokerAndSortedReplicas brokerAndReplicasToSearch();

  @Override
  public NavigableSet<ReplicaWrapper> replicasToSearch() {
    int brokerId = brokerAndReplicasToSearch().broker().id();
    if (brokerId != _brokerToOptimize.broker().id() && brokerId != _brokerToBalanceWith.broker().id()) {
      throw new IllegalArgumentException(String.format("Invalid broker id %d returned from brokerAndReplicasToSearch()."
                                                           + " The broker id must be either %d(brokerToOptimize) or"
                                                           + " %d(brokerToBalanceWith).",
                                                       brokerId, _brokerToOptimize.broker().id(),
                                                       _brokerToBalanceWith.broker().id()));
    }
    return brokerAndReplicasToSearch().sortedReplicas();
  }

  @Override
  public int evaluate(Replica replica) {
    BalancingAction action = getBalancingActionForReplica(replica);

    if ((action.sourceBrokerId() != _brokerToOptimize.broker().id()
        && action.sourceBrokerId() != _brokerToBalanceWith.broker().id())
        || (action.destinationBrokerId() != _brokerToOptimize.broker().id()
        && action.destinationBrokerId() != _brokerToBalanceWith.broker().id())) {
      throw new IllegalArgumentException(String.format("The balancing action %s returned by getBalancingActionForReplica() "
                                                           + "gives an action with invalid participant brokers. "
                                                           + "The valid brokers are %d(toOptimize) and %d(toBalanceWith)",
                                                       action, _brokerToOptimize.broker().id(),
                                                       _brokerToBalanceWith.broker().id()));
    }

    double brokerToOptimizeValueAfterAction = _distributionGoalHelper.brokerMetricValueAfterAction(_brokerToOptimize.broker(), action);
    double brokerToBalanceWithValueAfterAction = _distributionGoalHelper.brokerMetricValueAfterAction(_brokerToBalanceWith.broker(), action);
    boolean searchingInBrokerToOptimize = brokerAndReplicasToSearch().broker().id() == _brokerToOptimize.broker().id();

    if (brokerToOptimizeValueAfterAction >= _maxValueForBrokerToOptimize
        || brokerToBalanceWithValueAfterAction <= _maxValueForBrokerToBalanceWith) {
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
      value1 = _distributionGoalHelper.brokerMetricValueAfterAction(_brokerToOptimize.broker(), action);
    }
    if (r2 != null) {
      BalancingAction action = getBalancingActionForReplica(r2.replica());
      value2 = _distributionGoalHelper.brokerMetricValueAfterAction(_brokerToOptimize.broker(), action);
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

  @Override
  public ActionAcceptance isReplicaAcceptable(Replica replica) {
    return _distributionGoalHelper.checkAcceptance(getBalancingActionForReplica(replica));
  }
}
