/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.distributiongoal;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.AbstractGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.ReplicaFinder;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.ReplicaSearchAlgorithm;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.BrokerAndSortedReplicas;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.DoubleWrapper;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.ReplicaWrapper;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.ToDoubleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.distributiongoal.MetricDistributionGoal.SearchMode.LINEAR_ASCENDING;


/**
 * An abstract class that helps make sure the given metric between different brokers are evenly distributed.
 */
public abstract class MetricDistributionGoal extends AbstractGoal implements DistributionGoalHelper {
  private final Logger LOG = LoggerFactory.getLogger(getClass());
  private static final double BALANCE_MARGIN = 0.9;
  // the maximum allowed metric value difference
  private final String _name = getClass().getSimpleName();
  private final Map<Broker, BrokerAndSortedReplicas> _brokerReplicasMap = new HashMap<>();
  private NavigableSet<BrokerAndSortedReplicas> _allBrokers;
  private ClusterModel _clusterModel;
  private Set<String> _excludedTopics;
  private Set<Goal> _optimizedGoals;
  protected double _metricValueBalancePercent;
  protected double _averageMetricValue;
  protected double _metricValueUpperLimit;
  protected double _metricValueLowerLimit;

  // Flag to indicate whether the self healing failed to relocate all replicas away from dead brokers in its initial
  // attempt and currently omitting the resource balance limit to relocate remaining replicas.
  protected boolean _selfHealingOnly;
  private int _numIterations;
  private boolean _improvedInIteration;

  @Override
  protected void initGoalState(ClusterModel clusterModel, Set<Goal> optimizedGoals, Set<String> excludedTopics) {
    // Log a warning if all replicas are excluded.
    if (clusterModel.topics().equals(excludedTopics)) {
      LOG.warn("All replicas are excluded from {}.", name());
    }
    // Create broker comparator in descending order of broker disk usage.
    Comparator<BrokerAndSortedReplicas> brokerComparator =
        Comparator.comparingDouble((ToDoubleFunction<BrokerAndSortedReplicas>) this::metricValue)
            .thenComparingInt(bs -> bs.broker().id());
    // create a sorted set for all the brokers.
    _allBrokers = new TreeSet<>(brokerComparator);
    clusterModel.brokers().forEach(b -> {
      BrokerAndSortedReplicas brokerReplicas = new BrokerAndSortedReplicas(b, this::impactScore);
      _allBrokers.add(brokerReplicas);
      _brokerReplicasMap.put(b, brokerReplicas);
    });
    _improvedInIteration = false;
    _numIterations = 0;
    _excludedTopics = excludedTopics;
    _optimizedGoals = optimizedGoals;
    _clusterModel = clusterModel;
    _selfHealingOnly = false;
  }

  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    Set<String> excludedTopics) {
    updateLimits(clusterModel);
    BrokerAndSortedReplicas toOptimize = _brokerReplicasMap.get(broker);
    boolean improved = checkAndOptimize(_allBrokers, toOptimize, clusterModel);
    _improvedInIteration = _improvedInIteration || improved;
  }

  @Override
  protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws OptimizationFailureException {
    if (_improvedInIteration) {
      // continue the iteration.
      LOG.debug("Finished iteration {}", _numIterations);
      _improvedInIteration = false;
      _numIterations++;
      return;
    }

    // No improvement was made in this iteration.
    boolean allDeadBrokersAreEmpty = allDeadBrokersAreEmpty(clusterModel);
    if (!allDeadBrokersAreEmpty && !_selfHealingOnly) {
      // There are non-empty dead brokers. Turn on self-healing only mode so the goal ignores some less
      // critical rules but just move replicas off the dead brokers.
      _selfHealingOnly = true;
    } else {
      // Done optimization.
      _succeeded = isOptimized(clusterModel);
      LOG.debug("Finished optimization in {} iterations.", _numIterations);
      finish();
    }
  }

  /**
   * Check whether given action is acceptable by this goal. An action is acceptable by this goal if it satisfies the
   * following: (1) if both source and destination brokers were within the limit before the action, the corresponding
   * limits cannot be violated after the action, (2) otherwise, the action cannot increase the utilization difference
   * between brokers.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    // Ideally we don't want to update the balancing threshold in each check, but because the average metric value
    // and the balancing limit may change after some action, we need to check every time this method is invoked.
    updateLimits(clusterModel);
    Broker srcBroker = clusterModel.broker(action.sourceBrokerId());
    Broker destBroker = clusterModel.broker(action.destinationBrokerId());

    // Utilization before action
    double srcBrokerValueBeforeAction = metricValue(srcBroker);
    double destBrokerValueBeforeAction = metricValue(destBroker);

    // Utilization after action
    double srcBrokerValueAfterAction = brokerMetricValueAfterAction(srcBroker, action, _clusterModel);
    double destBrokerValueAfterAction = brokerMetricValueAfterAction(destBroker, action, _clusterModel);

    boolean srcBalancedBeforeAction =
        srcBrokerValueBeforeAction >= _metricValueLowerLimit && srcBrokerValueBeforeAction < _metricValueUpperLimit;
    boolean destBalancedBeforeAction =
        destBrokerValueBeforeAction >= _metricValueLowerLimit && destBrokerValueBeforeAction < _metricValueUpperLimit;
    boolean srcBalancedAfterAction =
        srcBrokerValueAfterAction >= _metricValueLowerLimit && srcBrokerValueAfterAction < _metricValueUpperLimit;
    boolean destBalancedAfterAction =
        destBrokerValueAfterAction >= _metricValueLowerLimit && destBrokerValueAfterAction < _metricValueUpperLimit;

    if (srcBalancedBeforeAction && destBalancedBeforeAction) {
      // Both source and destination were balanced before action, cannot make them unbalanced.
      return srcBalancedAfterAction && destBalancedAfterAction ? ACCEPT : REPLICA_REJECT;
    } else {
      // Either the source or destination was not balanced, the action should not make the utilization difference larger.
      double utilizationDiffBeforeAction = Math.abs(metricValue(srcBroker) - metricValue(destBroker));
      double utilizationDiffAfterAction = Math.abs(srcBrokerValueAfterAction - destBrokerValueAfterAction);
      if (utilizationDiffAfterAction <= utilizationDiffBeforeAction) {
        return ACCEPT;
      } else {
        return REPLICA_REJECT;
      }
    }
  }

  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    Replica sourceReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());

    double srcBrokerValueAfterAction = brokerMetricValueAfterAction(sourceReplica.broker(), action, clusterModel);
    double destBrokerValueAfterAction = brokerMetricValueAfterAction(destinationBroker, action, clusterModel);

    switch (action.actionType()) {
      case REPLICA_SWAP:
        double valueDiffBeforeAction = Math.abs(metricValue(sourceReplica.broker()) - metricValue(destinationBroker));
        double valueDiffAfterAction = Math.abs(srcBrokerValueAfterAction - destBrokerValueAfterAction);
        boolean result = valueDiffAfterAction < valueDiffBeforeAction;
        if (!result) {
          LOG.trace("Rejecting the balancing action {} due to valid diff increase");
        }
        return result;
      case REPLICA_MOVEMENT:
      case LEADERSHIP_MOVEMENT:
        // Always allow movement if we are doing self-healing only.
        if (!sourceReplica.broker().isAlive() && _selfHealingOnly) {
          return true;
        }
        //Check that current destination would not become more unbalanced.
        if (srcBrokerValueAfterAction < _metricValueLowerLimit) {
          LOG.trace("Rejecting the balancing action {} because source broker value after action is {} is smaller than"
                        + " lower limit of {}", action, srcBrokerValueAfterAction, _metricValueLowerLimit);
          return false;
        }
        if (destBrokerValueAfterAction >= _metricValueUpperLimit) {
          LOG.trace("Rejecting the balancing action {} because destination broker value after action is {} is greater "
                        + "than upper limit of {}", action, destBrokerValueAfterAction, _metricValueUpperLimit);
          return false;
        }
        return true;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.actionType() + " is provided.");
    }
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return null;
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(1, _minMonitoredPartitionPercentage, true);
  }

  @Override
  public String name() {
    return _name;
  }

  // methods inherited from GoalHelper
  @Override
  public ActionAcceptance checkAcceptance(BalancingAction balancingAction) {
    Replica srcReplica = _clusterModel.broker(balancingAction.sourceBrokerId())
                                      .replica(balancingAction.topicPartition());
    if (shouldExclude(srcReplica, _excludedTopics)) {
      return REPLICA_REJECT;
    }
    Replica destReplica = _clusterModel.broker(balancingAction.destinationBrokerId())
                                       .replica(balancingAction.topicPartition());
    if (shouldExclude(destReplica, _excludedTopics)) {
      return REPLICA_REJECT;
    }
    if (!_selfHealingOnly && !isBalancingActionAppropriate(_clusterModel, balancingAction)) {
      return REPLICA_REJECT;
    }
    return isBalancingActionAcceptable(_clusterModel, _optimizedGoals, balancingAction);
  }

  @Override
  public double brokerMetricValueAfterAction(Broker broker, BalancingAction action) {
    return brokerMetricValueAfterAction(broker, action, _clusterModel);
  }

  // Goal specific abstract method to be implemented.
  /**
   * Get the broker metric value of the given broker. Note that the metric value should be comparable among all the
   * brokers, even when the brokers are running in a heterogeneous environment. It means that a percentage based
   * utilization is needed when applicable, instead of absolute values. In some other cases, however, the values
   * are by nature comparable. For example, latencies.
   *
   * @param broker the given broker to get metric value.
   * @return the metric value of the given broker.
   */
  protected abstract double metricValue(Broker broker);

  /**
   * @return a double value that gives the broker metric after the action is taken.
   */
  protected abstract double brokerMetricValueAfterAction(Broker broker,
                                                         BalancingAction action,
                                                         ClusterModel clusterModel);

  /**
   * Get the average value of the metric that is to be balanced.
   *
   * Note that the metric value should be comparable among all the brokers, even when the brokers are running
   * in a heterogeneous environment. It means that a percentage based utilization is needed when applicable,
   * instead of absolute values. In some other cases, however, the values are by nature comparable.
   * For example, latencies.
   *
   * @param clusterModel the cluster model to balance.
   * @return the average utilization of the metric to be balanced.
   */
  protected abstract double averageMetricValueForCluster(ClusterModel clusterModel);

  /**
   * @return the action types that are possible to make the metric value more balanced.
   */
  protected abstract Set<ActionType> possibleActionTypes();

  /**
   * @return a double that stops the optimization if the two metric values are close enough to each other.
   */
  protected abstract double metricValueEqualityDelta();

  /**
   * @return The mode to search.
   */
  protected SearchMode searchMode() {
    return SearchMode.BINARY_AND_LINEAR;
  }

  /**
   * Update the average metric value and threshold.
   *
   * @param clusterModel the cluster model.
   */
  protected void updateLimits(ClusterModel clusterModel) {
    _averageMetricValue = averageMetricValueForCluster(clusterModel);
    _metricValueUpperLimit = _averageMetricValue * (1 + balancePercentageWithMargin());
    _metricValueLowerLimit = _averageMetricValue * Math.max(0, (1 - balancePercentageWithMargin()));
  }

  // private helper methods.
  /**
   * Optimize the broker if the metric value of the broker is not within the required range.
   *
   * @param allBrokers a sorted set of all the healthy brokers in the cluster.
   * @param toOptimize the broker to optimize
   * @param clusterModel the cluster model
   *
   * @return true if an action has been taken to improve the disk usage of the broker, false when a broker cannot or
   * does not need to be improved further.
   */
  private boolean checkAndOptimize(NavigableSet<BrokerAndSortedReplicas> allBrokers,
                                   BrokerAndSortedReplicas toOptimize,
                                   ClusterModel clusterModel) {
    LOG.trace("Optimizing broker {}. broker metric value = {}, average metric value = {}",
              toOptimize.broker(), dWrap(metricValue(toOptimize.broker())), dWrap(_averageMetricValue));
    double metricValue = metricValue(toOptimize.broker());
    Iterator<BrokerAndSortedReplicas> candidateBrokersToBalanceWithIter;

    if (metricValue > _metricValueUpperLimit) {
      LOG.debug("Broker {} metric value {} is above upper threshold of {}",
                toOptimize.broker().id(), dWrap(metricValue), dWrap(_metricValueUpperLimit));
      // Get the brokers whose disk usage is less than the broker to optimize. The list is in ascending order based on
      // broker disk usage.
      candidateBrokersToBalanceWithIter = allBrokers.headSet(toOptimize).iterator();

    } else if (metricValue < _metricValueLowerLimit) {
      LOG.debug("Broker {} metric value {} is below lower threshold of {}",
                toOptimize.broker().id(), dWrap(metricValue), dWrap(_metricValueLowerLimit));
      // Get the brokers whose disk usage is more than the broker to optimize. The list is in descending order based on
      // broker disk usage.
      candidateBrokersToBalanceWithIter = allBrokers.tailSet(toOptimize, false).descendingIterator();
    } else {
      // Nothing to optimize.
      return false;
    }

    // The broker toBalanceWith is the broker that the broker toOptimize will trade replicas with. The trade could be
    // leader movement, replica movement or replica swap.
    BrokerAndSortedReplicas balancedWith = null;
    try {
      while (candidateBrokersToBalanceWithIter.hasNext()) {
        BrokerAndSortedReplicas toBalanceWith = candidateBrokersToBalanceWithIter.next();
        if (toBalanceWith == toOptimize) {
          continue;
        }
        if (Math.abs(metricValue(toBalanceWith) - metricValue(toOptimize)) < metricValueEqualityDelta() && toOptimize.broker().isAlive()) {
          continue;
        }
        // Remove the brokers involved in swap from the tree set before swap.
        double targetValue = toOptimize.broker().isAlive() ? _averageMetricValue : -1.0;
        for (ActionType type : possibleActionTypes()) {
          if (balanceBetweenBrokers(toOptimize, toBalanceWith, targetValue, type, clusterModel)) {
            balancedWith = toBalanceWith;
            return true;
          }
        }
      }
    } finally {
      if (balancedWith != null) {
        allBrokers.removeAll(Arrays.asList(toOptimize, balancedWith));
        allBrokers.addAll(Arrays.asList(toOptimize, balancedWith));
      }
    }
    return false;
  }

  /**
   * Check whether the cluster model still has brokers whose disk usage are above upper threshold or below lower
   * threshold.
   *
   * @param clusterModel the cluster model to check
   *
   * @return true if all the brokers are within thresholds, false otherwise.
   */
  private boolean isOptimized(ClusterModel clusterModel) throws OptimizationFailureException {
    updateLimits(clusterModel);
    // Check if any broker is out of the allowed usage range.
    Set<Broker> brokersAboveUpperThreshold = new HashSet<>();
    Set<Broker> brokersUnderLowerThreshold = new HashSet<>();
    for (Broker broker : clusterModel.healthyBrokers()) {
      double metricValue = metricValue(broker);
      if (metricValue < _metricValueLowerLimit) {
        brokersUnderLowerThreshold.add(broker);
      } else if (metricValue > _metricValueUpperLimit) {
        brokersAboveUpperThreshold.add(broker);
      }
    }
    boolean selfHealing = !clusterModel.selfHealingEligibleReplicas().isEmpty();
    if (!brokersUnderLowerThreshold.isEmpty()) {
      StringJoiner joiner = new StringJoiner(", ");
      brokersUnderLowerThreshold.forEach(b -> joiner.add(String.format("%d:(%.3f)", b.id(), metricValue(b))));
      LOG.warn("There are still {} brokers under the lower threshold of {} after {}. The brokers are {}",
               brokersUnderLowerThreshold.size(), dWrap(_metricValueLowerLimit),
               selfHealing ? "self-healing" : "rebalance", joiner.toString());
    }
    if (!brokersAboveUpperThreshold.isEmpty()) {
      StringJoiner joiner = new StringJoiner(", ");
      brokersAboveUpperThreshold.forEach(b -> joiner.add(String.format("%d:(%.3f)", b.id(), metricValue(b))));
      LOG.warn("There are still {} brokers above the upper threshold of {} after {}. The brokers are {}",
               brokersAboveUpperThreshold.size(), dWrap(_metricValueUpperLimit),
               selfHealing ? "self-healing" : "rebalance", joiner.toString());
    }
    if (!allDeadBrokersAreEmpty(clusterModel)) {
      throw new OptimizationFailureException(
          "Self healing failed to move the replica away from decommissioned brokers.");
    }
    return brokersUnderLowerThreshold.isEmpty() && brokersAboveUpperThreshold.isEmpty();
  }

  /**
   * Balance between two brokers.
   * @param toOptimize the broker to optimize.
   * @param toBalanceWith the broker to balance with.
   * @param clusterModel the cluster model being optimized.
   * @return true if an action has been taken, false otherwise.
   */
  boolean balanceBetweenBrokers(BrokerAndSortedReplicas toOptimize,
                                BrokerAndSortedReplicas toBalanceWith,
                                double targetMetricValue,
                                ActionType actionType,
                                ClusterModel clusterModel) {
    LOG.debug("Rebalancing between broker {}({}) and broker {}({}) with action {}",
              toOptimize.broker().id(), dWrap(metricValue(toOptimize)), toBalanceWith.broker().id(),
              dWrap(metricValue(toBalanceWith)), actionType);
    DistributionGoalReplicaFinder replicaFinder;
    switch (actionType) {
      case LEADERSHIP_MOVEMENT:
        replicaFinder = new LeaderMovementReplicaFinder(toOptimize,
                                                        toBalanceWith,
                                                        targetMetricValue,
                                                        metricValue(toOptimize.broker()),
                                                        metricValue(toBalanceWith.broker()),
                                                        this);
        break;
      case REPLICA_MOVEMENT:
        replicaFinder = new ReplicaMovementReplicaFinder(toOptimize,
                                                         toBalanceWith,
                                                         targetMetricValue,
                                                         metricValue(toOptimize.broker()),
                                                         metricValue(toBalanceWith.broker()),
                                                         this);
        break;
      case REPLICA_SWAP:
        replicaFinder = new SwapReplicaFinder(toOptimize,
                                              toBalanceWith,
                                              targetMetricValue,
                                              metricValue(toOptimize.broker()),
                                              metricValue(toBalanceWith.broker()),
                                              this);
        break;
      default:
        throw new IllegalArgumentException("Not supported action type " + actionType);
    }

    while (replicaFinder.hasNextAttempt()) {
      replicaFinder = replicaFinder.nextAttempt();
      ReplicaWrapper replicaWrapper = searchForReplica(replicaFinder);
      if (replicaWrapper != null) {
        LOG.debug("Found replica " + replicaWrapper + " for " + actionType);
        BalancingAction balancingAction = replicaFinder.getBalancingActionForReplica(replicaWrapper.replica());

        if (balancingAction.sourceBrokerId() == toOptimize.broker().id()) {
          applyBalancingAction(balancingAction, toOptimize, toBalanceWith, clusterModel);
        } else {
          applyBalancingAction(balancingAction, toBalanceWith, toOptimize, clusterModel);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Broker {} value after action: {}, broker {} value after action: {}",
                    toOptimize.broker().id(), metricValue(toOptimize.broker()),
                    toBalanceWith.broker().id(), metricValue(toBalanceWith.broker()));
        }
        return true;
      }
    }
    return false;
  }

  private ReplicaWrapper searchForReplica(ReplicaFinder replicaFinder) {
    SearchMode searchMode = searchMode();
    switch (searchMode) {
      case BINARY_AND_LINEAR:
        return ReplicaSearchAlgorithm.searchForReplica(replicaFinder);
      case BINARY:
        return ReplicaSearchAlgorithm.binarySearchForReplica(replicaFinder);
      case LINEAR_ASCENDING:
      case LINEAR_DESCENDING:
        NavigableSet<ReplicaWrapper> replicasToSearch = replicaFinder.replicasToSearch();
        ReplicaWrapper startingReplica;
        if (replicasToSearch == null || replicasToSearch.isEmpty()) {
          return null;
        } else {
          startingReplica = searchMode == LINEAR_ASCENDING ? replicasToSearch.first() : replicasToSearch.last();
        }
        return ReplicaSearchAlgorithm.searchForClosestLegitReplica(replicaFinder, startingReplica);
      default:
        throw new IllegalArgumentException("Unknown search mode " + searchMode);
    }
  }

  private boolean allDeadBrokersAreEmpty(ClusterModel clusterModel) {
    for (Broker deadBroker : clusterModel.deadBrokers()) {
      if (!deadBroker.replicas().isEmpty()) {
        return false;
      }
    }
    return true;
  }

  /**
   * To avoid churns, we add a balance margin to the user specified rebalance threshold. e.g. when user sets the
   * threshold to be resourceBalancePercentage, we use (resourceBalancePercentage-1)*balanceMargin instead.
   * @return the rebalance threshold with a margin.
   */
  private double balancePercentageWithMargin() {
    return _metricValueBalancePercent * BALANCE_MARGIN;
  }

  private double metricValue(BrokerAndSortedReplicas bas) {
    return metricValue(bas.broker());
  }

  private DoubleWrapper dWrap(double value) {
    return new DoubleWrapper(value);
  }

  public enum SearchMode {
    BINARY_AND_LINEAR, BINARY, LINEAR_ASCENDING, LINEAR_DESCENDING
  }
}
