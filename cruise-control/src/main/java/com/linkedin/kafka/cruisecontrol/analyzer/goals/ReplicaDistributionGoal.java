/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.distributiongoal.MetricDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.Collections;
import java.util.Map;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.REPLICA_MOVEMENT;


/**
 * Class for achieving the following soft goal:
 * Generate replica movement proposals to ensure that the number of replicas on each broker is
 * <ul>
 * <li>Under: (the average number of replicas per broker) * (1 + replica count balance percentage)</li>
 * <li>Above: (the average number of replicas per broker) * Math.max(0, 1 - replica count balance percentage)</li>
 * </ul>
 * Also see: {@link com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig#REPLICA_COUNT_BALANCE_THRESHOLD_DOC}
 * and {@link #balancePercentageWithMargin()}.
 */
public class ReplicaDistributionGoal extends MetricDistributionGoal {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaDistributionGoal.class);
  private double _avgReplicasOnHealthyBroker;

  /**
   * Constructor for Replica Distribution Goal.
   */
  public ReplicaDistributionGoal() {

  }

  public ReplicaDistributionGoal(BalancingConstraint balancingConstraint) {
    _balancingConstraint = balancingConstraint;
    _metricValueBalancePercent = _balancingConstraint.replicaBalancePercentage() - 1;
  }

  @Override
  protected SearchMode searchMode() {
    return SearchMode.LINEAR_ASCENDING;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _metricValueBalancePercent = _balancingConstraint.replicaBalancePercentage() - 1;
  }

  /**
   * Check whether the given action is acceptable by this goal. An action is acceptable if the number of replicas at
   * (1) the source broker does not go under the allowed limit.
   * (2) the destination broker does not go over the allowed limit.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    switch (action.actionType()) {
      case REPLICA_SWAP:
      case LEADERSHIP_MOVEMENT:
        return ACCEPT;
      case REPLICA_MOVEMENT:
        return super.actionAcceptance(action, clusterModel);
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.actionType() + " is provided.");
    }
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new ReplicaDistributionGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(1, 0.0, true);
  }

  @Override
  protected double metricValue(Broker broker) {
    return broker.isAlive() ? broker.replicas().size() : Integer.MAX_VALUE;
  }

  @Override
  protected double brokerMetricValueAfterAction(Broker broker, BalancingAction action, ClusterModel clusterModel) {
    if (!broker.isAlive()) {
      return Integer.MAX_VALUE;
    }
    switch (action.actionType()) {
      case REPLICA_MOVEMENT:
        if (broker.id() == action.destinationBrokerId()) {
          return broker.replicas().size() + 1;
        } else if (broker.id() == action.sourceBrokerId()) {
          return broker.replicas().size() - 1;
        }
        // fall through.
      case LEADERSHIP_MOVEMENT:
      case REPLICA_SWAP:
        return broker.replicas().size();
      default:
        throw new IllegalArgumentException("Unsupported action type " + action.actionType());
    }
  }

  @Override
  protected double averageMetricValueForCluster(ClusterModel clusterModel) {
    return _avgReplicasOnHealthyBroker;
  }

  @Override
  protected Set<ActionType> possibleActionTypes() {
    return Collections.singleton(REPLICA_MOVEMENT);
  }

  @Override
  protected double metricValueEqualityDelta() {
    return 1;
  }

  /**
   * Get brokers that the rebalance process will go over to apply balancing actions to replicas they contain.
   *
   * @param clusterModel The state of the cluster.
   * @return A collection of brokers that the rebalance process will go over to apply balancing actions to replicas
   * they contain.
   */
  @Override
  protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
    return clusterModel.brokers();
  }

  /**
   * Initiates replica distribution goal.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, Set<Goal> optimizedGoals, Set<String> excludedTopics) {
    // Initialize the average replicas on a healthy broker.
    int numReplicasInCluster = clusterModel.getReplicaDistribution().values().stream().mapToInt(List::size).sum();
    _avgReplicasOnHealthyBroker = (numReplicasInCluster / (double) clusterModel.healthyBrokers().size());
    super.initGoalState(clusterModel, optimizedGoals, excludedTopics);
  }

  @Override
  public double impactScore(Replica replica) {
    return replica.load().expectedUtilizationFor(Resource.DISK);
  }

  private class ReplicaDistributionGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;
    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Standard deviation of number of replicas over brokers in the current must be less than the pre-optimized stats.
      double stDev1 = stats1.replicaStats().get(Statistic.ST_DEV).doubleValue();
      double stDev2 = stats2.replicaStats().get(Statistic.ST_DEV).doubleValue();
      int result = AnalyzerUtils.compare(stDev2, stDev1, AnalyzerUtils.EPSILON);
      if (result < 0) {
        _reasonForLastNegativeResult = String.format("Violated %s. [Std Deviation of Replica Distribution] post-"
                                                     + "optimization:%.3f pre-optimization:%.3f", name(), stDev1, stDev2);
      }
      return result;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }

  /**
   * Whether bring replica in or out.
   */
  protected enum ChangeType {
    ADD, REMOVE
  }
}
