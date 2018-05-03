/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.distributiongoal.MetricDistributionGoal;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Load;
import com.linkedin.kafka.cruisecontrol.model.Replica;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;

import java.util.TreeSet;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.*;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal.ChangeType.ADD;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ResourceDistributionGoal.ChangeType.REMOVE;


/**
 * Class for achieving the following soft goal:
 * <p>
 * SOFT GOAL#3: Balance resource distribution over brokers (e.g. cpu, disk, inbound / outbound network traffic).
 */
public abstract class ResourceDistributionGoal extends MetricDistributionGoal {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceDistributionGoal.class);

  /**
   * Constructor for Resource Distribution Goal.
   */
  public ResourceDistributionGoal() {

  }

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    _metricValueBalancePercent = _balancingConstraint.resourceBalancePercentage(resource()) - 1;
  }

  /**
   * Package private for unit test.
   */
  ResourceDistributionGoal(BalancingConstraint constraint) {
    _balancingConstraint = constraint;
    _metricValueBalancePercent = _balancingConstraint.resourceBalancePercentage(resource()) - 1;
  }

  protected abstract Resource resource();

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new ResourceDistributionGoalStatsComparator();
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
    Comparator<Broker> brokerComparator = Comparator.comparingDouble(this::metricValue)
                                                    .thenComparingInt(Broker::id);
    SortedSet<Broker> brokersToBalance = new TreeSet<>(brokerComparator);
    if (clusterModel.newBrokers().isEmpty()) {
      brokersToBalance.addAll(clusterModel.brokers());
    } else {
      brokersToBalance.addAll(clusterModel.newBrokers());
    }
    return brokersToBalance;
  }

  /**
   * Check if requirements of this goal are not violated if this action is applied to the given cluster state,
   * false otherwise. An action is acceptable if: (1) destination broker utilization for the given resource is less
   * than the source broker utilization. (2) movement is acceptable (i.e. under the broker balance limit for balanced
   * resources) for already balanced resources. Already balanced resources are the ones that have gone through the
   * "resource distribution" process specified in this goal.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model.
   * @return True if requirements of this goal are not violated if this action is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    Replica sourceReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
    // If the source broker is dead and currently self healing dead brokers only, unless it is replica swap, the action
    // must be executed.
    if (!sourceReplica.broker().isAlive() && _selfHealingOnly) {
      return action.actionType() != REPLICA_SWAP;
    }

    switch (action.actionType()) {
      case REPLICA_SWAP:
        Replica destinationReplica = destinationBroker.replica(action.destinationTopicPartition());
        double sourceUtilizationDelta = destinationReplica.load().expectedUtilizationFor(resource())
                                        - sourceReplica.load().expectedUtilizationFor(resource());

        return sourceUtilizationDelta != 0 && !isSwapViolatingLimit(sourceReplica, destinationReplica);
      case REPLICA_MOVEMENT:
      case LEADERSHIP_MOVEMENT:
        //Check that current destination would not become more unbalanced.
        return isLoadUnderBalanceUpperLimitAfterChange(sourceReplica.load(), destinationBroker, ADD) &&
               isLoadAboveBalanceLowerLimitAfterChange(sourceReplica.load(), sourceReplica.broker(), REMOVE);
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.actionType() + " is provided.");
    }
  }

  @Override
  protected double averageMetricValueForCluster(ClusterModel clusterModel) {
    return (clusterModel.load().expectedUtilizationFor(resource()) / clusterModel.capacityFor(resource()));
  }

  @Override
  protected double metricValueEqualityDelta() {
    return 0.001;
  }

  protected double metricValue(Broker broker) {
    return broker.isAlive() ? broker.load().expectedUtilizationFor(resource()) / broker.capacityFor(resource()) : 1;
  }

  @Override
  protected double brokerMetricValueAfterAction(Broker broker, BalancingAction action, ClusterModel clusterModel) {
    if (!broker.isAlive()) {
      return 1.0;
    }
    Replica srcReplica = clusterModel.broker(action.sourceBrokerId()).replica(action.topicPartition());
    Broker destBroker = clusterModel.broker(action.destinationBrokerId());
    double absoluteValue;
    switch (action.actionType()) {
      case LEADERSHIP_MOVEMENT:
        AggregatedMetricValues leaderLoadDelta = srcReplica.leaderLoadDelta();
        if (broker.id() == srcReplica.broker().id()) {
          double sourceBrokerLoad = srcReplica.broker().load().expectedUtilizationFor(resource());
          double leaderLoad = Load.expectedUtilizationFor(resource(), leaderLoadDelta, true);
          absoluteValue = sourceBrokerLoad - leaderLoad;
        } else if (broker.id() == destBroker.id()) {
          double destBrokerLoad = destBroker.load().expectedUtilizationFor(resource());
          double leaderLoad = Load.expectedUtilizationFor(resource(), leaderLoadDelta, true);
          absoluteValue = destBrokerLoad + leaderLoad;
        } else {
          absoluteValue = broker.load().expectedUtilizationFor(resource());
        }
        break;
      case REPLICA_MOVEMENT:
        if (broker.id() == srcReplica.broker().id()) {
          double sourceBrokerLoad = srcReplica.broker().load().expectedUtilizationFor(resource());
          absoluteValue = sourceBrokerLoad - srcReplica.load().expectedUtilizationFor(resource());
        } else if (broker.id() == destBroker.id()) {
          double destBrokerLoad = destBroker.load().expectedUtilizationFor(resource());
          absoluteValue = destBrokerLoad + srcReplica.load().expectedUtilizationFor(resource());
        } else {
          absoluteValue = broker.load().expectedUtilizationFor(resource());
        }
        break;
      case REPLICA_SWAP:
        Replica destReplica = destBroker.replica(action.destinationTopicPartition());
        double srcBrokerLoad = srcReplica.broker().load().expectedUtilizationFor(resource());
        double destBrokerLoad = destBroker.load().expectedUtilizationFor(resource());
        double srcReplicaLoad = srcReplica.load().expectedUtilizationFor(resource());
        double destReplicaLoad = destReplica.load().expectedUtilizationFor(resource());
        if (broker.id() == srcReplica.broker().id()) {
          absoluteValue = srcBrokerLoad + destReplicaLoad - srcReplicaLoad;
        } else if (broker.id() == destBroker.id()) {
          absoluteValue = destBrokerLoad + srcReplicaLoad - destReplicaLoad;
        } else {
          absoluteValue = broker.load().expectedUtilizationFor(resource());
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported action type " + action.actionType());
    }
    return absoluteValue / broker.capacityFor(resource());
  }

  @Override
  public Function<Replica, Double> replicaImpactScoreFunction() {
    return r -> r.load().expectedUtilizationFor(resource());
  }

  protected double balanceUpperThreshold() {
    return _metricValueUpperLimit;
  }

  protected double balanceLowerThreshold() {
    return _metricValueLowerLimit;
  }

  private boolean isLoadAboveBalanceLowerLimitAfterChange(Load load,
                                                          Broker broker,
                                                          ChangeType changeType) {
    double utilizationDelta = load == null ? 0 : load.expectedUtilizationFor(resource());

    double brokerBalanceLowerLimit = broker.capacityFor(resource()) * _metricValueLowerLimit;
    double brokerUtilization = broker.load().expectedUtilizationFor(resource());
    boolean isBrokerAboveLowerLimit = changeType == ADD ? brokerUtilization + utilizationDelta >= brokerBalanceLowerLimit :
                                      brokerUtilization - utilizationDelta >= brokerBalanceLowerLimit;

    if (resource().isHostResource()) {
      double hostBalanceLowerLimit = broker.host().capacityFor(resource()) * _metricValueLowerLimit;
      double hostUtilization = broker.host().load().expectedUtilizationFor(resource());
      boolean isHostAboveLowerLimit = changeType == ADD ? hostUtilization + utilizationDelta >= hostBalanceLowerLimit :
                                      hostUtilization - utilizationDelta >= hostBalanceLowerLimit;
      // As long as either the host or the broker is above the limit, we claim the host resource utilization is
      // above the limit. If the host is below limit, there must be at least one broker below limit. We should just
      // bring more load to that broker.
      return isHostAboveLowerLimit || isBrokerAboveLowerLimit;
    } else {
      return isBrokerAboveLowerLimit;
    }
  }

  private boolean isLoadUnderBalanceUpperLimitAfterChange(Load load,
                                                          Broker broker,
                                                          ChangeType changeType) {
    double utilizationDelta = load == null ? 0 : load.expectedUtilizationFor(resource());

    double brokerBalanceUpperLimit = broker.capacityFor(resource()) * _metricValueUpperLimit;
    double brokerUtilization = broker.load().expectedUtilizationFor(resource());
    boolean isBrokerUnderUpperLimit = changeType == ADD ? brokerUtilization + utilizationDelta <= brokerBalanceUpperLimit :
                                      brokerUtilization - utilizationDelta <= brokerBalanceUpperLimit;

    if (resource().isHostResource()) {
      double hostBalanceUpperLimit = broker.host().capacityFor(resource()) * _metricValueUpperLimit;
      double hostUtilization = broker.host().load().expectedUtilizationFor(resource());
      boolean isHostUnderUpperLimit = changeType == ADD ? hostUtilization + utilizationDelta <= hostBalanceUpperLimit :
                                      hostUtilization - utilizationDelta <= hostBalanceUpperLimit;
      // As long as either the host or the broker is under the limit, we claim the host resource utilization is
      // under the limit. If the host is above limit, there must be at least one broker above limit. We should just
      // move load off that broker.
      return isHostUnderUpperLimit || isBrokerUnderUpperLimit;
    } else {
      return isBrokerUnderUpperLimit;
    }
  }

  private boolean isSwapViolatingLimit(Replica sourceReplica, Replica destinationReplica) {
    double sourceUtilizationDelta = destinationReplica.load().expectedUtilizationFor(resource())
                                    - sourceReplica.load().expectedUtilizationFor(resource());
    // Check: Broker resource load within balance limit check.
    boolean swapViolatingBrokerLimit = isSwapViolatingContainerLimit(sourceUtilizationDelta,
                                                                     sourceReplica,
                                                                     destinationReplica,
                                                                     r -> r.broker().load(),
                                                                     r -> r.broker().capacityFor(resource()));

    if (!swapViolatingBrokerLimit || !resource().isHostResource()) {
      return swapViolatingBrokerLimit;
    }

    // Check: Host resource load within balance limit check.
    return isSwapViolatingContainerLimit(sourceUtilizationDelta,
                                         sourceReplica,
                                         destinationReplica,
                                         r -> r.broker().host().load(),
                                         r -> r.broker().host().capacityFor(resource()));
  }

  private boolean isSwapViolatingContainerLimit(double sourceUtilizationDelta,
                                                Replica sourceReplica,
                                                Replica destinationReplica,
                                                Function<Replica, Load> loadFunction,
                                                Function<Replica, Double> capacityFunction) {
    // Container could be host or broker.
    double sourceContainerUtilization = loadFunction.apply(sourceReplica).expectedUtilizationFor(resource());
    double destinationContainerUtilization = loadFunction.apply(destinationReplica).expectedUtilizationFor(resource());

    // 1. Container under balance upper limit check.
    boolean isContainerUnderUpperLimit;
    if (sourceUtilizationDelta > 0) {
      double sourceContainerBalanceUpperLimit = capacityFunction.apply(sourceReplica) * _metricValueUpperLimit;
      isContainerUnderUpperLimit = sourceContainerUtilization + sourceUtilizationDelta <= sourceContainerBalanceUpperLimit;
    } else {
      double destinationContainerBalanceUpperLimit = capacityFunction.apply(destinationReplica) * _metricValueUpperLimit;
      isContainerUnderUpperLimit = destinationContainerUtilization - sourceUtilizationDelta <= destinationContainerBalanceUpperLimit;
    }

    if (!isContainerUnderUpperLimit) {
      return true;
    }
    // 2. Container above balance lower limit check.
    boolean isContainerAboveLowerLimit;
    if (sourceUtilizationDelta < 0) {
      double sourceContainerBalanceLowerLimit = capacityFunction.apply(sourceReplica) * _metricValueLowerLimit;
      isContainerAboveLowerLimit = sourceContainerUtilization + sourceUtilizationDelta >= sourceContainerBalanceLowerLimit;
    } else {
      double destinationContainerBalanceLowerLimit = capacityFunction.apply(destinationReplica) * _metricValueLowerLimit;
      isContainerAboveLowerLimit = destinationContainerUtilization - sourceUtilizationDelta >= destinationContainerBalanceLowerLimit;
    }

    return !isContainerAboveLowerLimit;
  }

  private class ResourceDistributionGoalStatsComparator implements ClusterModelStatsComparator {
    private String _reasonForLastNegativeResult;

    @Override
    public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
      // Number of balanced brokers in the highest priority resource cannot be more than the pre-optimized
      // stats. This constraint is applicable for the rest of the resources, if their higher priority resources
      // have the same number of balanced brokers in their corresponding pre- and post-optimized stats.
        int numBalancedBroker1 = stats1.numBalancedBrokersByResource().get(resource());
        int numBalancedBroker2 = stats2.numBalancedBrokersByResource().get(resource());
        // First compare the number of balanced brokers
        if (numBalancedBroker2 > numBalancedBroker1) {
          // If the number of balanced brokers has increased, the standard deviation of utilization for the resource
          // must decrease. Otherwise, the goal is producing a worse cluster state.
          double afterUtilizationStd = stats1.resourceUtilizationStats().get(Statistic.ST_DEV).get(resource());
          double beforeUtilizationStd = stats2.resourceUtilizationStats().get(Statistic.ST_DEV).get(resource());
          if (Double.compare(beforeUtilizationStd, afterUtilizationStd) < 0) {
            _reasonForLastNegativeResult = String.format(
                "Violated %s. [Number of Balanced Brokers] for resource %s. post-optimization:%d pre-optimization:%d "
                + "without improving the standard dev. of utilization. post-optimization:%.2f pre-optimization:%.2f",
                name(), resource(), numBalancedBroker1, numBalancedBroker2, afterUtilizationStd, beforeUtilizationStd);
            return -1;
          }
        }
      return 1;
    }

    @Override
    public String explainLastComparison() {
      return _reasonForLastNegativeResult;
    }
  }

  /**
   * Whether bring load in or bring load out.
   */
  protected enum ChangeType {
    ADD, REMOVE
  }
}
