/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.internals.BrokerAndSortedReplicas;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.LEADERSHIP_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.REPLICA_MOVEMENT;


/**
 * An abstract class that helps make sure the given metric between different brokers are evenly distributed.
 */
public abstract class MetricDistributionGoal extends AbstractGoal {
  // the maximum allowed metric value difference 
  protected double _maxAllowedUtilizationDiffPercent;
  protected double _averageUtilization;
  private double _utilizationUpperLimit;
  private double _utilizationLowerLimit;

  @Override
  public boolean isActionAcceptable(BalancingAction action, ClusterModel clusterModel) {
    return actionAcceptance(action, clusterModel) == ACCEPT;
  }

  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    Broker srcBroker = clusterModel.broker(action.sourceBrokerId());
    Broker destBroker = clusterModel.broker(action.destinationBrokerId());
    
    // Utilization before action
    double srcUtilizationBeforeAction = metricValue(srcBroker);
    double destUtilizationBeforeAction = metricValue(destBroker);
    
    // Utilization after action
    double srcUtilizationAfterAction = brokerMetricValueAfterAction(srcBroker, action, clusterModel);
    double destUtilizationAfterAction = brokerMetricValueAfterAction(destBroker, action, clusterModel);

    boolean srcBalancedBeforeAction = metricValueWithinLimit(srcUtilizationBeforeAction);
    boolean destBalancedBeforeAction = metricValueWithinLimit(destUtilizationBeforeAction);
    boolean srcBalancedAfterAction = metricValueWithinLimit(srcUtilizationAfterAction);
    boolean destBalancedAfterAction = metricValueWithinLimit(destUtilizationAfterAction);

    if (srcBalancedBeforeAction && destBalancedBeforeAction) {
      // Both source and destination were balanced before action, cannot make them unbalanced.
      return srcBalancedAfterAction && destBalancedAfterAction ? ACCEPT : REPLICA_REJECT;
    } else {
      // Either the source or destination was not balanced, the action should not make the utilization difference larger.
      double utilizationDiffBeforeAction = Math.abs(metricValue(srcBroker) - metricValue(destBroker));
      double utilizationDiffAfterAction = Math.abs(srcUtilizationAfterAction - destUtilizationAfterAction);
      if (utilizationDiffAfterAction <= utilizationDiffBeforeAction) {
        return ACCEPT;
      } else {
        return REPLICA_REJECT;
      }
    }
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return null;
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return null;
  }

  @Override
  public String name() {
    return null;
  }

  @Override
  protected SortedSet<Broker> brokersToBalance(ClusterModel clusterModel) {
    return null;
  }

  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    return false;
  }

  @Override
  protected void initGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws OptimizationFailureException {
    _averageUtilization = averageMetricValueForCluster(clusterModel, excludedTopics);
    _utilizationLowerLimit = _averageUtilization * Math.max(0, 1 - _maxAllowedUtilizationDiffPercent);
    _utilizationUpperLimit = _averageUtilization * (1 + _maxAllowedUtilizationDiffPercent);
  }

  @Override
  protected void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws OptimizationFailureException {

  }

  @Override
  protected void rebalanceForBroker(Broker broker, ClusterModel clusterModel, Set<Goal> optimizedGoals,
                                    Set<String> excludedTopics) throws OptimizationFailureException {
    boolean requireLessLoad = !metricValueUnderUpperLimit(broker);
    boolean requireMoreLoad = !metricValueAboveLowerLimit(broker);
    if (broker.isAlive() && !requireMoreLoad && !requireLessLoad) {
      // return if the broker is already within limit.
      return;
    } else if (!clusterModel.deadBrokers().isEmpty() && requireLessLoad && broker.isAlive()
        && broker.immigrantReplicas().isEmpty()) {
      // return if the cluster is in self-healing mode and the broker requires less load but does not have any
      // immigrant replicas.
      return;
    }

    // First try leadership movement
    if (possibleActionType().contains(LEADERSHIP_MOVEMENT)) {
      if (requireLessLoad && !rebalanceByMovingLoadOut(broker, clusterModel, optimizedGoals,
                                                       LEADERSHIP_MOVEMENT, excludedTopics)) {
        LOG.debug("Successfully balanced {} for broker {} by moving out leaders.", resource(), broker.id());
        return;
      } else if (requireMoreLoad && !rebalanceByMovingLoadIn(broker, clusterModel, optimizedGoals,
                                                             LEADERSHIP_MOVEMENT, excludedTopics)) {
        LOG.debug("Successfully balanced {} for broker {} by moving in leaders.", resource(), broker.id());
        return;
      }
    }

    // Update broker ids over the balance limit for logging purposes.
    boolean unbalanced = false;
    if (requireLessLoad) {
      if (rebalanceByMovingLoadOut(broker, clusterModel, optimizedGoals, REPLICA_MOVEMENT, excludedTopics)) {
        unbalanced = rebalanceBySwappingLoadOut(broker, clusterModel, optimizedGoals, excludedTopics);
      }
    } else if (requireMoreLoad) {
      if (rebalanceByMovingLoadIn(broker, clusterModel, optimizedGoals, REPLICA_MOVEMENT, excludedTopics)) {
        unbalanced = rebalanceBySwappingLoadIn(broker, clusterModel, optimizedGoals, excludedTopics);
      }
    }

    if (!unbalanced) {
      LOG.debug("Successfully balanced {} for broker {} by moving leaders and replicas.", resource(), broker.id());
    }
  }

  // Goal specific abstract method to be implemented.
  /**
   * Get the broker metric value assuming the given action is taken.
   *
   * @param action the action to take.
   * @return the metric value of the broker assuming the given action is taken.
   */
  protected abstract double brokerMetricValueAfterAction(Broker broker, BalancingAction action, ClusterModel clusterModel);

  /**
   * Get the broker metric value of the given broker.
   * @param broker the given broker to get metric value.
   * @return the metric value of the given broker.
   */
  protected abstract double metricValue(Broker broker);

  /**
   * Get the minimum improvement that must be made to take each action.
   *
   * @return the minimum improvement that must be made in each action.
   */
  protected abstract double minActionImprovement();

  /**
   * Get the average utilization of the metric that is to be balanced.
   * @param clusterModel the cluster model to balance.
   * @return the average utilization of the metric to be balanced.
   */
  protected abstract double averageMetricValueForCluster(ClusterModel clusterModel, Set<String> excludedTopics);

  /**
   * Get the action types that are possible to make the metric value more balanced.
   * @return
   */
  protected abstract Set<ActionType> possibleActionType();

  /**
   * Get a comparator to compare the replicas when balance the cluster for the metric. 
   * The comparator should always sort the replica in ascending order.
   * @return An ascending comparator to sort the replicas in a broker for rebalance purpose.
   */
  protected abstract Comparator<Replica> replicaSortingComparator(); 
  
  // Private helper methods.
  private boolean rebalanceByMovingLoadIn(Broker broker,
                                          ClusterModel clusterModel,
                                          Set<Goal> optimizedGoals,
                                          ActionType actionType,
                                          Set<String> excludedTopics) {

    if (!clusterModel.newBrokers().isEmpty() && !broker.isNew()) {
      // We have new brokers and the current broker is not a new broker.
      return true;
    }
    // A priority queue in descending order.
    PriorityQueue<BrokerAndSortedReplicas> candidateBrokerPQ = new PriorityQueue<>((b1, b2) -> {
      int result = Double.compare(metricValue(b2.broker()), metricValue(b1.broker()));
      return result == 0 ? b1.broker().compareTo(b2.broker()) : result; 
    });
    // Sort the replicas initially to avoid sorting it every time.

    double clusterMetricValue = averageMetricValueForCluster(clusterModel, excludedTopics);
    for (Broker candidate : clusterModel.brokers()) {
      // Get candidate replicas on candidate broker to try moving load from -- sorted in the order of trial (descending load).
      if (metricValue(candidate) > clusterMetricValue) {
        BrokerAndSortedReplicas candidateBroker = new BrokerAndSortedReplicas(candidate, replicaSortingComparator());
        candidateBrokerPQ.add(candidateBroker);
      }
    }

    // Stop when all the replicas are leaders for leader movement or there is no replicas that can be moved in anymore
    // for replica movement.
    while (!candidateBrokerPQ.isEmpty() && (actionType == REPLICA_MOVEMENT ||
        (actionType == LEADERSHIP_MOVEMENT && broker.leaderReplicas().size() != broker.replicas().size()))) {
      BrokerAndSortedReplicas brokerAndReplicas = candidateBrokerPQ.poll();
      SortedSet<Replica> candidateReplicasToReceive = brokerAndReplicas.sortedReplicas();

      for (Iterator<Replica> iterator = candidateReplicasToReceive.iterator(); iterator.hasNext(); ) {
        Replica replica = iterator.next();
        Broker b = maybeApplyBalancingAction(clusterModel, replica, Collections.singletonList(broker), actionType, optimizedGoals);

        // Only need to check status if the action is taken. This will also handle the case that the source broker
        // has nothing to move in. In that case we will never reenqueue that source broker.
        if (b != null) {
          if (isLoadAboveBalanceLowerLimit(broker)) {
            return false;
          }
          // Remove the replica from its source broker if it was a replica movement.
          if (actionType == REPLICA_MOVEMENT) {
            iterator.remove();
          }

          // If the source broker has a lower utilization than the next broker in the eligible broker in the queue,
          // we reenqueue the source broker and switch to the next broker.
          if (!candidateBrokerPQ.isEmpty()
              && utilizationPercentage(brokerAndReplicas.broker()) < utilizationPercentage(candidateBrokerPQ.peek().broker())) {
            candidateBrokerPQ.add(brokerAndReplicas);
            break;
          }
        }
      }
    }
    return true;
  }
  
  private boolean metricValueWithinLimit(double metricValue) {
    return metricValue < _utilizationUpperLimit && metricValue >= _utilizationUpperLimit;
  }

  private boolean metricValueAboveLowerLimit(Broker broker) {
    // The action does not matter here because the load is null.
    return metricValue(broker) >= _utilizationLowerLimit;
  }

  private boolean metricValueUnderUpperLimit(Broker broker) {
    // The action does not matter here because the load is null.
    return metricValue(broker) < _utilizationUpperLimit;
  }
}
