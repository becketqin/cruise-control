/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.Goal;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;

import java.util.ArrayList;
import java.util.List;

import java.util.Map;
import java.util.SortedSet;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.*;


/**
 * An abstract class for goals. This class will be extended to crete custom goals for different purposes -- e.g.
 * balancing the distribution of replicas or resources in the cluster.
 */
public abstract class AbstractGoal implements Goal {
  private final Logger LOG = LoggerFactory.getLogger(getClass());
  private boolean _finished;
  protected final String _name;
  protected boolean _succeeded = true;
  protected BalancingConstraint _balancingConstraint;
  protected int _numWindows = 1;
  protected double _minMonitoredPartitionPercentage = 0.995;

  /**
   * Constructor of Abstract Goal class sets the _finished flag to false to signal that the goal requirements have not
   * been satisfied, yet.
   */
  public AbstractGoal() {
    _finished = false;
    _name = getClass().getSimpleName();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    _balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(configs, false));
    String numWindowsString = (String) configs.get(KafkaCruiseControlConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG);
    if (numWindowsString != null && !numWindowsString.isEmpty()) {
      _numWindows = Integer.parseInt(numWindowsString);
    }
    String minMonitoredPartitionPercentageString =
        (String) configs.get(KafkaCruiseControlConfig.MIN_VALID_PARTITION_RATIO_CONFIG);
    if (minMonitoredPartitionPercentageString != null
        && !minMonitoredPartitionPercentageString.isEmpty()) {
      _minMonitoredPartitionPercentage = Double.parseDouble(minMonitoredPartitionPercentageString);
    }
  }

  @Override
  public boolean optimize(ClusterModel clusterModel, Set<Goal> optimizedGoals, Set<String> excludedTopics)
      throws OptimizationFailureException {
    _succeeded = true;
    LOG.debug("Starting optimization for {}.", name());
    // Initialize pre-optimized stats.
    ClusterModelStats statsBeforeOptimization = clusterModel.getClusterStats(_balancingConstraint);
    LOG.trace("[PRE - {}] {}", name(), statsBeforeOptimization);
    _finished = false;
    long goalStartTime = System.currentTimeMillis();
    initGoalState(clusterModel, optimizedGoals, excludedTopics);
    Collection<Broker> deadBrokers = clusterModel.deadBrokers();

    while (!_finished) {
      for (Broker broker : brokersToBalance(clusterModel)) {
        rebalanceForBroker(broker, clusterModel, optimizedGoals, excludedTopics);
      }
      updateGoalState(clusterModel, excludedTopics);
    }
    ClusterModelStats statsAfterOptimization = clusterModel.getClusterStats(_balancingConstraint);
    LOG.trace("[POST - {}] {}", name(), statsAfterOptimization);
    LOG.debug("Finished optimization for {} in {}ms.", name(), System.currentTimeMillis() - goalStartTime);
    LOG.trace("Cluster after optimization is {}", clusterModel);
    // We only ensure the optimization did not make stats worse when it is not self-healing.
    if (deadBrokers.isEmpty()) {
      ClusterModelStatsComparator comparator = clusterModelStatsComparator();
      // Throw exception when the stats before optimization is preferred.
      if (comparator.compare(statsAfterOptimization, statsBeforeOptimization) < 0) {
        throw new OptimizationFailureException("Optimization for Goal " + name() + " failed because the optimized"
                                               + "result is worse than before. Detail reason: "
                                               + comparator.explainLastComparison());
      }
    }
    return _succeeded;
  }

  @Override
  public boolean isActionAcceptable(BalancingAction action, ClusterModel clusterModel) {
    return actionAcceptance(action, clusterModel) == ACCEPT;
  }

  @Override
  public String name() {
    return _name;
  }

  /**
   * Check whether the replica should be excluded from the rebalance. A replica should be excluded if its topic
   * is in the excluded topics set and its broker is still alive.
   * @param replica the replica to check.
   * @param excludedTopics the excluded topics set.
   * @return true if the replica should be excluded, false otherwise.
   */
  public boolean shouldExclude(Replica replica, Set<String> excludedTopics) {
    return replica != null
        && excludedTopics.contains(replica.topicPartition().topic())
        && replica.originalBroker().isAlive();
  }

  /**
   * Get sorted brokers that the rebalance process will go over to apply balancing actions to replicas they contain.
   *
   * @param clusterModel The state of the cluster.
   * @return A collection of brokers that the rebalance process will go over to apply balancing actions to replicas
   * they contain.
   */
  protected abstract SortedSet<Broker> brokersToBalance(ClusterModel clusterModel);

  /**
   * Check if requirements of this goal are not violated if this action is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model.
   * @return True if requirements of this goal are not violated if this action is applied to the given cluster state,
   * false otherwise.
   */
  protected abstract boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action);

  /**
   * Signal for finishing the process for rebalance or self-healing for this goal.
   */
  protected void finish() {
    _finished = true;
  }

  /**
   * (1) Initialize states that this goal requires -- e.g. in TopicReplicaDistributionGoal and ReplicaDistributionGoal,
   * this method is used to populate the ReplicaDistributionTarget(s). (2) Run sanity checks regarding minimum
   * requirements of hard goals.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  protected abstract void initGoalState(ClusterModel clusterModel, Set<Goal> optimizedGoals, Set<String> excludedTopics)
      throws OptimizationFailureException;

  /**
   * Update goal state after one round of self-healing / rebalance.
   *
   * @param clusterModel The state of the cluster.
   * @param excludedTopics The topics that should be excluded from the optimization action.
   */
  protected abstract void updateGoalState(ClusterModel clusterModel, Set<String> excludedTopics)
      throws OptimizationFailureException;

  /**
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
   *
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param excludedTopics The topics that should be excluded from the optimization proposals.
   */
  protected abstract void rebalanceForBroker(Broker broker,
                                             ClusterModel clusterModel,
                                             Set<Goal> optimizedGoals,
                                             Set<String> excludedTopics)
      throws OptimizationFailureException;

  /**
   * Attempt to apply the given balancing action to the given replica in the given cluster. The application
   * considers the candidate brokers as the potential destination brokers for replica movement or the location of
   * followers for leadership transfer. If the movement attempt succeeds, the function returns the broker id of the
   * destination, otherwise the function returns null.
   *
   * @param clusterModel    The state of the cluster.
   * @param replica         Replica to be applied the given balancing action.
   * @param candidateBrokers Candidate brokers as the potential destination brokers for replica movement or the location
   *                        of followers for leadership transfer.
   * @param action          Balancing action.
   * @param optimizedGoals  Optimized goals.
   * @return Broker id of the destination if the movement attempt succeeds, null otherwise.
   */
  protected Broker maybeApplyBalancingAction(ClusterModel clusterModel,
                                             Replica replica,
                                             Collection<Broker> candidateBrokers,
                                             ActionType action,
                                             Set<Goal> optimizedGoals) {
    // In self healing mode, allow a move only from dead to alive brokers.
    if (!clusterModel.deadBrokers().isEmpty() && replica.originalBroker().isAlive()) {
      LOG.trace("Applying {} to a replica in a healthy broker in self-healing mode.", action);
    }
    for (Broker broker : candidateBrokers) {
      if (!clusterModel.newBrokers().isEmpty() && !broker.isNew() && replica.originalBroker() != broker) {
        continue;
      }
      BalancingAction balancingAction =
          new BalancingAction(replica.topicPartition(), replica.broker().id(), broker.id(), action);
      // A replica should be moved if:
      // 0. The move is legit.
      // 1. The goal requirements are not violated if this action is applied to the given cluster state.
      // 2. The movement is acceptable by the previously optimized goals.

      if (!isBalancingActionLegit(clusterModel, balancingAction)) {
        LOG.debug("Replica move is not legit for {}.", balancingAction);
        continue;
      }

      if (!selfSatisfied(clusterModel, balancingAction)) {
        LOG.debug("Unable to self-satisfy proposal {}.", balancingAction);
        continue;
      }

      ActionAcceptance acceptance =
          AnalyzerUtils.isProposalAcceptableForOptimizedGoals(optimizedGoals, balancingAction, clusterModel);
      LOG.debug("Trying to apply legit and self-satisfied action {}, actionAcceptance = {}", balancingAction, acceptance);
      if (acceptance == ACCEPT) {
        if (action == ActionType.LEADERSHIP_MOVEMENT) {
          clusterModel.relocateLeadership(replica.topicPartition(), replica.broker().id(), broker.id());
        } else if (action == ActionType.REPLICA_MOVEMENT) {
          clusterModel.relocateReplica(replica.topicPartition(), replica.broker().id(), broker.id());
        }
        return broker;
      }
    }
    return null;
  }

  protected void applyBalancingAction(BalancingAction balancingAction,
                                      Broker srcBrokerReplica,
                                      Broker destBrokerReplica,
                                      ClusterModel clusterModel) {
    LOG.debug("Applying action {} to the cluster", balancingAction);
    switch (balancingAction.actionType()) {
      case LEADERSHIP_MOVEMENT:
        clusterModel.relocateLeadership(balancingAction.topicPartition(),
                                        balancingAction.sourceBrokerId(),
                                        balancingAction.destinationBrokerId());
        break;
      case REPLICA_MOVEMENT:
        clusterModel.relocateReplica(balancingAction.topicPartition(),
                                     balancingAction.sourceBrokerId(),
                                     balancingAction.destinationBrokerId());
        break;
      case REPLICA_SWAP:
        clusterModel.relocateReplica(balancingAction.topicPartition(),
                                     balancingAction.sourceBrokerId(),
                                     balancingAction.destinationBrokerId());
        clusterModel.relocateReplica(balancingAction.destinationTopicPartition(),
                                     balancingAction.destinationBrokerId(),
                                     balancingAction.sourceBrokerId());
        break;
      default:
        throw new IllegalArgumentException("Unsupported action type " + balancingAction.actionType());
    }
  }

  /**
   * Check if the given action is acceptable by this goal and all the optimized goals.
   *
   * @param clusterModel the cluster being optimized.
   * @param optimizedGoals the already optimized goals.
   * @param balancingAction the balancing action to make.
   * @return the ActionAcceptance for the given balancing action.
   */
  protected ActionAcceptance isBalancingActionAcceptable(ClusterModel clusterModel,
                                                         Set<Goal> optimizedGoals,
                                                         BalancingAction balancingAction) {
    if (!isBalancingActionLegit(clusterModel, balancingAction)) {
      LOG.trace("Rejected illegal action {}", balancingAction);
      return REPLICA_REJECT;
    } else if (!selfSatisfied(clusterModel, balancingAction)) {
      LOG.trace("Rejected action {} because it does not satisfy the current goal {}", balancingAction, name());
      return REPLICA_REJECT;
    } else {
      ActionAcceptance acceptance =
          AnalyzerUtils.isProposalAcceptableForOptimizedGoals(optimizedGoals, balancingAction, clusterModel);
      LOG.trace("Rejected action {} because the proposal is not acceptable by optimized goals.", balancingAction);
      return acceptance;
    }
  }

  /**
   * Check if the balancing action is appropriate. The following rules are used exactly in the following order:
   * 1. When there are brokers needs self-healing,
   *    a: A leadership movement is always allowed.
   *    b: A replica movement is allowed only if it is moving out a replica from a dead broker.
   *    c: A replica swap is not allowed.
   * 2. When there are new brokers in the cluster,
   *    a. A leadership movement is always allowed.
   *    b. A replica movement is only allowed if the destination broker is the original broker or a new broker.
   *    c. A replica swap is only allowed if the destination of both replicas are new brokers or their original brokers.
   *
   * @param clusterModel the cluster model.
   * @param balancingAction the balancing action to check.
   * @return whether the balancing action is appropriate.
   */
  protected static boolean isBalancingActionAppropriate(ClusterModel clusterModel, BalancingAction balancingAction) {
    if (!clusterModel.selfHealingEligibleReplicas().isEmpty()) {
      switch (balancingAction.actionType()) {
        case LEADERSHIP_MOVEMENT:
          return true;
        case REPLICA_MOVEMENT:
          return !clusterModel.broker(balancingAction.sourceBrokerId()).isAlive();
        case REPLICA_SWAP:
          return false;
        default:
          throw new IllegalArgumentException("Unsupported action type " + balancingAction.actionType());
      }
    } else if (!clusterModel.newBrokers().isEmpty()) {
      switch (balancingAction.actionType()) {
        case LEADERSHIP_MOVEMENT:
          return true;
        case REPLICA_MOVEMENT:
          Broker destBroker = clusterModel.broker(balancingAction.destinationBrokerId());
          TopicPartition tp = balancingAction.topicPartition();
          Replica replica = clusterModel.broker(balancingAction.sourceBrokerId()).replica(tp);
          return destBroker.isNew() || replica.originalBroker().id() == destBroker.id();
        case REPLICA_SWAP:
          Broker srcBroker = clusterModel.broker(balancingAction.sourceBrokerId());
          destBroker = clusterModel.broker(balancingAction.destinationBrokerId());
          Replica srcReplica = srcBroker.replica(balancingAction.topicPartition());
          Replica destReplica = destBroker.replica(balancingAction.destinationTopicPartition());
          return (srcBroker.isNew() && destBroker.isNew()) // both srcBroker and destBroker are new
              || (srcReplica.originalBroker() == destBroker && srcBroker.isNew()) // destBroker is old.
              || (destReplica.originalBroker() == srcBroker && destBroker.isNew()); // srcBroker is old.
        default:
          throw new IllegalArgumentException("Unsupported action type " + balancingAction.actionType());
      }
    }
    return true;
  }

  /**
   * Whether the balancing action violates any basic rules.
   *
   * @param clusterModel the cluster model to apply the balancing action.
   * @param balancingAction the balancing action to check.
   * @return true if the balancing action is legit, false otherwise.
   */
  private boolean isBalancingActionLegit(ClusterModel clusterModel, BalancingAction balancingAction) {
    Broker srcBroker;
    Broker destBroker;
    switch (balancingAction.actionType()) {
      case REPLICA_MOVEMENT:
        destBroker = clusterModel.broker(balancingAction.destinationBrokerId());
        return destBroker.replica(balancingAction.topicPartition()) == null;
      case LEADERSHIP_MOVEMENT:
        srcBroker = clusterModel.broker(balancingAction.sourceBrokerId());
        if (!srcBroker.replica(balancingAction.topicPartition()).isLeader()) {
          // The source replica must be leader.
          return false;
        }
        // The destination replica must exist.
        destBroker = clusterModel.broker(balancingAction.destinationBrokerId());
        return destBroker.replica(balancingAction.topicPartition()) != null;
      case REPLICA_SWAP:
        srcBroker = clusterModel.broker(balancingAction.sourceBrokerId());
        // Source broker cannot contain destination replica
        if (srcBroker.replica(balancingAction.destinationTopicPartition()) != null) {
          return false;
        }
        // The destination broker cannot contain source replica.
        destBroker = clusterModel.broker(balancingAction.destinationBrokerId());
        return destBroker.replica(balancingAction.topicPartition()) == null;
      default:
        return false;
    }
  }

  private Collection<Broker> getEligibleBrokers(ClusterModel clusterModel,
                                                Replica replica,
                                                Collection<Broker> candidateBrokers) {
    if (clusterModel.newBrokers().isEmpty()) {
      return candidateBrokers;
    } else {
      List<Broker> eligibleBrokers = new ArrayList<>();
      // When there are new brokers, we should only allow the replicas to be moved to the new brokers.
      candidateBrokers.forEach(b -> {
        if (b.isNew() || b == replica.originalBroker()) {
          eligibleBrokers.add(b);
        }
      });
      return eligibleBrokers;
    }
  }

  @Override
  public String toString() {
    return name();
  }
}
