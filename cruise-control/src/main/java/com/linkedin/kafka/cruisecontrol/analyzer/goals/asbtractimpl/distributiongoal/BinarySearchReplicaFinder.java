/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.distributiongoal;

import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.ReplicaFinder;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.ReplicaSearchResult;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaWrapper;

import java.util.NavigableSet;

/**
 * A replica finder that is just used to binary search for replicas in the {@link MetricDistributionGoal}.
 * It is responsible for returning a replica that is just within the upper bound and lower bound of the given
 */
public class BinarySearchReplicaFinder implements ReplicaFinder {
  private final double _boundaryValue;
  private final BoundaryType _boundaryType;
  private final BalancingAction _balancingAction;
  private final ClusterModel _clusterModel;
  private final NavigableSet<ReplicaWrapper> _replicasToSearch;
  private final MetricDistributionGoal _goal;
  
  BinarySearchReplicaFinder(double boundaryValue,
                            BoundaryType boundaryType,
                            BalancingAction balancingAction,
                            ClusterModel clusterModel,
                            MetricDistributionGoal goal) {
    _boundaryType = boundaryType;
    _boundaryValue = boundaryValue;
    _balancingAction = balancingAction;
    _clusterModel = clusterModel;
    _goal = goal;

    Broker srcBroker = _clusterModel.broker(balancingAction.sourceBrokerId());
    Broker destBroker = _clusterModel.broker(balancingAction.destinationBrokerId());
    double srcBrokerMetricValue = goal.metricValue(srcBroker);
    double destBrokerMetricValue = goal.metricValue(destBroker);
    _replicasToSearch = null;
  }

  @Override
  public NavigableSet<ReplicaWrapper> replicasToSearch() {
    return _replicasToSearch;
  }

  @Override
  public void checkReplica(Replica replica, ReplicaSearchResult replicaSearchResult) {

  }

  @Override
  public int evaluate(Replica replica) {
    return 0;
  }

  @Override
  public ReplicaWrapper choose(ReplicaWrapper r1, ReplicaWrapper r2) {
    return null;
  }

  enum BoundaryType {
    UPPER, LOWER
  }
}
