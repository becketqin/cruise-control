/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.LEADERSHIP_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.REPLICA_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.REPLICA_SWAP;


public class NetworkInboundUsageDistributionGoal extends ResourceDistributionGoal {
  private final Set<ActionType> _actionTypes =
      new HashSet<>(Arrays.asList(LEADERSHIP_MOVEMENT, REPLICA_MOVEMENT, REPLICA_SWAP));
  /**
   * Constructor for Resource Distribution Goal.
   */
  public NetworkInboundUsageDistributionGoal() {
    super();
  }

  /**
   * Package private for unit test.
   */
  NetworkInboundUsageDistributionGoal(BalancingConstraint constraint) {
    super(constraint);
  }

  @Override
  protected Resource resource() {
    return Resource.NW_IN;
  }

  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    // Leadership movement won't cause inbound network utilization change.
    return action.actionType() == LEADERSHIP_MOVEMENT ? ACCEPT : super.actionAcceptance(action, clusterModel);
  }

  @Override
  public String name() {
    return NetworkInboundUsageDistributionGoal.class.getSimpleName();
  }

  @Override
  protected Set<ActionType> possibleActionTypes() {
    return _actionTypes;
  }
}
