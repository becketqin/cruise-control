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
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.*;


public class DiskUsageDistributionGoal extends ResourceDistributionGoal {
  private final Set<ActionType> _actionTypes = new HashSet<>(Arrays.asList(REPLICA_MOVEMENT, REPLICA_SWAP));
  /**
   * Constructor for Resource Distribution Goal.
   */
  public DiskUsageDistributionGoal() {
    super();
  }

  /**
   * Package private for unit test.
   */
  DiskUsageDistributionGoal(BalancingConstraint constraint) {
    super(constraint);
  }

  @Override
  protected Resource resource() {
    return Resource.DISK;
  }

  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    // Leadership movement won't cause disk utilization change.
    return action.actionType() == ActionType.LEADERSHIP_MOVEMENT ? ACCEPT : super.actionAcceptance(action, clusterModel);
  }

  @Override
  protected Set<ActionType> possibleActionTypes() {
    return _actionTypes;
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(1, _minMonitoredPartitionPercentage, true);
  }
}
