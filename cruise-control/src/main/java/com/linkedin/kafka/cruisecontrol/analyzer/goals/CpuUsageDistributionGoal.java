/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.*;


public class CpuUsageDistributionGoal extends ResourceDistributionGoal {
  private final Set<ActionType> _actionTypes =
      new HashSet<>(Arrays.asList(LEADERSHIP_MOVEMENT, REPLICA_MOVEMENT, REPLICA_SWAP));
  /**
   * Constructor for Resource Distribution Goal.
   */
  public CpuUsageDistributionGoal() {
    super();
  }

  /**
   * Package private for unit test.
   */
  CpuUsageDistributionGoal(BalancingConstraint constraint) {
    super(constraint);
  }

  @Override
  protected Resource resource() {
    return Resource.CPU;
  }

  @Override
  public String name() {
    return CpuUsageDistributionGoal.class.getSimpleName();
  }

  @Override
  protected Set<ActionType> possibleActionTypes() {
    return _actionTypes;
  }
}
