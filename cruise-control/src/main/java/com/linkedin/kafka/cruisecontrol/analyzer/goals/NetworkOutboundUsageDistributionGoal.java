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

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.LEADERSHIP_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.REPLICA_MOVEMENT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionType.REPLICA_SWAP;


public class NetworkOutboundUsageDistributionGoal extends ResourceDistributionGoal {
  private final Set<ActionType> _actionTypes =
      new HashSet<>(Arrays.asList(LEADERSHIP_MOVEMENT, REPLICA_MOVEMENT, REPLICA_SWAP));
  /**
   * Constructor for Resource Distribution Goal.
   */
  public NetworkOutboundUsageDistributionGoal() {
    super();
  }

  /**
   * Package private for unit test.
   */
  NetworkOutboundUsageDistributionGoal(BalancingConstraint constraint) {
    super(constraint);
  }

  @Override
  protected Resource resource() {
    return Resource.NW_OUT;
  }

  @Override
  public String name() {
    return NetworkOutboundUsageDistributionGoal.class.getSimpleName();
  }

  @Override
  protected Set<ActionType> possibleActionTypes() {
    return _actionTypes;
  }
}
