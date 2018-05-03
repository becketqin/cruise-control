/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.distributiongoal;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.model.Broker;

/**
 * An interface that provides some useful methods to the metric distribution goals.
 */
public interface DistributionGoalHelper {

  /**
   * Get the broker metric value assuming the given action is taken.
   *
   * @param action the action to take.
   * @return the metric value of the broker assuming the given action is taken.
   */
  double brokerMetricValueAfterAction(Broker broker, BalancingAction action);

  /**
   * Whether a replica can be moved to a broker.
   *
   * @return true if the movement can be done, false otherwise.
   */
  ActionAcceptance checkAcceptance(BalancingAction balancingAction);
}
