/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.asbtractimpl.distributiongoal;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.Replica;


/**
 * An interface that provides some useful methods to multiple classes.
 */
public interface DistributionGoalHelper {

  /**
   * Get a score to indicate the impact of the replica on the metric value that is being optimized. The higher
   * the score is, the larger the impact the replica has on the metric that is being balanced.
   *
   * @param replica the replica to get the impact score.
   * @return the impact score of the given replica.
   */
  double impactScore(Replica replica);

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
