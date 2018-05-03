/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer;

import java.util.Arrays;
import java.util.List;


/**
 * Flags to indicate if an action is acceptable by the goal(s).
 *
 * The action acceptance serves two use cases.
 * 1. Simply determine whether a proposal can be accepted by a goal or not.
 * 2. When a goal is iterating over replicas in a broker or iterating over brokers, indicate whether the iteration
 *    should be terminated immediately, as the iteration is impossible to find a suitable candidate.
 * 
 * Potential early termination case:
 * 1. Holding a replica in the source broker, finding a replica in the destination broker to swap with.
 * 2. Holding a replica in the destination broker, finding a replica in the source broker to swap with.
 * 
 * From the optimized goals' perspective, there are three rejection reasons:
 * 1. The source replica cannot be put into the destination broker regardless of the destination replica.
 *    (Source broker reject)
 * 2. The destination replica cannot be put into the source broker regardless of the source replica.
 *    (Destination broker reject)
 * 3. The source replica can be put into the destination broker with a different destination replica, or the
 *    destination replica can be put into the source broker with a different source replica.
 * 
 * It is the caller's responsibility to determine how to interpret the returned rejection.
 * 1. If the caller is iterating over the replicas in the source broker for SWAP, and received a destination replica 
 *    reject, it should stop the iteration.
 * 2. If the caller is iterating over the replicas in the destination broker for SWAP, and received a source replica 
 *    reject, it should stop the iteration.
 * 3. Otherwise, there is no need to stop the iteration.
 * 
 * <ul>
 *   <li>
 *     {@link #ACCEPT}: Action is acceptable -- i.e. it does not violate goal constraints.
 *   </li>
 *   <li>
 *     {@link #REPLICA_REJECT}: Action is rejected at replica level; This could either be the source replica was
 *     rejected by the destination broker, or the destination replica was rejected by the source broker. The rejection
 *     was based on the particular action.
 *   </li>
 *   <li>
 *     {@link #SRC_BROKER_REJECT}: Action is rejected at broker level by the source broker, i.e. the destination
 *     replica cannot be applied the {@link ActionType} with this source broker. This reject type is currently only
 *     used by the {@link ActionType#REPLICA_SWAP}.
 *   </li>
 *   <li>
 *     {@link #DEST_BROKER_REJECT}: Action is rejected at broker level by the destination broker, i.e. the source
 *     replica cannot be applied the {@link ActionType} with this destination broker.
 *   </li>
 * </ul>
 */
public enum ActionAcceptance {
  ACCEPT, REPLICA_REJECT, SRC_BROKER_REJECT, DEST_BROKER_REJECT;

  private static final List<ActionAcceptance> CACHED_VALUES;
  static {
    CACHED_VALUES = Arrays.asList(ACCEPT, REPLICA_REJECT, SRC_BROKER_REJECT, DEST_BROKER_REJECT);
  }

  public static List<ActionAcceptance> cachedValues() {
    return CACHED_VALUES;
  }
}
