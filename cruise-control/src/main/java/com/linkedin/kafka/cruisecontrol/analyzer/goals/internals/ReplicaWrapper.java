/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.internals;

import com.linkedin.kafka.cruisecontrol.model.Replica;
import java.util.Objects;


/**
 * A class that helps host replica and its score. This class is useful for searching on top of sorted set.
 */
public class ReplicaWrapper implements Comparable<ReplicaWrapper> {
  private final Replica _replica;
  private final double _score;

  public ReplicaWrapper(Replica replica, double score) {
    _replica = replica;
    _score = score;
  }

  /**
   * @return The score associated with the replica for sorting purpose.
   */
  public double score() {
    return _score;
  }

  /**
   * @return the replica.
   */
  public Replica replica() {
    return _replica;
  }

  @Override
  public String toString() {
    return String.format("(Partition=%s,Broker=%d,Score=%f)",
                         _replica.topicPartition(), _replica.broker().id(), _score);
  }

  @Override
  public int compareTo(ReplicaWrapper o) {
    if (o == null) {
      throw new IllegalArgumentException("Cannot compare to a null object.");
    }
    int result = Double.compare(this.score(), o.score());
    if (result != 0) {
      return result;
    } else {
      if (this.replica() == Replica.MAX_REPLICA || o.replica() == Replica.MIN_REPLICA) {
        return 1;
      } else if (this.replica() == Replica.MIN_REPLICA || o.replica() == Replica.MAX_REPLICA) {
        return -1;
      } else {
        return this.replica().compareTo(o.replica());
      }
    }
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj instanceof ReplicaWrapper && ((ReplicaWrapper) obj).score() == _score
        && ((ReplicaWrapper) obj).replica().equals(_replica);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_replica, _score);
  }

  /**
   * Get a ReplicaWrapper for searching purpose on a collection. The returned ReplicaWrapper is:
   * 1. Smaller than any ReplicaWrapper whose score is greater than or equals to the given score.
   * 2. Greater than any ReplicaWrapper whose score is smaller than the given score.
   *
   * @param score the given score.
   * @return A ReplicaWrapper that meets the above condition.
   */
  public static ReplicaWrapper greaterThanOrEqualsTo(double score) {
    return new ReplicaWrapper(Replica.MIN_REPLICA, score);
  }

  /**
   * Get a ReplicaWrapper for searching purpose on a collection. The returned ReplicaWrapper is:
   * 1. Smaller than any ReplicaWrapper whose score is greater than the given score.
   * 2. Greater than any ReplicaWrapper whose score is smaller than or equals to the given score.
   *
   * @param score the given score.
   * @return A ReplicaWrapper that meets the above condition.
   */
  public static ReplicaWrapper greaterThan(double score) {
    return new ReplicaWrapper(Replica.MAX_REPLICA, score);
  }

  /**
   * Get a ReplicaWrapper for searching purpose on a collection. The returned ReplicaWrapper is:
   * 1. Greater than any ReplicaWrapper whose score is smaller than or equals to the given score.
   * 2. Smaller than any ReplicaWrapper whose score is greater than the given score.
   *
   * @param score the given score.
   * @return A ReplicaWrapper that meets the above condition.
   */
  public static ReplicaWrapper lessThanOrEqualsTo(double score) {
    return new ReplicaWrapper(Replica.MAX_REPLICA, score);
  }

  /**
   * Get a ReplicaWrapper for searching purpose on a collection. The returned ReplicaWrapper is:
   * 1. Greater than any ReplicaWrapper whose score is smaller than the given score.
   * 2. Smaller than any ReplicaWrapper whose score is greater than or equals to the given score.
   *
   * @param score the given score.
   * @return A ReplicaWrapper that meets the above condition.
   */
  public static ReplicaWrapper lessThan(double score) {
    return new ReplicaWrapper(Replica.MIN_REPLICA, score);
  }
}
