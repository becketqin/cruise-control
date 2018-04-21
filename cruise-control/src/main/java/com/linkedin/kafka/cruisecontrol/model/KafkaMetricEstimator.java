/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.model;

import com.linkedin.cruisecontrol.metricdef.MetricDef;
import com.linkedin.cruisecontrol.metricdef.MetricInfo;
import com.linkedin.cruisecontrol.model.Entity;
import com.linkedin.cruisecontrol.model.estimator.MetricEstimator;
import com.linkedin.cruisecontrol.model.estimator.impl.HeterogeneousLinearRegressionEstimator;
import com.linkedin.cruisecontrol.monitor.sampling.MetricSample;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.AggregatedMetricValues;
import com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricValues;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.cruisecontrol.model.regression.LinearRegression;
import com.linkedin.kafka.cruisecontrol.monitor.metricdefinition.KafkaMetricDef;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerEntity;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.BrokerMetricSample;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.common.record.CompressionType;


public class KafkaMetricEstimator extends HeterogeneousLinearRegressionEstimator implements MetricEstimator {

  // The static model
  /**
   * The contribution weight of leader bytes in on the CPU utilization of a broker.
   */
  static double CPU_WEIGHT_OF_LEADER_BYTES_IN_RATE = 0.6;
  /**
   * The contribution weight of leader bytes out on the CPU utilization of a broker.
   */
  static double CPU_WEIGHT_OF_LEADER_BYTES_OUT_RATE = 0.1;
  /**
   * The contribution weight of follower bytes in on the CPU utilization of a broker.
   */
  static double CPU_WEIGHT_OF_FOLLOWER_BYTES_IN_RATE = 0.3;

  private KafkaMetricEstimator() {

  }

  @Override
  protected MetricDef metricDef() {
    return KafkaMetricDef.commonMetricDef();
  }

  @Override
  protected String entityType(Entity entity) {
    //TODO: Expand BrokerCapacityConfigResolver to get general broker information to support heterogeneous cluster.
    return "HomogeneousBroker";
  }

  public static void init(KafkaCruiseControlConfig config) {
    CPU_WEIGHT_OF_LEADER_BYTES_IN_RATE =
        config.getDouble(KafkaCruiseControlConfig.LEADER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG);
    CPU_WEIGHT_OF_LEADER_BYTES_OUT_RATE =
        config.getDouble(KafkaCruiseControlConfig.LEADER_NETWORK_OUTBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG);
    CPU_WEIGHT_OF_FOLLOWER_BYTES_IN_RATE =
        config.getDouble(KafkaCruiseControlConfig.FOLLOWER_NETWORK_INBOUND_WEIGHT_FOR_CPU_UTIL_CONFIG);
  }

  public static Double getCoefficient(LinearRegression.ModelMetric name) {
    return 
  }

  public static boolean trainingCompleted() {
    return LINEAR_REGRESSION_PARAMETERS.trainingCompleted();
  }

  public static double modelCoefficientTrainingCompleteness() {
    return LINEAR_REGRESSION_PARAMETERS.modelCoefficientTrainingCompleteness();
  }

  /**
   * Trigger the calculation of the model parameters.
   * @return true if the parameters are generated, otherwise false;
   */
  public static boolean updateModelCoefficient() {
    return LINEAR_REGRESSION_PARAMETERS.updateModelCoefficient();
  }

  public static void addMetricObservation(Collection<BrokerMetricSample> trainingData) {
    LINEAR_REGRESSION_PARAMETERS.addMetricSamples(trainingData);
  }

  public static LinearRegression.LinearRegressionState linearRegressionModelState() {
    return LINEAR_REGRESSION_PARAMETERS.modelState();
  }
}
