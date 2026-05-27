#!/bin/bash
BENCHMARK=${1:-all}
MASTER_IP="192.168.1.73"
export BENCH_PLATFORM="yarn"
spark-submit \
  --master yarn \
  --deploy-mode client \
  --name "benchmark-${BENCHMARK}" \
  --conf spark.driver.host=${MASTER_IP} \
  --num-executors 4 \
  --executor-memory 4g \
  --executor-cores 2 \
  --driver-memory 2g \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///home/ursal/spark-events \
  --conf spark.ui.prometheus.enabled=true \
  --conf spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet \
  --conf spark.metrics.conf.*.sink.prometheusServlet.path=/metrics/prometheus \
  ~/benchmarks.py ${BENCHMARK}
