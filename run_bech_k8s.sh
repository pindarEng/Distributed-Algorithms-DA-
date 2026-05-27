#!/bin/bash
BENCHMARK=${1:-all}
MASTER_IP="192.168.1.73"
export BENCH_PLATFORM="k8s"
spark-submit \
  --master k8s://https://${MASTER_IP}:6443 \
  --deploy-mode client \
  --name "benchmark-${BENCHMARK}" \
  --conf spark.kubernetes.namespace=spark \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-sa \
  --conf spark.kubernetes.container.image=apache/spark:4.0.2 \
  --conf spark.pyspark.driver.python=python3.10 \
  --conf spark.pyspark.python=python3 \
  --conf spark.executor.instances=2 \
  --conf spark.executor.memory=2g \
  --conf spark.executor.cores=2 \
  --conf spark.driver.memory=2g \
  --conf spark.driver.host=${MASTER_IP} \
  --conf spark.driver.port=29413 \
  --conf spark.driver.blockManager.port=29414 \
  --conf spark.blockManager.port=29415 \
  --conf spark.port.maxRetries=4 \
  --conf spark.ui.port=4040 \
  --conf spark.kubernetes.executor.podTemplateFile=/home/ursal/executor-template.yaml \
  --conf spark.hadoop.fs.defaultFS=file:/// \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///home/ursal/spark-events \
  --conf spark.ui.prometheus.enabled=true \
  --conf spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet \
  --conf spark.metrics.conf.*.sink.prometheusServlet.path=/metrics/prometheus \
  ~/benchmarks.py ${BENCHMARK}
