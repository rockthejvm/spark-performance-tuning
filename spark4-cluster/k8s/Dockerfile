# App image for the Spark-on-Kubernetes CPU-throttling demo.
#
# It is the official Spark 4.1.1 runtime with our thin application jar baked in. Driver and
# executor pods both run from this image, so the jar must be addressable as a local:// path
# inside the container.
#
# The jar is produced by `sbt package` (NOT assembly): it contains ONLY our compiled classes.
# All Spark/Scala classes come from /opt/spark/jars in the base image — so the versions match
# exactly and the image stays small.
#
# Build context is this directory; 01-build-and-import.sh copies the jar here as app.jar first.
FROM apache/spark:4.1.1-scala

COPY app.jar /opt/spark-apps/app.jar
