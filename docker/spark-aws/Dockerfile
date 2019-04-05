#
# Copyright © 2016-2018 The Thingsboard Authors
# Modifications © 2017-2018 Hashmap, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM gcr.io/spark-operator/spark:v2.4.0

ARG AWS_JAVA_SDK_VERSION=1.7.4
ARG HADOOP_AWS_VERSION=2.7.3

ADD http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_AWS_VERSION/hadoop-aws-$HADOOP_AWS_VERSION.jar $SPARK_HOME/jars
ADD http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/$AWS_JAVA_SDK_VERSION/aws-java-sdk-$AWS_JAVA_SDK_VERSION.jar $SPARK_HOME/jars
ADD http://central.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.0/jets3t-0.9.0.jar $SPARK_HOME/jars

ENTRYPOINT ["/opt/entrypoint.sh"]