#!/bin/bash
set -ex

wget -q https://emr-public-sh.oss-cn-shanghai.aliyuncs.com/native_spark/gluten-settings.xml
mv gluten-settings.xml ~/.m2/settings.xml
sed -i "s/SS_MAVEN_USERNAME/${SS_MAVEN_USERNAME}/g; s/SS_MAVEN_PASSWORD/${SS_MAVEN_PASSWORD}/g" ~/.m2/settings.xml

cd ${WORKSPACE}/paimon

PAIMON_VERSION=$(sed -n 's/.*<version>\([^<]*\)<\/version>.*/\1/p' pom.xml | sed -n '2p')
echo "PAIMON_VERSION: ${PAIMON_VERSION}"
if echo "$PAIMON_VERSION" | grep -iq "snapshot"; then
  EMR_DEPLOY_REPO="emr-release::default::https://emr-maven.alibaba.net/repository/emr-snapshot/"
else
  EMR_DEPLOY_REPO="emr-release::default::https://emr-maven.alibaba.net/repository/emr-release/"
fi

# === spark3 (JDK8) ===
# Full deploy: builds all modules (incl. paimon-common etc.) and installs them to ~/.m2,
# which spark4 step reuses via -pl filter.
java -version
echo "start deploy paimon for spark3"
mvn clean -DskipTests deploy -Pspark3,flink1,deploy-aliyun -Demr.deploy.repo=${EMR_DEPLOY_REPO} -ntp
echo "finish deploy paimon for spark3"

# === switch to JDK17 for spark4 ===
yum install -y java-17-openjdk-devel 2>/dev/null || true
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PATH=$JAVA_HOME/bin:$PATH
java -version

# Only build & deploy spark4-specific modules; shared deps already in ~/.m2 from spark3 step.
echo "start deploy paimon for spark4"
mvn clean -DskipTests deploy -Pspark4,flink1,deploy-aliyun -Demr.deploy.repo=${EMR_DEPLOY_REPO} -ntp -pl org.apache.paimon:paimon-spark-common_2.13,org.apache.paimon:paimon-spark-ut_2.13,org.apache.paimon:paimon-spark4-common_2.13,org.apache.paimon:paimon-spark-4.0_2.13,org.apache.paimon:paimon-spark-4.1_2.13
echo "finish deploy paimon for spark4"
