#!/bin/bash
set -ex

mkdir -p ${WORKSPACE}/package

java -version

if [[ ${SPARK_VERSION} == 4* ]]; then
    spark_profile="-Pspark4,flink1"
else
    spark_profile="-Pspark3,flink1"
fi

wget -q https://emr-public-sh.oss-cn-shanghai.aliyuncs.com/native_spark/gluten-settings.xml
mv gluten-settings.xml ~/.m2/settings.xml
sed -i "s/SS_MAVEN_USERNAME/${SS_MAVEN_USERNAME}/g; s/SS_MAVEN_PASSWORD/${SS_MAVEN_PASSWORD}/g" ~/.m2/settings.xml

echo "start install paimon"
cd ${WORKSPACE}/paimon
mvn clean -DskipTests -pl paimon-spark/paimon-spark-${SPARK_VERSION}/ -am install ${spark_profile} -ntp
echo "finish install paimon"

PAIMON_VERSION=$(sed -n 's/.*<version>\([^<]*\)<\/version>.*/\1/p' ${WORKSPACE}/paimon/pom.xml | sed -n '2p')
echo "paimon-ali will use PAIMON_VERSION: ${PAIMON_VERSION}"

format_package() {
  ORIGINAL_FILE_PATH="$1"
  TODAY=$(date +%Y%m%d)
  ORIGINAL_FILE=$(ls $ORIGINAL_FILE_PATH | grep -v "sources.jar")
  if [ -f "$ORIGINAL_FILE" ]; then
      FILE_BASENAME=$(basename "$ORIGINAL_FILE" .jar)
      if [ "${FORMAT_PACKAGE}" = true ]; then
        PAIMON_BRANCH_COMMIT_ID=$(git -C ${WORKSPACE}/paimon rev-parse --short=8 HEAD)
        PAIMON_ALI_BRANCH_COMMIT_ID=$(git -C ${WORKSPACE}/paimon-ali rev-parse --short=8 HEAD)
        NEW_FILE="${FILE_BASENAME}-${PAIMON_BRANCH_COMMIT_ID}-${PAIMON_ALI_BRANCH_COMMIT_ID}-${TODAY}.jar"
      else
        NEW_FILE="${FILE_BASENAME}-${TODAY}.jar"
      fi
      echo "cp $ORIGINAL_FILE to $NEW_FILE"
      cp "$ORIGINAL_FILE" "${WORKSPACE}/package/$NEW_FILE"
  fi
}

if [ "${PAIMON_ALI_EMR_SPARK}" = true ]; then
  echo "start build paimon-ali-emr-spark"
  cd ${WORKSPACE}/paimon-ali
  if [ "${JDK17}" = true ]; then
      other_profile="-Dtarget.java.version=17"
  else
      other_profile="-Dtarget.java.version=1.8"
  fi
  mvn clean package -DskipTests -Dpaimon.version=${PAIMON_VERSION} -pl paimon-ali-emr/paimon-ali-emr-spark-${SPARK_VERSION} -am ${other_profile} -ntp
  echo "finish build paimon-ali-emr-spark"
  format_package "paimon-ali-emr/paimon-ali-emr-spark-${SPARK_VERSION}/target/paimon-ali-emr-spark-*.jar"
  #format_package "paimon-ali-emr/paimon-ali-emr-spark-common/target/paimon-ali-emr-spark-*.jar"
fi

if [ "${PAIMON_HIVE}" = true ]; then
  echo "start build paimon-hive"
  cd ${WORKSPACE}/paimon
  mvn clean package -DskipTests -pl paimon-hive/paimon-hive-connector-2.3,paimon-hive/paimon-hive-connector-3.1 -am -ntp
  echo "finish build paimon-hive"
  format_package "paimon-hive/paimon-hive-connector-2.3/target/paimon-hive-*.jar"
  format_package "paimon-hive/paimon-hive-connector-3.1/target/paimon-hive-*.jar"
fi

if [ "${PAIMON_ALI_PANGU}" = true ]; then
  echo "start build paimon-ali-pangu"
  cd ${WORKSPACE}/paimon-ali
  mvn clean package -DskipTests -Dpaimon.version=${PAIMON_VERSION} -pl paimon-ali-filesystems/paimon-ali-pangu -am -s -ntp
  echo "finish build paimon-ali-pangu"
  format_package "paimon-ali-filesystems/paimon-ali-pangu/target/paimon-ali-pangu-*.jar"
fi

if [ "${PAIMON_ALI_ALIORC}" = true ]; then
  echo "start build paimon-ali-aliorc"
  cd ${WORKSPACE}/paimon-ali
  mvn clean package -DskipTests -Dpaimon.version=${PAIMON_VERSION} -pl paimon-ali-aliorc -am -ntp
  echo "finish build paimon-ali-aliorc"
  format_package "paimon-ali-aliorc/target/paimon-ali-aliorc-*.jar"
fi
