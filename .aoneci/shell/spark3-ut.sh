#!/bin/bash
set -ex

java -version

cd ${WORKSPACE}/paimon

echo "start run paimon ut"
test_modules=""
for suffix in ut 3.5 3.4; do
test_modules+="org.apache.paimon:paimon-spark-${suffix}_2.12,"
done
test_modules="${test_modules%,}"

# install
mvn -T 2C -B install -DskipTests -pl "${test_modules}" -am -ntp

# run test
mvn -T 2C -B verify -pl "${test_modules}" -ntp

echo "finish run paimon ut"
