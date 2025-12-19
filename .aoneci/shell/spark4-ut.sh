#!/bin/bash
set -ex

java -version

cd ${WORKSPACE}/paimon

echo "start run paimon ut"
test_modules=""
for suffix in ut 4.1 4.0; do
test_modules+="org.apache.paimon:paimon-spark-${suffix}_2.13,"
done
test_modules="${test_modules%,}"

# install
mvn -T 2C -B install -DskipTests -pl "${test_modules}" -am -Pspark4 -ntp

# run test
mvn -T 2C -B verify -pl "${test_modules}" -Pspark4 -ntp

echo "finish run paimon ut"
