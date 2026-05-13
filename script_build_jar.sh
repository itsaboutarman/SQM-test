# Create m2 pvc
kubectl apply -f  yamls/m2-pvc.yaml  -n $namespace --validate=false

#maven settings
kubectl create configmap maven-setting-file --from-file=configs/settings.xml  -n $namespace || true
kubectl create configmap jmx-exporter-config --from-file=configs/jmx-exporter-config.yaml -n $namespace || true

# Create maven container
sed -i  "s/maven-container/maven-container-${CI_COMMIT_SHORT_SHA}/g"  yamls/maven.yaml
kubectl apply -f  yamls/maven.yaml  -n $namespace

kubectl wait --for=jsonpath='{.status.phase}'=Running pod/maven-container-${CI_COMMIT_SHORT_SHA} -n $namespace --timeout=100s

# Copy project to maven
kubectl cp -n $namespace sqm maven-container-${CI_COMMIT_SHORT_SHA}:/tmp/sqm

# build project using mvn 
kubectl exec -ti -n $namespace maven-container-${CI_COMMIT_SHORT_SHA} -- mvn package -gs /tmp/.m2/settings.xml -f /tmp/sqm/pom-for-${SPARK_VERSION}.xml 

# Rename jar file
kubectl exec -ti -n $namespace maven-container-${CI_COMMIT_SHORT_SHA} -- cp /tmp/sqm/target/SQM-1.0-SNAPSHOT-with-dependencies.jar /tmp/sqm/target/${SPARK_MAIN_APP_FILE_NAME_PREFIX}-${CI_COMMIT_SHORT_SHA}-${SPARK_VERSION}.jar 

# Push to Nexus :
kubectl exec -ti -n $namespace maven-container-${CI_COMMIT_SHORT_SHA} -- curl -k -u "${NEXUS_CREDENTIALS}" --upload-file /tmp/sqm/target/${SPARK_MAIN_APP_FILE_NAME_PREFIX}-${CI_COMMIT_SHORT_SHA}-${SPARK_VERSION}.jar ${NEXUS_ADDRESS}/repository/Raw/${SPARK_MAIN_APP_FILE_NAME_PREFIX}-${CI_COMMIT_SHORT_SHA}-${SPARK_VERSION}.jar

# Push to Nexus :
kubectl exec -ti -n $namespace maven-container-${CI_COMMIT_SHORT_SHA} -- curl -k -u "${NEXUS_CREDENTIALS}" -X DELETE ${NEXUS_ADDRESS}/repository/Raw/latest/${SPARK_MAIN_APP_FILE_NAME_PREFIX}-${SPARK_VERSION}.jar

# Push to Nexus :
kubectl exec -ti -n $namespace maven-container-${CI_COMMIT_SHORT_SHA} -- curl -k -u "${NEXUS_CREDENTIALS}" --upload-file /tmp/sqm/target/${SPARK_MAIN_APP_FILE_NAME_PREFIX}-${CI_COMMIT_SHORT_SHA}-${SPARK_VERSION}.jar ${NEXUS_ADDRESS}/repository/Raw/latest/${SPARK_MAIN_APP_FILE_NAME_PREFIX}-${SPARK_VERSION}.jar

kubectl delete -f  yamls/maven.yaml  -n $namespace

