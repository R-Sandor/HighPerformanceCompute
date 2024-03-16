![volt draft](./docs/assets/volt_draft.png)
Volt is geared towards security professionals to streamline and automate the vulnerability assessment process.

- Leverages Spark and OpenSearch
- "Autocomplete" for the security workflow:
  - View similar CVEs
    - Bulk justify/mitigate similar CVEs where the same justification would apply
  - Apply intelligent justifications based on previous justifications. 
  - Determine applicability of vulnerability based on your config.
  - Generate smart adaptable reports.
    - Different teams will have different reporting needs.
- Enhanced IAVM integration:
  - Assess IAVM posts to determine applicability
  - Determine if mitigation steps are already applied
    - Mitigation step is to upgrade to version 3.5 or later, will check config if that is already done
  - Intelligent alerting based on IAVM find dates and program SLAs. 
- Handle near-real-time data:
  - Complete list of all CVEs
  - Ingest data from services such as Anchore, Twistlock, IAVM, etc.


# Configuration: 
 - Java 17 
 - Apache Spark 3.5.0
 - Kubernetes 1.27.x
  
# Getting Started
<details>
  <summary>Setup Guide</summary>

  ## Assumptions
  - Docker, Kind, and Kubectl have all been installed. 
    - If not refer to the blog below and install the latest editions of each tool.

  ## References 
  - https://medium.com/@SaphE/deploying-apache-spark-on-a-local-kubernetes-cluster-a-comprehensive-guide-d4a59c6b1204
  - https://medium.com/@SaphE/deploying-apache-spark-on-kubernetes-using-helm-charts-simplified-cluster-management-and-ee5e4f2264fd

  # Download Apache Spark 

  ```sh
  wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz
  ```

  # Security Check

  ## Get the SHA512

  - Always check the SHAs.

  ```sh
  wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz.sha512
  ```

  ### Verify
  ```sh
  sha512sum spark-3.5.0-bin-hadoop3-scala2.13.tgz
  cat spark-3.5.0-bin-hadoop3-scala2.13.tgz.sha512
  ```
  - Should match!

  _See Instructions for non-linux_
  https://www.apache.org/info/verification.html

  ## Get the Signature
  ```sh
  wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz.asc
  ````

  ### Verify
  ```sh
  gpg --verify spark-3.5.0-bin-hadoop3-scala2.13.tgz.asc spark-3.5.0-bin-hadoop3-scala2.13.tgz
  ```
  ```sh
  gpg: Signature made Fri 08 Sep 2023 09:40:47 PM EDT
  gpg:                using RSA key FC3AE3A7EAA1BAC98770840E7E1ABCC53AAA2216
  gpg:                issuer "liyuanjian@apache.org"
  gpg: Can't check signature: No public key
  ```

  ```sh
  gpg --receive-keys FC3AE3A7EAA1BAC98770840E7E1ABCC53AAA2216
  ```

  - Quick Verification:

  ```sh
  gpg --verify spark-3.5.0-bin-hadoop3-scala2.13.tgz.asc spark-3.5.0-bin-hadoop3-scala2.13.tgz
  gpg: Signature made Fri 08 Sep 2023 09:40:47 PM EDT
  gpg:                using RSA key FC3AE3A7EAA1BAC98770840E7E1ABCC53AAA2216
  gpg:                issuer "liyuanjian@apache.org"
  gpg: Good signature from "Yuanjian Li (CODE SIGNING KEY) <liyuanjian@apache.org>" [unknown]
  gpg: WARNING: This key is not certified with a trusted signature!
  gpg:          There is no indication that the signature belongs to the owner.
  Primary key fingerprint: FC3A E3A7 EAA1 BAC9 8770  840E 7E1A BCC5 3AAA 2216
  ```
  ```sh
  gpg --fingerprint FC3AE3A7EAA1BAC98770840E7E1ABCC53AAA2216
  pub   rsa4096 2023-07-17 [SC]
  FC3A E3A7 EAA1 BAC9 8770  840E 7E1A BCC5 3AAA 2216
  uid           [ unknown] Yuanjian Li (CODE SIGNING KEY) <liyuanjian@apache.org>
  sub   rsa4096 2023-07-17 [E]
  ```

  - Extract the tar and create the image:

  ```sh
  tar -xvf spark-3.5.0-bin-hadoop3-scala2.tgz
  cd spark-3.5.0-bin-hadoop3-scala2
  SPARK_HOME=$(realpath ./)
  ./bin/docker-image-tool.sh -t spark-k8sBuild build
  ```

  - Create the kind cluster & service account:

  ## KIND 

  ```sh
  kind create cluster
  kind load docker-image spark:spark-k8sBuild
  kubectl create serviceaccount spark
  ```


  ## Minikube
  - Recommend rebuilding the image if Minikube. The reason is to save configuration headache of adding a registry for the cluster.

  ```sh
  minikube create cluster
  # https://minikube.sigs.k8s.io/docs/handbook/pushing/
  eval $(minikube docker-env)
  ./bin/docker-image-tool.sh -t spark-k8sBuild build
  ```

 
  ```sh
  kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=default
  ```

  - Guide recommendation: The second command establishes a cluster-level role binding for the service account, granting it the necessary permissions within the default namespace.


  - Get your cluster IP: 
  ```sh
  kubectl cluster-info
  ```
  - Run the sample project:
  ```sh
  ./bin/spark-submit --master k8s://https://127.0.0.1:36935 --deploy-mode cluster --name spark-pi --class org.apache.spark.examples.JavaSparkPi --conf spark.executor.instances=2 --conf spark.kubernetes.container.image=spark:spark-k8sBuild --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark local:///opt/spark/examples/jars/spark-examples_2.13-3.5.0.jar 100
  ```

</details>
