#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
METADATA_FILE="${ROOT_DIR}/release/metadata.env"
ALM_EXAMPLES_FILE="${ROOT_DIR}/bundle/alm-examples.json"
CHART_FILE="${ROOT_DIR}/charts/fuseki-operator/Chart.yaml"
VALUES_FILE="${ROOT_DIR}/charts/fuseki-operator/values.yaml"
CSV_FILE="${ROOT_DIR}/bundle/manifests/fuseki-operator.clusterserviceversion.yaml"
ANNOTATIONS_FILE="${ROOT_DIR}/bundle/metadata/annotations.yaml"

. "${METADATA_FILE}"

CHART_VERSION="${CHART_VERSION:-${RELEASE_VERSION}}"
CHART_APP_VERSION="${CHART_APP_VERSION:-${CONTROLLER_IMAGE##*:}}"
CONTROLLER_IMAGE_REPOSITORY="${CONTROLLER_IMAGE%:*}"
CONTROLLER_IMAGE_TAG="${CONTROLLER_IMAGE##*:}"

cat >"${CHART_FILE}" <<EOF
apiVersion: v2
name: fuseki-operator
description: Helm chart for installing the fuseki-operator controller and CRDs
type: application
version: ${CHART_VERSION}
appVersion: ${CHART_APP_VERSION}
EOF

cat >"${VALUES_FILE}" <<EOF
fullnameOverride: fuseki-operator-controller-manager

image:
  repository: ${CONTROLLER_IMAGE_REPOSITORY}
  tag: ${CONTROLLER_IMAGE_TAG}
  pullPolicy: IfNotPresent
  pullSecrets: []

replicaCount: 1
leaderElection: true

manager:
  extraArgs: []

podAnnotations: {}

nodeSelector: {}

tolerations: []

affinity: {}

serviceAccount:
  create: true
  name: fuseki-operator-controller-manager
  annotations: {}

rbac:
  clusterRoleName: fuseki-operator-manager-role
  clusterRoleBindingName: fuseki-operator-manager-rolebinding

resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    cpu: 500m
    memory: 512Mi

metricsService:
  enabled: true
  name: fuseki-operator-controller-manager-metrics
  port: 8080
  annotations: {}
EOF

cat >"${ANNOTATIONS_FILE}" <<EOF
annotations:
  operators.operatorframework.io.bundle.mediatype.v1: registry+v1
  operators.operatorframework.io.bundle.manifests.v1: manifests/
  operators.operatorframework.io.bundle.metadata.v1: metadata/
  operators.operatorframework.io.bundle.package.v1: fuseki-operator
  operators.operatorframework.io.bundle.channels.v1: ${BUNDLE_CHANNELS}
  operators.operatorframework.io.bundle.channel.default.v1: ${BUNDLE_DEFAULT_CHANNEL}
EOF

cat >"${CSV_FILE}" <<EOF
apiVersion: operators.coreos.com/v1alpha1
kind: ClusterServiceVersion
metadata:
  name: fuseki-operator.v${RELEASE_VERSION}
  annotations:
    alm-examples: |
EOF
sed 's/^/      /' "${ALM_EXAMPLES_FILE}" >>"${CSV_FILE}"
printf '\n' >>"${CSV_FILE}"
cat >>"${CSV_FILE}" <<EOF
    capabilities: Basic Install
    categories: Database
    containerImage: ${CONTROLLER_IMAGE}
    createdAt: "${BUNDLE_CREATED_AT}"
    description: Operator for Apache Jena Fuseki clusters, RDF Delta replication, security, and backup or restore workflows.
    operators.operatorframework.io/builder: manual
    operators.operatorframework.io/project_layout: go.kubebuilder.io/v4
spec:
  displayName: Fuseki Operator
  description: |
    Fuseki Operator manages Apache Jena Fuseki clusters on Kubernetes.

    The operator reconciles Fuseki clusters, RDF Delta servers, datasets, security profiles,
    ingress endpoints, UI resources, and backup or restore workflows.
  version: ${RELEASE_VERSION}
  maturity: alpha
  minKubeVersion: 1.33.0
  provider:
    name: fuseki-operator
  keywords:
    - fuseki
    - rdf-delta
    - sparql
    - kubernetes
  installModes:
    - type: OwnNamespace
      supported: false
    - type: SingleNamespace
      supported: false
    - type: MultiNamespace
      supported: false
    - type: AllNamespaces
      supported: true
  customresourcedefinitions:
    owned:
      - name: backuppolicies.fuseki.apache.org
        version: v1alpha1
        kind: BackupPolicy
        displayName: Backup Policy
        description: Configures scheduled RDF Delta backups and target object storage settings.
      - name: datasets.fuseki.apache.org
        version: v1alpha1
        kind: Dataset
        displayName: Dataset
        description: Describes dataset bootstrapping and storage settings for Fuseki workloads.
      - name: endpoints.fuseki.apache.org
        version: v1alpha1
        kind: Endpoint
        displayName: Endpoint
        description: Publishes ingress or route-facing access to Fuseki services.
      - name: fusekiclusters.fuseki.apache.org
        version: v1alpha1
        kind: FusekiCluster
        displayName: Fuseki Cluster
        description: Defines a managed Fuseki cluster paired with RDF Delta and attached datasets.
      - name: fusekiservers.fuseki.apache.org
        version: v1alpha1
        kind: FusekiServer
        displayName: Fuseki Server
        description: Defines a standalone Fuseki server deployment.
      - name: fusekiuis.fuseki.apache.org
        version: v1alpha1
        kind: FusekiUI
        displayName: Fuseki UI
        description: Deploys and exposes a Fuseki UI companion workload.
      - name: rdfdeltaservers.fuseki.apache.org
        version: v1alpha1
        kind: RDFDeltaServer
        displayName: RDF Delta Server
        description: Manages the RDF Delta replication service used for write coordination and backups.
      - name: restorerequests.fuseki.apache.org
        version: v1alpha1
        kind: RestoreRequest
        displayName: Restore Request
        description: Triggers and tracks restoration of RDF Delta state from a captured backup object.
      - name: securityprofiles.fuseki.apache.org
        version: v1alpha1
        kind: SecurityProfile
        displayName: Security Profile
        description: Configures admin credentials, TLS, and identity-provider integration.
  relatedImages:
    - name: controller
      image: ${CONTROLLER_IMAGE}
    - name: fuseki
      image: ${FUSEKI_IMAGE}
    - name: rdf-delta
      image: ${RDF_DELTA_IMAGE}
  install:
    strategy: deployment
    spec:
      clusterPermissions:
        - serviceAccountName: fuseki-operator-controller-manager
          rules:
            - apiGroups:
                - ""
              resources:
                - events
                - pods
                - services
                - configmaps
                - secrets
                - persistentvolumeclaims
              verbs:
                - get
                - list
                - watch
                - create
                - update
                - patch
            - apiGroups:
                - ""
              resources:
                - pods
                - services
                - configmaps
                - secrets
                - persistentvolumeclaims
              verbs:
                - delete
            - apiGroups:
                - apps
              resources:
                - statefulsets
                - deployments
              verbs:
                - get
                - list
                - watch
                - create
                - update
                - patch
            - apiGroups:
                - networking.k8s.io
              resources:
                - ingresses
              verbs:
                - get
                - list
                - watch
                - create
                - update
                - patch
                - delete
            - apiGroups:
                - gateway.networking.k8s.io
              resources:
                - httproutes
              verbs:
                - get
                - list
                - watch
                - create
                - update
                - patch
                - delete
            - apiGroups:
                - batch
              resources:
                - cronjobs
                - jobs
              verbs:
                - get
                - list
                - watch
                - create
                - update
                - patch
                - delete
            - apiGroups:
                - coordination.k8s.io
              resources:
                - leases
              verbs:
                - get
                - list
                - watch
                - create
                - update
                - patch
            - apiGroups:
                - fuseki.apache.org
              resources:
                - '*'
              verbs:
                - '*'
      deployments:
        - name: fuseki-operator-controller-manager
          spec:
            replicas: 1
            selector:
              matchLabels:
                app.kubernetes.io/name: fuseki-operator
                control-plane: controller-manager
            template:
              metadata:
                labels:
                  app.kubernetes.io/name: fuseki-operator
                  control-plane: controller-manager
              spec:
                serviceAccountName: fuseki-operator-controller-manager
                containers:
                  - name: manager
                    image: ${CONTROLLER_IMAGE}
                    imagePullPolicy: IfNotPresent
                    command:
                      - /manager
                    args:
                      - --leader-elect
                    ports:
                      - containerPort: 8080
                        name: metrics
                      - containerPort: 8081
                        name: probes
                    resources:
                      requests:
                        cpu: 100m
                        memory: 128Mi
                      limits:
                        cpu: 500m
                        memory: 512Mi
EOF
