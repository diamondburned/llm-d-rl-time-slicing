# Donor Controller Helm Chart

> [!WARNING]
> This chart and controller are an experimental prototype / draft. The controller does not currently implement leader election. It must run with `replicas: 1` and `strategy.type: Recreate` to avoid concurrent controller instances mutating the same nodes.

This directory contains the Helm chart for deploying the donor controller in a Kubernetes cluster.

For architectural details, rules, and policy specification, see [../../pkg/donor-controller](../../pkg/donor-controller).

## Overview

The donor controller coordinates node donation for RL time-slicing workloads. It:
- Watches all `Node` resources and label-selected `Pod` resources (`timeslice.io/donor=true` and `timeslice.io/guest=true`).
- Applies sharing labels and isolation taints to donor nodes when donor pods are scheduled.
- Tracks node idle TTL and unshares nodes after donor pods complete.
- Cleans up orphaned virtual nodes when underlying donor hosts disappear.

## Values

| Key | Type | Default | Description |
|---|---|---|---|
| `image.repository` | string | `ghcr.io/llm-d-incubation/llm-d-rl-time-slicing/donor-controller` | Image repository |
| `image.tag` | string | `"latest"` | Image tag (defaults to `Chart.appVersion` if empty) |
| `image.pullPolicy` | string | `"IfNotPresent"` | Image pull policy |
| `imagePullSecrets` | list | `[]` | Secret names for pulling images from private registries |
| `nameOverride` | string | `""` | Override chart name in resource names |
| `fullnameOverride` | string | `""` | Override full resource names |
| `controller.workers` | int | `2` | Number of worker goroutines |
| `controller.idleTTL` | string | `"5m"` | Idle TTL before unsharing a node |
| `controller.resyncPeriod` | string | `"10m"` | Informer resync period |
| `controller.dryRun` | bool | `false` | When true, logs intended mutations without making live API calls |
| `controller.isolationTaint` | string | `""` | Isolation taint for shared nodes (e.g. `timeslice.io/shared=true:NoSchedule`); empty disables taint management |
| `extraArgs` | list | `[]` | Extra command-line arguments to pass to the binary |
| `resources.requests.cpu` | string | `"50m"` | CPU request |
| `resources.requests.memory` | string | `"64Mi"` | Memory request |
| `resources.limits.memory` | string | `"256Mi"` | Memory limit |
| `nodeSelector` | object | `{}` | Node selector for pod assignment |
| `tolerations` | list | `[]` | Node tolerations for pod assignment |
| `affinity` | object | `{}` | Node/pod affinities for pod assignment |
| `priorityClassName` | string | `""` | Pod priority class name |
| `serviceAccount.create` | bool | `true` | Whether to create a ServiceAccount |
| `serviceAccount.annotations` | object | `{}` | Annotations to add to the ServiceAccount |
| `serviceAccount.name` | string | `""` | Name of existing ServiceAccount to use |
| `rbac.create` | bool | `true` | Whether to create ClusterRole and ClusterRoleBinding |
| `podAnnotations` | object | `{}` | Annotations to add to controller pod |
| `podSecurityContext` | object | `{runAsNonRoot: true, seccompProfile: {type: RuntimeDefault}}` | Pod security context |
| `securityContext` | object | `{allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}` | Container security context |
