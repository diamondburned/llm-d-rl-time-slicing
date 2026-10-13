# Donor Controller Helm Chart

> [!NOTE]
> Leader election is enabled by default via `coordination.k8s.io` Leases, allowing safe RollingUpdate deployments and multi-replica standby setups.

This directory contains the Helm chart for deploying the donor controller in a Kubernetes cluster.

For architectural details, rules, and policy specification, see [../../pkg/donor-controller](../../pkg/donor-controller). For step-by-step installs, see the [KIND lab](../../guides/donor-controller/kind.md) and the [GKE guide](../../guides/donor-controller/gke.md).

## Overview

The donor controller coordinates node donation for RL time-slicing workloads. It:
- Watches all `Node` resources, donor pods (`timeslice.io/donor=true`) and the guest kubelet's mirror pods (`timeslice.io/mirror-node` or `timeslice.io/role`). Its cache holds every pod, trimmed to a few fields, since no label selector matches both kinds.
- Labels a host `timeslice.io/donor=true` and `timeslice.io/group=<group>` (and adds the isolation taint, if configured) while donor pods of one group are bound to it; donor pods of another group get a `GroupConflict` Warning Event.
- Keeps the host shared while its group's orchestrator lock is active (`controller.orchestratorAddr`), and for `controller.idleTTL` after that; then unshares it. The guest-kubelet DaemonSet follows the donor label off the host.
- Releases the virtual nodes of a host that is gone or was recreated: deletes them and removes the `timeslice.io/virtual-node-protection` finalizer.
- Removes a stale GPU fence: the guest kubelet's `timeslice.io/gpu-fence=<virtual node>` taint, once that virtual node has been gone for `controller.staleFenceGrace` and no mirror pod is left on the host. The guest kubelet adds and removes the fence itself; the controller steps in only when the guest kubelet is gone and can't.

## Values

| Key | Type | Default | Description |
|---|---|---|---|
| `replicaCount` | int | `1` | Number of controller replicas (leader election ensures single active leader) |
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
| `controller.orchestratorAddr` | string | `""` | Orchestrator gRPC address (`host:port`). A shared host stays shared while its group's lock is active or unknown; empty disables the lock check |
| `controller.groupLockPollInterval` | string | `"2s"` | How often an idle shared host re-reads its group's lock state |
| `controller.clearStaleFences` | bool | `true` | Remove the guest kubelet's `timeslice.io/gpu-fence` taint once the virtual node it names has been gone for `staleFenceGrace` and no mirror pod is left on the host |
| `controller.staleFenceGrace` | string | `"1m"` | How long a GPU fence must have been stale before it is removed |
| `controller.leaderElect` | bool | `true` | Enable leader election via `coordination.k8s.io` Lease |
| `controller.metricsBindAddress` | string | `":8080"` | Metrics endpoint bind address (`"0"` disables) |
| `controller.healthProbeBindAddress` | string | `":8081"` | Health probe endpoint bind address |
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
