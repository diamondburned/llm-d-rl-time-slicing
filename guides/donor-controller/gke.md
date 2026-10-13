# Donor Controller on GKE

This guide builds a GKE cluster that runs the full time-slicing stack from
the parent Helm chart:

- the NVIDIA DRA driver;
- the orchestrator;
- the snapshot agent;
- the donor controller.

It then walks through the controller's work on real GPU hosts, ending with
a real VM death.

If you only want to see the controller's behavior, the [KIND lab](kind.md)
is faster and free. This guide focuses on what's different in production:

- GPU node pools;
- the cloud controller manager;
- wiring the DaemonSets to the donor label;
- high availability;
- monitoring;
- cleanup.

> [!NOTE]
> Steps marked *(CPU-only OK)* also work on a cluster without GPUs. Use
> them to try the controller cheaply on GKE: skip section 2.2 and set
> `nvidia-dra-driver-gpu.enabled=false`.

---

## 1. Prerequisites

| Tool or access | Notes |
| --- | --- |
| `gcloud` | Authenticated (`gcloud auth login`), with a recent SDK (`gcloud components update`) |
| A GCP project | Kubernetes Engine and Artifact Registry APIs enabled, plus GPU quota in your zone (`NVIDIA_L4_GPUS` for the example below) |
| `kubectl` | 1.35+, with `gke-gcloud-auth-plugin` (`gcloud components install gke-gcloud-auth-plugin`) |
| Helm | 3.14+ |
| Docker | To build the donor controller image |
| Go | Only for the integration suite |

Set these once per shell. Every command passes `--project` and `--location`
explicitly, so your gcloud defaults are never used or changed.

```bash
export PROJECT=my-gcp-project
export REGION=us-central1
export ZONE=us-central1-a                 # needs L4 capacity
export CLUSTER="${USER}-timeslice"
export REGISTRY="${REGION}-docker.pkg.dev/${PROJECT}/timeslice"
export TAG="$(git rev-parse --short HEAD)$(git diff --quiet || echo -dirty)"
```

## 2. Create the cluster

### 2.1 Control plane and system pool *(CPU-only OK)*

DRA on GKE needs version 1.35 or later. The regular channel already
defaults to that.

```bash
gcloud container clusters create "$CLUSTER" \
  --project "$PROJECT" --location "$ZONE" \
  --release-channel regular \
  --machine-type e2-standard-4 --num-nodes 2

gcloud container clusters get-credentials "$CLUSTER" --project "$PROJECT" --location "$ZONE"
kubectl version   # server must be v1.35 or later
```

`get-credentials` adds a context to your kubeconfig and switches to it. To
keep this cluster in its own file instead, run
`export KUBECONFIG=~/.kube/${CLUSTER}.yaml` first.

The system pool runs the orchestrator, the donor controller and other
system pods. It never gets shared.

### 2.2 GPU pool for DRA

This follows GKE's [DRA setup](https://cloud.google.com/kubernetes-engine/docs/how-to/set-up-dra):

- turn off the automatic driver install and the default device plugin;
- mark the nodes for the DRA kubelet plugin;
- install the driver with GKE's COS installer, which puts it under
  `/home/kubernetes/bin/nvidia`. That's the path the parent chart expects.

```bash
gcloud container node-pools create gpu-l4 \
  --project "$PROJECT" --cluster "$CLUSTER" --location "$ZONE" \
  --machine-type g2-standard-8 \
  --accelerator type=nvidia-l4,count=1,gpu-driver-version=disabled \
  --num-nodes 2 \
  --node-labels=gke-no-default-nvidia-gpu-device-plugin=true,nvidia.com/gpu.present=true

kubectl apply -f https://raw.githubusercontent.com/GoogleCloudPlatform/container-engine-accelerators/master/nvidia-driver-installer/cos/daemonset-preloaded-latest.yaml
kubectl -n kube-system rollout status ds/nvidia-driver-installer --timeout=15m
```

GKE taints GPU nodes with `nvidia.com/gpu=present:NoSchedule`, and labels
them `cloud.google.com/gke-gpu=true`. Two nodes let you see one host shared
while its neighbor stays untouched.

## 3. Build and push the donor controller image *(CPU-only OK)*

The orchestrator and snapshot agent have published images, and the chart
uses them by default. The donor controller doesn't, so build it:

```bash
gcloud artifacts repositories create timeslice \
  --project "$PROJECT" --location "$REGION" --repository-format docker
gcloud auth configure-docker "${REGION}-docker.pkg.dev"

docker build -f docker/donor-controller/Dockerfile -t "${REGISTRY}/donor-controller:${TAG}" .
docker push "${REGISTRY}/donor-controller:${TAG}"
```

GKE nodes can pull from Artifact Registry in the same project with no extra
setup. For another project, grant the node service account
`roles/artifactregistry.reader` on that repository.

## 4. Install the stack with Helm

### 4.1 Values

The parent chart ships its subcharts unconnected. This values file connects
them:

1. It turns the donor controller on, with two replicas, an isolation taint,
   and the orchestrator's address, so a host stays shared while its group's
   lock is active.
2. It points the snapshot agent at shared hosts (`timeslice.io/donor=true`)
   instead of every GPU node. That's the reaction the design calls W1: the
   agent appears on a host when it becomes shared, and leaves when the host
   unshares.

The parent [values.yaml](../../deploy/values.yaml) already makes the snapshot
agent and DRA kubelet plugin tolerate `timeslice.io/shared=true:NoSchedule`,
so this guide uses that exact taint.

```bash
cat > /tmp/values-donor-gke.yaml <<EOF
donor-controller:
  enabled: true
  image:
    repository: ${REGISTRY}/donor-controller
    tag: "${TAG}"
  replicaCount: 2
  controller:
    idleTTL: 5m
    isolationTaint: timeslice.io/shared=true:NoSchedule
    orchestratorAddr: timeslice-timesliceorchestrator.timeslice-system:50051
    leaderElect: true
  # Keep replicas apart, and off the GPU pool.
  nodeSelector:
    cloud.google.com/gke-nodepool: default-pool
  affinity:
    podAntiAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          topologyKey: kubernetes.io/hostname
          labelSelector:
            matchLabels:
              app.kubernetes.io/name: donor-controller

snapshot-agent:
  # Merged with values-gke.yaml: GPU nodes AND shared.
  nodeSelector:
    timeslice.io/donor: "true"
EOF
```

> [!WARNING]
> The donor controller labels hosts `timeslice.io/group=<group>`. main's
> orchestrator only reads `group.timeslice.io/<group>`, so with main's chart
> the orchestrator won't see shared hosts (the check in 6.1 finds nothing).
> #208's orchestrator reads both with `--node-group-labels=either`.

> [!IMPORTANT]
> Set node-selector values from a values file, or with `--set-string`. A
> plain `--set ...timeslice\.io/donor=true` turns `"true"` into a boolean,
> which isn't a valid `nodeSelector` value.

Before going live, render the chart to check how the values compose:

```bash
helm dependency update deploy
helm template timeslice deploy -n timeslice-system \
  -f deploy/values-gke.yaml -f /tmp/values-donor-gke.yaml \
  | grep -A3 'nodeSelector:'
```

`helm dependency update` downloads the NVIDIA chart into `deploy/charts/`
(git-ignored) and refreshes `deploy/Chart.lock`.

### 4.2 First rollout in dry run (recommended)

Starting a node-writing controller in dry run costs nothing. It plans
everything, logs `would apply`, `would taint` and `would delete` lines, and
writes nothing:

```bash
helm upgrade --install timeslice deploy \
  --namespace timeslice-system --create-namespace \
  -f deploy/values-gke.yaml -f /tmp/values-donor-gke.yaml \
  --set donor-controller.controller.dryRun=true

kubectl -n timeslice-system rollout status deploy/timeslice-donor-controller
kubectl -n timeslice-system get pods -o wide
```

With the release named `timeslice`, the controller's objects are named
`timeslice-donor-controller`.

The parent chart renders the `timeslice-system` Namespace itself. If that
namespace already exists and Helm didn't create it (say, from an earlier
standalone install or `run.sh`), the install fails on ownership metadata.
Delete the namespace first, or install into a fresh cluster.

Check the plan against your cluster. On a fresh cluster with no donor pods,
there should be nothing:

```bash
kubectl -n timeslice-system logs -l app.kubernetes.io/name=donor-controller --tail=-1 | grep '"would ' || echo "no planned writes"
```

### 4.3 Go live

```bash
helm upgrade timeslice deploy -n timeslice-system \
  -f deploy/values-gke.yaml -f /tmp/values-donor-gke.yaml \
  --set donor-controller.controller.dryRun=false
kubectl -n timeslice-system rollout status deploy/timeslice-donor-controller
```

The Deployment uses RollingUpdate. The old leader releases the Lease on
shutdown, so upgrades hand over in seconds, and the controller's state lives
on the Nodes, so nothing is lost.

### 4.4 Check the install *(CPU-only OK)*

```bash
# Controller: both replicas Ready, one leader
kubectl -n timeslice-system get pods -l app.kubernetes.io/name=donor-controller -o wide
kubectl -n timeslice-system get lease donor-controller.timeslice.io -o jsonpath='{.spec.holderIdentity}{"\n"}'
kubectl -n timeslice-system logs -l app.kubernetes.io/name=donor-controller --tail=-1 | grep 'Starting donor-controller'

# Snapshot agent: zero pods until a host is shared
kubectl -n timeslice-system get ds -o wide

# RBAC: no forbidden errors
kubectl -n timeslice-system logs -l app.kubernetes.io/name=donor-controller --tail=-1 | grep -ci forbidden
```

## 5. Virtual nodes and the cloud controller

The guest kubelet registers one virtual node per shared host. The donor
controller recognizes it by the label `timeslice.io/virtual-node=true` and an
owner reference to the host Node; its name doesn't matter. The guest kubelet
also puts the finalizer `timeslice.io/virtual-node-protection` on it, which
the donor controller removes only when the host is gone.

GKE's cloud controller manager deletes *NotReady* Node objects whose VM it
can't find. In testing, it deleted fake NotReady nodes within 0.5 to 3.5
seconds ("does not exist in the cloud provider"). A real guest kubelet
heartbeats, so its node stays Ready and is left alone. The fake virtual nodes
in this guide never become Ready, so they set
`spec.providerID: virtual-kubelet://<name>`, which the cloud controller
ignores.

## 6. Watch it work

Pick two GPU hosts, then open the observation panes from the
[overview](README.md#observing-the-controller). On GKE, a compact
`watch` is handy:

```bash
export HOST=$(kubectl get nodes -l cloud.google.com/gke-gpu=true -o jsonpath='{.items[0].metadata.name}')
export OTHER=$(kubectl get nodes -l cloud.google.com/gke-gpu=true -o jsonpath='{.items[1].metadata.name}')

watch -n2 "kubectl get nodes -l 'cloud.google.com/gke-gpu=true' -o custom-columns='NAME:.metadata.name,DONOR:.metadata.labels.timeslice\.io/donor,GROUP:.metadata.labels.timeslice\.io/group,IDLE-SINCE:.metadata.annotations.timeslice\.io/idle-since,TAINTS:.spec.taints[*].key'; echo; kubectl get nodes -l timeslice.io/virtual-node=true -o custom-columns='NAME:.metadata.name,HOST:.metadata.ownerReferences[0].name'; echo; kubectl -n timeslice-system get pods -o wide | grep snapshot-agent"
```

For a quicker demo, shorten the grace period. Put it back to `5m`
afterwards:

```bash
helm upgrade timeslice deploy -n timeslice-system \
  -f deploy/values-gke.yaml -f /tmp/values-donor-gke.yaml \
  --set donor-controller.controller.idleTTL=1m
kubectl create namespace donor-demo
```

### 6.1 An RL job lands on a host (W1, W2, W5)

A real RL pod claims GPUs through a DRA `ResourceClaim` (see the
[orchestrator guide](../timeslice-orchestrator/README.md)). The controller
only cares that a pod labeled `timeslice.io/donor=true` is bound to the
host, so `pause` stands in here. It's placed by the scheduler, the same way
a real job is:

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: rl-rollout-0
  namespace: donor-demo
  labels:
    timeslice.io/donor: "true"
    timeslice.io/group: rollout
    timeslice.io/job-id: demo-job
spec:
  nodeSelector:
    kubernetes.io/hostname: ${HOST}
  tolerations:
  - {key: nvidia.com/gpu, operator: Exists, effect: NoSchedule}
  - {key: timeslice.io/shared, operator: Exists, effect: NoSchedule}
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.10
EOF
```

Within a second of the pod binding:

- **The host:** `HOST` gets `timeslice.io/donor=true`,
  `timeslice.io/group=rollout`, and the `timeslice.io/shared` taint.
  `OTHER` is untouched.
- **The snapshot agent:** its DaemonSet schedules a pod onto `HOST` (W1's
  downstream effect).
- **The orchestrator:** it adds `HOST` to group `rollout`.

Check the orchestrator:

```bash
kubectl -n timeslice-system logs -l app.kubernetes.io/name=timesliceorchestrator --tail=50 | grep 'Updated nodes for group'
```

And check the Events:

```bash
kubectl get events -n default --field-selector source=donor-controller,involvedObject.name="$HOST"
```

### 6.2 Guests borrow the host, then the job ends (W3)

Simulate the guest kubelet registering its virtual node. This helper creates
one the way the guest kubelet does: labeled, owned by the host, protected by
the finalizer, and named `vk-<hash of the host name>`:

```bash
vk_name() { echo "vk-$(printf %s "$1" | sha256sum | cut -c1-10)"; }

vnode() {  # usage: vnode <host>
local host=$1 uid; uid=$(kubectl get node "$1" -o jsonpath='{.metadata.uid}')
local name; name=$(vk_name "$host")
kubectl apply -f - <<EOF
apiVersion: v1
kind: Node
metadata:
  name: ${name}
  labels: {timeslice.io/virtual-node: "true", type: virtual-kubelet}
  finalizers: [timeslice.io/virtual-node-protection]
  ownerReferences: [{apiVersion: v1, kind: Node, name: ${host}, uid: ${uid}}]
spec:
  providerID: virtual-kubelet://${name}
  taints: [{key: timeslice.io/guest, value: "true", effect: NoSchedule}]
EOF
}

vnode "$HOST"
kubectl -n donor-demo delete pod rl-rollout-0
```

The virtual node doesn't keep `HOST` shared:

1. **Right away:** `idle-since` is stamped on `HOST`, unless the group's lock
   is still active, in which case the clock starts when it goes idle.
2. **One minute later:** the labels, annotation and taint are released
   together.
3. **Then:** the snapshot agent pod leaves `HOST`, and the orchestrator drops
   it from `rollout`. A real guest kubelet's DaemonSet pod leaves too, and on
   its way out it deletes its virtual node and removes the finalizer.

If a donor pod of `rollout` lands on `HOST` during that minute, the clock is
cancelled instead. Before releasing anything, the controller checks the API
server for donor pods and the orchestrator for lock activity, not its caches.

Since nothing here plays the guest kubelet's exit, remove the virtual node by
hand:

```bash
kubectl delete node "$(vk_name "$HOST")" --wait=false
kubectl patch node "$(vk_name "$HOST")" --type=json -p '[{"op":"remove","path":"/metadata/finalizers"}]'
```

### 6.3 A second group is rejected

Share `HOST` as `rollout` again, then bind a donor of group `train` to it:

```bash
for g in rollout train; do
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: rl-${g}-0
  namespace: donor-demo
  labels: {timeslice.io/donor: "true", timeslice.io/group: ${g}}
spec:
  nodeSelector: {kubernetes.io/hostname: ${HOST}}
  tolerations:
  - {key: nvidia.com/gpu, operator: Exists, effect: NoSchedule}
  - {key: timeslice.io/shared, operator: Exists, effect: NoSchedule}
  containers: [{name: pause, image: registry.k8s.io/pause:3.10}]
EOF
sleep 5
done
kubectl get events -A --field-selector source=donor-controller,type=Warning
```

`HOST` keeps `timeslice.io/group=rollout`, and `rl-train-0` and `HOST` each get
one `GroupConflict` Warning Event. Clean up with
`kubectl -n donor-demo delete pod rl-rollout-0 rl-train-0`.

### 6.4 A real VM dies (W4)

Give `OTHER` a virtual node (with the `vnode` helper from 6.2), then delete
its VM through the managed instance group, the way a preemption or repair
would:

```bash
vnode "$OTHER"

MIG=$(basename "$(gcloud container node-pools describe gpu-l4 \
  --project "$PROJECT" --cluster "$CLUSTER" --location "$ZONE" \
  --format='value(instanceGroupUrls[0])')")

gcloud compute instance-groups managed delete-instances "$MIG" \
  --project "$PROJECT" --zone "$ZONE" --instances "$OTHER"

kubectl get node "$OTHER" "$(vk_name "$OTHER")" -w
```

There are two delays:

1. **The cloud.** In two test runs, GKE took about two minutes (112s and
   114s) to delete the `$OTHER` Node object after the VM went away.
2. **The controller.** Once the Node was gone, it released the virtual node
   in under a second (0.37s measured, with the earlier rule set), after
   checking with the API server that the host really was gone. Without the
   donor controller, the finalizer would keep the virtual node, and its
   guests, around forever.

The Events read `Deleted by rule release virtual nodes of dead host` and
`Removed finalizer timeslice.io/virtual-node-protection by rule release
virtual nodes of dead host`.

The MIG then creates a replacement VM with a new name. It joins as an
ordinary, unshared node.

### 6.5 Clean up the demo

```bash
kubectl delete namespace donor-demo
helm upgrade timeslice deploy -n timeslice-system \
  -f deploy/values-gke.yaml -f /tmp/values-donor-gke.yaml
```

## 7. Monitoring

**Logs.** The controller logs JSON to stdout, so Cloud Logging parses each
line into `jsonPayload`. This Logs Explorer query shows every write:

```text
resource.type="k8s_container"
resource.labels.namespace_name="timeslice-system"
resource.labels.container_name="donor-controller"
jsonPayload.msg=~"applied|tainted|untainted|deleted|precondition failed"
```

**Metrics.** Managed Service for Prometheus is on by default for new
clusters. Scrape the controller's `metrics` port:

```bash
kubectl apply -f - <<'EOF'
apiVersion: monitoring.googleapis.com/v1
kind: PodMonitoring
metadata:
  name: donor-controller
  namespace: timeslice-system
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: donor-controller
  endpoints:
  - port: metrics
    interval: 30s
EOF
```

Useful PromQL in Metrics Explorer:

| Query | Healthy looks like |
| --- | --- |
| `sum by (result) (rate(controller_runtime_reconcile_total{controller="donor-controller"}[5m]))` | Bursts when pods or Nodes change. `requeue_after` covers idle-clock wake-ups |
| `rate(controller_runtime_reconcile_errors_total{controller="donor-controller"}[5m])` | Zero, apart from rare spikes from write conflicts (retried) |
| `workqueue_depth{name="donor-controller"}` | Close to zero |
| `max(leader_election_master_status{name="donor-controller.timeslice.io"})` | 1 |

Only the leader reconciles, so the reconcile series come from one pod at a
time.

**Events.** Every write is also a Kubernetes Event on the Node it touched, in
namespace `default`, with the rule names in the message. That's the
quickest way to answer "why does this node have this label?":

```bash
kubectl get events -n default --field-selector source=donor-controller,involvedObject.name="$NODE" --sort-by=.lastTimestamp
```

## 8. Run the integration suite on the cluster *(CPU-only OK)*

The suite is hermetic. It creates fake host Nodes (with `virtual-kubelet://`
provider IDs, per section 5) and never touches your GPU nodes, so it's safe
on a cluster with real workloads. It does need permission to create and
delete Nodes.

`run.sh` installs its own copy of the donor controller chart with a 20s
grace period, runs the suite in a pod, and uninstalls afterwards. Run it
against a cluster **without** the `timeslice` release above, or uninstall
that release first. Two controllers would fight over the same Nodes.

```bash
helm uninstall timeslice -n timeslice-system   # if installed
./tests/integration/run.sh --phase donor --donor-image "${REGISTRY}/donor-controller:${TAG}"
```

It takes about 5 minutes. See the
[suite README](../../tests/integration/donor-controller/README.md) for the
scenario catalog and how to run it from a workstation.

## 9. Uninstall and clean up node state

Uninstalling the chart stops the controller, but it doesn't unshare hosts.
Whatever it last wrote stays on the Nodes:

- labels;
- the `idle-since` annotation;
- the taint.

Wait until no host is shared (`kubectl get nodes -l timeslice.io/donor=true`
is empty) before uninstalling. Otherwise, clean up by hand afterwards:

```bash
helm uninstall timeslice -n timeslice-system

for n in $(kubectl get nodes -l timeslice.io/donor=true -o name); do
  groups=$(kubectl get "$n" -o go-template='{{range $k, $v := .metadata.labels}}{{$k}}{{"\n"}}{{end}}' | grep '^group\.timeslice\.io/' | sed 's/$/-/')
  kubectl label "$n" timeslice.io/donor- $groups
  kubectl annotate "$n" timeslice.io/idle-since-
  kubectl taint nodes "${n#node/}" timeslice.io/shared:NoSchedule- || true
done
for vk in $(kubectl get nodes -l timeslice.io/virtual-node=true -o name); do
  kubectl delete "$vk" --wait=false
  kubectl patch "$vk" --type=json -p '[{"op":"remove","path":"/metadata/finalizers"}]'
done
```

The last loop releases the virtual nodes. Only run it if no guest kubelet is
still serving them.

To delete the cluster and the image repository:

```bash
gcloud container clusters delete "$CLUSTER" --project "$PROJECT" --location "$ZONE" --quiet
gcloud artifacts repositories delete timeslice --project "$PROJECT" --location "$REGION" --quiet
```

## 10. Production checklist

- [ ] **High availability.** `replicaCount: 2` with leader election on (the
  default), spread across nodes, and off the GPU pool. A lone replica on a
  host that dies dies with it, and isn't rescheduled until the Node object is
  gone and its pods are garbage-collected. Until then, nothing cleans up that
  host's virtual node.
- [ ] **Isolation taint.** Everything that should run on shared hosts
  tolerates it: the snapshot agent, the DRA kubelet plugin, the guest kubelet,
  and the RL pods themselves.
- [ ] **DaemonSet selectors.** The guest kubelet, gpu-shadow plugin and
  snapshot agent select `timeslice.io/donor=true`, so they follow the host's
  sharing.
- [ ] **Virtual nodes.** The guest kubelet labels its node
  `timeslice.io/virtual-node=true`, sets an owner reference to the host Node,
  and adds the `timeslice.io/virtual-node-protection` finalizer.
- [ ] **Group labels on donor pods.** Every donor pod carries a valid
  `timeslice.io/group` (the webhook sets it). Watch for `InvalidGroup` and
  `GroupConflict` Warnings.
- [ ] **Orchestrator.** `controller.orchestratorAddr` points at the
  orchestrator, and the orchestrator reads `timeslice.io/group` from nodes
  (`--node-group-labels=ns` or `either`).
- [ ] **Grace period.** `idleTTL` is longer than the longest gap between
  steps of a job in which it has no donor pods and its lock is idle.
  Otherwise a host can unshare mid-job. The default is 5 minutes.
- [ ] **One host per group.** Keep one donor host per group while the
  orchestrator has that limit (see [known limits](README.md#known-limits)).
- [ ] **Dry run first.** The first rollout to a new cluster used
  `controller.dryRun=true`, and the `would ...` log lines were reviewed.
- [ ] **Alerting.** Alert on `controller_runtime_reconcile_errors_total`
  and on `forbidden` in the logs.
- [ ] **Uninstall procedure.** Section 9 is in the runbook.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| A fake virtual node disappears seconds after creation, and the Event isn't from `donor-controller` | It's NotReady and has no `spec.providerID`, so the cloud controller deleted it (section 5) |
| Snapshot agent never lands on a shared host | `snapshot-agent.nodeSelector` doesn't include `timeslice.io/donor: "true"`, or the agent doesn't tolerate the isolation taint |
| RL pod is `Pending` after its host is shared | The pod doesn't tolerate `timeslice.io/shared` |
| Host unshares mid-job | `idleTTL` is shorter than a gap with no donor pods and an idle lock, or `controller.orchestratorAddr` isn't set |
| Host never unshares after a job | A donor pod of its group is still bound, or the lock is active or unreadable (`GroupLockUnknown` Warning: check `controller.orchestratorAddr`) |
| A virtual node survives its host's deletion | It lacks `timeslice.io/virtual-node=true` or an owner reference of kind `Node`, so the controller doesn't treat it as a virtual node |
| Orchestrator never adds a shared host to its group | The orchestrator reads `group.timeslice.io/<g>` (main) instead of `timeslice.io/group` |
| `ImagePullBackOff` | Wrong `image.repository` or `image.tag`, or the node service account can't read the repository |
| Both replicas reconcile | `controller.leaderElect=false`. Turn it back on |

For anything else, start with the [KIND lab troubleshooting](kind.md#9-troubleshooting)
table. Everything there applies here too.
