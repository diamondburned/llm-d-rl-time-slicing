# Donor Controller Lab on KIND

This guide builds a throwaway [KIND](https://kind.sigs.k8s.io/) cluster and
walks through every donor controller journey by hand. Each step tells you
what to run and what you should see. You need no GPUs and no cloud account.
Plan on about 30 minutes.

What KIND can and cannot show:

| Shown for real | Simulated |
| --- | --- |
| Labels, taints and the idle annotation written to real worker Nodes | The guest kubelet: a virtual node you create by hand |
| A DaemonSet reacting to the donor label (a stand-in for the snapshot agent and guest kubelet) | GPU workloads: donor pods run `pause` |
| Host death: stop a worker container and delete its Node | The cloud controller deleting the Node: you delete it yourself |
| Restarts, dry run, leader election, metrics and Events | |

For background on the labels and rules, see the [overview](README.md).

---

## 1. Prerequisites

| Tool | Version | Notes |
| --- | --- | --- |
| Docker | 24+ | Or Podman with `KIND_EXPERIMENTAL_PROVIDER=podman` |
| [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) | 0.27+ | |
| kubectl | 1.33+ | |
| [Helm](https://helm.sh/docs/intro/install/) | 3.14+ | On Nix: `nix shell nixpkgs#kubernetes-helm` |
| Go | the version in [go.mod](../../go.mod) | Only for the integration suite and the out-of-cluster loop |
| jq | any | Optional, for reading the controller's JSON logs |

Run every command from the repository root.

## 2. Create the cluster

Use one control plane and three workers. The workers play the "real hosts"
that RL jobs donate.

The cluster gets its own kubeconfig file so your default
`~/.kube/config` stays untouched:

```bash
export CLUSTER=donor-lab
export KUBECONFIG="${HOME}/.kube/${CLUSTER}.yaml"

cat > /tmp/${CLUSTER}-kind.yaml <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
- role: worker
- role: worker
EOF

kind create cluster --name "$CLUSTER" --config /tmp/${CLUSTER}-kind.yaml --kubeconfig "$KUBECONFIG"
kubectl get nodes
```

Give the three workers short names for the rest of the guide:

```bash
export HOST_A=${CLUSTER}-worker HOST_B=${CLUSTER}-worker2 HOST_C=${CLUSTER}-worker3
```

> [!NOTE]
> Every new shell needs `CLUSTER`, `KUBECONFIG` and the `HOST_*` variables.
> Re-run the two `export` blocks above in each terminal you open.

## 3. Build and load the image

```bash
docker build -f docker/donor-controller/Dockerfile -t donor-controller:dev .
kind load docker-image donor-controller:dev --name "$CLUSTER"
```

`make image-build-donor-controller` also works. It tags the image as
`$(REGISTRY)/llm-d-rl-time-slicing/donor-controller:$(VERSION)`, so pass that
name to `kind load` and to `image.repository` and `image.tag` below.

## 4. Install the controller

Install the standalone subchart with a 45 second idle grace period, so the
idle clock is quick to watch but still leaves time to type. Also turn on the
isolation taint, and run the controller on the control-plane node.

The control-plane placement matters: in step 6.8 you kill a worker, and a
controller running on that worker would die with it. A lone replica on a dead
node isn't rescheduled until the Node object is gone and its pods are
garbage-collected, which takes over a minute.

```bash
cat > /tmp/${CLUSTER}-values.yaml <<'EOF'
image:
  repository: donor-controller
  tag: dev
controller:
  idleTTL: 45s
  isolationTaint: timeslice.io/shared=true:NoSchedule
nodeSelector:
  node-role.kubernetes.io/control-plane: ""
tolerations:
- key: node-role.kubernetes.io/control-plane
  operator: Exists
  effect: NoSchedule
EOF

helm upgrade --install donor-controller deploy/donor-controller \
  --namespace timeslice-system --create-namespace \
  -f /tmp/${CLUSTER}-values.yaml

kubectl -n timeslice-system rollout status deploy/donor-controller
kubectl -n timeslice-system get pods -o wide   # NODE is the control plane
```

With release name `donor-controller`, every object, including the
Deployment, is named `donor-controller`.

Check that it started with the flags you expect:

```bash
kubectl -n timeslice-system logs deploy/donor-controller | grep 'Starting donor-controller'
```

## 5. Open the observation panes

Open three extra terminals and leave them running. Re-export the variables
from step 2 in each one first.

**Pane 1: node state.** This shows one row per Node with the donor label,
the group, whether it's a virtual node and whose, the idle clock, and taint
keys:

```bash
watch -n1 "kubectl get nodes -o custom-columns='NAME:.metadata.name,DONOR:.metadata.labels.timeslice\.io/donor,GROUP:.metadata.labels.timeslice\.io/group,VIRTUAL-OF:.metadata.ownerReferences[0].name,IDLE-SINCE:.metadata.annotations.timeslice\.io/idle-since,TAINTS:.spec.taints[*].key'"
```

**Pane 2: Events.** The controller records one Event per write, and every
Event names the rule that caused it. It also records Warning Events, such as
group conflicts. Node Events land in the `default` namespace, and pod Events
in the pod's namespace:

```bash
kubectl get events -A --field-selector source=donor-controller -w
```

**Pane 3: controller log.** It logs JSON, apart from a few plain-text
leader-election lines. This filter skips the text lines and keeps only the
lines that describe writes:

```bash
kubectl -n timeslice-system logs deploy/donor-controller -f --since=1s \
  | jq -cR 'fromjson? | select(.msg | test("applied|tainted|untainted|deleted|finalizer|warned|would|precondition|stale")) | {time, msg, ref: .ref.Name, node, key, rules, rule, object} | with_entries(select(.value != null))'
```

If you don't have jq, drop the pipe and read the raw JSON.

Everything below runs in your original terminal.

## 6. Walk the journeys

Create a namespace for the demo pods, and add a hand label to `HOST_A`.
The hand label is there to show that the controller never touches labels it
doesn't own:

```bash
kubectl create namespace donor-lab
kubectl label node "$HOST_A" team=foo
```

### 6.1 Stand-in DaemonSet (what reacts to sharing)

In a real cluster, the guest kubelet, the GPU shadow plugin and the snapshot
agent are DaemonSets that select shared hosts. This `pause` DaemonSet does the
same, so you can see the donor label pull pods onto a host, and push them off
again:

```bash
kubectl apply -f - <<'EOF'
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fake-guest-kubelet
  namespace: donor-lab
spec:
  selector:
    matchLabels: {app: fake-guest-kubelet}
  template:
    metadata:
      labels: {app: fake-guest-kubelet}
    spec:
      nodeSelector:
        timeslice.io/donor: "true"
      tolerations:
      - key: timeslice.io/shared
        operator: Exists
      containers:
      - name: pause
        image: registry.k8s.io/pause:3.10
EOF
```

It has no pods yet because no Node carries the donor label.

### 6.2 Share a host (W1, W2, W5)

A *donor pod* is any pod labeled `timeslice.io/donor=true` that is bound to a
Node. Its `timeslice.io/group` label names the time-slicing group the host
should join; a donor pod without one shares nothing.

Pin one to `HOST_A`:

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: donor-rollout
  namespace: donor-lab
  labels:
    timeslice.io/donor: "true"
    timeslice.io/group: rollout
spec:
  nodeName: ${HOST_A}
  tolerations:
  - key: timeslice.io/shared
    operator: Exists
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.10
EOF
```

Within a second you should see:

- **Pane 1:** `HOST_A` shows `DONOR=true`, `GROUP=rollout`, and the taint
  `timeslice.io/shared`.
- **Pane 2:** an `Applied` Event, `Applied by rules [share node while donor
  pods are bound]: set labels=[...]`, and a `Tainted` Event.
- **The DaemonSet:** a `fake-guest-kubelet` pod is now on `HOST_A`.

Check the DaemonSet pod:

```bash
kubectl -n donor-lab get pods -o wide -l app=fake-guest-kubelet
```

Check that the hand label survived, and see exactly which fields the
controller owns:

```bash
kubectl get node "$HOST_A" -o jsonpath='{.metadata.labels.team}{"\n"}'
kubectl get node "$HOST_A" --show-managed-fields \
  -o jsonpath='{range .metadata.managedFields[?(@.manager=="timeslice-donor-controller")]}{.operation}{" "}{.fieldsV1}{"\n"}{end}'
```

You should see two entries:

- **`Apply`:** the controller writes labels and annotations with server-side
  apply, so this entry lists only `timeslice.io/donor` and
  `timeslice.io/group`.
- **`Update` with `{"f:spec":{"f:taints":{}}}`:** taints are a single list
  shared by every writer, so the controller updates it with a merge patch
  guarded by the resourceVersion. Kubernetes doesn't track ownership per
  taint, so the controller only ever adds or removes the one taint it was
  configured with.

### 6.3 A second group is rejected (W2)

A host belongs to one group at a time, and the first group wins. Bind a donor
of another group to the same host:

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: donor-train
  namespace: donor-lab
  labels:
    timeslice.io/donor: "true"
    timeslice.io/group: train
spec:
  nodeName: ${HOST_A}
  tolerations:
  - key: timeslice.io/shared
    operator: Exists
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.10
EOF
```

- **Pane 1:** `HOST_A` still shows `GROUP=rollout`.
- **Pane 2:** two `GroupConflict` Warning Events, one on `node/HOST_A` and
  one on `pod/donor-train`: `node ... is in group "rollout", so donor pod
  donor-lab/donor-train of group "train" does not share it; the first group
  wins (rule reject donor pods outside the node's group)`. Each warning is
  emitted once, however often the host is re-evaluated.

The conflicting pod changes nothing else. If the `rollout` donors leave, the
host still runs out its idle clock for `rollout` before it can be shared as
`train`. Remove it:

```bash
kubectl -n donor-lab delete pod donor-train --grace-period=0 --force
```

### 6.4 Guests borrow the host, but don't keep it (W3)

While the RL job's trainer is idle, the guest kubelet runs guests on the host
through a virtual node. It registers that Node itself, with:

- the label `timeslice.io/virtual-node=true`;
- an owner reference to the host Node (name and UID);
- the finalizer `timeslice.io/virtual-node-protection`, which keeps the
  virtual node, and its guests, through a guest kubelet outage.

Its name doesn't matter to the donor controller, which finds virtual nodes by
owner reference. These helpers create one the same way, named
`vk-<hash of the host name>`:

```bash
vk_name() { echo "vk-$(printf %s "$1" | sha256sum | cut -c1-10)"; }

vnode() {  # usage: vnode <host> [<host uid>]
local host=$1 uid=${2:-$(kubectl get node "$1" -o jsonpath='{.metadata.uid}')}
local name; name=$(vk_name "$host")
kubectl apply -f - <<EOF
apiVersion: v1
kind: Node
metadata:
  name: ${name}
  labels:
    timeslice.io/virtual-node: "true"
    type: virtual-kubelet
  finalizers:
  - timeslice.io/virtual-node-protection
  ownerReferences:
  - apiVersion: v1
    kind: Node
    name: ${host}
    uid: ${uid}
spec:
  providerID: virtual-kubelet://${name}
  taints:
  - key: timeslice.io/guest
    value: "true"
    effect: NoSchedule
EOF
}

vnode "$HOST_A"
```

The `virtual-kubelet://` provider ID only matters on GKE, for fake nodes like
this one that never become Ready (see the [GKE guide](gke.md#5-virtual-nodes-and-the-cloud-controller)).

Pane 1 shows the new row with `VIRTUAL-OF=HOST_A`. Over time, the node
lifecycle controller adds `node.kubernetes.io/...` taints to it, because no
kubelet heartbeats for it. That's expected, and the controller leaves those
taints alone.

Now remove the donor, the way an RL job finishing would:

```bash
kubectl -n donor-lab delete pod donor-rollout --grace-period=0 --force
```

Here's what happens, even though the virtual node is still there:

1. **Right away:** `IDLE-SINCE` on `HOST_A` gets a timestamp. That's rule
   `keep node shared during idle grace period`, which also schedules a
   wake-up for when the grace period ends.
2. **After 45s:** the donor and group labels, the taint and the annotation all
   disappear at once. Pane 2 shows `Applied ... released ...` and `Untainted
   timeslice.io/shared:NoSchedule: no rule asserts it`.
3. **Then:** the DaemonSet controller removes the `fake-guest-kubelet` pod
   from `HOST_A`, because the Node no longer matches its selector.

In a real cluster, step 3 is where the guest kubelet goes: its DaemonSet pod
is removed, and on its way out it deletes its virtual node and removes the
finalizer. Here, do that by hand:

```bash
kubectl delete node "$(vk_name "$HOST_A")" --wait=false
kubectl patch node "$(vk_name "$HOST_A")" --type=json -p '[{"op":"remove","path":"/metadata/finalizers"}]'
```

With an orchestrator configured (`controller.orchestratorAddr`), the host
would also stay shared while its group's lock is active, and the clock would
start only once the lock goes idle. This lab has no orchestrator, so donor
pods alone decide.

### 6.5 Release by omission

`team=foo` is still there:

```bash
kubectl get node "$HOST_A" --show-labels | tr ',' '\n' | grep -E 'team|timeslice'
```

Nothing ever wrote "remove": no rule asserts the labels any more, so server-side
apply drops the fields the controller owned. That's *release by omission*;
the [package README](../../pkg/donor-controller/README.md) explains why it's
safe.

### 6.6 A donor returns during the grace period (E7)

Share `HOST_B`, remove its donor, and start the clock. The `donor` helper
defined here is reused in 6.7 and 6.9, and only exists in this shell. Paste
the function again if you switch terminals.

```bash
donor() {  # usage: donor <name> <host> <group>
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $1
  namespace: donor-lab
  labels:
    timeslice.io/donor: "true"
    timeslice.io/group: $3
spec:
  nodeName: $2
  tolerations:
  - key: timeslice.io/shared
    operator: Exists
  containers:
  - name: pause
    image: registry.k8s.io/pause:3.10
EOF
}

donor donor-b "$HOST_B" rollout
sleep 3
kubectl -n donor-lab delete pod donor-b --grace-period=0 --force
```

When `IDLE-SINCE` appears on `HOST_B`, bring a donor back before 45s pass:

```bash
donor donor-b2 "$HOST_B" rollout
```

`IDLE-SINCE` clears and the labels never flicker. Before it unshares, the
controller lists donor pods straight from the API server (not its cache), so a
pod that bound a moment ago still stops the unshare.

Clean up and let it unshare:

```bash
kubectl -n donor-lab delete pod donor-b2 --grace-period=0 --force
```

### 6.7 The idle clock survives a restart

The clock lives on the Node as an annotation, not in controller memory.
Start it on `HOST_B` again, then restart the controller while it runs:

```bash
donor donor-b3 "$HOST_B" rollout
sleep 3
kubectl -n donor-lab delete pod donor-b3 --grace-period=0 --force
sleep 5
kubectl get node "$HOST_B" -o jsonpath='{.metadata.annotations.timeslice\.io/idle-since}{"\n"}'
kubectl -n timeslice-system rollout restart deploy/donor-controller
kubectl -n timeslice-system rollout status deploy/donor-controller
kubectl get node "$HOST_B" -o jsonpath='{.metadata.annotations.timeslice\.io/idle-since}{"\n"}'
```

Both timestamps match, and `HOST_B` unshares about 45s after the *first* one,
not 45s after the restart. Pane 3 drops out when its pod goes away; re-run its
command.

### 6.8 Host death releases the virtual node (W4)

When a host dies, its guest kubelet dies with it, so nobody is left to remove
the virtual node's finalizer. The donor controller does it. This uses the
`vnode` and `vk_name` helpers from 6.4; paste them again in a new shell.

Give `HOST_C` a virtual node, and add a stray one whose host never existed
(any name, any UID):

```bash
vnode "$HOST_C"
vnode ghost "$(cat /proc/sys/kernel/random/uuid)"
```

`$(vk_name ghost)` disappears within a second or two. Its host is gone, so
Kubernetes' garbage collector deletes it (it has an owner reference to a
missing owner), and the donor controller deletes it too and removes the
finalizer, which nothing else would. Pane 2 shows `Removed finalizer
timeslice.io/virtual-node-protection by rule release virtual nodes of dead
host`. `$(vk_name "$HOST_C")` stays, because `HOST_C` is alive.

Now kill `HOST_C`. On a cloud provider, the cloud controller deletes the
Node object when its VM goes away. KIND has no cloud controller, so stop the
container and delete the Node yourself:

```bash
docker stop "$HOST_C"
kubectl delete node "$HOST_C"
```

`$(vk_name "$HOST_C")` is gone within a second. Before releasing it, the
controller confirms with the API server that the host really is gone, so a
stale cache can't release a live host's virtual node. The same happens if a
host Node is recreated under the same name: the new Node has a new UID, which
no longer matches the virtual node's owner reference.

A Node without the `timeslice.io/virtual-node` label is never touched, even
if its owner is gone. The garbage collector marks it for deletion, but the
finalizer keeps it, because the donor controller only releases virtual nodes:

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Node
metadata:
  name: not-a-virtual-node
  finalizers:
  - timeslice.io/virtual-node-protection
  ownerReferences:
  - apiVersion: v1
    kind: Node
    name: ghost
    uid: $(cat /proc/sys/kernel/random/uuid)
spec:
  providerID: virtual-kubelet://not-a-virtual-node
EOF
sleep 10; kubectl get node not-a-virtual-node -o jsonpath='{.metadata.deletionTimestamp} {.metadata.finalizers}{"\n"}'
kubectl patch node not-a-virtual-node --type=json -p '[{"op":"remove","path":"/metadata/finalizers"}]'
```

Bring the worker back. Its kubelet registers the Node again on startup:

```bash
docker start "$HOST_C"
until kubectl get node "$HOST_C" >/dev/null 2>&1; do sleep 2; done
kubectl wait --for=condition=Ready node/"$HOST_C" --timeout=120s
```

### 6.9 Dry run

Dry run plans and logs every write but makes none:

```bash
helm upgrade donor-controller deploy/donor-controller -n timeslice-system \
  --reuse-values --set controller.dryRun=true
kubectl -n timeslice-system rollout status deploy/donor-controller

donor donor-dry "$HOST_B" rollout
sleep 10
kubectl get node "$HOST_B" -o jsonpath='{.metadata.labels.timeslice\.io/donor}{"\n"}'   # empty
kubectl -n timeslice-system logs deploy/donor-controller | grep '"would '
```

The log has `would apply` and `would taint` lines with the full desired
content and the rules behind it. Turn dry run off, and the same pod shares the
host for real:

```bash
helm upgrade donor-controller deploy/donor-controller -n timeslice-system \
  --reuse-values --set controller.dryRun=false
kubectl -n timeslice-system rollout status deploy/donor-controller
sleep 5
kubectl get node "$HOST_B" -o jsonpath='{.metadata.labels.timeslice\.io/donor}{"\n"}'   # true
kubectl -n donor-lab delete pod donor-dry --grace-period=0 --force
```

### 6.10 Metrics

The controller serves the standard controller-runtime metrics on port 8080:

```bash
kubectl -n timeslice-system port-forward deploy/donor-controller 8080:8080 &
sleep 2
curl -s localhost:8080/metrics | grep -E '^(controller_runtime_reconcile_total|controller_runtime_reconcile_errors_total|workqueue_depth)\{.*donor-controller'
kill %1
```

The useful series are:

- `controller_runtime_reconcile_total{result="success"}` grows with every
  change.
- `result="requeue_after"` counts the idle-clock wake-ups.
- `controller_runtime_reconcile_errors_total` should stay flat.

### 6.11 Leader election and failover

Run two replicas. Only the Lease holder reconciles:

```bash
helm upgrade donor-controller deploy/donor-controller -n timeslice-system \
  --reuse-values --set replicaCount=2
kubectl -n timeslice-system rollout status deploy/donor-controller

LEADER=$(kubectl -n timeslice-system get lease donor-controller.timeslice.io -o jsonpath='{.spec.holderIdentity}')
echo "$LEADER"
kubectl -n timeslice-system delete pod "${LEADER%%_*}"
sleep 5
kubectl -n timeslice-system get lease donor-controller.timeslice.io -o jsonpath='{.spec.holderIdentity}{"\n"}'
```

The holder changes to the other pod within a few seconds. A clean shutdown
releases the Lease rather than waiting for it to expire.

Go back to one replica:

```bash
helm upgrade donor-controller deploy/donor-controller -n timeslice-system \
  --reuse-values --set replicaCount=1
```

## 7. Run the integration suite against the lab

The [donor integration suite](../../tests/integration/donor-controller/README.md)
automates all of the above and more (S0 to S16). It creates its own fake host
Nodes, so it doesn't touch your workers.

Start from a clean slate, then point the suite at the 45s grace period:

```bash
kubectl delete namespace donor-lab --wait=false
DONOR_CHART_DEPLOYED=1 DONOR_IDLE_TTL=45s go test -count=1 -v -timeout 30m \
  -run '^TestDonorController$' ./tests/integration/donor-controller/
```

Without `DONOR_CHART_DEPLOYED`, the test skips. That keeps a stray
`go test ./...` from creating Nodes in whatever cluster you're pointed at.

It takes about 8 minutes at 45s. For a faster run, reinstall with
`--set controller.idleTTL=20s` and drop `DONOR_IDLE_TTL`, since the suite
defaults to 20s.

`DONOR_NAMESPACE` and `DONOR_ISOLATION_TAINT` default to the values used in
this guide.

## 8. Iterate without rebuilding images

For fast edit-run loops, stop the in-cluster controller and run it from
source against the same cluster:

```bash
kubectl -n timeslice-system scale deploy/donor-controller --replicas=0

go run ./cmd/donor-controller \
  --idle-ttl=45s \
  --isolation-taint='timeslice.io/shared=true:NoSchedule' \
  --metrics-bind-address=:18080 \
  --health-probe-bind-address=:18081
```

It reads `$KUBECONFIG` (or `--kubeconfig`). Leader election is off by
default outside the chart. Logs go to stdout as JSON, so you can pipe them
through the same jq filter.

When you're done, press Ctrl-C and scale back up:

```bash
kubectl -n timeslice-system scale deploy/donor-controller --replicas=1
```

## 9. Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `ErrImagePull` / `ImagePullBackOff` | The image wasn't loaded with `kind load docker-image`, or the tag doesn't match `image.tag` |
| Host never gets `timeslice.io/donor` | The pod label must be exactly `timeslice.io/donor: "true"`. The pod must have `spec.nodeName` set (bound), and must not be `Succeeded` or `Failed`. Dry run may be on: check the `Starting donor-controller` log line |
| Host not shared, `InvalidGroup` Warning | The donor pod has no `timeslice.io/group` label, or its value isn't a valid label value |
| Host not shared, `GroupConflict` Warning | The host already belongs to another group: its `timeslice.io/group` label, possibly set by hand, or an older donor pod's group wins |
| Host never unshares | A donor pod of the host's group is still bound (`kubectl get pods -A -l timeslice.io/donor=true -o wide`), or, with an orchestrator, the group's lock is active or unreadable (look for a `GroupLockUnknown` Warning) |
| Virtual node isn't released after host death | It lacks `timeslice.io/virtual-node=true` or an owner reference of kind `Node`, or the host Node still exists (`NotReady` isn't deleted) |
| `forbidden` in the logs | RBAC drift. Compare with [clusterrole.yaml](../../deploy/donor-controller/templates/clusterrole.yaml) |
| `stale observation, requeueing` in the logs | Normal. Another writer changed the Node between read and write, so the controller re-reads it and retries |
| Pane 2 is empty | Events expire after an hour. Node Events are in `default` and pod Events in the pod's namespace, not `timeslice-system` |

## 10. Tear down

```bash
kind delete cluster --name "$CLUSTER"
rm -f "$KUBECONFIG" /tmp/${CLUSTER}-kind.yaml
```
