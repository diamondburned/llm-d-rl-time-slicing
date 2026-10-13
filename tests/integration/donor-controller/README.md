# Donor Controller Integration Suite

Hermetic end-to-end integration test suite for the time-slicing donor controller.

The suite verifies the controller's level-triggered state machine, server-side apply ownership, virtual node lifecycle, and failure recovery across scenarios S0 through S16.

## Key Design Decision: Hermetic Fake Hosts

The suite does **NOT** mutate or disrupt real cluster nodes:
- Creates isolated fake "real host" `Node` objects per run (`dc-e2e-<runid>-h1..h3`, labeled `timeslice.io/e2e=<runid>`).
- Sets `spec.providerID = donorcontroller.VirtualNodeProviderID(name)` (`virtual-kubelet://<name>`). On GKE, this prevents the Cloud Controller Manager (CCM) from deleting unbacked nodes.
- Binds donor pods to fake hosts via `spec.nodeName` (pods stay `Pending` with toleration `operator: Exists`, which suffices for controller binding observation).
- Host death is cleanly simulated by deleting the fake host `Node` object.
- Tolerates `node.kubernetes.io/unreachable` and `node.kubernetes.io/not-ready` taints posted by K8s node lifecycle controllers on unmanaged nodes, doubling as a regression test for foreign taint preservation.

## Scenario Catalog

| ID | Steps | Assertions | Why It Exists |
|---|---|---|---|
| **S0** | Create fake hosts `h1..h3`; wait 15s. | No timeslice labels/annotations; no `timeslice-donor-controller` in `managedFields`. | Proves quiet baseline and no spurious writes. |
| **S1** | Create DaemonSet; create donor pod (`g1`) on `h1`. | `timeslice.io/donor=true`, `timeslice.io/group=g1`, isolation taint applied. | Validates era start. |
| **S2** | Create second donor pod (`g2`) on `h1`. | `group=g1` remains; Warning `GroupConflict` events on pod and node within 15s. | Group conflict handling: first group wins. |
| **S3** | Create `vk-h1` owned by `h1`; delete donor pods on `h1`. | `idle-since` appears; `h1` unshares after TTL+15s despite `vk-h1` existence. | Virtual nodes no longer hold the host open. |
| **S4** | Delete donors on unshared host. | DaemonSet pod removed. | Complete unshare cleanup. |
| **S5** | E7 Reshare (share, delete, immediately return donor). | `idle-since` cleared, `donor=true` stays. | Resets idle clock on fast return. |
| **S6** | Restart controller during idle clock. | Clock persists, unshares ~TTL after original timestamp. | Validates annotation clock persistence. |
| **S7** | W4 Guards (create various ghost nodes). | Nonexistent owner -> removed; no label -> kept; live owner -> kept. | Orphan node deletion checks. |
| **S8** | Create `vk-h3` and delete `h3`. | `vk-h3` released within 20s. | Host death cleanup. |
| **S8b** | Recreate `h4` with new UID. | `vk-h4` released within 20s. | UID mismatch cleanup. |
| **S9** | SSA Create Guard. | API server rejects stale node upsert. | Prevents ghost node recreations. |
| **S10** | Dry Run. | No mutations made while dry-run is enabled. | Validates dry-run protection. |
| **S11** | Ownership inspect on `h1`. | Apply owns exactly `donor` and `group` (and `idle-since`). | Validates SSA hygiene. |
| **S12** | Add foreign taint to `h2`, share, unshare. | Foreign taint preserved. | Preserves system taints. |
| **S13** | Events Audit. | `Applied`, `Tainted`, `Untainted`, `FinalizerRemoved`, `GroupConflict` events present. | Standard Kubernetes Event audit trail. |
| **S14** | Leader Election lease check. | Lease acquired by running pod. | HA leader election works. |
| **S15** | Donor pod without group on `h2`. | `InvalidGroup` warning on pod/node, `h2` not shared. | Invalid group handling. |
| **S16** | Fresh host `hf` with `timeslice.io/gpu-fence=vk-hf` and a foreign taint: (a) `vk-hf` exists; (b) `vk-hf` deleted, a mirror pod (`timeslice.io/mirror-node=vk-hf`) bound to `hf`; (c) mirror deleted. | (a), (b): fence kept, no `stale-fence-since`, for grace+10s. (c): `stale-fence-since` appears, then after the grace period the fence alone is removed, the annotation released, and an `Untainted` Event names `remove stale gpu fence`. | A departed guest kubelet can't remove its fence; the controller must, but never while the guest kubelet or a mirror could still need it. |
| **FinalCheck** | RBAC & Health log inspect. | 0 forbidden errors, no hot loops. | Ensures clean execution. |


> **Note for Helm 4 users**: The `S10_DryRun` helper temporarily patches the Deployment args using the `donor-controller.test` field manager. If a test is aborted mid-flight before `SetDryRun` can restore the args and remove its manager entry, Helm 4 (server-side apply) may fail the next `helm upgrade` with an SSA conflict on `.spec.template.spec.containers[name=donor-controller].args`. You may need to run `helm upgrade --force-conflicts`.

## Running the Suite

### From a Workstation (Development)

Prerequisites: active `KUBECONFIG` pointing to the target cluster where the donor-controller chart is deployed.

The suite has no build tag, so gopls and `go vet` see it like any other package. Instead, `TestDonorController` calls `t.Skip` unless `DONOR_CHART_DEPLOYED` is set, the same gate the orchestrator and snapshot-agent suites use (`ORCH_CHART_DEPLOYED`, `SA_CHART_DEPLOYED`). A plain `go test ./...` therefore never creates Node objects in whatever cluster `KUBECONFIG` points at. `run.sh` sets the variable when it installs the chart.

```bash
export KUBECONFIG=/path/to/kubeconfig
export DONOR_CHART_DEPLOYED=1

# Run the entire suite
go test -count=1 -v -timeout 30m -run '^TestDonorController$' ./tests/integration/donor-controller/

# Run a specific scenario (e.g. S1 Share)
go test -count=1 -v -run '^TestDonorController/S1_Share$' ./tests/integration/donor-controller/
```

### In-Cluster (via `run.sh`)

`run.sh` installs the Helm fixture into `timeslice-system`, starts a test-runner pod, copies workspace source into it, and executes the suite in-cluster:

```bash
./tests/integration/run.sh --phase donor --donor-image <registry>/donor-controller:<tag>
```

To run both snapshot-agent, orchestrator, and donor controller:

```bash
./tests/integration/run.sh --phase all --donor-image <donor-image> --agent-image <agent-image> --orch-image <orch-image>
```

## Not Covered

1. **Real Host Death via Cloud Provider VM Removal**:
   Simulated via `Node` object deletion. Real GKE host removal (deleting the VM from its managed instance group) was tested manually; see [the GKE guide](../../../guides/donor-controller/gke.md#64-a-real-vm-dies-w4):
   - GKE took **112 seconds** to detect instance termination and delete the Kubernetes `Node` object.
   - The donor controller deleted `vk-<host>` **0.37 seconds** after the `Node` object disappeared.
2. **Real Guest Kubelet (`virtual-kubelet`) Execution**:
   Simulated via fake virtual node objects with `timeslice.io/virtual-node=true` and `spec.providerID`.
3. **Group Locks**: The group lock check is off in this suite (no orchestrator). It is covered by unit tests.
4. **Real Accelerated Workloads**:
   The suite uses pause containers (`registry.k8s.io/pause:3.10`). Actual GPU/TPU slicing workloads are exercised in the `snapshot-agent` and `orchestrator` suites.
