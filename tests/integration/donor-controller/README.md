# Donor Controller Integration Suite

Hermetic end-to-end integration test suite for the time-slicing donor controller.

The suite verifies the controller's level-triggered state machine, server-side apply ownership, virtual node lifecycle, and failure recovery across scenarios S0 through S12.

## Key Design Decision: Hermetic Fake Hosts

The suite does **NOT** mutate or disrupt real cluster nodes:
- Creates isolated fake "real host" `Node` objects per run (`dc-e2e-<runid>-h1..h3`, labeled `timeslice.io/e2e=<runid>`).
- Sets `spec.providerID = wellknown.VirtualNodeProviderID(name)` (`virtual-kubelet://<name>`). On GKE, this prevents the Cloud Controller Manager (CCM) from deleting unbacked nodes.
- Binds donor pods to fake hosts via `spec.nodeName` (pods stay `Pending` with toleration `operator: Exists`, which suffices for controller binding observation).
- Host death is cleanly simulated by deleting the fake host `Node` object.
- Tolerates `node.kubernetes.io/unreachable` and `node.kubernetes.io/not-ready` taints posted by K8s node lifecycle controllers on unmanaged nodes, doubling as a regression test for foreign taint preservation.

## Scenario Catalog

| ID | Design Doc Journey / Write | Steps | Assertions | Why It Exists |
|---|---|---|---|---|
| **S0** | Baseline | Create fake hosts `h1..h3`; wait 15s quiet period. | No `timeslice.io/*` or `group.timeslice.io/*` labels/annotations; no `timeslice-donor-controller` in `managedFields`. Hand label `team=foo` survives. | Proves the controller is quiet on unshared nodes and performs no spurious writes on startup. |
| **S1** | Share (W1/W2/W5) | Create DaemonSet; create donor pod (`group=g1`) bound to `h1`. | `timeslice.io/donor=true`, `group.timeslice.io/g1=true`, isolation taint `timeslice.io/shared=true:NoSchedule` applied. `team=foo` survives. DaemonSet pod lands on `h1`. `h2` untouched. | Validates era start: label mutation, taint isolation, DaemonSet scheduling reaction, and field non-interference. |
| **S2** | Multi-Group | Create second donor pod (`group=g2`) bound to `h1`. | Both `group.timeslice.io/g1=true` and `group.timeslice.io/g2=true` present on `h1`. | Verifies level-triggered accumulation of multiple time-slicing groups within an active era. |
| **S3** | Hold (W3) | Create `vk-h1`; delete donor pods on `h1`; wait > TTL+10s. | Labels and taint retained; `timeslice.io/idle-since` annotation NOT set. `vk-h1` retained. | Verifies the node remains shared while a virtual node or guest pods are active, preventing premature reclamation. |
| **S4** | Unshare (W3) | Delete `vk-h1`. | `timeslice.io/idle-since` appears within 15s; after TTL, donor and group labels, taint, and annotation removed. `team=foo` survives. DaemonSet pod removed. | Validates complete era teardown upon workload drain and idle TTL expiration. |
| **S5** | E7 Reshare | Share `h1`, delete donor, wait for `idle-since`, then immediately return donor. Sample every 1s for TTL+10s. | `timeslice.io/donor=true` present in 100% of samples; `idle-since` cleared. Clean unshare after final delete. | Protects against race condition E7: ensures a returning donor immediately resets the idle clock without flapping labels. |
| **S6** | Restart | Start idle clock on `h1`; restart controller pod during clock. | `idle-since` annotation unchanged across restart; unshare occurs ~TTL after the original idle timestamp (+15s tolerance). | Proves clock persistence: restarting the controller does not reset or corrupt the idle clock stored in node annotations. |
| **S7** | W4 Guards | Create orphan `vk-ghost` (with guest label); create `vk-ghost2` (no guest label); create `vk-h2` (`h2` exists). | `vk-ghost` deleted within 15s. `vk-ghost2` and `vk-h2` retained after 15s. | Proves W4 orphan deletion fires only when guest label is present and host is genuinely absent (false-positive prevention). |
| **S8** | Host Death (W4) | Create `vk-h3`; delete fake host Node `h3`. | `vk-h3` deleted by controller within 20s. | Validates host death handling: virtual node is cleaned up immediately when the real host node is deleted. |
| **S9** | SSA Create Guard | Server-side apply with field manager `e2e-probe` of nonexistent Node carrying `metadata.uid`. | API server rejects apply with `uid mismatch`; node is not created. | Regression guard for SSA upsert vulnerability where stale cache could recreate dead host nodes. |
| **S10** | Dry Run | Patch deployment with `--dry-run`; create donor on `h2`; wait 15s; toggle dry-run off. | No donor label on `h2` during dry run; sharing occurs promptly when dry-run disabled; clean unshare afterwards. | Verifies `--dry-run` flag completely inhibits live mutations while logging intentions. |
| **S11** | Ownership | Share `h1` with `g1` and `g2`; inspect `managedFields`. | `timeslice-donor-controller` owns exactly `f:metadata.f:labels` (`donor`, `g1`, `g2`). No other fields owned under `f:metadata` (except `idle-since` if idle); no `f:spec` owned. | Validates SSA hygiene: controller owns only its designated fields and never claims foreign labels or specs. |
| **S12** | Foreign Taint Preservation | Add `e2e/foreign=x:NoSchedule` to `h2`; share with donor; unshare. | `e2e/foreign` taint present continuously through both share and unshare. | Proves read-modify-write taint patching preserves foreign/system taints across lifecycle transitions. |
| **S13** | Events Audit Trail | Inspect Kubernetes Events in namespace `default` for fake host and virtual node. | `Applied` event with rules `[W1/W2]` and `Tainted`/`Untainted` events exist on host Node; `Deleted` event with rule `W4` exists on `vk-h3`. | Validates that all controller mutations emit standard Kubernetes Events documenting rules and actions. |
| **S14** | Leader Election | Inspect `coordination.k8s.io` Lease `donor-controller.timeslice.io` in controller namespace. | Lease exists, `holderIdentity` is non-empty and matches a running controller pod. | Verifies high-availability leader election lease acquisition and active pod assignment. |
| **FinalCheck** | RBAC & Health | Inspect controller pod logs. | 0 occurrences of `forbidden`; no host key reconciled > 50 times per minute; 0 "Reconciler error" lines; error-level counts reported. | Ensures no RBAC permission gaps, absence of hot loops, and clean reconciler execution. |

## Running the Suite

### From a Workstation (Development)

Prerequisites: active `KUBECONFIG` pointing to the target cluster where the donor-controller chart is deployed.

```bash
export KUBECONFIG=/path/to/kubeconfig

# Run the entire suite
go test -tags=integration -count=1 -v -timeout 30m -run '^TestDonorController$' ./tests/integration/donor-controller/

# Run a specific scenario (e.g. S1 Share)
go test -tags=integration -count=1 -v -run '^TestDonorController/S1_Share$' ./tests/integration/donor-controller/
```

### In-Cluster (via `run.sh`)

`run.sh` installs the Helm fixture into `timeslice-system`, starts a test-runner pod, copies workspace source into it, and executes the suite in-cluster:

```bash
./tests/integration/run.sh --phase donor --donor-image gcr.io/gke-gkit-dev/diamondburned/donor-controller:df49323-rulekit
```

To run both snapshot-agent, orchestrator, and donor controller:

```bash
./tests/integration/run.sh --phase all --donor-image <donor-image> --agent-image <agent-image> --orch-image <orch-image>
```

## Not Covered

1. **Real Host Death via Cloud Provider VM Removal**:
   Simulated via `Node` object deletion. Real GKE host removal was manually tested on MIG `dburned-donor-e2e`:
   - GKE took **112 seconds** to detect instance termination and delete the Kubernetes `Node` object.
   - The donor controller deleted `vk-<host>` **0.37 seconds** after the `Node` object disappeared.
2. **Real Guest Kubelet (`virtual-kubelet`) Execution**:
   Simulated via fake virtual node objects with `timeslice.io/guest=true` and `spec.providerID`.
3. **Real Accelerated Workloads**:
   The suite uses pause containers (`registry.k8s.io/pause:3.10`). Actual GPU/TPU slicing workloads are exercised in the `snapshot-agent` and `orchestrator` suites.
