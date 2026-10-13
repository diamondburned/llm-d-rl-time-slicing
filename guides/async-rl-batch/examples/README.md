# A tiny async RL job on two L4s, end to end

This walkthrough uses the platform from
[#208](https://github.com/llm-d-incubation/llm-d-rl-time-slicing/pull/208) and the guide in
[#209](https://github.com/llm-d-incubation/llm-d-rl-time-slicing/pull/209), applied to a real
job you can run in about an hour. It runs:

- a verl fully-async RL job, Qwen2.5-0.5B-Instruct on a 256-example GSM8K subset;
- a vLLM batch server that borrows the trainer's L4 while the trainer waits for samples;
- a load generator, so you can watch the batch server being lent and reclaimed.

#209 explains *what* to change in your own RayJob. This guide gives you a complete one, a
cluster to run it on, and what to look at.

> [!NOTE]
> Last run end to end on 2026-10-12. Stack:
> - #208 at `9f82ea2`, built into `gcr.io/gke-gkit-dev/diamondburned/*:pr208-9f82ea2`;
> - GKE 1.36.4 in us-east7-b;
> - KubeRay 1.7.1.
>
> Three #208 bugs showed up along the way (see [Findings](#findings-for-208)). One of them needs a
> patched guest kubelet on GKE 1.36 ([step 3](#3-build-the-images)).

Files in this directory:

| File | What it is |
| --- | --- |
| [cloudbuild-pr208.yaml](cloudbuild-pr208.yaml) | Builds all eight platform images in parallel |
| [guest-kubelet-status-fix.patch](guest-kubelet-status-fix.patch) and [cloudbuild-guest-kubelet.yaml](cloudbuild-guest-kubelet.yaml) | The guest kubelet fix for GKE 1.36, and a build for that one image |
| [values-gcr.yaml](values-gcr.yaml) | Points #208's chart at your images |
| [namespace.yaml](namespace.yaml) | `rl-demo` |
| [rayjob-tiny.yaml](rayjob-tiny.yaml) | The RL job: #209's three changes, plus what an L4-sized run needs |
| [guest-vllm.yaml](guest-vllm.yaml) | The batch server (guest) and its Service |
| [load-generator.yaml](load-generator.yaml) | Sends requests continuously and logs completions every 5s |
| [values-gcr-appaware.yaml](values-gcr-appaware.yaml), [rayjob-appaware.yaml](rayjob-appaware.yaml), [guest-vllm-appaware.yaml](guest-vllm-appaware.yaml) | The application-aware variant ([step 9](#9-variant-application-aware-suspend-faster-lend-and-reclaim)) |

---

## 1. What you need

- **A GCP project** with GKE, Compute and Cloud Build enabled.
- **Quota:** 2 `NVIDIA_L4_GPUS` in one region, plus about 30 vCPUs.
- **Tools:** `gcloud`, `kubectl`, Helm 3.14+, and a checkout of #208:

  ```bash
  git fetch origin pull/208/head && git checkout FETCH_HEAD
  ```

Set these once per shell:

```bash
export PROJECT=my-project ZONE=us-east7-b CLUSTER=${USER}-rl-l4
export REGISTRY=gcr.io/${PROJECT}/timeslice TAG=pr208-$(git rev-parse --short HEAD)
export EX=<path to this examples directory>
```

> [!TIP]
> If your gcloud config has a `billing/quota_project` other than `$PROJECT`, add
> `--billing-project $PROJECT` to the `gcloud builds` commands. Otherwise Cloud Build checks
> the API in the wrong project.

## 2. Find L4 capacity first

L4s stock out often. During this run, every zone in us-central1 was out of capacity for both
on-demand and Spot. Probe before you create anything. The
[find_machine_capacity](https://source.corp.google.com/piper///depot/google3/experimental/users/jesschen/find_machine_capacity/SKILL.md)
skill's prober creates one VM per zone, records whether it started, and deletes it:

```bash
python3 /google/src/files/head/depot/google3/experimental/users/jesschen/find_machine_capacity/scripts/discover_and_probe.py \
  --project "$PROJECT" --machine-type g2-standard-16 --region-filter '^us-' --models STANDARD SPOT
```

Pick a zone that reports `AVAILABLE` for `STANDARD`. Check two regional quotas there:

- `NVIDIA_L4_GPUS`.
- `SSD_TOTAL_GB`: us-east7 has only 500 GB, which matters in [step 4](#4-create-the-cluster).

```bash
gcloud compute regions describe ${ZONE%-*} --project "$PROJECT" --format=json \
  | jq -r '.quotas[] | select(.metric|test("NVIDIA_L4_GPUS|SSD_TOTAL_GB|^CPUS$")) | "\(.metric) \(.usage)/\(.limit)"'
```

## 3. Build the images

From the #208 checkout, apply the guest kubelet fix, then build all eight images. The verl
image is about 12 GB, and the build took about 6 minutes on `E2_HIGHCPU_32`.

```bash
git apply $EX/guest-kubelet-status-fix.patch
sed "s#gcr.io/\$PROJECT_ID/diamondburned#${REGISTRY}#g" $EX/cloudbuild-pr208.yaml > /tmp/cloudbuild.yaml
gcloud builds submit --project "$PROJECT" --config /tmp/cloudbuild.yaml --substitutions=_TAG=$TAG .
```

**Why the patch.** The guest kubelet copies the mirror pod's status onto the guest pod,
including `allocatedResourcesStatus`, which names `timeslice.io/gpu-shadow`. GKE 1.36's API
server rejects that, because the guest container requests `nvidia.com/gpu`:

```text
Pod "guest-vllm-..." is invalid: status.containerStatuses[0].allocatedResourcesStatus[0].name:
Invalid value: "timeslice.io/gpu-shadow": must match one of the container's resource requests
```

Without the patch, the guest is served by its mirror but never turns Ready, so its Service has
no endpoints.

## 4. Create the cluster

You need:

- a CPU pool for the platform, the Ray head and the load generator;
- one L4 node for the trainer, with enough host memory for the trainer's GPU checkpoint;
- one L4 node for rollout.

```bash
gcloud container clusters create "$CLUSTER" --project "$PROJECT" --location "$ZONE" \
  --release-channel regular --machine-type e2-standard-8 --num-nodes 2 --disk-size 200

G="--project $PROJECT --cluster $CLUSTER --location $ZONE"
gcloud container node-pools create l4-trainer $G --machine-type g2-standard-16 \
  --accelerator type=nvidia-l4,count=1,gpu-driver-version=latest --num-nodes 1 \
  --disk-type pd-standard --disk-size 100 --ephemeral-storage-local-ssd count=1
gcloud container node-pools create l4-rollout $G --machine-type g2-standard-8 \
  --accelerator type=nvidia-l4,count=1,gpu-driver-version=latest --num-nodes 1 \
  --disk-type pd-standard --disk-size 100 --ephemeral-storage-local-ssd count=1
```

> [!IMPORTANT]
> Keep `--ephemeral-storage-local-ssd count=1` on the GPU pools. It puts container images on
> the VM's local NVMe SSD.
>
> Our first attempt used 200 GB `pd-standard` boot disks, because the 500 GB `SSD_TOTAL_GB`
> quota ruled out `pd-balanced`. Unpacking the verl image (12 GB) and the vLLM image (10.7 GB)
> saturated that disk. The kernel's writeback stayed in D-state for 35 minutes and wedged
> containerd: a container that had already printed its last line could neither exit nor be
> killed. Local SSD doesn't count against `SSD_TOTAL_GB`.

The GKE-managed driver (`gpu-driver-version=latest`) installs under `/home/kubernetes/bin/nvidia`,
which is where #208's snapshot agent expects it. The nodes advertise `nvidia.com/gpu` a few
minutes after they turn Ready.

## 5. Install KubeRay and the platform

```bash
helm install kuberay-operator kuberay-operator --repo https://ray-project.github.io/kuberay-helm/ \
  -n kuberay-system --create-namespace

sed "s#gcr.io/gke-gkit-dev/diamondburned#${REGISTRY}#; s#pr208-9f82ea2#${TAG}#" $EX/values-gcr.yaml > /tmp/values-gcr.yaml
cd deploy && helm dependency update .
helm install timeslice . -n timeslice-system --create-namespace -f /tmp/values-gcr.yaml \
  --set donor-controller.flags.era-ttl=3m
kubectl -n timeslice-system get pods -o wide
```

`era-ttl=3m` shortens how long a host stays a donor after its trainer leaves. The default is
15 minutes. That's reasonable in production but slow for a demo, and it also controls how long
a group conflict blocks the host ([Findings](#findings-for-208)).

You should see:

- on the CPU pool: the orchestrator, two webhook replicas and the donor controller;
- on both L4 nodes: the snapshot agent and the DCGM exporter.

The guest kubelet and the gpu-shadow plugin appear only once a trainer has made its host a
donor.

## 6. Run the job, the guest and the load

```bash
kubectl apply -f $EX/namespace.yaml
sed "s#\${VERL_IMAGE}#${REGISTRY}/verl:${TAG}#g" $EX/rayjob-tiny.yaml | kubectl apply -f -
kubectl apply -f $EX/guest-vllm.yaml -f $EX/load-generator.yaml
```

What [rayjob-tiny.yaml](rayjob-tiny.yaml) adds beyond #209's three changes, and why:

| Setting | Why |
| --- | --- |
| `timeslice.io/job-id: tiny-rl` and `timeslice.io/group: rl-demo.tiny-rl.trainer` on the trainer group | A stable identity across resubmits. Without them the webhook names the group after the RayCluster, and KubeRay gives every submission a new RayCluster name. A resubmitted job then gets a new group, which conflicts with the old group's label on the host until that era ends. Meanwhile the trainer's `Acquire` times out after 10 minutes |
| `num-cpus: "16"` on both worker groups | Ray's logical CPUs. verl's fully-async trainer actor asks for 10 CPUs, and the trainer's placement group for 3 more on the same node. The pod's real CPU request stays at 11 |
| Trainer CPU request `11` | The guest kubelet's virtual node offers what the host has left after the trainer's requests (`guest-budget: computed`). With 14 it offered 821m, and the guest couldn't schedule |
| GSM8K prep in an init container on every pod | Each pod reads the parquet from the same local path, with no shared storage. The prompt asks for the answer after `####`, and `ground_truth` is the bare number, which is what verl's gsm8k reward compares |
| Guest requests `500m` CPU and `8Gi` | Same reason as the trainer CPU: it must fit in what the virtual node offers |

## 7. Watch it work

Allow about 10 minutes for the first image pulls. Then:

```bash
# The trainer host becomes a donor of the job's group, gpu-shadow advertises its GPU,
# and the guest kubelet registers a virtual node next to it.
kubectl get nodes -o custom-columns='NAME:.metadata.name,GPU:.status.allocatable.nvidia\.com/gpu,SHADOW:.status.allocatable.timeslice\.io/gpu-shadow,DONOR:.metadata.labels.timeslice\.io/donor,GROUP:.metadata.labels.timeslice\.io/group,VIRTUAL:.metadata.labels.timeslice\.io/virtual-node'

# The guest runs as a mirror pod (<guest>-m) on the trainer host.
kubectl -n rl-demo get pods -o wide

# Lend and reclaim, from the guest kubelet.
kubectl -n timeslice-system logs ds/timeslice-guest-kubelet -f | grep 'host command'

# The trainer's lock, from the job.
kubectl -n rl-demo logs -f $(kubectl -n rl-demo get pods -o name | grep -E 'pod/tiny-rl-[a-z0-9]+$' | grep -v head) | grep -E 'ACQUIRE|RELEASE|step:'

# Batch throughput.
kubectl -n rl-demo logs -f deploy/batch-load-generator
```

What one cycle looked like in our run:

| Time (UTC) | Event |
| --- | --- |
| 20:40:57 | `COMMAND_RESUME`: the idle trainer GPU is lent and the guest serves. The load generator shows `completed=13` every 5s |
| 20:43:21 | The trainer needs the GPU, so `COMMAND_VACATE` runs. The snapshot agent suspends vLLM (`SuspendAll`, 9.1s) |
| 20:43:32 | `[timeslice] job=tiny-rl ACQUIRE ... waited=11000ms context_restored=True`, then `step:0` |
| ~20:43:41 | `RELEASE ... snapshot_deferred=True` |
| 20:43:44 | `COMMAND_RESUME` (`ResumeAll`, 3.3s) and `guest Ready`. The guest was out of service for about 23s, then the load generator was back to 13 per 5s |

This job is tiny: `rollout.total_rollout_steps=32` gives one training step, so you see one
reclaim. Raise it, to 256 for example, to watch several cycles.

## 8. End the era

When the job is done, delete it:

```bash
kubectl -n rl-demo delete rayjob tiny-rl
```

Then:

1. **Right away:** the guest kubelet cordons the virtual node, and the guest goes back to
   `Pending`. Guests run only on lent time.
2. **After `era-ttl` (3 minutes):** the donor controller starts the era end. It waits up to 1
   minute for the virtual node to deregister. In practice that wait always expires (`ended_by=grace`),
   because the guest kubelet only leaves when its DaemonSet stops selecting the host.
3. **Then:** it removes `timeslice.io/donor` and `timeslice.io/group`. The guest kubelet and
   gpu-shadow pods leave the host, and the virtual node goes with them.

## 9. Variant: application-aware suspend (faster lend and reclaim)

[Edwinhr716's `ws/slim/appaware-trainer`](https://github.com/Edwinhr716/llm-d-rl-time-slicing/tree/ws/slim/appaware-trainer)
(`cc29903`) is #208 plus two opt-in suspend paths that replace `cuda-checkpoint`:

- **Trainer, `timeslice.io/backend: app_channel` on the trainer group.** The verl plugin
  registers with the node's snapshot agent. When told to park, it moves its optimizer, model and
  gradients to host RAM through verl's offload control, and moves them back to restore. The
  agent lends the GPU only if the trainer keeps at most 4 GiB on it. The agent also won't lend
  before the trainer has registered, so the GPU is no longer lent before the trainer has started.
- **Guest, `timeslice.io/backend: app_endpoint` on the guest.** Start vLLM with
  `--enable-sleep-mode` and `VLLM_SERVER_DEV_MODE=1`. On a reclaim the agent calls `POST /sleep`
  (weights to host RAM, KV cache dropped), and on a lend `POST /wake_up`.

Same cluster, same tiny job, last run 2026-10-12:

| | #208 (`cuda-checkpoint`) | `cc29903` (app-aware) |
| --- | --- | --- |
| Reclaim (`COMMAND_VACATE`) | 9.4s | 1.5–2.4s |
| Trainer waits in `Acquire` for its GPU | 11s | 3–4s |
| Trainer park / restore | n/a | 0.5–2.9s / 0.51–0.55s (3 cycles) |
| Lend (`COMMAND_RESUME`, frozen guest to Ready) | 3.7s | 0.62s |
| Guest out of service per reclaim (vacate start to guest Ready, including the trainer's step) | 23s | 10s |

To run this variant, change four things compared with steps 3 to 6:

1. **Build** from that branch, applying the same guest kubelet patch. The branch has no
   `docker/verl`, so copy `docker/verl/` from #208. It installs the checkout's own timeslice
   client and verl plugin, which is what `app_channel` needs.

   ```bash
   git fetch https://github.com/Edwinhr716/llm-d-rl-time-slicing.git ws/slim/appaware-trainer && git checkout FETCH_HEAD
   git apply $EX/guest-kubelet-status-fix.patch
   git fetch https://github.com/llm-d-incubation/llm-d-rl-time-slicing.git pull/208/head
   git checkout FETCH_HEAD -- docker/verl
   gcloud builds submit --project "$PROJECT" --config /tmp/cloudbuild.yaml --substitutions=_TAG=appaware-$(git rev-parse --short HEAD) .
   ```

2. **Install** with [values-gcr-appaware.yaml](values-gcr-appaware.yaml). This chart has no
   `global.imageTag`, so the file sets every component's tag. It also turns off the webhook's
   package injection (`timeslice-webhook.rlIntegration.enabled=false`), because the verl image
   already carries the packages. It sets `era-ttl` to 3m too.
3. **Run** [rayjob-appaware.yaml](rayjob-appaware.yaml). It's the tiny job plus the
   `app_channel` annotation on the trainer group, and `total_rollout_steps=160`, which gives 3
   training steps and so 3 reclaims.
4. **Serve** with [guest-vllm-appaware.yaml](guest-vllm-appaware.yaml): the `app_endpoint`
   annotation, sleep mode, and a `/health` readiness probe.

In the trainer's log, look for:

```text
[timeslice] app-park: workload of job=tiny-rl group=rl-demo.tiny-rl.trainer registered with agents ['10.196.15.222:9001']
[timeslice] app-park took=0.530s parks=2 caches=[...]
[timeslice] app-restore source=agent took=0.507s restores=2
[timeslice] job=tiny-rl ACQUIRE group=rl-demo.tiny-rl.trainer waited=3001ms context_restored=True
```

Count parks and restores from the trainer's own `parks=` and `restores=` counters, not from
grepped lines, which repeat if two log streams overlap.

Two more things to expect:

- **The first lend is a cold start.** A new guest's mirror needed 2 minutes to load the model and
  report Ready. Later lends wake it from sleep.
- **No lend after the driver exits.** Once the RayJob's driver has finished, the guest stays
  `Suspended` until the era ends: with no registered trainer to park, the agent fails closed and
  doesn't lend.

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| GPU workers `Pending: Insufficient nvidia.com/gpu` right after the pools are created | The GKE driver installer is still running. Wait a few minutes |
| A worker sits in `Init` long after its image is pulled; `kubectl exec` hangs; deleting it leaves it `Terminating` | The boot disk is saturated (see [step 4](#4-create-the-cluster)). Recreate the GPU pools with local SSD ephemeral storage |
| Ray autoscaler: `No available node types can fulfill placement group requests ... {'trainer_node': 1.0, 'GPU': 1.0, 'CPU': 3.0}` | The trainer group's Ray `num-cpus` is too low. It needs at least 13 |
| Guest `Pending` with `Insufficient cpu` on the virtual node | Its requests exceed what the host has left after the trainer. Lower the guest's requests or the trainer's |
| Guest's mirror pod runs, but the guest never turns Ready; the guest kubelet logs `allocatedResourcesStatus ... must match one of the container's resource requests` | You're on GKE 1.36 without the guest kubelet patch from [step 3](#3-build-the-images) |
| Trainer fails with `OrchestratorTimeoutError: ... Deadline Exceeded` in `acquire`; the donor controller logs `group conflict` | The host is labeled for an older group, usually after a resubmit without a stable `timeslice.io/group`. Wait out `era-ttl`, then resubmit with explicit labels |
| The host stays a donor long after the job is gone; the donor controller logs `held=lock-active` | The orchestrator left the old group in `STATE_VACATING` (see [Findings](#findings-for-208)). `kubectl -n timeslice-system rollout restart deploy/timeslice-timesliceorchestrator` clears it |
| The trainer's memory limit is far above the node's memory (`timeslice.io/donor-memory-raised: 80Gi/gpu`) | The webhook assumed an 80 GB GPU. It doesn't matter for scheduling, since only requests count, but it's wrong for L4 |

## Findings for #208

| # | Component | What happened | Suggested fix |
| --- | --- | --- | --- |
| 1 | guest kubelet | On GKE 1.36, copying the mirror's `allocatedResourcesStatus` onto the guest is rejected, so the guest never turns Ready | Drop the mirror's per-container resource fields when translating status (`guest-kubelet-status-fix.patch`) |
| 2 | webhook / donor controller | The derived group name includes the RayCluster name, which KubeRay suffixes randomly per submission. A resubmitted job's group conflicts with the previous era's label (first group wins), and its trainer's `Acquire` times out | Derive the job id from the RayJob (the RayCluster's owner) rather than the RayCluster |
| 3 | orchestrator | A job deleted while its group was `STATE_VACATING` left the group stuck there (`activeJob` pointing at the dead job). The donor controller counts VACATING as lock activity and held the host indefinitely | Clear or abort a vacate whose job has no pods left |
| 4 | webhook | Raised the trainer's memory limit by `80Gi/gpu` on an L4 (24 GB) node | Use the node's GPU model, or document the default |
| 5 | orchestrator | Keeps re-enqueueing a deleted group, logging `failed to get group ... resource not found` every 30s | Drop work items for deleted groups |

## Clean up

```bash
gcloud container clusters delete "$CLUSTER" --project "$PROJECT" --location "$ZONE" --quiet
```
