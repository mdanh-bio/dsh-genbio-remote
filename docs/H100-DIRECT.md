# Constrained genbioh100 direct tools

The generic schema-v2 project surface remains Slurm/HPC-oriented. This plugin also contains fixed-surface `genbioh100` adapters for approved direct-host workloads where no Slurm scheduler is available.

## Direct projects

Direct-project manifests are stored outside the package in `h100DirectProjectsDir`. The adapter exposes:

- `genbio_h100_direct_stage`
- `genbio_h100_direct_job`
- `genbio_h100_direct_status`
- `genbio_h100_direct_fetch`

The direct layer is deliberately CPU-only. It rejects nonzero GPU requests, validates CPU and memory limits against the active `genbioh100` policy and session envelope, stages allowlisted files with rclone and SHA-256 verification, and launches one detached clean-environment runner. Run identity is bound to a high-entropy token plus PID/PGID evidence. Transport or identity ambiguity becomes `reconciling`; it never authorizes an automatic second launch.

## AI.zymes verification surface

`genbio_aizyme_h100_stage2` and `genbio_aizyme_h100_status` retain a fixed verification workflow with GPU 0 binding, CPU/thread/memory constraints, immutable input checks, bounded evidence, and durable run-registry pair locking. GPU 1 and the protected `gpu_util` process remain outside the permitted surface.

## Curated mirror

The mirror adapter reads one external `h100MirrorManifestPath` and exposes:

- `genbio_h100_mirror_plan`
- `genbio_h100_mirror_execute`
- `genbio_h100_mirror_status`

Planning inventories the allowlisted source and binds it to an immutable hash. Execution rechecks drift, copies through rclone into a temporary destination, verifies content, writes a receipt, and atomically promotes only when the final destination does not already exist.

## Configuration

Use `cordis.patch.example.yml` for these installation-specific paths:

```yaml
runRegistryDir: /absolute/path/to/genbio-run-registry
h100DirectProjectsDir: /absolute/path/to/genbio-h100-direct-projects
h100MirrorManifestPath: /absolute/path/to/genbio-h100-mirror/mirror-manifest.yaml
aizymeLocalRemoteBundle: /absolute/path/to/aizyme_v1/remote
```

The manifests, policy, rclone configuration, and credentials stay outside the package. Installing these tools does not authorize a remote launch.
