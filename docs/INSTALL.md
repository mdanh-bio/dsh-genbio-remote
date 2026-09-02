# Installation and release

## Requirements

- DeepSeek Harness Desktop compatible with the `0.1.1-rc.2` DSH package line.
- Node.js 22 or newer for repository validation.
- A separately maintained Genbio compute policy.
- Native OpenSSH aliases and rclone SFTP remotes defined outside this repository.

## Configure

Copy `cordis.patch.example.yml` to the user-owned DSH profile and replace placeholder paths and rclone remote names. Do not commit credentials or SSH key material. The policy, central project manifests, run registries, and rclone configuration remain external.

## Validate

```bash
npm ci --ignore-scripts
npm run check
```

Repository CI performs only local syntax and test validation. It does not use SSH, rclone, Slurm, deployment credentials, or cluster secrets.

## Deploy

Deployment into DSH Desktop is a separate consequential operation. Review the tag and changelog, rebuild the plugin bundle through the existing DSH profile workflow, refresh the existing DSH Web GUI, and then perform a separately approved policy smoke test. Never treat a GitHub release as cluster authorization.

## Release checklist

1. Confirm `git status` is clean.
2. Run `git diff --check` and the credential/path scan.
3. Validate a clean checkout with `npm ci --ignore-scripts && npm run check`.
4. Verify GitHub Actions succeeds.
5. Update `CHANGELOG.md` and the package version.
6. Create a signed/reviewed tag such as `v0.2.0`.
7. Create the private GitHub release.
8. Request separate approval before installing or running remote tests.
