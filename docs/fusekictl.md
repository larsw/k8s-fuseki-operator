# fusekictl

`fusekictl` is the first M6 command-line entry point for installing the operator, inspecting cluster state, and triggering an on-demand RDF Delta backup.

## Build

```sh
make build-fusekictl
```

Or run it directly from source:

```sh
make run-fusekictl ARGS='version'
```

## Install The Operator

The current install flow uses the repository's `config/default` kustomize bundle. `kubectl` must be available in `PATH`.

```sh
./bin/fusekictl install
```

If you are not running the command from the repository, point it at the bundle explicitly:

```sh
./bin/fusekictl install --kustomize-dir /path/to/fuseki-operator/config/default
```

The install bundle creates the `fuseki-system` namespace, installs the CRDs, service account, RBAC binding, deployment, and metrics service.

Remove the operator with:

```sh
./bin/fusekictl uninstall
```

## Show Status

Inspect the operator plus all Fuseki custom resources:

```sh
./bin/fusekictl status
```

Limit the report to one namespace or emit JSON:

```sh
./bin/fusekictl status -n default
./bin/fusekictl status -o json
```

## Apply Resources

Apply the core custom resources directly from typed CLI flags. The command creates the object if it does not exist and updates its `spec` if it already exists:

```sh
./bin/fusekictl apply rdfdeltaserver example-delta --image ghcr.io/larsw/k8s-fuseki-operator/rdf-delta:latest -n default
./bin/fusekictl apply dataset example-dataset --dataset-name primary --spatial -n default
./bin/fusekictl apply fusekicluster example --image ghcr.io/larsw/k8s-fuseki-operator/fuseki:6.0.0 --rdf-delta-server example-delta --dataset example-dataset -n default
./bin/fusekictl apply restore example-restore --target example-delta --backup-object 20260308T120000Z-example-delta -n default
```

## Create Resources

Create the core custom resources directly from typed CLI flags:

```sh
./bin/fusekictl create rdfdeltaserver example-delta --image ghcr.io/larsw/k8s-fuseki-operator/rdf-delta:latest -n default
./bin/fusekictl create dataset example-dataset --dataset-name primary --spatial -n default
./bin/fusekictl create fusekicluster example --image ghcr.io/larsw/k8s-fuseki-operator/fuseki:6.0.0 --rdf-delta-server example-delta --dataset example-dataset -n default
```

Delete those resources with the matching typed delete commands:

```sh
./bin/fusekictl delete fusekicluster example -n default
./bin/fusekictl delete dataset example-dataset -n default
./bin/fusekictl delete rdfdeltaserver example-delta -n default
```

## Trigger A Backup

Create a manual backup job from the RDF Delta server's managed backup CronJob:

```sh
./bin/fusekictl backup trigger example-delta -n default
```

Use `--wait=false` to return immediately after the Job is created.

## Trigger A Restore

Create a restore request for an RDF Delta server and optionally wait for completion. While waiting, the CLI now prints phase changes plus the controller reason/message it is currently acting on:

```sh
./bin/fusekictl restore trigger example-delta -n default --backup-object 20260308T120000Z-example-delta
```

Typical progress output looks like:

```text
restorerequest example-delta-restore-abcde phase=Pending target=example-delta backup=20260308T120000Z-example-delta reason=ScalingDown message="Waiting for RDFDeltaServer \"example-delta\" to scale down before restore."
restorerequest example-delta-restore-abcde phase=Running target=example-delta backup=20260308T120000Z-example-delta job=example-delta-restore-abcde-restore reason=RestoreJobRunning message="Restore Job \"example-delta-restore-abcde-restore\" is still running."
restorerequest example-delta-restore-abcde phase=Succeeded target=example-delta backup=20260308T120000Z-example-delta job=example-delta-restore-abcde-restore reason=RestoreCompleted message="Restore Job \"example-delta-restore-abcde-restore\" completed successfully."
```

If a restore fails, the CLI exits with the reported controller reason and message instead of a generic failure string.

Inspect an existing restore request without creating a new one:

```sh
./bin/fusekictl restore describe example-restore -n default
./bin/fusekictl restore describe example-restore -n default -o json
```

The describe output includes the current restore conditions plus any associated restore Job state.

Stream restore logs from the associated Job pod:

```sh
./bin/fusekictl restore logs example-restore -n default
./bin/fusekictl restore logs example-restore -n default --follow
```

If a Job produced multiple Pods, `fusekictl` picks the best candidate automatically, or you can force a specific Pod with `--pod`.

If you need to remove a request after inspection:

```sh
./bin/fusekictl delete restore example-delta-restore-abcde -n default
```
