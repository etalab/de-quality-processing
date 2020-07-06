 # orchestrator

Get the latest modified datasets/resources from data.gouv.fr and sends a check to link-proxy.

Keeps tab on when the script has been run and which checks have been created.

## Usage

```
python orchestrator.py run
```

## Questions

- how are plunger/link-proxy temporary files handled? looks like it's filling up /tmp/ with discarded files.
