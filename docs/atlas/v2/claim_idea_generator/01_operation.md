# Operation

Run against an existing Atlas V2 ledger:

```bash
python3 -m ops.atlas.v2_claim_idea_generator --ledger-root /tmp/atlas_v2_claim_idea_generator_v1_ledgers --max-claims 10
```

The default `max_claims` is 10. V1 refuses values above 100. Generated claims with status `GENERATED` can be adapted into `ExternalStrategyClaim` payloads and passed to `ops.atlas.v2_autonomous_research_loop_runner` in mock or historical mode.
