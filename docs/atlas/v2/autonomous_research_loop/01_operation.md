# Operation

Run the default fixture loop:

```bash
python3 -m ops.atlas.v2_autonomous_research_loop_runner --mode mock --max-loop-count 10
```

Run with supplied claim JSON in mock or historical mode:

```bash
python3 -m ops.atlas.v2_autonomous_research_loop_runner --mode historical --claims-file /path/to/claims.json
```

The output summary reports claims, hypotheses, specs, result records, ExperienceEvents, LearningEstimator records, and the LabelIntegrityReport id.
