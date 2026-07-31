# Atlas V2 Failure Conditions

Atlas V2 fails its mandate if it:

- Silently overwrites historical truth.
- Records state changes without timestamp, reason, and triggering object.
- Drops rejections or non-decisions.
- Treats unknown as failed.
- Creates or authorizes trading, sleeve, candidate, paper-position, validation, discovery, generation, or capital-allocation artifacts.
- Emits ExperienceEvent without Decision, Prediction, and Outcome links.
- Records BehaviorChange without a trigger and reason.
- Optimizes for idea count, hypothesis count, opportunity count, claim count, or source count.
