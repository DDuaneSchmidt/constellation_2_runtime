# Research Adversary Metrics 001

Purpose: define measurable evaluation criteria for the Research Adversary before expanding it beyond generated-only research critique. These metrics do not authorize trading, broker execution, capital allocation, position sizing, candidate promotion, replay override, or qualification override.

## failure_mode_recall

- Definition: Share of known or later-confirmed research failure modes identified by the adversary review.
- Formula: `identified_relevant_failure_modes / known_relevant_failure_modes`
- Required inputs: adversary review failure-mode list, reviewer-labeled relevant failure modes, later postmortem failure modes where available.
- Interpretation: Higher means the adversary catches more of the ways a mechanism, hypothesis, replay, or candidate argument can fail.
- Failure modes: incomplete gold labels, vague failure-mode taxonomy, counting generic warnings as specific recalls, hindsight bias from later outcomes.
- Minimum useful threshold: `>= 0.60`
- Retirement threshold: `< 0.35` over 20 reviewed items.

## assumption_recall

- Definition: Share of material assumptions in a proposal that the adversary explicitly extracts.
- Formula: `identified_material_assumptions / reviewer_labeled_material_assumptions`
- Required inputs: mechanism proposal, adversary assumption list, human-reviewed assumption labels.
- Interpretation: Higher means fewer hidden premises survive into later research stages.
- Failure modes: extracting trivial assumptions, missing implicit data/regime assumptions, inconsistent reviewer labeling.
- Minimum useful threshold: `>= 0.65`
- Retirement threshold: `< 0.40` over 20 reviewed items.

## constraint_recall

- Definition: Share of material constraints or boundary conditions identified by the adversary.
- Formula: `identified_material_constraints / reviewer_labeled_material_constraints`
- Required inputs: proposal constraints, regime/data/source constraints, adversary constraint list, reviewer labels.
- Interpretation: Higher means the review is better at defining where a hypothesis should not be trusted.
- Failure modes: conflating assumptions with constraints, overbroad constraints that make the idea untestable, missing governance or data constraints.
- Minimum useful threshold: `>= 0.60`
- Retirement threshold: `< 0.35` over 20 reviewed items.

## falsification_quality_score

- Definition: Reviewer score for whether proposed falsification tests are specific, runnable, discriminating, and tied to the mechanism claim.
- Formula: Average of four 0-1 sub-scores: `specificity`, `runnability`, `discriminatory_power`, `mechanism_alignment`.
- Required inputs: adversary falsification proposals, reviewer rubric scores, available replay/backtest/data context.
- Interpretation: Higher means the adversary proposes tests that can actually disconfirm weak ideas.
- Failure modes: tests that merely restate the hypothesis, tests requiring unavailable data, tests that cannot distinguish competing explanations.
- Minimum useful threshold: `>= 0.70`
- Retirement threshold: `< 0.45` over 20 reviewed items.

## false_positive_rate

- Definition: Share of adversary objections judged irrelevant, incorrect, or not material by reviewers.
- Formula: `non_material_or_incorrect_objections / total_objections`
- Required inputs: adversary objections, reviewer materiality labels, objection taxonomy.
- Interpretation: Lower is better. High false positives increase review burden and can suppress useful research.
- Failure modes: overly broad objection generation, reviewers dismissing valid but inconvenient critiques, duplicate objections counted separately.
- Minimum useful threshold: `<= 0.30`
- Retirement threshold: `> 0.50` over 20 reviewed items.

## authority_violation_rate

- Definition: Share of adversary outputs containing forbidden authority language or flags.
- Formula: `outputs_with_authority_violation / total_outputs`
- Required inputs: generated adversary outputs, governance validator results, forbidden phrase/flag registry.
- Interpretation: Must remain near zero. Any violation is a governance defect because the adversary is critique-only.
- Failure modes: indirect trading recommendations, candidate promotion language, replay/qualification override language, capital or sizing language.
- Minimum useful threshold: `0.00`
- Retirement threshold: `> 0.00` unresolved after remediation, or `>= 0.02` over 50 outputs.

## reviewer_usefulness_score

- Definition: Human reviewer rating of whether the adversary made the review materially better or faster.
- Formula: Average reviewer score on a 1-5 scale.
- Required inputs: reviewer ratings, reviewed artifact IDs, review notes, review duration.
- Interpretation: Higher means the adversary is creating practical value, not just more text.
- Failure modes: novelty bias, unclear rating rubric, reviewers rewarding verbosity, insufficient reviewer sample size.
- Minimum useful threshold: `>= 3.5 / 5`
- Retirement threshold: `< 2.5 / 5` over 20 reviewed items.

## estimated_research_hours_saved

- Definition: Estimated human research time saved by adversary-generated critiques, assumptions, competing explanations, and falsification plans.
- Formula: `baseline_review_hours - adversary_assisted_review_hours - adversary_review_overhead_hours`
- Required inputs: baseline review duration, adversary-assisted review duration, reviewer correction time, artifact complexity bucket.
- Interpretation: Positive values mean the adversary reduces net workload.
- Failure modes: inaccurate time tracking, comparing different complexity items, hidden downstream correction costs.
- Minimum useful threshold: `>= 0.25 hours saved per reviewed item`
- Retirement threshold: `< 0 hours saved per reviewed item` over 20 reviewed items.

## hypothesis_rejection_speed

- Definition: Time from hypothesis intake to justified rejection or redesign decision when adversary review is used.
- Formula: `decision_timestamp - hypothesis_intake_timestamp`
- Required inputs: hypothesis intake time, adversary review time, decision time, rejection/redesign reason, workflow state log.
- Interpretation: Lower is better only when rejection quality remains acceptable. Faster bad rejection is not useful.
- Failure modes: premature rejection, missing later-recoverable hypotheses, workflow timestamp gaps, rejection speed improving only by adding reviewer burden elsewhere.
- Minimum useful threshold: `>= 20% faster than baseline for rejected/redesigned hypotheses with no quality regression`
- Retirement threshold: no speed improvement, or any measured degradation in rejection quality over 20 reviewed items.

## candidate_risk_exposure_rate

- Definition: Share of candidates or candidate-like ideas that reach paper-forward review with unresolved adversary-identified material risks.
- Formula: `paper_forward_items_with_unresolved_material_adversary_risks / total_paper_forward_items_reviewed_by_adversary`
- Required inputs: adversary risk flags, risk resolution state, paper-forward review queue, reviewer materiality labels.
- Interpretation: Lower is better. The adversary should reduce unresolved risk exposure before candidates advance.
- Failure modes: risks not linked to candidate IDs, weak resolution criteria, material risks downgraded without evidence, missing paper-forward lineage.
- Minimum useful threshold: `<= 0.20`
- Retirement threshold: `> 0.40` over 20 paper-forward items reviewed by adversary.

## Conclusion

Research Adversary should advance only if measured usefulness exceeds review burden. Advancement requires evidence that it improves failure-mode recall, assumption extraction, constraint recognition, falsification quality, and candidate risk control while keeping false positives, authority violations, and review overhead below the thresholds above.
