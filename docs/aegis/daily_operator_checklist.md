# Aegis Daily Operator Checklist

This checklist is for HUMAN_REVIEWED_PAPER_MODE operation. It does not enable broker execution, autonomous execution, live trading, manual capture, promotion bypass, or trade advice unless runtime truth explicitly allows the relevant capability.

## Start Of Day

1. Run the runtime audit.

   ```bash
   npm run aegis:audit
   ```

2. Refresh governed market data inputs when the session requires current data.

   ```bash
   npm run aegis:refresh-market-data
   ```

3. Repair context readiness through the allowlisted command path.

   ```bash
   npm run aegis:repair-context-readiness
   ```

4. Re-run the tomorrow-readiness smoke before relying on the operator workflow.

   ```bash
   npm run aegis:tomorrow-smoke
   ```

## Candidate Review

5. Review candidate diagnostics, signal evidence, and candidate contracts.

   ```bash
   npm run aegis:candidate-diagnostics
   npm run aegis:signal-evidence-graph
   npm run aegis:candidate-contracts
   ```

6. Review paper candidates in the paper review queue.

   ```bash
   npm run aegis:paper:review-queue
   ```

7. Approve or reject paper candidates explicitly. Approval is for paper review only and must not be treated as broker, autonomous, or live-trading permission.

   ```bash
   npm run aegis:paper:review -- --candidate-id <candidate_id> --decision APPROVE --reason "<operator reason>"
   npm run aegis:paper:review -- --candidate-id <candidate_id> --decision REJECT --reason "<operator reason>"
   ```

## Paper Receipt

8. Record paper receipt evidence only after explicit human review.

   ```bash
   npm run aegis:paper:receipt -- --candidate-id <candidate_id> --decision-id <decision_id>
   ```

9. Rebuild the paper queue/outcomes after receipt capture.

   ```bash
   npm run aegis:paper:review-queue
   ```

## Safety Verification

10. Verify policy gates remain closed unless runtime truth explicitly says otherwise.

    ```bash
    npm run aegis:query -- "claim: trade advice allowed"
    npm run aegis:query -- "claim: broker submit/transmit"
    npm run aegis:query -- "claim: autonomous execution allowed"
    ```

Expected default state: trade advice is false, broker submit/transmit is disabled, autonomous execution is disabled, and live trading has no enabled path.
