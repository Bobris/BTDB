# Proofs

* [`PlannerLowerBound.lean`](PlannerLowerBound.lean): serving tenants in consumption order, as the BackendActions
  coordinator does, needs Ω(log n) comparisons per served tenant. It proves that a comparison-based procedure that
  serves every order of n tenants correctly makes at least log₂ n! comparisons in the worst case, and at least
  j·2^j for n = 2^(j+1). It also proves that naming one of n candidates, a worker or a class, needs log₂ n
  comparisons. The planner meets these bounds per step; see
  [coordinator cost](../BackendActionsPlan.md#coordinator-cost).

* [`PlannerSelection.lean`](PlannerSelection.lean): the indexed planner serves exactly the reference's plan. Taking
  the least startable lane by (starts in this plan, consumption, tenant, action) gives the reference's rounds, and
  the reference's next start is unique. Lazily repaired entries find the true least, and a cost fits some worker
  exactly when it is at most the largest room. See [correctness](../BackendActionsPlan.md#correctness).

Check a proof with Lean 4 (tested with 4.35); it needs no libraries:

```sh
lean Doc/proofs/PlannerLowerBound.lean
```

```sh
lean Doc/proofs/PlannerSelection.lean
```

No output means every theorem is proven. `#print axioms PlannerLowerBound.serving_needs_log` lists only Lean's
standard `propext`, `Classical.choice` and `Quot.sound`.
