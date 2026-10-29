/-!
# Enumerating objects lists every object once

A model of `ObjectDBTransaction.Enumerate` (`../../ODBLayer/ObjectDBTransaction.cs`). It lists the oids stored in the
transaction, `stored`, in key order (a modified object comes from the transaction's cache), and then the dirty ones,
`dirty`, objects stored or created in the transaction, that are not stored: new objects not flushed yet. Before the
2026-10-29 fix it added the dirty ones above the last stored oid instead.

* `enumerate_complete`: every stored or dirty oid is listed.
* `enumerate_nodup`: none is listed twice.
* `old_misses`: the old rule misses a new object created before one flushed with `StoreAndFlush`
  (`ObjectDBTransactionCoverageTest.EnumerateIncludesDirtyObjectsBeforeFlushedOnes`).
-/

namespace Btdb.EnumerateDirty

def enumerate (stored dirty : List Nat) : List Nat := stored ++ dirty.filter (fun d => d ∉ stored)

def oldEnumerate (stored dirty : List Nat) : List Nat :=
  stored ++ dirty.filter (fun d => decide (stored.getLast?.getD 0 < d))

theorem enumerate_complete (stored dirty : List Nat) (o : Nat) (h : o ∈ stored ∨ o ∈ dirty) :
    o ∈ enumerate stored dirty := by
  unfold enumerate
  by_cases hs : o ∈ stored
  · exact List.mem_append.2 (Or.inl hs)
  · exact List.mem_append.2 (Or.inr (List.mem_filter.2 ⟨h.resolve_left hs, by simpa using hs⟩))

theorem enumerate_nodup (stored dirty : List Nat) (hs : stored.Nodup) (hd : dirty.Nodup) :
    (enumerate stored dirty).Nodup := by
  unfold enumerate
  refine List.nodup_append.2 ⟨hs, hd.filter _, fun a ha b hb => ?_⟩
  have := (List.mem_filter.1 hb).2
  intro hab
  subst hab
  simp_all

/-- Object 1 created and kept dirty, object 2 flushed. -/
theorem old_misses : 1 ∉ oldEnumerate [2] [1, 2] ∧ 1 ∈ enumerate [2] [1, 2] := by
  decide

end Btdb.EnumerateDirty
