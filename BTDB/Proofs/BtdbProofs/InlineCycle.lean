import BtdbProofs.DictionaryFree

/-!
# Writing inline objects ends, refusing a reference cycle

ObjectDB writes an object stored inline inside the one holding it, through `ObjectDBTransaction.WriteInlineObject`
(`../ODBLayer/`). Each inline object gets a writer context of its own, whose reference tracking never sees the objects
enclosing it, so before the 2026-10-28 fix an object containing itself was written inside itself until the stack
overflowed. The fix keeps the chain of inline objects being written and refuses an object already in it. Unlike freeing
dictionaries (`DictionaryFree.lean`) this cannot skip an object met before elsewhere: an object held in two places is
written in both. Objects are numbers below `n`, `children o` lists the inline objects `o` holds, fuel bounds the
nesting as the stack does, and a write gives `some true` when written, `some false` when refused.

* `guarded_ends`: with the check, writing any object ends within `n + 1` levels, written or refused.
* `refuses_self`: an object holding itself is refused.
* `shared_written`: an object held twice, with no cycle, is written.
* `unguarded_never_ends`: without the check, an object holding itself exhausts any nesting bound.
-/

namespace Btdb.InlineCycle

open Btdb.DictionaryFree (bounded_nodup_length)

variable (children : Nat → List Nat)

/-- Writing `o` inside the inline objects `chain`; children are written in order until one is refused. -/
def write : Nat → List Nat → Nat → Option Bool
  | 0, _, _ => none
  | fuel + 1, chain, o =>
    if o ∈ chain then some false
    else (children o).foldl (fun acc c => acc.bind fun ok => if ok then write fuel (o :: chain) c else some false)
      (some true)

/-- Writing before the fix. -/
def unguarded : Nat → Nat → Option Unit
  | 0, _ => none
  | fuel + 1, o => (children o).foldl (fun acc c => acc.bind fun _ => unguarded fuel c) (some ())

theorem fold_ends (step : Nat → Option Bool) :
    ∀ (cs : List Nat) (acc : Option Bool), (∀ c ∈ cs, step c ≠ none) → acc ≠ none →
      cs.foldl (fun acc c => acc.bind fun ok => if ok then step c else some false) acc ≠ none
  | [], _, _, hacc => hacc
  | c :: cs, acc, h, hacc => by
    apply fold_ends step cs _ (fun x hx => h x (by simp [hx]))
    cases acc with
    | none => exact absurd rfl hacc
    | some ok =>
      cases ok
      · simp
      · simpa using h c (by simp)

theorem write_ends (n : Nat) (hc : ∀ x, ∀ c ∈ children x, c < n) :
    ∀ fuel chain o, (∀ x ∈ chain, x < n) → chain.Nodup → o < n → n - chain.length < fuel →
      write children fuel chain o ≠ none
  | 0, _, _, _, _, _, h => absurd h (Nat.not_lt_zero _)
  | fuel + 1, chain, o, hb, hnd, ho, hfuel => by
    by_cases hin : o ∈ chain
    · simp [write, hin]
    · have hnd' : (o :: chain).Nodup := List.nodup_cons.mpr ⟨hin, hnd⟩
      have hb' : ∀ x ∈ o :: chain, x < n := by
        intro x hx
        simp only [List.mem_cons] at hx
        rcases hx with rfl | hx
        · exact ho
        · exact hb x hx
      have hlen := bounded_nodup_length n _ hnd' hb'
      simp only [List.length_cons] at hlen
      simp only [write, hin, ite_false]
      apply fold_ends _ (children o) (some true) _ (by simp)
      intro c hcc
      exact write_ends n hc fuel (o :: chain) c hb' hnd' (hc o c hcc) (by simp only [List.length_cons]; omega)

theorem guarded_ends (n : Nat) (hc : ∀ x, ∀ c ∈ children x, c < n) (o : Nat) (ho : o < n) :
    write children (n + 1) [] o ≠ none :=
  write_ends children n hc (n + 1) [] o (fun _ h => by simp at h) List.nodup_nil ho (by simp)

theorem refuses_self (o fuel : Nat) (hself : children o = [o]) : write children (fuel + 2) [] o = some false := by
  simp [write, hself]

/-- Object `0` holds object `1` twice; `1` holds nothing. -/
theorem shared_written : write (fun o => if o = 0 then [1, 1] else []) 3 [] 0 = some true := by decide

theorem unguarded_never_ends (o : Nat) (hself : children o = [o]) : ∀ fuel, unguarded children fuel o = none
  | 0 => rfl
  | fuel + 1 => by
    simp only [unguarded, hself, List.foldl, unguarded_never_ends o hself fuel]
    rfl

end Btdb.InlineCycle
