/-!
# Choosing the newest manifest among listed objects

A model of how `LsmKeyValueDB.ReadNewestManifestAsync` and `ReadNewerManifestAsync` (`../LsmKeyValueDB.Recovery.cs`,
`../LsmKeyValueDB.Reader.cs`) pick the manifest to load from the objects listed under `manifest/`:

```csharp
if (TryParseObjectNumber(info.Key, out var sequence) && sequence > newest) { newest = sequence; manifestKey = info.Key; }
```

Each listed object is modelled by its number, or `none` when its name is not made of digits, like a `.DS_Store` a file
browser left there. The listing order is arbitrary.

* `newest_is_listed_and_greatest`: the chosen manifest is a listed one, and its number is at least every listed
  manifest's, whatever stray objects lie among them and in whatever order they are listed.
* `none_without_manifests`: nothing is chosen exactly when no listed name is a manifest's. Before the 2026-10-29
  fix the last listed name was chosen, so a stray name sorting after the digits was decoded as the newest manifest
  and failed every open (`LsmRegressionTest.StrayObjectsBesideWalChunksAndManifestsAreIgnored`).
-/

namespace Lsm.NewestManifest

def step (best : Option Nat) : Option Nat → Option Nat
  | none => best
  | some n => match best with
    | none => some n
    | some b => if n > b then some n else some b

def newest (listed : List (Option Nat)) : Option Nat := listed.foldl step none

theorem fold_spec (listed : List (Option Nat)) (best : Option Nat) :
    (∀ m, listed.foldl step best = some m →
      (best = some m ∨ some m ∈ listed) ∧ (∀ b, best = some b → b ≤ m) ∧ ∀ n, some n ∈ listed → n ≤ m) ∧
    (listed.foldl step best = none ↔ best = none ∧ ∀ n, some n ∉ listed) := by
  induction listed generalizing best with
  | nil =>
    refine ⟨fun m hm => ⟨Or.inl hm, fun b hb => ?_, by simp⟩, by simp⟩
    simp only [List.foldl_nil] at hm
    rw [hm] at hb
    cases hb
    exact Nat.le_refl _
  | cons x rest ih =>
    obtain ⟨ih1, ih2⟩ := ih (step best x)
    simp only [List.foldl_cons]
    refine ⟨fun m hm => ?_, ?_⟩
    · obtain ⟨hmem, hge, hall⟩ := ih1 m hm
      refine ⟨?_, ?_, ?_⟩
      · rcases hmem with h | h
        · cases x with
          | none => exact Or.inl (by simpa [step] using h)
          | some n =>
            cases best with
            | none => exact Or.inr (by simp [step] at h; simp [h])
            | some b =>
              simp only [step] at h
              split at h
              · exact Or.inr (by simp at h; simp [h])
              · exact Or.inl h
        · exact Or.inr (List.mem_cons_of_mem _ h)
      · intro b hb
        subst hb
        cases x with
        | none => exact hge b (by simp [step])
        | some n =>
          simp only [step] at hge
          split at hge
          · have := hge n rfl; omega
          · exact hge b rfl
      · intro n hn
        rcases List.mem_cons.mp hn with h | h
        · subst h
          cases best with
          | none => exact hge n (by simp [step])
          | some b =>
            simp only [step] at hge
            split at hge
            · exact hge n rfl
            · have := hge b rfl; omega
        · exact hall n h
    · rw [ih2]
      constructor
      · rintro ⟨hs, hn⟩
        cases x with
        | none => exact ⟨by simpa [step] using hs, fun n h => by
            rcases List.mem_cons.mp h with h | h
            · cases h
            · exact hn n h⟩
        | some n =>
          cases best with
          | none => simp [step] at hs
          | some b => simp only [step] at hs; split at hs <;> cases hs
      · rintro ⟨hb, hn⟩
        subst hb
        cases x with
        | none => exact ⟨rfl, fun n h => hn n (List.mem_cons_of_mem _ h)⟩
        | some n => exact absurd List.mem_cons_self (hn n)

theorem newest_is_listed_and_greatest (listed : List (Option Nat)) (m : Nat) (h : newest listed = some m) :
    some m ∈ listed ∧ ∀ n, some n ∈ listed → n ≤ m := by
  obtain ⟨hmem, _, hall⟩ := (fold_spec listed none).1 m h
  exact ⟨hmem.resolve_left (by simp), hall⟩

theorem none_without_manifests (listed : List (Option Nat)) :
    newest listed = none ↔ ∀ n, some n ∉ listed := by
  rw [newest, (fold_spec listed none).2]
  simp

end Lsm.NewestManifest
