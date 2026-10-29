/-!
# With a service provider each instance is disposed once

With `UseBtdbIoc` (`../IOC/ServiceProviderIntegration.cs`, `../IOC/ContainerImpl.cs`) the BTDB container owns the
disposable instances it creates (`owned`), and the service provider disposes the disposable instances its export
factories returned (`tracked`), which the integration remembers. Since the 2026-10-29 fix the container skips those when
it disposes. Each list holds an instance once: the container adds an instance when it creates it, and the provider
tracks what a singleton or scoped registration returns once per lifetime.

* `fixed_once`: every owned or tracked instance is disposed exactly once.
* `previous_twice`: before the fix an instance both owned and tracked was disposed twice.
-/

namespace Btdb.ProviderDisposal

def disposals (owned tracked : List Nat) : List Nat := owned.filter (fun x => !tracked.contains x) ++ tracked

def previous (owned tracked : List Nat) : List Nat := owned ++ tracked

theorem count_filter_not (owned tracked : List Nat) (x : Nat) (h : x ∈ tracked) :
    (owned.filter (fun y => !tracked.contains y)).count x = 0 := by
  rw [List.count_eq_zero]
  intro hx
  rw [List.mem_filter] at hx
  simp [h] at hx

theorem count_filter_keep (owned tracked : List Nat) (x : Nat) (h : x ∉ tracked) :
    (owned.filter (fun y => !tracked.contains y)).count x = owned.count x := by
  rw [List.count_filter]
  simp [h]

theorem fixed_once (owned tracked : List Nat) (ho : owned.Nodup) (ht : tracked.Nodup) (x : Nat)
    (hx : x ∈ owned ∨ x ∈ tracked) : (disposals owned tracked).count x = 1 := by
  unfold disposals
  rw [List.count_append]
  by_cases h : x ∈ tracked
  · rw [count_filter_not _ _ _ h, List.Nodup.count_of_mem ht h]
  · rw [count_filter_keep _ _ _ h, List.count_eq_zero.2 h]
    have hxo : x ∈ owned := hx.resolve_right h
    rw [List.Nodup.count_of_mem ho hxo]

theorem previous_twice : (previous [5] [5]).count 5 = 2 := by decide

end Btdb.ProviderDisposal
