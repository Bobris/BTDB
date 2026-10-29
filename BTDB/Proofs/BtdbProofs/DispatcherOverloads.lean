/-!
# A generated dispatcher registers one accessible method for each message type a handler handles

`FindDispatcherMethods` (`../../BTDB.SourceGenerator/Main.cs`) walks the methods with the dispatcher name of a handler
class, then of its base classes, then the default interface methods, and keeps an accessible one-parameter instance
method for each message type it has not handled yet. Registrations are keyed by the message type and a later one
replaces an earlier one, so each type must be registered once. Before the 2026-10-29 fix the first method with the name
and one parameter was used, whatever its accessibility. `methods` lists the methods in the order the walk meets them,
`msg` is the parameter type and `ok` says the generated code can call the method.

* `select_ok`: every registered method can be called from the generated code.
* `select_nodup`: no message type is registered twice.
* `select_complete`: a callable method for a message type no earlier callable method handles is registered.
* `previous_picks_uncallable`, `previous_misses_second`: the first method could be uncallable, breaking compilation, and
  a handler of two message types was registered for one.
-/

namespace Btdb.DispatcherOverloads

structure Method where
  msg : Nat
  ok : Bool
  deriving DecidableEq

def step (out : List Method) (m : Method) : List Method :=
  if m.ok = true ∧ m.msg ∉ out.map (·.msg) then out ++ [m] else out

def select (methods : List Method) : List Method := methods.foldl step []

/-- Before the fix: the first method with the name and one parameter. -/
def previous (methods : List Method) : List Method := methods.head?.toList

def Inv (out : List Method) : Prop := (out.map (·.msg)).Nodup ∧ ∀ x ∈ out, x.ok = true

theorem step_inv (out : List Method) (m : Method) (h : Inv out) : Inv (step out m) := by
  obtain ⟨hnd, hok⟩ := h
  unfold step
  split
  · next hm =>
    refine ⟨?_, ?_⟩
    · rw [List.map_append, List.nodup_append]
      refine ⟨hnd, by simp, ?_⟩
      intro a ha b hb
      simp only [List.map_cons, List.map_nil, List.mem_singleton] at hb
      subst hb
      intro hab; subst hab; exact hm.2 ha
    · intro x hx
      rcases List.mem_append.1 hx with hx | hx
      · exact hok x hx
      · simp only [List.mem_singleton] at hx; subst hx; exact hm.1
  · exact ⟨hnd, hok⟩

theorem foldl_inv (l : List Method) (out : List Method) (h : Inv out) : Inv (l.foldl step out) := by
  induction l generalizing out with
  | nil => exact h
  | cons m rest ih => exact ih _ (step_inv out m h)

theorem select_inv (methods : List Method) : Inv (select methods) :=
  foldl_inv _ [] ⟨List.nodup_nil, fun _ h => by simp at h⟩

theorem select_ok (methods : List Method) : ∀ m ∈ select methods, m.ok = true := (select_inv methods).2

theorem select_nodup (methods : List Method) : ((select methods).map (·.msg)).Nodup := (select_inv methods).1

/-- Walking `l` only adds methods of `l`. -/
theorem foldl_sub (l : List Method) (out : List Method) (x : Method) (h : x ∈ l.foldl step out) :
    x ∈ out ∨ x ∈ l := by
  induction l generalizing out with
  | nil => exact Or.inl h
  | cons m rest ih =>
    rcases ih _ h with h | h
    · unfold step at h
      split at h
      · rcases List.mem_append.1 h with h | h
        · exact Or.inl h
        · simp only [List.mem_singleton] at h; subst h; exact Or.inr List.mem_cons_self
      · exact Or.inl h
    · exact Or.inr (List.mem_cons_of_mem _ h)

/-- A method registered stays registered. -/
theorem foldl_keeps (l : List Method) (out : List Method) (m : Method) (h : m ∈ out) : m ∈ l.foldl step out := by
  induction l generalizing out with
  | nil => exact h
  | cons c rest ih =>
    apply ih
    unfold step
    split
    · exact List.mem_append_left _ h
    · exact h

theorem select_complete (methods pre post : List Method) (m : Method)
    (hsplit : methods = pre ++ m :: post) (hok : m.ok = true)
    (hfirst : ∀ x ∈ pre, x.ok = true → x.msg ≠ m.msg) : m ∈ select methods := by
  unfold select
  rw [hsplit, List.foldl_append, List.foldl_cons]
  apply foldl_keeps
  have hnot : m.msg ∉ (pre.foldl step []).map (·.msg) := by
    intro hm
    obtain ⟨x, hx, hxm⟩ := List.mem_map.1 hm
    have hxok := (foldl_inv pre [] ⟨List.nodup_nil, fun _ h => by simp at h⟩).2 x hx
    rcases foldl_sub pre [] x hx with h | h
    · simp at h
    · exact hfirst x h hxok hxm
  unfold step
  split
  · exact List.mem_append_right _ (List.mem_singleton_self m)
  · next h => exact absurd ⟨hok, hnot⟩ h

/-- A private helper overload declared first was used and the generated code did not compile. -/
theorem previous_picks_uncallable : ∃ m ∈ previous [⟨0, false⟩, ⟨1, true⟩], m.ok = false := by
  decide

/-- A handler of two message types was registered for the first one only. -/
theorem previous_misses_second :
    ⟨1, true⟩ ∉ previous [⟨0, true⟩, ⟨1, true⟩] ∧ ⟨1, true⟩ ∈ select [⟨0, true⟩, ⟨1, true⟩] := by
  decide

end Btdb.DispatcherOverloads
