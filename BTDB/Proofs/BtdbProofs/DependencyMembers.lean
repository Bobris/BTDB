/-!
# One dependency per member name, the most derived

A model of how `SingleRegistration.CollectDependencies` (`../../IOC/SingleRegistration.cs`) and the source generator's
`DependencyMembers` collect the `[Dependency]` members of a type: they walk its classes from the most derived to the
base and take a member only when no member of its name was met before, so one hidden with `new` or overridden in a
derived class is taken from the most derived class. Members are their names here, listed in walking order.

* `collect_nodup`: no name is taken twice, so no dependency is injected twice.
* `collect_complete`: every name met is taken.
* `collect_first`: a member of a name not met before is taken where it is met and no later member of its name is,
  so the most derived one is injected.
-/

namespace Btdb.DependencyMembers

def collect : List String → List String → List String
  | _, [] => []
  | seen, m :: rest => if m ∈ seen then collect seen rest else m :: collect (m :: seen) rest

theorem collect_fresh : ∀ (seen members : List String) (m : String), m ∈ collect seen members → m ∉ seen
  | _, [], _, h => by simp [collect] at h
  | seen, x :: rest, m, h => by
    unfold collect at h
    split at h
    · exact collect_fresh seen rest m h
    · rcases List.mem_cons.1 h with e | e
      · subst e; assumption
      · intro hs; exact collect_fresh (x :: seen) rest m e (List.mem_cons_of_mem _ hs)

theorem collect_nodup : ∀ (seen members : List String), (collect seen members).Nodup
  | _, [] => by simp [collect]
  | seen, x :: rest => by
    unfold collect
    split
    · exact collect_nodup seen rest
    · refine List.nodup_cons.2 ⟨fun h => ?_, collect_nodup (x :: seen) rest⟩
      exact collect_fresh (x :: seen) rest x h (List.mem_cons_self)

theorem collect_complete : ∀ (seen members : List String) (m : String), m ∈ members → m ∉ seen →
    m ∈ collect seen members
  | _, [], _, h, _ => by simp at h
  | seen, x :: rest, m, h, hs => by
    unfold collect
    split
    · rename_i hx
      rcases List.mem_cons.1 h with e | e
      · subst e; exact absurd hx hs
      · exact collect_complete seen rest m e hs
    · rcases List.mem_cons.1 h with e | e
      · subst e; exact List.mem_cons_self
      · by_cases hmx : m = x
        · subst hmx; exact List.mem_cons_self
        · exact List.mem_cons_of_mem _ (collect_complete (x :: seen) rest m e (by simp [hmx, hs]))

/-- A member whose name was not met before is taken where it is met, ahead of any later member of its name, which
is then skipped: walking from the most derived class, the most derived member of a name is the one injected. -/
theorem collect_first (seen rest : List String) (m : String) (h : m ∉ seen) :
    collect seen (m :: rest) = m :: collect (m :: seen) rest ∧ m ∉ collect (m :: seen) rest := by
  refine ⟨by simp [collect, h], fun hm => collect_fresh (m :: seen) rest m hm List.mem_cons_self⟩

end Btdb.DependencyMembers
