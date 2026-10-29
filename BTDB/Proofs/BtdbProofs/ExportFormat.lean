/-!
# A key value export reads back as written

A model of `KeyValueDBExportImporter.Export` and `Import` (`../KVDBLayer/Helpers/KeyValueDBExportImporter.cs`) at the
level of fields: a number field stands for its little-endian bytes and a byte field for one byte. The export is the
pair count, each key and value prefixed by its length, then the commit ulong when it or the ulong count is not zero,
then the ulong count and the ulongs when there are any. Import reads the same and treats a missing trailer as zero.

* `import_export`: importing an export gives back its pairs, commit ulong and ulongs.
-/

namespace Btdb.ExportFormat

inductive Field where
  | num (n : Nat)
  | byte (b : Nat)
  deriving DecidableEq

def bytes (l : List Nat) : List Field := l.map Field.byte

def encodePairs : List (List Nat × List Nat) → List Field
  | [] => []
  | (k, v) :: rest => Field.num k.length :: bytes k ++ Field.num v.length :: bytes v ++ encodePairs rest

def exportFields (pairs : List (List Nat × List Nat)) (commit : Nat) (ulongs : List Nat) : List Field :=
  Field.num pairs.length :: encodePairs pairs ++
    (if commit ≠ 0 ∨ ulongs ≠ [] then Field.num commit :: [] else []) ++
    (if ulongs ≠ [] then Field.num ulongs.length :: ulongs.map Field.num else [])

/-- Reads `n` byte fields. -/
def readBytes : Nat → List Field → Option (List Nat × List Field)
  | 0, fs => some ([], fs)
  | n + 1, Field.byte b :: fs => (readBytes n fs).map fun (bs, r) => (b :: bs, r)
  | _ + 1, _ => none

def readNums : Nat → List Field → Option (List Nat × List Field)
  | 0, fs => some ([], fs)
  | n + 1, Field.num x :: fs => (readNums n fs).map fun (xs, r) => (x :: xs, r)
  | _ + 1, _ => none

def readPairs : Nat → List Field → Option (List (List Nat × List Nat) × List Field)
  | 0, fs => some ([], fs)
  | n + 1, Field.num kl :: fs =>
    match readBytes kl fs with
    | some (k, Field.num vl :: fs') =>
      match readBytes vl fs' with
      | some (v, fs'') => (readPairs n fs'').map fun (ps, r) => ((k, v) :: ps, r)
      | none => none
    | _ => none
  | _ + 1, _ => none

/-- The trailer: commit ulong and ulongs, zero and none when missing. -/
def readTrailer : List Field → Option (Nat × List Nat)
  | [] => some (0, [])
  | [Field.num c] => some (c, [])
  | Field.num c :: Field.num n :: fs => (readNums n fs).map fun (xs, _) => (c, xs)
  | _ => none

def importFields (fs : List Field) : Option (List (List Nat × List Nat) × Nat × List Nat) :=
  match fs with
  | Field.num n :: fs =>
    match readPairs n fs with
    | some (ps, rest) => (readTrailer rest).map fun (c, us) => (ps, c, us)
    | none => none
  | _ => none

theorem read_bytes (l : List Nat) (rest : List Field) : readBytes l.length (bytes l ++ rest) = some (l, rest) := by
  induction l with
  | nil => rfl
  | cons b l ih => simp [bytes, readBytes] at *; simp [ih]

theorem read_nums (l : List Nat) (rest : List Field) :
    readNums l.length (l.map Field.num ++ rest) = some (l, rest) := by
  induction l with
  | nil => rfl
  | cons x l ih => simp [readNums, ih]

theorem read_pairs (ps : List (List Nat × List Nat)) (rest : List Field) :
    readPairs ps.length (encodePairs ps ++ rest) = some (ps, rest) := by
  induction ps with
  | nil => rfl
  | cons p ps ih =>
    obtain ⟨k, v⟩ := p
    simp only [encodePairs, List.length_cons, readPairs, List.cons_append, List.append_assoc]
    simp [read_bytes, ih]

theorem import_export (pairs : List (List Nat × List Nat)) (commit : Nat) (ulongs : List Nat) :
    importFields (exportFields pairs commit ulongs) = some (pairs, commit, ulongs) := by
  unfold exportFields importFields
  simp only [List.cons_append, List.append_assoc]
  rw [read_pairs]
  simp only [Option.map]
  by_cases hu : ulongs = []
  · subst hu
    by_cases hc : commit = 0
    · simp [hc, readTrailer]
    · simp [hc, readTrailer]
  · have hn := read_nums ulongs []
    simp only [List.append_nil] at hn
    simp [hu, readTrailer, hn]

end Btdb.ExportFormat
