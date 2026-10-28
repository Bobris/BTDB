import BtdbProofs

open Btdb

#print axioms KeyRange.lower_correct
#print axioms KeyRange.upper_correct
#print axioms VUInt.vuint_order
#print axioms VUInt.vuint_prefix_free
#print axioms VInt.vint_order
#print axioms VInt.vint_prefix_free
#print axioms KeyRange.byte_encoding
#print axioms VUInt.encoding
#print axioms VInt.encoding
#print axioms Cursor.block
#print axioms Cursor.previous_in_block
#print axioms Cursor.previous_outside_next_in_block
#print axioms Cursor.previous_outside_next_outside
#print axioms Cursor.previous_outside_no_next
#print axioms Cursor.next_in_block
#print axioms Cursor.next_outside
#print axioms Seek.seek_exact
#print axioms Seek.seek_next
#print axioms Seek.seek_previous
#print axioms Seek.seek_previous_none
#print axioms Seek.seek_exclusive
#print axioms Seek.seek_exclusive_none
#print axioms Seek.seek_exclusive_kept
#print axioms Seek.seek_last_inclusive
#print axioms Seek.not_past_inclusive
#print axioms Seek.seek_last_previous
#print axioms Seek.seek_last_exact
#print axioms Seek.seek_last_exact_none
#print axioms Seek.seek_last_next_none
#print axioms StringOrdered.starts_with_correct
#print axioms StringOrdered.high_surrogate_needed
#print axioms StringOrdered.encOpt_prefix_free
#print axioms ReadOnlyCompare.compare_correct
#print axioms Boundary.search_correct
#print axioms Boundary.before_lt_mono
#print axioms Boundary.before_cut_mono
#print axioms Boundary.descend
#print axioms Boundary.child_before
#print axioms Boundary.child_after
#print axioms Boundary.cut_key_mono
