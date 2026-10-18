#!/usr/bin/env bash
# Publishes AotQuerySmoke with NativeAOT, fails on trim/AOT warnings from the relation query code path, then runs the
# native binary. Other BTDB areas still use Reflection.Emit and produce warnings; those are reported as a count only.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out_dir="${1:-$script_dir/../artifacts/aot-query-smoke}"
log_file="$out_dir.publish.log"
query_path_pattern='BTDB/(ODBLayer/RelationQuery[A-Za-z]*|Interpreter/[A-Za-z]+)\.cs'

mkdir -p "$(dirname "$log_file")"
# An up to date publish skips the NativeAOT compiler and reports no warnings, so start clean.
dotnet clean "$script_dir/AotQuerySmoke.csproj" -c Release > /dev/null
dotnet publish "$script_dir/AotQuerySmoke.csproj" -c Release -o "$out_dir" 2>&1 | tee "$log_file"

query_warnings="$(grep -E "warning (IL[0-9]+|AOT)" "$log_file" | grep -E "$query_path_pattern" | sort -u || true)"
all_warnings="$( (grep -E "warning IL[0-9]+" "$log_file" || true) | sort -u | wc -l | tr -d ' ')"
echo "Trim/AOT warnings in total (other BTDB areas still use Reflection.Emit): $all_warnings"
if [ -n "$query_warnings" ]; then
    echo "Trim/AOT warnings in the relation query path:"
    echo "$query_warnings"
    exit 1
fi

binary="$out_dir/AotQuerySmoke"
[ -f "$binary.exe" ] && binary="$binary.exe"
"$binary"
