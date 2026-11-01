#!/usr/bin/env bash
# Publishes AotActionsSmoke with NativeAOT, fails on trim/AOT warnings from BackendActions and EventSourcing, then runs
# the native binary. Other BTDB areas still have reflection paths this smoke does not reach; those are counted only.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out_dir="${1:-$script_dir/../artifacts/aot-actions-smoke}"
log_file="$out_dir.publish.log"
# The warning's source file; MSBuild appends every line's project path too.
checked_path_pattern='^[^(]*/(BTDB\.BackendActions|BTDB\.Replication\.EventSourcing|AotActionsSmoke)/[^(]*\.cs\('

mkdir -p "$(dirname "$log_file")"
# An up to date publish skips the NativeAOT compiler and reports no warnings, so start clean.
dotnet clean "$script_dir/AotActionsSmoke.csproj" -c Release > /dev/null
dotnet publish "$script_dir/AotActionsSmoke.csproj" -c Release -o "$out_dir" 2>&1 | tee "$log_file"

checked_warnings="$(grep -E "warning (IL[0-9]+|AOT)" "$log_file" | grep -E "$checked_path_pattern" | sort -u || true)"
all_warnings="$( (grep -E "warning IL[0-9]+" "$log_file" || true) | sort -u | wc -l | tr -d ' ')"
echo "Trim/AOT warnings in total (other BTDB areas still use reflection): $all_warnings"
if [ -n "$checked_warnings" ]; then
    echo "Trim/AOT warnings in BackendActions or EventSourcing:"
    echo "$checked_warnings"
    exit 1
fi

binary="$out_dir/AotActionsSmoke"
[ -f "$binary.exe" ] && binary="$binary.exe"
"$binary"
