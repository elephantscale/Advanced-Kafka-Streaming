#!/bin/bash
# To generate slides
$ES_HOME/utils/presentations/slides-assembler.sh slide-list.txt

# The assembler numbers entries sequentially from 01, so the intro deck
# (about) comes out as 01__about and each module N as 0(N+1). Shift every
# prefix down by one so the intro is 00__about and module N is 0N__moduleN.
# Processed in ascending order, so each target slot is already vacated.
if [ -d assembly.out ]; then
  ( cd assembly.out && for f in [0-9][0-9]__*; do
      [ -e "$f" ] || continue
      n=$((10#${f%%__*}))
      printf -v new "%02d__%s" "$((n - 1))" "${f#*__}"
      mv -- "$f" "$new"
    done )
fi

