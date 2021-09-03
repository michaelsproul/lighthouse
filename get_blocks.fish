#!/usr/bin/env fish

for file in (ls block_stats/*.json)
    set -x slot (string replace block_ "" (basename $file .json))
    set -x block (block.sh $slot)
    if [ "$status" -ne 0 ]
        echo "block_$slot.json is bad"
    else
        echo $block > blocks/block_$slot.json
    end
end
