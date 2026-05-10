#!/bin/bash

# --- 极简 Rolling 动画 ---
# 颜色定义
CYAN='\033[0;36m'
NC='\033[0m' # 无颜色

# 滚动帧 (模拟一个 8-bit 方块在翻转)
frames=(
"  ▄▀▀▄  "
" █    █ "
"  ▀▄▄▀  "

"  ▄▄▄▄  "
" █    █ "
"  ▀▀▀▀  "

" █▀▀▀▀█ "
" █    █ " 
" █▄▄▄▄█ "

"  ▀▄▄▀  "
" █    █ "
"  ▄▀▀▄  "
)

clear
echo -e "${CYAN}--- ROLLING ENGINE STARTING ---${NC}"

# 模拟横向滚动偏移
offset=0
width=$(tput cols)

while true; do
    # 计算偏移量，实现横向循环滚动
    prefix=$(printf ' %.0s' $(seq 1 $offset))
    
    # 打印动画帧
    for j in {0..2}; do
        # 每一帧由 3 行组成，取当前循环的 3 行
        frame_idx=$(( ( (SECONDS * 10) % 4 ) * 3 + j ))
        echo -e "${prefix}${CYAN}${frames[$frame_idx]}${NC}"
    done

    # 回到起始位置准备刷新
    sleep 0.1
    printf "\033[3A" # 向上移动 3 行
    
    offset=$(( (offset + 2) % (width / 2) ))
done
