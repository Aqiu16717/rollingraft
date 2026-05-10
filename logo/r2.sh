#!/bin/bash

# --- 8-bit Rolling Gear Animation ---
# 用于 CLI 界面，展示分布式系统 "滚动" 的 Term。

# 颜色
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RESET='\033[0m'

# 齿轮动画帧 (严格 8-bit 风格)
frames=(
"   █▀█   "
" ▀▀   ▀▀ "
" █▄ ▄ ▄█ "
" ▀▀   ▀▀ "
"   █▀█   "

"   ▄▀▄   "
" ▀  █  ▀ "
" █▄▄▀▄▄█ "
" ▄  █  ▄ "
"   ▀▄▀   "

"   █▀█   "
" ▀█▄ ▄█▀ "
"  ▄▀ ▀▄  "
" ▀█▄ ▄█▀ "
"   █▀█   "
"         " # 最后一帧留空，防止重影
)

clear
echo -e "${YELLOW}[rollingraft] --- Core Engine ---${RESET}\n"

current_term=0
frame_count=3 # 实际有效的帧数
lines_per_frame=5 # 齿轮本身的高度

# 优雅退出
trap "echo -e '\n${RESET}[rollingraft] stopped.'; exit" INT TERM

while true; do
    # 计算当前帧索引
    frame_idx=$(( (current_term % frame_count) * lines_per_frame ))

    # 打印齿轮
    for i in $(seq 0 $((lines_per_frame - 1))); do
        printf "${BLUE}${frames[$((frame_idx + i))]}${RESET}\n"
    done

    # 向上移动光标准备刷新
    printf "\033[${lines_per_frame}A"

    # 在齿轮旁边打印 Term 信息 (模拟系统运行)
    tput cup 3 15 # 将光标移动到第三行第15列 (齿轮右侧)
    echo -e "${YELLOW}>> State: LEADER  |  Current Term: $((current_term / 2)) <<${RESET}"

    # 更新
    sleep 0.15
    ((current_term++))
done
