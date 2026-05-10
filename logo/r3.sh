#!/bin/bash

# --- 8-bit ROLLING 逐字沉淀动画 ---

CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 1. 定义滚动帧 (3行一组)
frames=(
"  ▄▀▀▄  " " █    █ " "  ▀▄▄▀  "
"  ▄▄▄▄  " " █    █ " "  ▀▀▀▀  "
" █▀▀▀▀█ " " █    █ " " █▄▄▄▄█ "
"  ▀▄▄▀  " " █    █ " "  ▄▀▀▄  "
)

# 2. 定义字母帧 (每一个字母也是3行)
# R, O, L, L, I, N, G
letter_R=(" █▀▀▀▄ " " █▄▄▄▀ " " █  ▀▄ ")
letter_O=(" ▄███▄ " " █   █ " " ▀███▀ ")
letter_L=(" █     " " █     " " █▄▄▄█ ")
letter_I=("  ▄█▄  " "   █   " "  ▀█▀  ")
letter_N=(" █▄  █ " " █ █ █ " " █  ▀█ ")
letter_G=(" ▄███▄ " " █  ▄▄ " " ▀███▀ ")

# 字母数组列表
letters_raw=("letter_R" "letter_O" "letter_L" "letter_L" "letter_I" "letter_N" "letter_G")
target_word="ROLLING"

clear
tput civis # 隐藏光标
trap "tput cnorm; echo -e '$NC'; exit" INT TERM

fixed_content=("" "" "") # 存储已经固定下来的字母

# 逐个渲染字母
for (( i=0; i<${#target_word}; i++ )); do
    char_var=${letters_raw[$i]}
    
    # 模拟从左侧滚动到当前位置
    # 目标位置根据已有的字母长度计算
    target_pos=$(( i * 8 )) 
    
    # 滚动动画
    for (( pos=0; pos<=target_pos; pos+=2 )); do
        # 清除当前行（向上3行并清除）
        printf "\033[3A\r\033[J"
        
        # 打印已经固定的部分
        for row in {0..2}; do
            echo -e "${YELLOW}${fixed_content[$row]}${NC}"
        done
        
        # 打印正在滚动的 8-bit 方块
        prefix=$(printf '%*s' $pos "")
        frame_offset=$(( ( (pos + i) % 4 ) * 3 ))
        
        for row in {0..2}; do
            echo -e "${prefix}${CYAN}${frames[$((frame_offset + row))]}${NC}"
        done
        
        # 视觉停留
        sleep 0.05
        # 再次回到固定部分之前，准备下一帧刷新
        printf "\033[3A" 
    done

    # 变身：将当前的方块替换为字母并存入 fixed_content
    # 获取当前要落位的字母数组
    declare -n current_letter=$char_var
    for row in {0..2}; do
        fixed_content[$row]+="${current_letter[$row]} "
    done
    
    # 短暂暂停增加“落位”感
    sleep 0.1
done

# 最后打印完整的结果并保持
clear
echo -e "${YELLOW}--- ROLLING COMPLETE ---${NC}\n"
for row in {0..2}; do
    echo -e "${YELLOW}${fixed_content[$row]}${NC}"
done
echo -e "\n"

tput cnorm # 恢复光标
