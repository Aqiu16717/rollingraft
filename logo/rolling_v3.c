#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/ioctl.h>

// --- 颜色配置区 ---
// 只要修改这里的数值，就能瞬间改变整个动画的色调
#define COLOR_ROLLING "\033[1;35m"  // 滚动方块：紫色 (1;35)
#define COLOR_FIXED   "\033[1;31m"  // 固定字母：红色 (1;31)
#define COLOR_INFO    "\033[1;36m"  // 提示文字：青色 (1;36)
#define RESET         "\033[0m"

// 8-bit 方块帧
const char *frames[4][3] = {
    {"  ▄▀▀▄  ", " █    █ ", "  ▀▄▄▀  "},
    {"  ▄▄▄▄  ", " █    █ ", "  ▀▀▀▀  "},
    {" █▀▀▀▀█ ", " █    █ ", " █▄▄▄▄█ "},
    {"  ▀▄▄▀  ", " █    █ ", "  ▄▀▀▄  "}
};

// ROLLING 字母点阵
const char *letters[7][3] = {
    {" █▀▀▀▄ ", " █▄▄▄▀ ", " █  ▀▄ "}, // R
    {" ▄███▄ ", " █   █ ", " ▀███▀ "}, // O
    {" █     ", " █     ", " █▄▄▄█ "}, // L
    {" █     ", " █     ", " █▄▄▄█ "}, // L
    {"  ▄█▄  ", "   █   ", "  ▀█▀  "}, // I
    {" █▄  █ ", " █ █ █ ", " █  ▀█ "}, // N
    {" ▄███▄ ", " █  ▄▄ ", " ▀███▀ "}  // G
};

void clear_lines(int n) {
    for (int i = 0; i < n; i++) printf("\033[A\033[2K");
    printf("\r");
}

int main() {
    char fixed_text[3][1024] = {"", "", ""};
    int letter_count = 7;
    
    struct winsize w;
    ioctl(STDOUT_FILENO, TIOCGWINSZ, &w);
    int term_width = (w.ws_col > 0) ? w.ws_col : 80;

    printf("\033[?25l"); // 隐藏光标
    printf("\n" COLOR_INFO "[SYS] Rollingraft Engine Initializing..." RESET "\n\n\n");

    for (int i = 0; i < letter_count; i++) {
        int target_pos = i * 8; 
        int start_pos = term_width - 12; // 从屏幕右侧飞入

        for (int pos = start_pos; pos >= target_pos; pos -= 2) {
            clear_lines(3);
            for (int r = 0; r < 3; r++) {
                // 1. 打印已固定的部分
                printf(COLOR_FIXED "%s" RESET, fixed_text[r]);
                
                // 2. 计算方块偏移
                int current_fixed_len = strlen(fixed_text[r]);
                int spaces = pos - current_fixed_len;
                if (spaces > 0) for (int s = 0; s < spaces; s++) printf(" ");
                
                // 3. 打印滚动方块
                int f_idx = (pos / 2 + i) % 4;
                printf(COLOR_ROLLING "%s" RESET "\n", frames[f_idx][r]);
            }
            usleep(30000);
        }
        
        // 沉淀字母
        for (int r = 0; r < 3; r++) {
            strcat(fixed_text[r], letters[i][r]);
            strcat(fixed_text[r], " ");
        }

        // 碰撞反馈
        clear_lines(3);
        for (int r = 0; r < 3; r++) printf(COLOR_FIXED "%s" RESET "\n", fixed_text[r]);
        usleep(15000);
    }

    printf("\n" COLOR_INFO "[DONE] Project: ROLLINGRAFT" RESET "\n\n");
    printf("\033[?25h\n"); 
    return 0;
}
