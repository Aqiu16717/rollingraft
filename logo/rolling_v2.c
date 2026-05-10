#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/ioctl.h> // 用于获取终端宽度

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
    for (int i = 0; i < n; i++) {
        printf("\033[A\033[2K"); 
    }
    printf("\r");
}

int main() {
    char fixed_text[3][1024] = {"", "", ""};
    int letter_count = 7;
    
    // 获取终端宽度
    struct winsize w;
    ioctl(STDOUT_FILENO, TIOCGWINSZ, &w);
    int term_width = w.ws_col > 0 ? w.ws_col : 80;

    printf("\033[?25l"); // 隐藏光标
    printf("\n\033[1;36m[SYS] Rollingraft Engine: Right-to-Left Mode\033[0m\n\n\n");

    for (int i = 0; i < letter_count; i++) {
        int target_pos = i * 8; // 最终落位坐标
        int start_pos = term_width - 10; // 从右侧开始（留一点边距）

        // 滚动阶段：从右向左 (pos 递减)
        for (int pos = start_pos; pos >= target_pos; pos -= 2) {
            clear_lines(3);
            
            for (int r = 0; r < 3; r++) {
                // 1. 先打印左侧已经固定的字母
                printf("\033[1;33m%s\033[0m", fixed_text[r]);
                
                // 2. 计算当前滚动块相对于已固定文字末尾的空格
                int current_fixed_len = strlen(fixed_text[r]);
                int spaces_needed = pos - current_fixed_len;
                
                if (spaces_needed > 0) {
                    for (int s = 0; s < spaces_needed; s++) printf(" ");
                }
                
                // 3. 打印滚动方块 (青色)
                int frame_idx = (pos / 2 + i) % 4;
                printf("\033[0;36m%s\033[0m\n", frames[frame_idx][r]);
            }
            usleep(35000); // 稍微加快一点，让滚动更有冲击力
        }
        
        // 沉淀：将当前字母拼入固定数组
        for (int r = 0; r < 3; r++) {
            strcat(fixed_text[r], letters[i][r]);
            strcat(fixed_text[r], " ");
        }

        // 碰撞后的瞬间反馈
        clear_lines(3);
        for (int r = 0; r < 3; r++) printf("\033[1;33m%s\033[0m\n", fixed_text[r]);
        usleep(20000);
    }

    printf("\n\033[1;32m[DONE] ROLLING Sequence Complete.\033[0m\n\n");
    printf("\033[?25h\n"); 
    return 0;
}
