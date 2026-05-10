#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/ioctl.h>

#define COLOR_ROLLING "\033[1;35m" 
#define COLOR_FIXED   "\033[0;35m" 
#define COLOR_INFO    "\033[1;36m" 
#define RESET         "\033[0m"

// 增加到 8 帧，模拟更细致的翻滚动作（加入了倾斜和压缩的中间态）
const char *frames[8][3] = {
    {"  ▄▀▀▄  ", " █    █ ", "  ▀▄▄▀  "}, // 0: 圆润正位
    {"   ▄▄   ", "  █  █  ", "   ▀▀   "}, // 1: 挤压态
    {"  █▀▀█  ", "  █  █  ", "  █▄▄█  "}, // 2: 垂直窄长态
    {"   ▀▀   ", "  █  █  ", "   ▄▄   "}, // 3: 挤压态
    {"  ▀▄▄▀  ", " █    █ ", "  ▄▀▀▄  "}, // 4: 翻转位
    {"  ▄▄▄▄  ", " █    █ ", "  ▀▀▀▀  "}, // 5: 扁平态
    {" █▀▀▀▀█ ", " █    █ ", " █▄▄▄▄█ "}, // 6: 宽扁态
    {"  ▀▀▀▀  ", " █    █ ", "  ▄▄▄▄  "}  // 7: 扁平态
};

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

    printf("\033[?25l"); 
    printf("\n" COLOR_INFO "[SYS] Rollingraft: Ultra-Smooth Pink Engine..." RESET "\n\n\n");

    for (int i = 0; i < letter_count; i++) {
        int target_pos = i * 8; 
        int start_pos = term_width - 12;

        // 步进减小到 1，速度调，让平移更顺滑
        for (int pos = start_pos; pos >= target_pos; pos--) {
            clear_lines(3);
            for (int r = 0; r < 3; r++) {
                printf(COLOR_FIXED "%s" RESET, fixed_text[r]);
                
                int current_fixed_len = strlen(fixed_text[r]);
                int spaces = pos - current_fixed_len;
                if (spaces > 0) for (int s = 0; s < spaces; s++) printf(" ");
                
                // 每一像素位移都对应一个不同的旋转帧，实现“随地滚转”的效果
                int f_idx = (pos % 8); 
                printf(COLOR_ROLLING "%s" RESET "\n", frames[f_idx][r]);
            }
            // 降低延迟配合更细的步进
            usleep(18000); 
        }
        
        for (int r = 0; r < 3; r++) {
            strcat(fixed_text[r], letters[i][r]);
            strcat(fixed_text[r], " ");
        }

        clear_lines(3);
        for (int r = 0; r < 3; r++) printf(COLOR_FIXED "%s" RESET "\n", fixed_text[r]);
        usleep(10000);
    }

    printf("\n" COLOR_INFO "[DONE] All Terms Replicated." RESET "\n\n");
    printf("\033[?25h\n"); 
    return 0;
}
