#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/ioctl.h>

// --- 配色配置 ---
#define P_BRIGHT "\033[1;35m" // 霓虹粉 (高亮)
#define P_DIM    "\033[0;35m" // 霓虹粉 (暗色，用于落位)
#define CYAN     "\033[1;36m" 
#define RESET    "\033[0m"

// --- 5x5 精修位图 (ROLLING) ---
// 每一行是一个 5-bit 的整数，0x11 代表二进制 10001
const unsigned char font[7][5] = {
    {0x1E, 0x11, 0x1E, 0x12, 0x11}, // R
    {0x0E, 0x11, 0x11, 0x11, 0x0E}, // O
    {0x10, 0x10, 0x10, 0x10, 0x1F}, // L
    {0x10, 0x10, 0x10, 0x10, 0x1F}, // L
    {0x0E, 0x04, 0x04, 0x04, 0x0E}, // I
    {0x11, 0x19, 0x15, 0x13, 0x11}, // N
    {0x0E, 0x11, 0x17, 0x11, 0x0E}  // G (重新设计，确保开口圆润)
};

// 旋转方块中间态 (3行)
const char *blocks[4][3] = {
    {" ▄▀▀▄ ", " █  █ ", " ▀▄▄▀ "},
    {"  ▄▄  ", " █  █ ", "  ▀▀  "},
    {" █▀▀█ ", " █  █ ", " █▄▄█ "},
    {"  ▀▀  ", " █  █ ", "  ▄▄  "}
};

int main() {
    struct winsize w;
    ioctl(STDOUT_FILENO, TIOCGWINSZ, &w);
    int term_width = (w.ws_col > 0) ? w.ws_col : 80;

    printf("\033[2J\033[?25l"); // 全局清屏 + 隐藏光标

    for (int l = 0; l < 7; l++) {
        int target_x = l * 7; // 每个字母占 5 像素 + 2 间隔

        // 从右向左滚动
        for (int x = term_width - 10; x >= target_x; x--) {
            // 使用 \033[H 回到左上角，彻底杜绝闪烁和重影
            printf("\033[H\n" CYAN "[ROLLINGRAFT KERNEL] Booting Animation..." RESET "\n\n");

            for (int r = 0; r < 5; r++) {
                // 1. 扫描并绘制左侧已固定的字母
                for (int prev = 0; prev < l; prev++) {
                    printf(P_DIM);
                    for (int bit = 4; bit >= 0; bit--) {
                        printf((font[prev][r] >> bit) & 1 ? "█" : " ");
                    }
                    printf("  " RESET);
                }

                // 2. 计算动态偏移
                int current_fixed_width = l * 7;
                int spaces = x - current_fixed_width;
                if (spaces > 0) {
                    for (int s = 0; s < spaces; s++) printf(" ");
                }

                // 3. 绘制滚动方块 (只在中间 3 行渲染)
                if (r >= 1 && r <= 3) {
                    printf(P_BRIGHT "%s" RESET, blocks[x % 4][r - 1]);
                } else {
                    printf("      "); // 保持占位
                }
                printf("\n");
            }
            usleep(25000); // 40 FPS
        }
    }

    printf("\n" CYAN "[SUCCESS] ROLLING Logo Initialized." RESET "\n\n");
    printf("\033[?25h"); // 恢复光标
    return 0;
}
