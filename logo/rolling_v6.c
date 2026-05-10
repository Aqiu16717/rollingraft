#include <stdio.h>
#include <string.h>
#include <unistd.h>

// 保持不变的 8-bit 方块帧
const char* frames[4][3] = {{"  ▄▀▀▄  ", " █    █ ", "  ▀▄▄▀  "},
                            {"  ▄▄▄▄  ", " █    █ ", "  ▀▀▀▀  "},
                            {" █▀▀▀▀█ ", " █    █ ", " █▄▄▄▄█ "},
                            {"  ▀▄▄▀  ", " █    █ ", "  ▄▀▀▄  "}};

// 保持不变的 ROLLING 每个字母的点阵
const char* letters[7][3] = {
    {" █▀▀▀▄ ", " █▄▄▄▀ ", " █  ▀▄ "},  // R
    {" ▄▀▀▀▄ ", " █   █ ", " ▀▄▄▄▀ "},  // O
    {" █     ", " █     ", " █▄▄▄█ "},  // L
    {" █     ", " █     ", " █▄▄▄█ "},  // L
    {"  ▄█▄  ", "   █   ", "  ▀█▀  "},  // I
    {" █▄  █ ", " █ █ █ ", " █  ▀█ "},  // N
    {" ▄▀▀▀▄ ", " █ ▀▀█ ", " ▀▄▄▄▀ "}   // G
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

  printf("\033[?25l");
  printf("\n\033[1;33m[INFO] Init Rollingraft Native Engine...\033[0m\n\n\n");

  for (int i = 0; i < letter_count; i++) {
    int target_pos = i * 8;

    // --- 核心修复：让 pos 从 target_pos - 12 开始，确保 R 也有滑动轨迹 ---
    for (int pos = target_pos - 12; pos <= target_pos; pos += 2) {
      clear_lines(3);

      for (int r = 0; r < 3; r++) {
        // 1. 打印已经固定的字母
        if (i > 0) {
          printf("\033[1;33m%s\033[0m", fixed_text[r]);
        }

        // 2. 打印滚动中的方块前方的空格
        int current_block_prefix_len = pos - (i * 8);
        if (current_block_prefix_len > 0) {
          for (int s = 0; s < current_block_prefix_len; s++) printf(" ");
        }

        // 3. 打印滚动中的方块 (只有当 pos 达到或超过当前固定区域时才显示，或者
        // i=0 时强制显示)
        if (pos >= (i * 8) || i == 0) {
          // 使用 abs 防止负数取模逻辑异常
          int frame_idx = (pos < 0 ? (12 + pos) / 2 : pos / 2 + i) % 4;
          printf("\033[0;36m%s\033[0m\n", frames[frame_idx][r]);
        } else {
          printf("\n");  // 占位，防止闪烁
        }
      }
      usleep(45000);
    }

    // 沉淀阶段
    for (int r = 0; r < 3; r++) {
      strcat(fixed_text[r], letters[i][r]);
      strcat(fixed_text[r], " ");
    }

    clear_lines(3);
    for (int r = 0; r < 3; r++) {
      printf("\033[1;33m%s\033[0m\n", fixed_text[r]);
    }
    usleep(30000);
  }

  clear_lines(3);
  // 最终粉色显示
  for (int r = 0; r < 3; r++) {
    printf("\033[1;35m%s\033[0m\n", fixed_text[r]);
  }

  sleep(1);
  printf("\033[?25h\n");
  return 0;
}
