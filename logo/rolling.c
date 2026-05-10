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
    {"  ▀█▀  ", "   █   ", "  ▄█▄  "},  // I
    {" █▄  █ ", " █ █ █ ", " █  ▀█ "},  // N
    {" ▄▀▀▀▄ ", " █ ▀▀█ ", " ▀▄▄▄▀ "}   // G
};

void clear_lines(int n) {
  for (int i = 0; i < n; i++) {
    // 精确向上移一行 (\033[A) 并清除行 (\033[2K)
    printf("\033[A\033[2K");
  }
  printf("\r");
}

int main() {
  // 存储已经固定的字母 (增加容量防止溢出)
  char fixed_text[3][1024] = {"", "", ""};
  int letter_count = 7;
  // int terminal_width = 80;  // 默认终端宽度，用于计算偏移

  // 隐藏光标，防止闪烁
  printf("\033[?25l");
  printf(
      "\n\033[1;33m[INFO] Init Rollingraft Native Engine...\033[0m\n\n\n");  // 预留 3 行空间

  for (int i = 0; i < letter_count; i++) {
    // 目标位置：每个字母占 8 个字符宽度
    int target_pos = i * 8;

    // 滚动阶段：从左侧（pos=0）滑动到目标位置
    for (int pos = 0; pos <= target_pos; pos += 2) {
      // if (pos == 0) {
      //   usleep(45000);  // 45ms 帧间隔，获得丝滑感
      // }
      clear_lines(3);  // 清除上一帧的 3 行

      for (int r = 0; r < 3; r++) {
        // 1. 打印已经固定的字母 (黄色)
        if (i > 0) {
          printf("\033[1;33m%s\033[0m", fixed_text[r]);
        }

        // 2. 打印滚动中的方块前方的空格 (用于对齐)
        int current_block_prefix_len = pos - (i * 8);
        // 只有当滚动方块领先于已固定字母时才需要打印空格
        if (current_block_prefix_len > 0) {
          for (int s = 0; s < current_block_prefix_len; s++) printf(" ");
        }

        // 3. 打印滚动中的方块 (青色)
        int frame_idx = (pos / 2 + i) % 4;  // 方块自身旋转
        printf("\033[0;36m%s\033[0m\n", frames[frame_idx][r]);
      }
      usleep(45000);  // 45ms 帧间隔，获得丝滑感
    }

    // 沉淀阶段：方块“落位”，将其替换为最终字母点阵并存入 fixed_text
    for (int r = 0; r < 3; r++) {
      strcat(fixed_text[r], letters[i][r]);
      strcat(fixed_text[r], " ");  // 每个字母间加一个空格
    }

    // --- 核心修复部分 ---
    // 即使在滚动循环内，最后一个字母 G
    // 落位后，也要立即显示完整的固定效果，而不是跳过
    clear_lines(3);
    for (int r = 0; r < 3; r++) {
      printf("\033[1;33m%s\033[0m\n", fixed_text[r]);
    }
    usleep(30000);  // 短暂暂停，增加“撞击落位”的物理感
  }

  // 循环结束后，再次确认打印完整的 ROLLING，并保持一段时间
  clear_lines(3);  // 清除循环中最后一次打印，准备最终稳定显示
  // for (int r = 0; r < 3; r++) {
  //   // 用最醒目的黄色打印完整的 ROLLING
  //   printf("\033[1;33m%s\033[0m\n", fixed_text[r]);
  // }

  // 粉色
  for (int r = 0; r < 3; r++) {
    printf("\033[1;35m%s\033[0m\n", fixed_text[r]);
  }

  // 青色
  // for (int r = 0; r < 3; r++) {
  //   printf("\033[1;36m%s\033[0m\n", fixed_text[r]);
  // }

  // printf(
  //     "\n\033[1;32m[SUCCESS] ROLLING LOGO INITIALIZED (Raft Terms are "
  //     "stable).\033[0m\n\n");

  // 停留一段时间，让用户看清楚 G
  sleep(1);

  // 恢复光标
  printf("\033[?25h\n");
  return 0;
}
