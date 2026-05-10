#include <stdio.h>
#include <unistd.h>
#include <sys/ioctl.h>

#define CYAN "\033[0;36m"
#define NC "\033[0m"
#define FRAME_WIDTH 8
#define FRAME_ROWS 3

const char *frames[4][FRAME_ROWS] = {
    {"  ▄▀▀▄  ", " █    █ ", "  ▀▄▄▀  "},
    {"  ▄▄▄▄  ", " █    █ ", "  ▀▀▀▀  "},
    {" █▀▀▀▀█ ", " █    █ ", " █▄▄▄▄█ "},
    {"  ▀▄▄▀  ", " █    █ ", "  ▄▀▀▄  "}
};

int get_terminal_width() {
    struct winsize w;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == 0) {
        return w.ws_col;
    }
    return 80;
}

void print_spaces(int n) {
    for (int i = 0; i < n; i++) {
        putchar(' ');
    }
}

void clear_screen() {
    printf("\033[2J\033[H");
}

void move_up(int n) {
    printf("\033[%dA", n);
}

int main() {
    int width = get_terminal_width();
    int offset = width - FRAME_WIDTH;
    int frame = 0;

    clear_screen();
    printf("%s--- ROLLING ENGINE STARTING ---%s\n", CYAN, NC);

    /* 预打印3行空行，使首次 move_up 生效 */
    for (int i = 0; i < FRAME_ROWS; i++) {
        putchar('\n');
    }
    fflush(stdout);
    usleep(500000);

    while (1) {
        move_up(FRAME_ROWS);

        for (int j = 0; j < FRAME_ROWS; j++) {
            print_spaces(offset);
            printf("%s%s%s\n", CYAN, frames[frame][j], NC);
        }
        fflush(stdout);

        usleep(100000);

        offset -= 2;
        if (offset < 0) {
            offset = width - FRAME_WIDTH;
        }
        frame = (frame + 1) % 4;
    }

    return 0;
}
