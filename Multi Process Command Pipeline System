/*
 * pipeexec.c - Run multiple Linux commands concurrently and move output
 *              between processes using pipes.
 *
 * Modes
 *   1) pipeline : cmd1 | cmd2 | cmd3 ...  (all stages run at the same time,
 *                 stdout of stage i is piped into stdin of stage i+1)
 *   2) parallel : independent commands run at the same time; the parent
 *                 collects each child's output through its own pipe and
 *                 prints it line by line with a [n] prefix.
 *
 * Build : gcc -Wall -Wextra -o pipeexec pipeexec.c
 * Usage : ./pipeexec pipeline "ls -l /etc" "grep conf" "wc -l"
 *         ./pipeexec parallel "ls /etc" "date" "uname -a" "sleep 1"
 *         ./pipeexec demo
 */
#define _GNU_SOURCE
#include <errno.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define MAX_CMDS 32
#define MAX_ARGS 64
#define BUF_SZ   4096

/* Split a command string on spaces/tabs into an argv array (modifies s). */
static int parse(char *s, char **argv)
{
    int n = 0;
    char *save = NULL;
    for (char *t = strtok_r(s, " \t", &save); t && n < MAX_ARGS - 1;
         t = strtok_r(NULL, " \t", &save))
        argv[n++] = t;
    argv[n] = NULL;
    return n;
}

static void die(const char *msg)
{
    perror(msg);
    exit(EXIT_FAILURE);
}

static void report_status(const char *label, int status)
{
    if (WIFEXITED(status))
        fprintf(stderr, "[%s] exited with status %d\n", label, WEXITSTATUS(status));
    else if (WIFSIGNALED(status))
        fprintf(stderr, "[%s] killed by signal %d\n", label, WTERMSIG(status));
}

/* ---------------------------------------------------------------- */
/* Mode 1: pipeline  cmd1 | cmd2 | ... | cmdN                       */
/* ---------------------------------------------------------------- */
static int run_pipeline(int n, char **cmds)
{
    pid_t pids[MAX_CMDS];
    int prev_read = -1;               /* read end of previous pipe */

    for (int i = 0; i < n; i++) {
        int fd[2] = {-1, -1};
        if (i < n - 1 && pipe(fd) < 0)
            die("pipe");

        char *copy = strdup(cmds[i]);
        char *argv[MAX_ARGS];
        if (!copy || parse(copy, argv) == 0) {
            fprintf(stderr, "empty command at stage %d\n", i + 1);
            return 1;
        }

        pid_t pid = fork();
        if (pid < 0)
            die("fork");

        if (pid == 0) {               /* ---- child ---- */
            if (prev_read != -1) {    /* stdin  <- previous stage */
                dup2(prev_read, STDIN_FILENO);
                close(prev_read);
            }
            if (fd[1] != -1) {        /* stdout -> next stage */
                dup2(fd[1], STDOUT_FILENO);
                close(fd[0]);
                close(fd[1]);
            }
            execvp(argv[0], argv);
            fprintf(stderr, "exec '%s' failed: %s\n", argv[0], strerror(errno));
            _exit(127);
        }

        /* ---- parent: close fds it no longer needs ---- */
        pids[i] = pid;
        free(copy);
        if (prev_read != -1)
            close(prev_read);
        if (fd[1] != -1)
            close(fd[1]);
        prev_read = fd[0];
    }

    int rc = 0;
    for (int i = 0; i < n; i++) {
        int status;
        waitpid(pids[i], &status, 0);
        char label[64];
        snprintf(label, sizeof label, "stage %d: %s", i + 1, cmds[i]);
        report_status(label, status);
        if (i == n - 1 && WIFEXITED(status))
            rc = WEXITSTATUS(status);
    }
    return rc;
}

/* ---------------------------------------------------------------- */
/* Mode 2: parallel - run all at once, collect output through pipes  */
/* ---------------------------------------------------------------- */
typedef struct {
    pid_t pid;
    int   fd;                         /* read end of child's output pipe */
    char  buf[BUF_SZ * 2];
    size_t len;
} Child;

static void flush_lines(Child *c, int idx, int final)
{
    char *start = c->buf, *nl;
    while ((nl = memchr(start, '\n', c->len - (start - c->buf)))) {
        printf("[%d] %.*s\n", idx + 1, (int)(nl - start), start);
        start = nl + 1;
    }
    size_t left = c->len - (start - c->buf);
    if (final && left > 0) {
        printf("[%d] %.*s\n", idx + 1, (int)left, start);
        left = 0;
    }
    memmove(c->buf, start, left);
    c->len = left;
    fflush(stdout);
}

static int run_parallel(int n, char **cmds)
{
    Child ch[MAX_CMDS];
    memset(ch, 0, sizeof ch);

    for (int i = 0; i < n; i++) {
        int fd[2];
        if (pipe(fd) < 0)
            die("pipe");

        char *copy = strdup(cmds[i]);
        char *argv[MAX_ARGS];
        if (!copy || parse(copy, argv) == 0) {
            fprintf(stderr, "empty command %d\n", i + 1);
            return 1;
        }

        pid_t pid = fork();
        if (pid < 0)
            die("fork");

        if (pid == 0) {               /* ---- child ---- */
            close(fd[0]);
            dup2(fd[1], STDOUT_FILENO);
            dup2(fd[1], STDERR_FILENO);
            close(fd[1]);
            for (int j = 0; j < i; j++)   /* drop earlier siblings' pipes */
                close(ch[j].fd);
            execvp(argv[0], argv);
            fprintf(stderr, "exec '%s' failed: %s\n", argv[0], strerror(errno));
            _exit(127);
        }

        close(fd[1]);                 /* parent keeps only the read end */
        free(copy);
        ch[i].pid = pid;
        ch[i].fd  = fd[0];
    }

    /* Multiplex all pipes with poll() until every child closes its end. */
    struct pollfd pfds[MAX_CMDS];
    int open_fds = n;
    for (int i = 0; i < n; i++) {
        pfds[i].fd = ch[i].fd;
        pfds[i].events = POLLIN;
    }

    while (open_fds > 0) {
        if (poll(pfds, n, -1) < 0) {
            if (errno == EINTR) continue;
            die("poll");
        }
        for (int i = 0; i < n; i++) {
            if (pfds[i].fd < 0 || !(pfds[i].revents & (POLLIN | POLLHUP)))
                continue;
            ssize_t r = read(ch[i].fd, ch[i].buf + ch[i].len,
                             sizeof ch[i].buf - ch[i].len);
            if (r > 0) {
                ch[i].len += (size_t)r;
                flush_lines(&ch[i], i, 0);
                if (ch[i].len == sizeof ch[i].buf)   /* very long line */
                    flush_lines(&ch[i], i, 1);
            } else {                                 /* EOF or error */
                flush_lines(&ch[i], i, 1);
                close(ch[i].fd);
                pfds[i].fd = -1;                     /* poll ignores it */
                open_fds--;
            }
        }
    }

    int rc = 0;
    for (int i = 0; i < n; i++) {
        int status;
        waitpid(ch[i].pid, &status, 0);
        char label[64];
        snprintf(label, sizeof label, "cmd %d: %s", i + 1, cmds[i]);
        report_status(label, status);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
            rc = 1;
    }
    return rc;
}

/* ---------------------------------------------------------------- */
static void usage(const char *p)
{
    fprintf(stderr,
        "Usage:\n"
        "  %s pipeline \"cmd1\" \"cmd2\" ...   run cmd1 | cmd2 | ... concurrently\n"
        "  %s parallel \"cmd1\" \"cmd2\" ...   run independent commands concurrently\n"
        "  %s demo                          run built-in examples\n", p, p, p);
}

int main(int argc, char **argv)
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (!strcmp(argv[1], "demo")) {
        char *p1[] = {"ls -l /etc", "grep conf", "sort -k9", "head -5"};
        char *p2[] = {"echo hello from A", "uname -a", "sleep 1", "whoami", "date"};
        puts("=== Pipeline: ls -l /etc | grep conf | sort -k9 | head -5 ===");
        run_pipeline(4, p1);
        puts("\n=== Parallel commands ===");
        run_parallel(5, p2);
        return 0;
    }

    int n = argc - 2;
    if (n < 1 || n > MAX_CMDS) { usage(argv[0]); return 1; }

    if (!strcmp(argv[1], "pipeline")) return run_pipeline(n, argv + 2);
    if (!strcmp(argv[1], "parallel")) return run_parallel(n, argv + 2);

    usage(argv[0]);
    return 1;
}
