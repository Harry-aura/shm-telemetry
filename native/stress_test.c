#include "shm_common.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define NPROD 4
#define NCONS 4
#define MAX_KIDS (NPROD + NCONS)

typedef struct {
    uint64_t count;
    uint64_t hash_sum;
    uint32_t producer_idx;
    uint32_t pid;
} ProducerReport;

typedef struct {
    uint64_t count;
    uint64_t hash_sum;
    uint64_t crc_errors;
    uint64_t order_errors;
} ConsumerReport;

static char sibling_dir[4096];

static int locate_siblings(void)
{
    char exe[4096];
    ssize_t n = readlink("/proc/self/exe", exe, sizeof(exe) - 1);
    if (n < 0) {
        perror("readlink: /proc/self/exe");
        return -1;
    }
    exe[n] = '\0';
    char *slash = strrchr(exe, '/');
    if (slash == NULL) {
        fprintf(stderr, "stress_test: cannot dirname '%s'\n", exe);
        return -1;
    }
    *slash = '\0';
    size_t len = strlen(exe);
    if (len + 1 >= sizeof sibling_dir) {
        fprintf(stderr, "stress_test: exe dir too long\n");
        return -1;
    }
    memcpy(sibling_dir, exe, len + 1);
    return 0;
}

static int read_full(int fd, void *buf, size_t len)
{
    char *p = buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

static int read_writer_totals(const MetricsWriter *w, uint64_t *p, uint64_t *c)
{
    for (int t = 0; t < 8; t++) {
        uint32_t s1 = atomic_load_explicit(&w->seq, memory_order_acquire);
        if (s1 & 1u)
            continue;
        *p = atomic_load_explicit(&w->produced, memory_order_relaxed);
        *c = atomic_load_explicit(&w->consumed, memory_order_relaxed);
        atomic_thread_fence(memory_order_acquire);
        uint32_t s2 = atomic_load_explicit(&w->seq, memory_order_relaxed);
        if (s1 == s2)
            return 0;
    }
    return -1;
}

static int read_totals(const ShmRO *ro, uint64_t *produced, uint64_t *consumed)
{
    uint64_t p = 0, c = 0;
    for (uint32_t i = 0; i < SHM_MAX_WRITERS; i++) {
        uint64_t wp = 0, wc = 0;
        if (read_writer_totals(&ro->mx->w[i], &wp, &wc) == 0) {
            p += wp;
            c += wc;
        }
    }
    *produced = p;
    *consumed = c;
    return 0;
}

int main(int argc, char **argv)
{
    const char *id = "stress";
    long duration_s = 10;
    int pos = 0;

    for (int i = 1; i < argc; i++) {
        if (pos == 0) {
            id = argv[i];
            pos++;
        } else if (pos == 1) {
            char *end = NULL;
            duration_s = strtol(argv[i], &end, 10);
            if (end == NULL || *end != '\0' || duration_s < 1 || duration_s > 3600) {
                fprintf(stderr, "stress_test: invalid duration_s '%s'\n", argv[i]);
                return 2;
            }
            pos++;
        } else {
            fprintf(stderr, "usage: stress_test [instance_id] [duration_s]\n");
            return 2;
        }
    }

    if (shm_valid_id(id) != 0)
        return 1;
    if (locate_siblings() != 0)
        return 1;

    if (crc32_ieee("123456789", 9) != 0xCBF43926u) {
        fprintf(stderr, "stress_test: CRC-32 self-test failed\n");
        return 1;
    }

    char ring_path[128];
    snprintf(ring_path, sizeof ring_path, "/dev/shm/shm_ring_%s", id);
    {
        struct stat st;
        if (stat(ring_path, &st) == 0) {
            fprintf(stderr, "stress_test: %s already exists; run ./cleanup.sh first\n", ring_path);
            return 1;
        }
        if (errno != ENOENT) {
            perror("stat: ring path");
            return 1;
        }
    }

    char prod_path[4200], cons_path[4200];
    snprintf(prod_path, sizeof prod_path, "%s/producer", sibling_dir);
    snprintf(cons_path, sizeof cons_path, "%s/consumer", sibling_dir);

    int prod_pipe[NPROD][2], cons_pipe[NCONS][2];
    pid_t prod_pid[NPROD], cons_pid[NCONS];

    for (int i = 0; i < NPROD; i++) {
        if (pipe(prod_pipe[i]) != 0) {
            perror("pipe: producer report");
            return 1;
        }
    }
    for (int i = 0; i < NCONS; i++) {
        if (pipe(cons_pipe[i]) != 0) {
            perror("pipe: consumer report");
            return 1;
        }
    }

    char dur_arg[32];
    snprintf(dur_arg, sizeof dur_arg, "%ld", duration_s);

    for (int i = 0; i < NPROD; i++) {
        pid_t pid = fork();
        if (pid < 0) {
            perror("fork: producer");
            return 1;
        }
        if (pid == 0) {
            close(prod_pipe[i][0]);
            for (int j = 0; j < NPROD; j++) {
                if (j != i) {
                    close(prod_pipe[j][0]);
                    close(prod_pipe[j][1]);
                }
            }
            for (int j = 0; j < NCONS; j++) {
                close(cons_pipe[j][0]);
                close(cons_pipe[j][1]);
            }
            char fd_arg[32];
            snprintf(fd_arg, sizeof fd_arg, "%d", prod_pipe[i][1]);
            char *cargs[] = { (char *)"producer", (char *)id, (char *)"0", dur_arg,
                              (char *)"--report-fd", fd_arg, NULL };
            execv(prod_path, cargs);
            perror("execv: producer");
            _exit(127);
        }
        prod_pid[i] = pid;
        close(prod_pipe[i][1]);
    }

    for (int i = 0; i < NCONS; i++) {
        pid_t pid = fork();
        if (pid < 0) {
            perror("fork: consumer");
            return 1;
        }
        if (pid == 0) {
            close(cons_pipe[i][0]);
            for (int j = 0; j < NPROD; j++) {
                close(prod_pipe[j][0]);
                close(prod_pipe[j][1]);
            }
            for (int j = 0; j < NCONS; j++) {
                if (j != i) {
                    close(cons_pipe[j][0]);
                    close(cons_pipe[j][1]);
                }
            }
            char fd_arg[32];
            snprintf(fd_arg, sizeof fd_arg, "%d", cons_pipe[i][1]);
            char *cargs[] = { (char *)"consumer", (char *)id, (char *)"0",
                              (char *)"--report-fd", fd_arg, NULL };
            execv(cons_path, cargs);
            perror("execv: consumer");
            _exit(127);
        }
        cons_pid[i] = pid;
        close(cons_pipe[i][1]);
    }

    ShmRO ro;
    memset(&ro, 0, sizeof ro);
    uint64_t attach_deadline = now_ns() + 15000000000ull;
    for (;;) {
        if (shm_attach_ro(&ro, id) == 0)
            break;
        if (now_ns() >= attach_deadline) {
            perror("shm_attach_ro: stress segment never appeared");
            for (int i = 0; i < NPROD; i++)
                kill(prod_pid[i], SIGTERM);
            for (int i = 0; i < NCONS; i++)
                kill(cons_pid[i], SIGTERM);
            for (int i = 0; i < NPROD; i++)
                waitpid(prod_pid[i], NULL, 0);
            for (int i = 0; i < NCONS; i++)
                waitpid(cons_pid[i], NULL, 0);
            return 1;
        }
        struct timespec sl = { 0, 100000000L };
        nanosleep(&sl, NULL);
    }

    for (int i = 0; i < NPROD; i++) {
        int status = 0;
        if (waitpid(prod_pid[i], &status, 0) < 0) {
            perror("waitpid: producer");
            return 1;
        }
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            fprintf(stderr, "stress_test: producer %d exited with status %d\n", (int)prod_pid[i],
                    status);
            for (int j = 0; j < NCONS; j++)
                kill(cons_pid[j], SIGTERM);
            for (int j = 0; j < NCONS; j++)
                waitpid(cons_pid[j], NULL, 0);
            return 1;
        }
    }

    uint64_t produced = 0, consumed = 0;
    uint64_t poll_deadline = now_ns() + 10000000000ull;
    uint64_t stable_since = 0;
    uint64_t last_p = UINT64_MAX;
    int converged = 0;
    while (now_ns() < poll_deadline) {
        if (read_totals(&ro, &produced, &consumed) == 0 && produced == consumed && produced > 0) {
            uint64_t n = now_ns();
            if (produced != last_p) {
                last_p = produced;
                stable_since = n;
            } else if (n - stable_since >= 200000000ull) {
                converged = 1;
                break;
            }
        } else {
            last_p = UINT64_MAX;
            stable_since = 0;
        }
        struct timespec sl = { 0, 100000000L };
        nanosleep(&sl, NULL);
    }
    if (!converged) {
        read_totals(&ro, &produced, &consumed);
        fprintf(stderr,
                "stress_test: drain did not converge in 10s (produced=%llu consumed=%llu)\n",
                (unsigned long long)produced, (unsigned long long)consumed);
    }

    for (int i = 0; i < NCONS; i++) {
        if (kill(cons_pid[i], SIGTERM) != 0)
            perror("kill: consumer SIGTERM");
    }
    for (int i = 0; i < NCONS; i++) {
        int status = 0;
        if (waitpid(cons_pid[i], &status, 0) < 0) {
            perror("waitpid: consumer");
            return 1;
        }
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            fprintf(stderr, "stress_test: consumer %d exited with status %d\n", (int)cons_pid[i],
                    status);
            converged = 0;
        }
    }

    ProducerReport preps[NPROD];
    ConsumerReport creps[NCONS];
    uint64_t prod_count_sum = 0, cons_count_sum = 0;
    uint64_t prod_hash_sum = 0, cons_hash_sum = 0;
    uint64_t bp_total = 0;

    for (int i = 0; i < NPROD; i++) {
        if (read_full(prod_pipe[i][0], &preps[i], sizeof(ProducerReport)) != 0) {
            fprintf(stderr, "stress_test: short read of producer report %d\n", i);
            return 1;
        }
        prod_count_sum += preps[i].count;
        prod_hash_sum += preps[i].hash_sum;
    }
    for (int i = 0; i < NCONS; i++) {
        if (read_full(cons_pipe[i][0], &creps[i], sizeof(ConsumerReport)) != 0) {
            fprintf(stderr, "stress_test: short read of consumer report %d\n", i);
            return 1;
        }
        cons_count_sum += creps[i].count;
        cons_hash_sum += creps[i].hash_sum;
    }

    uint64_t final_produced = 0, final_consumed = 0;
    int final_read_ok = read_totals(&ro, &final_produced, &final_consumed) == 0;

    for (uint32_t i = 0; i < SHM_MAX_WRITERS; i++) {
        uint32_t s1 = atomic_load_explicit(&ro.mx->w[i].seq, memory_order_acquire);
        if (s1 & 1u)
            continue;
        bp_total += atomic_load_explicit(&ro.mx->w[i].backpressure_events, memory_order_relaxed);
    }

    int fail = 0;
    if (!converged) {
        fprintf(stderr, "FAIL: consumers did not drain the ring\n");
        fail = 1;
    }
    if (prod_count_sum != cons_count_sum) {
        fprintf(stderr, "FAIL: producer count sum %llu != consumer count sum %llu\n",
                (unsigned long long)prod_count_sum, (unsigned long long)cons_count_sum);
        fail = 1;
    }
    if (prod_count_sum <= 2ull * SHM_CAPACITY) {
        fprintf(stderr, "FAIL: only %llu messages; the ring never wrapped (need > %llu)\n",
                (unsigned long long)prod_count_sum, 2ull * SHM_CAPACITY);
        fail = 1;
    }
    if (prod_hash_sum != cons_hash_sum) {
        fprintf(stderr, "FAIL: producer hash sum %llu != consumer hash sum %llu\n",
                (unsigned long long)prod_hash_sum, (unsigned long long)cons_hash_sum);
        fail = 1;
    }
    for (int i = 0; i < NCONS; i++) {
        if (creps[i].crc_errors != 0 || creps[i].order_errors != 0) {
            fprintf(stderr, "FAIL: consumer %d crc_errors=%llu order_errors=%llu\n", i,
                    (unsigned long long)creps[i].crc_errors,
                    (unsigned long long)creps[i].order_errors);
            fail = 1;
        }
    }
    if (!final_read_ok || final_produced != prod_count_sum || final_consumed != cons_count_sum) {
        fprintf(stderr, "FAIL: final metrics produced=%llu consumed=%llu, reports produced=%llu consumed=%llu\n",
                (unsigned long long)final_produced, (unsigned long long)final_consumed,
                (unsigned long long)prod_count_sum, (unsigned long long)cons_count_sum);
        fail = 1;
    }

    shm_detach_ro(&ro);

    {
        char ring_name_check[128];
        snprintf(ring_name_check, sizeof ring_name_check, "/shm_ring_%s", id);
        int sfd = shm_open(ring_name_check, O_RDONLY, 0);
        if (sfd >= 0) {
            close(sfd);
            fprintf(stderr, "FAIL: %s still exists after all children exited (last-out unlink broken)\n",
                    ring_name_check);
            fail = 1;
        } else if (errno != ENOENT) {
            perror("shm_open: final unlink check");
            fail = 1;
        }
    }

    double secs = (double)duration_s;
    printf("=== stress results (id=%s, %lds) ===\n", id, duration_s);
    printf("producers            %d\n", NPROD);
    printf("consumers            %d\n", NCONS);
    printf("messages             %llu\n", (unsigned long long)prod_count_sum);
    printf("throughput           %.0f msgs/s\n", secs > 0 ? (double)prod_count_sum / secs : 0.0);
    printf("backpressure events  %llu\n", (unsigned long long)bp_total);

    if (fail) {
        fprintf(stderr, "STRESS FAIL\n");
        return 1;
    }
    printf("STRESS PASS\n");
    return 0;
}
