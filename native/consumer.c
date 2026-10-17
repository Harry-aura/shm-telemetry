#include "shm_common.h"

#include <errno.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define LAT_RING 1024u

static volatile sig_atomic_t g_stop = 0;

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

static int install_handlers(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (sigaction(SIGINT, &sa, NULL) != 0) {
        perror("sigaction: SIGINT");
        return -1;
    }
    if (sigaction(SIGTERM, &sa, NULL) != 0) {
        perror("sigaction: SIGTERM");
        return -1;
    }
    return 0;
}

static int write_full(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

static int cmp_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

static uint32_t lat_bucket_of(uint64_t dt)
{
    if (dt < 1000ull)
        return 0;
    if (dt < 10000ull)
        return 1;
    if (dt < 100000ull)
        return 2;
    if (dt < 1000000ull)
        return 3;
    return 4;
}

static int acquire_occupied_slot(ShmHandle *h, MxLocal *local, uint64_t start, long duration_s)
{
    if (sem_trywait(h->sem_occ) == 0)
        return 1;
    if (errno != EAGAIN) {
        perror("sem_trywait: occupied slot");
        return -1;
    }
    for (;;) {
        if (g_stop)
            return 0;
        if (duration_s > 0 && now_ns() - start >= (uint64_t)duration_s * 1000000000ull)
            return 0;
        struct timespec abs;
        if (clock_gettime(CLOCK_REALTIME, &abs) != 0) {
            perror("clock_gettime: CLOCK_REALTIME");
            return -1;
        }
        abs.tv_nsec += 100000000L;
        if (abs.tv_nsec >= 1000000000L) {
            abs.tv_sec += 1;
            abs.tv_nsec -= 1000000000L;
        }
        if (sem_timedwait(h->sem_occ, &abs) == 0)
            return 1;
        if (errno == EAGAIN || errno == EINTR || errno == ETIMEDOUT) {
            if (local->unsynced > 0)
                mx_flush(h->mx, local);
            continue;
        }
        perror("sem_timedwait: occupied slot");
        return -1;
    }
}

int main(int argc, char **argv)
{
    const char *id = "0";
    long duration_s = 0;
    int report_fd = -1;
    int pos = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--report-fd") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "consumer: --report-fd requires an argument\n");
                return 2;
            }
            char *end = NULL;
            long v = strtol(argv[i + 1], &end, 10);
            if (end == NULL || *end != '\0' || v < 0 || v > 1 << 20) {
                fprintf(stderr, "consumer: invalid --report-fd value '%s'\n", argv[i + 1]);
                return 2;
            }
            report_fd = (int)v;
            i++;
        } else if (pos == 0) {
            id = argv[i];
            pos++;
        } else if (pos == 1) {
            char *end = NULL;
            duration_s = strtol(argv[i], &end, 10);
            if (end == NULL || *end != '\0' || duration_s < 0 || duration_s > 86400L) {
                fprintf(stderr, "consumer: invalid duration_s '%s'\n", argv[i]);
                return 2;
            }
            pos++;
        } else {
            fprintf(stderr, "usage: consumer [instance_id] [duration_s] [--report-fd N]\n");
            return 2;
        }
    }

    if (install_handlers() != 0)
        return 1;

    ShmHandle h;
    if (shm_join(&h, id, SHM_ROLE_CONSUMER, 5000) != 0)
        return 1;

    MxLocal local;
    mx_local_begin(&local, h.mx, h.writer_idx);

    uint64_t count = 0;
    uint64_t hash_sum = 0;
    uint64_t crc_errors = 0;
    uint64_t seq_errors = 0;
    uint64_t last_seq[SHM_MAX_WRITERS];
    uint32_t last_pid[SHM_MAX_WRITERS];
    memset(last_seq, 0, sizeof last_seq);
    memset(last_pid, 0, sizeof last_pid);

    uint64_t lat_ring[LAT_RING];
    uint32_t lat_n = 0;
    uint32_t lat_pos = 0;
    memset(lat_ring, 0, sizeof lat_ring);

    uint64_t start = now_ns();
    uint64_t next_report = start + 1000000000ull;
    uint64_t crc_log_ns = 0;

    for (;;) {
        int got = acquire_occupied_slot(&h, &local, start, duration_s);
        if (got < 0)
            return 1;
        if (got == 0)
            break;

        uint64_t pos64 = atomic_fetch_add_explicit(&h.rb->dequeue_pos, 1, memory_order_relaxed);
        MessageSlot *slot = &h.rb->slots[pos64 & SHM_MASK];

        uint64_t spins = 0;
        uint64_t spin_start = now_ns();
        for (;;) {
            if (atomic_load_explicit(&slot->seq, memory_order_acquire) == pos64 + 1)
                break;
            spins++;
            if ((spins & 63u) == 0)
                sched_yield();
            if (now_ns() - spin_start > 2000000000ull) {
                fprintf(stderr, "consumer: slot stuck, producer dead? (pos=%llu idx=%llu)\n",
                        (unsigned long long)pos64, (unsigned long long)(pos64 & SHM_MASK));
                exit(3);
            }
        }

        MessageSlot copy = *slot;
        uint64_t recv_ns = now_ns();
        atomic_store_explicit(&slot->seq, pos64 + SHM_CAPACITY, memory_order_release);
        if (sem_post(h.sem_free) != 0) {
            perror("sem_post: free slot");
            return 1;
        }

        uint32_t crc = crc32_ieee(copy.payload, copy.payload_len);
        if (crc != copy.crc32) {
            crc_errors++;
            if (crc_log_ns == 0 || recv_ns - crc_log_ns >= 1000000000ull) {
                crc_log_ns = recv_ns;
                fprintf(stderr, "consumer: CRC mismatch on msg %llu from pid %u (total %llu)\n",
                        (unsigned long long)copy.msg_id, copy.sender_pid,
                        (unsigned long long)crc_errors);
            }
        }

        if (copy.producer_idx < SHM_MAX_WRITERS) {
            if (last_pid[copy.producer_idx] != copy.sender_pid) {
                last_pid[copy.producer_idx] = copy.sender_pid;
                last_seq[copy.producer_idx] = copy.msg_id;
            } else {
                if (copy.msg_id <= last_seq[copy.producer_idx])
                    seq_errors++;
                last_seq[copy.producer_idx] = copy.msg_id;
            }
        } else {
            seq_errors++;
        }

        if (copy.timestamp_ns != 0 && recv_ns >= copy.timestamp_ns) {
            uint64_t dt = recv_ns - copy.timestamp_ns;
            local.sum_lat += dt;
            if (dt < local.min_lat)
                local.min_lat = dt;
            if (dt > local.max_lat)
                local.max_lat = dt;
            local.lat_bucket[lat_bucket_of(dt)]++;
            lat_ring[lat_pos] = dt;
            lat_pos = (lat_pos + 1) % LAT_RING;
            if (lat_n < LAT_RING)
                lat_n++;
        }

        hash_sum += mix64(((uint64_t)copy.producer_idx << 40) | (copy.msg_id & 0xFFFFFFFFFFull));
        count++;
        local.consumed = count;
        local.crc_errors = crc_errors;
        local.seq_errors = seq_errors;
        local.last_ts_ns = recv_ns;
        mx_flush_due(h.mx, &local);

        if (now_ns() >= next_report) {
            uint64_t sorted[LAT_RING];
            memcpy(sorted, lat_ring, lat_n * sizeof(uint64_t));
            double p50 = 0.0, p99 = 0.0;
            if (lat_n > 0) {
                qsort(sorted, lat_n, sizeof(uint64_t), cmp_u64);
                size_t i50 = (size_t)lat_n * 50u / 100u;
                size_t i99 = (size_t)lat_n * 99u / 100u;
                if (i50 >= lat_n)
                    i50 = lat_n - 1;
                if (i99 >= lat_n)
                    i99 = lat_n - 1;
                p50 = (double)sorted[i50] / 1000.0;
                p99 = (double)sorted[i99] / 1000.0;
            }
            printf("consumed=%llu crc_err=%llu seq_err=%llu p50=%.2fus p99=%.2fus\n",
                   (unsigned long long)count, (unsigned long long)crc_errors,
                   (unsigned long long)seq_errors, p50, p99);
            fflush(stdout);
            next_report += 1000000000ull;
            if (now_ns() >= next_report + 1000000000ull)
                next_report = now_ns() + 1000000000ull;
        }
    }

    local.consumed = count;
    local.crc_errors = crc_errors;
    local.seq_errors = seq_errors;
    mx_flush(h.mx, &local);

    if (report_fd >= 0) {
        struct {
            uint64_t count;
            uint64_t hash_sum;
            uint64_t crc_errors;
            uint64_t order_errors;
        } rep;
        rep.count = count;
        rep.hash_sum = hash_sum;
        rep.crc_errors = crc_errors;
        rep.order_errors = seq_errors;
        if (write_full(report_fd, &rep, sizeof rep) != 0) {
            perror("write: consumer report");
            return 1;
        }
    }

    shm_leave(&h);
    if (report_fd >= 0)
        close(report_fd);
    return 0;
}
