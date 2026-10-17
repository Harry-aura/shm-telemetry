#include "shm_common.h"

#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

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

static uint32_t payload_len_for(uint64_t msg_id)
{
    return 24u + (uint32_t)((msg_id * 2654435761ull) % 177u);
}

static int acquire_free_slot(ShmHandle *h, MxLocal *local, uint64_t *backpressure)
{
    if (sem_trywait(h->sem_free) == 0)
        return 1;
    if (errno != EAGAIN) {
        perror("sem_trywait: free slot");
        return -1;
    }
    (*backpressure)++;
    for (;;) {
        if (g_stop)
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
        if (sem_timedwait(h->sem_free, &abs) == 0)
            return 1;
        if (errno == EAGAIN || errno == EINTR || errno == ETIMEDOUT) {
            local->backpressure = *backpressure;
            if (local->unsynced > 0)
                mx_flush(h->mx, local);
            continue;
        }
        perror("sem_timedwait: free slot");
        return -1;
    }
}

int main(int argc, char **argv)
{
    const char *id = "0";
    long rate_hz = 100;
    long duration_s = 0;
    int report_fd = -1;
    int pos = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--report-fd") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "producer: --report-fd requires an argument\n");
                return 2;
            }
            char *end = NULL;
            long v = strtol(argv[i + 1], &end, 10);
            if (end == NULL || *end != '\0' || v < 0 || v > 1 << 20) {
                fprintf(stderr, "producer: invalid --report-fd value '%s'\n", argv[i + 1]);
                return 2;
            }
            report_fd = (int)v;
            i++;
        } else if (pos == 0) {
            id = argv[i];
            pos++;
        } else if (pos == 1) {
            char *end = NULL;
            rate_hz = strtol(argv[i], &end, 10);
            if (end == NULL || *end != '\0' || rate_hz < 0 || rate_hz > 10000000L) {
                fprintf(stderr, "producer: invalid rate_hz '%s'\n", argv[i]);
                return 2;
            }
            pos++;
        } else if (pos == 2) {
            char *end = NULL;
            duration_s = strtol(argv[i], &end, 10);
            if (end == NULL || *end != '\0' || duration_s < 0 || duration_s > 86400L) {
                fprintf(stderr, "producer: invalid duration_s '%s'\n", argv[i]);
                return 2;
            }
            pos++;
        } else {
            fprintf(stderr, "usage: producer [instance_id] [rate_hz] [duration_s] [--report-fd N]\n");
            return 2;
        }
    }

    if (install_handlers() != 0)
        return 1;

    ShmHandle h;
    if (shm_join(&h, id, SHM_ROLE_PRODUCER, 5000) != 0)
        return 1;

    MxLocal local;
    mx_local_begin(&local, h.mx, h.writer_idx);

    uint64_t msg_id = 0;
    uint64_t count = 0;
    uint64_t hash_sum = 0;
    uint64_t backpressure = 0;
    uint64_t period_ns = rate_hz > 0 ? 1000000000ull / (uint64_t)rate_hz : 0;
    uint64_t start = now_ns();
    uint64_t next = start + period_ns;

    for (;;) {
        if (g_stop)
            break;
        if (duration_s > 0 && now_ns() - start >= (uint64_t)duration_s * 1000000000ull)
            break;
        if (rate_hz > 0) {
            uint64_t n = now_ns();
            if ((int64_t)(n - next) > 1000000000LL)
                next = n;
            struct timespec ts;
            ts.tv_sec = (time_t)(next / 1000000000ull);
            ts.tv_nsec = (long)(next % 1000000000ull);
            int rc = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, NULL);
            if (rc != 0 && rc != EINTR) {
                errno = rc;
                perror("clock_nanosleep: pacing");
                break;
            }
            next += period_ns;
            if (g_stop)
                break;
        }

        int got = acquire_free_slot(&h, &local, &backpressure);
        if (got < 0)
            return 1;
        if (got == 0)
            break;

        uint64_t pos64 = atomic_fetch_add_explicit(&h.rb->enqueue_pos, 1, memory_order_relaxed);
        MessageSlot *slot = &h.rb->slots[pos64 & SHM_MASK];

        uint64_t spins = 0;
        uint64_t spin_start = now_ns();
        for (;;) {
            if (atomic_load_explicit(&slot->seq, memory_order_acquire) == pos64)
                break;
            spins++;
            if ((spins & 63u) == 0)
                sched_yield();
            if (now_ns() - spin_start > 2000000000ull) {
                fprintf(stderr,
                        "producer: slot %llu stuck for 2s after claiming position (dead consumer holds it)\n",
                        (unsigned long long)(pos64 & SHM_MASK));
                return 1;
            }
        }

        char pbuf[SHM_PAYLOAD_MAX];
        int pre = snprintf(pbuf, sizeof pbuf, "p%u #%llu ", (unsigned)h.writer_idx,
                           (unsigned long long)msg_id);
        if (pre < 0)
            pre = 0;
        uint32_t plen = payload_len_for(msg_id);
        if (plen < (uint32_t)pre)
            plen = (uint32_t)pre;
        if (plen > SHM_PAYLOAD_MAX)
            plen = SHM_PAYLOAD_MAX;
        for (uint32_t i = (uint32_t)pre; i < plen; i++)
            pbuf[i] = (char)('a' + ((msg_id + i) % 26));

        slot->msg_id = msg_id;
        slot->producer_idx = (uint32_t)h.writer_idx;
        slot->sender_pid = (uint32_t)getpid();
        slot->payload_len = plen;
        slot->crc32 = crc32_ieee(pbuf, plen);
        memcpy(slot->payload, pbuf, plen);
        slot->timestamp_ns = now_ns();

        atomic_store_explicit(&slot->seq, pos64 + 1, memory_order_release);
        if (sem_post(h.sem_occ) != 0) {
            perror("sem_post: occupied slot");
            return 1;
        }

        hash_sum += mix64(((uint64_t)h.writer_idx << 40) | (msg_id & 0xFFFFFFFFFFull));
        msg_id++;
        count++;
        local.produced = count;
        local.backpressure = backpressure;
        local.last_ts_ns = slot->timestamp_ns;
        mx_flush_due(h.mx, &local);
    }

    local.produced = count;
    local.backpressure = backpressure;
    mx_flush(h.mx, &local);

    if (report_fd >= 0) {
        struct {
            uint64_t count;
            uint64_t hash_sum;
            uint32_t producer_idx;
            uint32_t pid;
        } rep;
        rep.count = count;
        rep.hash_sum = hash_sum;
        rep.producer_idx = (uint32_t)h.writer_idx;
        rep.pid = (uint32_t)getpid();
        if (write_full(report_fd, &rep, sizeof rep) != 0) {
            perror("write: producer report");
            return 1;
        }
    }

    shm_leave(&h);
    if (report_fd >= 0)
        close(report_fd);
    return 0;
}
