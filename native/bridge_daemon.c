#include "shm_common.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define JSON_CAP (64u * 1024u)
#define B64_CAP (4u * ((SHM_CAPACITY + 2u) / 3u) + 4u)
#define RECENT_SLOTS 64u

static volatile sig_atomic_t g_stop = 0;

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

static void install_handlers(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (sigaction(SIGINT, &sa, NULL) != 0)
        perror("sigaction: SIGINT");
    if (sigaction(SIGTERM, &sa, NULL) != 0)
        perror("sigaction: SIGTERM");
    signal(SIGPIPE, SIG_IGN);
}

typedef struct {
    uint64_t produced, consumed, backpressure, crc_errors, seq_errors;
    uint64_t min_lat, max_lat, sum_lat, last_ts;
    uint64_t lat_bucket[SHM_LAT_BUCKETS];
} MxRead;

static size_t ja(char *b, size_t cap, size_t off, const char *fmt, ...)
    __attribute__((format(printf, 4, 5)));

static size_t ja(char *b, size_t cap, size_t off, const char *fmt, ...)
{
    if (off >= cap)
        return cap;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(b + off, cap - off, fmt, ap);
    va_end(ap);
    if (n < 0)
        return cap;
    if ((size_t)n >= cap - off)
        return cap;
    return off + (size_t)n;
}

static const char B64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static size_t base64_encode(const unsigned char *src, size_t n, char *out)
{
    size_t o = 0;
    for (size_t i = 0; i < n; i += 3) {
        uint32_t v = (uint32_t)src[i] << 16;
        size_t rem = n - i;
        if (rem > 1)
            v |= (uint32_t)src[i + 1] << 8;
        if (rem > 2)
            v |= (uint32_t)src[i + 2];
        out[o++] = B64[(v >> 18) & 63u];
        out[o++] = B64[(v >> 12) & 63u];
        out[o++] = rem > 1 ? B64[(v >> 6) & 63u] : '=';
        out[o++] = rem > 2 ? B64[v & 63u] : '=';
    }
    out[o] = '\0';
    return o;
}

static int read_writer(const MetricsWriter *w, MxRead *out)
{
    for (int t = 0; t < 8; t++) {
        uint32_t s1 = atomic_load_explicit(&w->seq, memory_order_acquire);
        if (s1 & 1u)
            continue;
        out->produced = atomic_load_explicit(&w->produced, memory_order_relaxed);
        out->consumed = atomic_load_explicit(&w->consumed, memory_order_relaxed);
        out->backpressure = atomic_load_explicit(&w->backpressure_events, memory_order_relaxed);
        out->crc_errors = atomic_load_explicit(&w->crc_errors, memory_order_relaxed);
        out->seq_errors = atomic_load_explicit(&w->seq_errors, memory_order_relaxed);
        out->min_lat = atomic_load_explicit(&w->min_latency_ns, memory_order_relaxed);
        out->max_lat = atomic_load_explicit(&w->max_latency_ns, memory_order_relaxed);
        out->sum_lat = atomic_load_explicit(&w->sum_latency_ns, memory_order_relaxed);
        out->last_ts = atomic_load_explicit(&w->last_ts_ns, memory_order_relaxed);
        for (uint32_t b = 0; b < SHM_LAT_BUCKETS; b++)
            out->lat_bucket[b] = atomic_load_explicit(&w->lat_bucket[b], memory_order_relaxed);
        atomic_thread_fence(memory_order_acquire);
        uint32_t s2 = atomic_load_explicit(&w->seq, memory_order_relaxed);
        if (s1 == s2)
            return 0;
    }
    return -1;
}

__attribute__((no_sanitize("thread")))
static void snapshot_slot(const MessageSlot *slot, MessageSlot *dst)
{
    dst->msg_id = slot->msg_id;
    dst->timestamp_ns = slot->timestamp_ns;
    dst->sender_pid = slot->sender_pid;
    dst->producer_idx = slot->producer_idx;
    dst->payload_len = slot->payload_len;
    dst->crc32 = slot->crc32;
    uint32_t n = dst->payload_len <= SHM_PAYLOAD_MAX ? dst->payload_len : SHM_PAYLOAD_MAX;
    memcpy(dst->payload, slot->payload, n);
}

static size_t json_escape_preview(char *out, size_t cap, const char *p, uint32_t len)
{
    uint32_t n = len > 32u ? 32u : len;
    size_t o = 0;
    for (uint32_t i = 0; i < n && o + 2 < cap; i++) {
        unsigned char c = (unsigned char)p[i];
        if (c == '"') {
            out[o++] = '\\';
            out[o++] = '"';
        } else if (c == '\\') {
            out[o++] = '\\';
            out[o++] = '\\';
        } else if (c >= 0x20 && c <= 0x7e) {
            out[o++] = (char)c;
        } else {
            out[o++] = '.';
        }
    }
    out[o] = '\0';
    return o;
}

static uint64_t epoch_ms(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        perror("clock_gettime: CLOCK_REALTIME");
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)(ts.tv_nsec / 1000000L);
}

int main(int argc, char **argv)
{
    const char *id = "0";
    char sock_path[sizeof(((struct sockaddr_un *)0)->sun_path)];
    unsigned interval_ms = 50;
    int interval_set = 0;
    int pos = 0;

    for (int i = 1; i < argc; i++) {
        if (pos == 0) {
            id = argv[i];
            pos++;
        } else if (pos == 1) {
            snprintf(sock_path, sizeof sock_path, "%s", argv[i]);
            pos++;
        } else if (pos == 2) {
            char *end = NULL;
            long v = strtol(argv[i], &end, 10);
            if (end == NULL || *end != '\0' || v < 1 || v > 60000) {
                fprintf(stderr, "bridge_daemon: invalid interval_ms '%s'\n", argv[i]);
                return 2;
            }
            interval_ms = (unsigned)v;
            interval_set = 1;
            pos++;
        } else {
            fprintf(stderr, "usage: bridge_daemon [instance_id] [socket_path] [interval_ms]\n");
            return 2;
        }
    }
    (void)interval_set;
    if (shm_valid_id(id) != 0)
        return 1;
    if (pos < 2)
        snprintf(sock_path, sizeof sock_path, "/tmp/shm_bridge_%s.sock", id);
    if (strlen(sock_path) >= sizeof(((struct sockaddr_un *)0)->sun_path)) {
        fprintf(stderr, "bridge_daemon: socket path too long for sun_path\n");
        return 1;
    }

    install_handlers();
    umask(077);

    int listener = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listener < 0) {
        perror("socket: AF_UNIX");
        return 1;
    }
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof addr);
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof addr.sun_path, "%s", sock_path);
    unlink(sock_path);
    if (bind(listener, (struct sockaddr *)&addr, sizeof addr) != 0) {
        perror("bind: bridge socket");
        close(listener);
        return 1;
    }
    if (listen(listener, 4) != 0) {
        perror("listen: bridge socket");
        close(listener);
        unlink(sock_path);
        return 1;
    }
    if (fcntl(listener, F_SETFL, O_NONBLOCK) != 0) {
        perror("fcntl: listener O_NONBLOCK");
        close(listener);
        unlink(sock_path);
        return 1;
    }

    static char json[JSON_CAP];
    static unsigned char slot_states[SHM_CAPACITY];
    static char b64[B64_CAP];

    int client = -1;
    ShmRO ro;
    memset(&ro, 0, sizeof ro);
    ro.ring_fd = -1;
    ro.mx_fd = -1;
    int attached = 0;
    uint64_t frame_seq = 0;
    uint64_t now = now_ns();
    uint64_t next = now + (uint64_t)interval_ms * 1000000ull;
    uint64_t last_attach_try = 0;
    uint64_t last_current_check = now;

    while (!g_stop) {
        now = now_ns();
        if ((int64_t)(now - next) > 1000000000LL)
            next = now;
        if (now < next) {
            struct timespec ts;
            ts.tv_sec = (time_t)(next / 1000000000ull);
            ts.tv_nsec = (long)(next % 1000000000ull);
            int rc = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, NULL);
            if (rc != 0 && rc != EINTR) {
                errno = rc;
                perror("clock_nanosleep: bridge pacing");
                break;
            }
        }
        next += (uint64_t)interval_ms * 1000000ull;
        if (g_stop)
            break;

        for (;;) {
            int c = accept(listener, NULL, NULL);
            if (c < 0)
                break;
            if (client >= 0)
                close(client);
            client = c;
            int sndbuf = 1 << 20;
            if (setsockopt(client, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof sndbuf) != 0)
                perror("setsockopt: SO_SNDBUF");
        }

        now = now_ns();
        if (!attached) {
            if (now - last_attach_try >= 500000000ull) {
                last_attach_try = now;
                if (shm_attach_ro(&ro, id) == 0) {
                    attached = 1;
                    last_current_check = now;
                }
            }
        } else {
            if (now - last_current_check >= 1000000000ull) {
                last_current_check = now;
                if (!shm_ro_is_current(&ro, id)) {
                    shm_detach_ro(&ro);
                    attached = 0;
                }
            }
        }

        size_t off = 0;
        uint64_t t_ms = epoch_ms();

        uint64_t produced = 0, consumed = 0, backpressure = 0, crc_err = 0, seq_err = 0;
        uint64_t sum_lat = 0;
        uint64_t lat_bucket[SHM_LAT_BUCKETS] = { 0, 0, 0, 0, 0 };
        uint64_t min_lat = 0, max_lat = 0, avg_lat = 0;
        uint64_t enq = 0, deq = 0, occ = 0;
        uint32_t refcount = 0;

        if (attached && ro.rb != NULL && ro.mx != NULL) {
            deq = atomic_load_explicit(&ro.rb->dequeue_pos, memory_order_acquire);
            enq = atomic_load_explicit(&ro.rb->enqueue_pos, memory_order_acquire);
            occ = enq >= deq ? enq - deq : 0;
            refcount = (uint32_t)atomic_load_explicit(&ro.rb->refcount, memory_order_relaxed);

            MxRead wr;
            uint64_t wmin = UINT64_MAX, wmax = 0;
            int have_min = 0;
            for (uint32_t i = 0; i < SHM_MAX_WRITERS; i++) {
                if (read_writer(&ro.mx->w[i], &wr) != 0)
                    continue;
                produced += wr.produced;
                consumed += wr.consumed;
                backpressure += wr.backpressure;
                crc_err += wr.crc_errors;
                seq_err += wr.seq_errors;
                sum_lat += wr.sum_lat;
                for (uint32_t b = 0; b < SHM_LAT_BUCKETS; b++)
                    lat_bucket[b] += wr.lat_bucket[b];
                if (wr.consumed > 0 && wr.min_lat > 0) {
                    if (!have_min || wr.min_lat < wmin) {
                        wmin = wr.min_lat;
                        have_min = 1;
                    }
                    if (wr.max_lat > wmax)
                        wmax = wr.max_lat;
                }
            }
            if (have_min) {
                min_lat = wmin;
                max_lat = wmax;
            }
            avg_lat = consumed > 0 ? sum_lat / consumed : 0;

            for (uint32_t i = 0; i < SHM_CAPACITY; i++) {
                uint64_t s = atomic_load_explicit(&ro.rb->slots[i].seq, memory_order_acquire);
                unsigned state;
                if (s % SHM_CAPACITY == (uint64_t)i) {
                    state = s < enq ? 2u : 0u;
                } else {
                    state = (s - 1ull) < deq ? 3u : 1u;
                }
                slot_states[i] = (unsigned char)state;
            }
            base64_encode(slot_states, SHM_CAPACITY, b64);
        } else {
            memset(slot_states, 0, sizeof slot_states);
            base64_encode(slot_states, SHM_CAPACITY, b64);
        }

        off = ja(json, JSON_CAP, off,
                 "{\"t\":%llu,\"frame_seq\":%llu,\"interval_ms\":%u,\"attached\":%s,\"metrics\":{"
                 "\"produced_total\":%llu,\"consumed_total\":%llu,\"backpressure_events\":%llu,"
                 "\"crc_errors\":%llu,\"seq_errors\":%llu,\"min_latency_ns\":%llu,"
                 "\"max_latency_ns\":%llu,\"avg_latency_ns\":%llu,"
                 "\"latency_buckets\":[%llu,%llu,%llu,%llu,%llu],\"enqueue_pos\":%llu,"
                 "\"dequeue_pos\":%llu,\"occupancy\":%llu,\"capacity\":%u,\"refcount\":%u},"
                 "\"writers\":[",
                 (unsigned long long)t_ms, (unsigned long long)frame_seq, interval_ms,
                 attached ? "true" : "false", (unsigned long long)produced,
                 (unsigned long long)consumed, (unsigned long long)backpressure,
                 (unsigned long long)crc_err, (unsigned long long)seq_err,
                 (unsigned long long)min_lat, (unsigned long long)max_lat,
                 (unsigned long long)avg_lat, (unsigned long long)lat_bucket[0],
                 (unsigned long long)lat_bucket[1], (unsigned long long)lat_bucket[2],
                 (unsigned long long)lat_bucket[3], (unsigned long long)lat_bucket[4],
                 (unsigned long long)enq, (unsigned long long)deq, (unsigned long long)occ,
                 SHM_CAPACITY, refcount);

        if (attached && ro.mx != NULL) {
            MxRead wr;
            int first = 1;
            for (uint32_t i = 0; i < SHM_MAX_WRITERS; i++) {
                uint32_t role = atomic_load_explicit(&ro.mx->w[i].role, memory_order_relaxed);
                if (role == 0)
                    continue;
                if (read_writer(&ro.mx->w[i], &wr) != 0)
                    continue;
                uint32_t pid = atomic_load_explicit(&ro.mx->w[i].pid, memory_order_relaxed);
                int alive = pid != 0 && (kill((pid_t)pid, 0) == 0 || errno == EPERM);
                uint64_t seen = 0;
                if (wr.last_ts != 0 && now >= wr.last_ts)
                    seen = (now - wr.last_ts) / 1000000ull;
                off = ja(json, JSON_CAP, off,
                         "%s{\"slot\":%u,\"pid\":%u,\"role\":\"%s\",\"alive\":%s,\"produced\":%llu,"
                         "\"consumed\":%llu,\"backpressure_events\":%llu,\"crc_errors\":%llu,"
                         "\"seq_errors\":%llu,\"last_seen_ms_ago\":%llu}",
                         first ? "" : ",", i, pid, role == 1 ? "producer" : "consumer",
                         alive ? "true" : "false", (unsigned long long)wr.produced,
                         (unsigned long long)wr.consumed, (unsigned long long)wr.backpressure,
                         (unsigned long long)wr.crc_errors, (unsigned long long)wr.seq_errors,
                         (unsigned long long)seen);
                first = 0;
            }
        }

        off = ja(json, JSON_CAP, off, "],\"slot_states\":\"%s\",\"slots\":[", b64);

        if (attached && ro.rb != NULL) {
            uint64_t lo = enq > RECENT_SLOTS ? enq - RECENT_SLOTS : 0;
            int sfirst = 1;
            for (uint64_t spos = lo; spos < enq; spos++) {
                const MessageSlot *slot = &ro.rb->slots[spos & SHM_MASK];
                uint64_t s1 = atomic_load_explicit(&slot->seq, memory_order_acquire);
                if (s1 != spos + 1ull && s1 != spos + SHM_CAPACITY)
                    continue;
                MessageSlot snap;
                memset(&snap, 0, sizeof snap);
                snapshot_slot(slot, &snap);
                uint64_t s2 = atomic_load_explicit(&slot->seq, memory_order_acquire);
                if (s2 != s1)
                    continue;
                uint64_t age = now >= snap.timestamp_ns ? now - snap.timestamp_ns : 0;
                uint32_t crc = crc32_ieee(snap.payload, snap.payload_len);
                char prev[128];
                json_escape_preview(prev, sizeof prev, snap.payload, snap.payload_len);
                off = ja(json, JSON_CAP, off,
                         "%s{\"idx\":%llu,\"pos\":%llu,\"msg_id\":%llu,\"sender_pid\":%u,"
                         "\"producer_idx\":%u,\"age_ns\":%llu,\"payload_len\":%u,\"crc32\":%u,"
                         "\"crc_ok\":%s,\"preview\":\"%s\"}",
                         sfirst ? "" : ",", (unsigned long long)(spos & SHM_MASK),
                         (unsigned long long)spos, (unsigned long long)snap.msg_id, snap.sender_pid,
                         snap.producer_idx, (unsigned long long)age, snap.payload_len, snap.crc32,
                         crc == snap.crc32 ? "true" : "false", prev);
                sfirst = 0;
            }
        }

        off = ja(json, JSON_CAP, off, "]}\n");

        frame_seq++;

        if (off >= JSON_CAP) {
            fprintf(stderr, "bridge_daemon: frame exceeded %u byte buffer, dropping\n", JSON_CAP);
            continue;
        }
        if (client >= 0) {
            ssize_t sent = send(client, json, off, MSG_NOSIGNAL | MSG_DONTWAIT);
            if (sent < 0 || (size_t)sent != off) {
                close(client);
                client = -1;
            }
        }
    }

    if (client >= 0)
        close(client);
    close(listener);
    unlink(sock_path);
    if (attached)
        shm_detach_ro(&ro);
    return 0;
}
