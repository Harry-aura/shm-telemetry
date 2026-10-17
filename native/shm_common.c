#include "shm_common.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define SHM_NAME_MAX 96
#define SHM_FLUSH_PERIOD_NS 10000000ull
#define SHM_FLUSH_MSGS 64u

static void ring_name(char *b, size_t n, const char *id) { snprintf(b, n, "/shm_ring_%s", id); }
static void metrics_name(char *b, size_t n, const char *id) { snprintf(b, n, "/shm_metrics_%s", id); }
static void sem_free_name(char *b, size_t n, const char *id) { snprintf(b, n, "/sem_free_%s", id); }
static void sem_occ_name(char *b, size_t n, const char *id) { snprintf(b, n, "/sem_occ_%s", id); }

static void sleep_ms(unsigned ms)
{
    struct timespec sl;
    sl.tv_sec = (time_t)(ms / 1000u);
    sl.tv_nsec = (long)(ms % 1000u) * 1000000L;
    nanosleep(&sl, NULL);
}

int shm_valid_id(const char *id)
{
    if (id == NULL) {
        fprintf(stderr, "shm_valid_id: id is NULL (want ^[A-Za-z0-9_]{1,16}$)\n");
        return -1;
    }
    size_t len = strlen(id);
    if (len < 1 || len > 16) {
        fprintf(stderr, "shm_valid_id: invalid instance id '%s' (want 1..16 chars of [A-Za-z0-9_])\n", id);
        return -1;
    }
    for (size_t i = 0; i < len; i++) {
        char c = id[i];
        int ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
        if (!ok) {
            fprintf(stderr, "shm_valid_id: invalid instance id '%s' (want ^[A-Za-z0-9_]{1,16}$)\n", id);
            return -1;
        }
    }
    return 0;
}

uint64_t now_ns(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        perror("clock_gettime: CLOCK_MONOTONIC");
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

uint64_t mix64(uint64_t x)
{
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ull;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebull;
    x ^= x >> 31;
    return x;
}

static uint32_t crc_table[256];
static pthread_once_t crc_once = PTHREAD_ONCE_INIT;

static void crc_init(void)
{
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++)
            c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        crc_table[i] = c;
    }
}

uint32_t crc32_ieee(const void *data, size_t len)
{
    pthread_once(&crc_once, crc_init);
    const unsigned char *p = (const unsigned char *)data;
    uint32_t c = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++)
        c = crc_table[(c ^ p[i]) & 0xFFu] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

void mx_local_begin(MxLocal *l, SharedMetrics *mx, int writer_idx)
{
    memset(l, 0, sizeof *l);
    l->idx = writer_idx;
    l->min_lat = UINT64_MAX;
    if (writer_idx < 0 || mx == NULL)
        return;
    MetricsWriter *w = &mx->w[writer_idx];
    uint32_t s = atomic_load_explicit(&w->seq, memory_order_relaxed);
    if (s & 1u)
        atomic_store_explicit(&w->seq, s + 1u, memory_order_relaxed);
    atomic_thread_fence(memory_order_release);
    l->base_produced = atomic_load_explicit(&w->produced, memory_order_relaxed);
    l->base_consumed = atomic_load_explicit(&w->consumed, memory_order_relaxed);
    l->base_backpressure = atomic_load_explicit(&w->backpressure_events, memory_order_relaxed);
    l->base_crc_errors = atomic_load_explicit(&w->crc_errors, memory_order_relaxed);
    l->base_seq_errors = atomic_load_explicit(&w->seq_errors, memory_order_relaxed);
    l->base_min_lat = atomic_load_explicit(&w->min_latency_ns, memory_order_relaxed);
    l->base_max_lat = atomic_load_explicit(&w->max_latency_ns, memory_order_relaxed);
    l->base_sum_lat = atomic_load_explicit(&w->sum_latency_ns, memory_order_relaxed);
    l->base_last_ts = atomic_load_explicit(&w->last_ts_ns, memory_order_relaxed);
    for (uint32_t b = 0; b < SHM_LAT_BUCKETS; b++)
        l->base_lat_bucket[b] = atomic_load_explicit(&w->lat_bucket[b], memory_order_relaxed);
    l->flush_deadline_ns = now_ns() + SHM_FLUSH_PERIOD_NS;
}

void mx_flush(SharedMetrics *mx, MxLocal *l)
{
    if (mx == NULL || l == NULL || l->idx < 0)
        return;
    MetricsWriter *w = &mx->w[l->idx];
    uint64_t minv = l->base_min_lat;
    if (l->min_lat != UINT64_MAX && (minv == 0 || l->min_lat < minv))
        minv = l->min_lat;
    uint64_t maxv = l->base_max_lat > l->max_lat ? l->base_max_lat : l->max_lat;
    uint32_t s = atomic_load_explicit(&w->seq, memory_order_relaxed);
    if (s & 1u) {
        atomic_store_explicit(&w->seq, s + 1u, memory_order_relaxed);
        s += 1u;
    }
    atomic_store_explicit(&w->seq, s + 1u, memory_order_relaxed);
    atomic_thread_fence(memory_order_release);
    atomic_store_explicit(&w->produced, l->base_produced + l->produced, memory_order_relaxed);
    atomic_store_explicit(&w->consumed, l->base_consumed + l->consumed, memory_order_relaxed);
    atomic_store_explicit(&w->backpressure_events, l->base_backpressure + l->backpressure,
                          memory_order_relaxed);
    atomic_store_explicit(&w->crc_errors, l->base_crc_errors + l->crc_errors, memory_order_relaxed);
    atomic_store_explicit(&w->seq_errors, l->base_seq_errors + l->seq_errors, memory_order_relaxed);
    atomic_store_explicit(&w->min_latency_ns, minv, memory_order_relaxed);
    atomic_store_explicit(&w->max_latency_ns, maxv, memory_order_relaxed);
    atomic_store_explicit(&w->sum_latency_ns, l->base_sum_lat + l->sum_lat, memory_order_relaxed);
    atomic_store_explicit(&w->last_ts_ns, l->last_ts_ns != 0 ? l->last_ts_ns : l->base_last_ts,
                          memory_order_relaxed);
    for (uint32_t b = 0; b < SHM_LAT_BUCKETS; b++)
        atomic_store_explicit(&w->lat_bucket[b], l->base_lat_bucket[b] + l->lat_bucket[b],
                              memory_order_relaxed);
    atomic_store_explicit(&w->seq, s + 2u, memory_order_release);
    l->flush_deadline_ns = now_ns() + SHM_FLUSH_PERIOD_NS;
    l->unsynced = 0;
}

void mx_flush_due(SharedMetrics *mx, MxLocal *l)
{
    l->unsynced++;
    if (l->unsynced >= SHM_FLUSH_MSGS || now_ns() >= l->flush_deadline_ns)
        mx_flush(mx, l);
}

static int mx_claim_writer(SharedMetrics *mx, ShmRole role, int *out_idx)
{
    uint32_t me = (uint32_t)getpid();
    for (uint32_t i = 0; i < SHM_MAX_WRITERS; i++) {
        MetricsWriter *w = &mx->w[i];
        uint32_t expect = 0;
        if (atomic_compare_exchange_strong_explicit(&w->pid, &expect, me, memory_order_acq_rel,
                                                    memory_order_relaxed)) {
            atomic_store_explicit(&w->role, (uint32_t)role, memory_order_release);
            *out_idx = (int)i;
            return 0;
        }
        if (kill(expect, 0) == -1 && errno == ESRCH) {
            uint32_t old = expect;
            if (atomic_compare_exchange_strong_explicit(&w->pid, &old, me, memory_order_acq_rel,
                                                        memory_order_relaxed)) {
                atomic_store_explicit(&w->role, (uint32_t)role, memory_order_release);
                *out_idx = (int)i;
                return 0;
            }
        }
    }
    fprintf(stderr, "shm_join: all %u metrics writer slots are held by live processes\n",
            SHM_MAX_WRITERS);
    return -1;
}

static void ring_init_creator(SharedRingBuffer *rb, SharedMetrics *mx)
{
    memset(rb, 0, sizeof *rb);
    memset(mx, 0, sizeof *mx);
    rb->version = SHM_VERSION;
    rb->capacity = SHM_CAPACITY;
    rb->slot_size = (uint32_t)sizeof(MessageSlot);
    rb->total_size = (uint32_t)sizeof(SharedRingBuffer);
    atomic_store_explicit(&rb->refcount, 0, memory_order_relaxed);
    atomic_store_explicit(&rb->creator_pid, (uint32_t)getpid(), memory_order_relaxed);
    atomic_store_explicit(&rb->enqueue_pos, 0, memory_order_relaxed);
    atomic_store_explicit(&rb->dequeue_pos, 0, memory_order_relaxed);
    for (uint32_t i = 0; i < SHM_CAPACITY; i++)
        atomic_store_explicit(&rb->slots[i].seq, (uint64_t)i, memory_order_relaxed);
    mx->version = SHM_VERSION;
    mx->writers = SHM_MAX_WRITERS;
    for (uint32_t i = 0; i < SHM_MAX_WRITERS; i++) {
        atomic_store_explicit(&mx->w[i].seq, 0, memory_order_relaxed);
        atomic_store_explicit(&mx->w[i].pid, 0, memory_order_relaxed);
        atomic_store_explicit(&mx->w[i].role, 0, memory_order_relaxed);
    }
}

static void maybe_force_recreate(const char *ring, const char *met, const char *sf, const char *so)
{
    const char *env = getenv("SHM_FORCE_RECREATE");
    if (env != NULL && strcmp(env, "1") == 0) {
        shm_unlink(ring);
        shm_unlink(met);
        sem_unlink(sf);
        sem_unlink(so);
    }
}

static void warn_if_stale(const SharedRingBuffer *rb, const SharedMetrics *mx, int writer_idx)
{
    if (atomic_load_explicit(&rb->refcount, memory_order_acquire) <= 1)
        return;
    int other_live = 0;
    for (uint32_t i = 0; i < SHM_MAX_WRITERS; i++) {
        if ((int)i == writer_idx)
            continue;
        uint32_t pid = atomic_load_explicit(&mx->w[i].pid, memory_order_relaxed);
        if (pid != 0 && (kill(pid, 0) == 0 || errno == EPERM))
            other_live++;
    }
    if (other_live == 0)
        fprintf(stderr,
                "shm_join: stale segment (refcount>1 but no other live writers); "
                "run ./cleanup.sh or start with SHM_FORCE_RECREATE=1\n");
}

static int shm_join_existing(ShmHandle *h, const char *id, ShmRole role, uint64_t deadline,
                             const char *ring, const char *met, const char *sf, const char *so)
{
    int ring_fd = -1, mx_fd = -1;
    SharedRingBuffer *rb = NULL;
    SharedMetrics *mx = NULL;
    sem_t *sem_free = SEM_FAILED, *sem_occ = SEM_FAILED;
    struct stat st;

    for (;;) {
        ring_fd = shm_open(ring, O_RDWR, 0600);
        if (ring_fd >= 0)
            break;
        if (errno != ENOENT || now_ns() >= deadline) {
            perror("shm_open: join ring");
            return -1;
        }
        sleep_ms(1);
    }
    for (;;) {
        if (fstat(ring_fd, &st) != 0) {
            perror("fstat: ring");
            goto jfail;
        }
        if (st.st_size == (off_t)sizeof(SharedRingBuffer))
            break;
        if (now_ns() >= deadline) {
            fprintf(stderr, "shm_join: ring '%s' never reached expected size (creator stuck or killed)\n",
                    ring);
            goto jfail;
        }
        sleep_ms(1);
    }

    for (;;) {
        mx_fd = shm_open(met, O_RDWR, 0600);
        if (mx_fd >= 0)
            break;
        if (errno != ENOENT || now_ns() >= deadline) {
            perror("shm_open: join metrics");
            goto jfail;
        }
        sleep_ms(1);
    }
    for (;;) {
        if (fstat(mx_fd, &st) != 0) {
            perror("fstat: metrics");
            goto jfail;
        }
        if (st.st_size == (off_t)sizeof(SharedMetrics))
            break;
        if (now_ns() >= deadline) {
            fprintf(stderr, "shm_join: metrics '%s' never reached expected size\n", met);
            goto jfail;
        }
        sleep_ms(1);
    }

    rb = mmap(NULL, sizeof(SharedRingBuffer), PROT_READ | PROT_WRITE, MAP_SHARED, ring_fd, 0);
    if (rb == MAP_FAILED) {
        rb = NULL;
        perror("mmap: ring");
        goto jfail;
    }
    mx = mmap(NULL, sizeof(SharedMetrics), PROT_READ | PROT_WRITE, MAP_SHARED, mx_fd, 0);
    if (mx == MAP_FAILED) {
        mx = NULL;
        perror("mmap: metrics");
        goto jfail;
    }

    for (;;) {
        if (atomic_load_explicit(&rb->magic, memory_order_acquire) == SHM_MAGIC)
            break;
        if (now_ns() >= deadline) {
            fprintf(stderr, "shm_join: ring '%s' magic never appeared (creator stuck or killed)\n", ring);
            goto jfail;
        }
        sleep_ms(1);
    }
    if (rb->version != SHM_VERSION || rb->capacity != SHM_CAPACITY ||
        rb->slot_size != (uint32_t)sizeof(MessageSlot) ||
        rb->total_size != (uint32_t)sizeof(SharedRingBuffer)) {
        fprintf(stderr,
                "shm_join: ring layout mismatch (version=%u cap=%u slot=%u total=%u, want %u/%u/%zu/%zu)\n",
                rb->version, rb->capacity, rb->slot_size, rb->total_size, SHM_VERSION,
                SHM_CAPACITY, sizeof(MessageSlot), sizeof(SharedRingBuffer));
        goto jfail;
    }
    for (;;) {
        if (atomic_load_explicit(&mx->magic, memory_order_acquire) == SHM_MAGIC)
            break;
        if (now_ns() >= deadline) {
            fprintf(stderr, "shm_join: metrics magic never appeared on '%s'\n", met);
            goto jfail;
        }
        sleep_ms(1);
    }

    for (;;) {
        sem_free = sem_open(sf, 0);
        if (sem_free != SEM_FAILED)
            break;
        if (errno != ENOENT || now_ns() >= deadline) {
            perror("sem_open: free slots");
            goto jfail;
        }
        sleep_ms(1);
    }
    for (;;) {
        sem_occ = sem_open(so, 0);
        if (sem_occ != SEM_FAILED)
            break;
        if (errno != ENOENT || now_ns() >= deadline) {
            perror("sem_open: occupied slots");
            goto jfail;
        }
        sleep_ms(1);
    }

    close(ring_fd);
    close(mx_fd);

    atomic_fetch_add_explicit(&rb->refcount, 1, memory_order_acq_rel);
    int writer_idx = -1;
    if (mx_claim_writer(mx, role, &writer_idx) != 0) {
        atomic_fetch_sub_explicit(&rb->refcount, 1, memory_order_acq_rel);
        sem_close(sem_free);
        sem_close(sem_occ);
        munmap(rb, sizeof(SharedRingBuffer));
        munmap(mx, sizeof(SharedMetrics));
        return -1;
    }
    warn_if_stale(rb, mx, writer_idx);

    h->rb = rb;
    h->mx = mx;
    h->sem_free = sem_free;
    h->sem_occ = sem_occ;
    h->writer_idx = writer_idx;
    h->created = 0;
    memcpy(h->id, id, strlen(id) + 1);
    return 0;

jfail:
    if (ring_fd >= 0)
        close(ring_fd);
    if (mx_fd >= 0)
        close(mx_fd);
    if (sem_free != SEM_FAILED)
        sem_close(sem_free);
    if (sem_occ != SEM_FAILED)
        sem_close(sem_occ);
    if (rb != NULL)
        munmap(rb, sizeof(SharedRingBuffer));
    if (mx != NULL)
        munmap(mx, sizeof(SharedMetrics));
    return -1;
}

int shm_join(ShmHandle *h, const char *id, ShmRole role, unsigned timeout_ms)
{
    if (h == NULL) {
        errno = EINVAL;
        return -1;
    }
    memset(h, 0, sizeof *h);
    h->writer_idx = -1;
    if (shm_valid_id(id) != 0) {
        errno = EINVAL;
        return -1;
    }
    if (timeout_ms == 0)
        timeout_ms = 5000;

    char ring[SHM_NAME_MAX], met[SHM_NAME_MAX], sf[SHM_NAME_MAX], so[SHM_NAME_MAX];
    ring_name(ring, sizeof ring, id);
    metrics_name(met, sizeof met, id);
    sem_free_name(sf, sizeof sf, id);
    sem_occ_name(so, sizeof so, id);
    maybe_force_recreate(ring, met, sf, so);

    uint64_t deadline = now_ns() + (uint64_t)timeout_ms * 1000000ull;

    int ring_fd = shm_open(ring, O_RDWR | O_CREAT | O_EXCL, 0600);
    if (ring_fd < 0) {
        if (errno == EEXIST)
            return shm_join_existing(h, id, role, deadline, ring, met, sf, so);
        perror("shm_open: create ring");
        return -1;
    }

    int mx_fd = -1;
    SharedRingBuffer *rb = NULL;
    SharedMetrics *mx = NULL;
    sem_t *sem_free = SEM_FAILED, *sem_occ = SEM_FAILED;

    if (ftruncate(ring_fd, (off_t)sizeof(SharedRingBuffer)) != 0) {
        perror("ftruncate: ring");
        goto cfail;
    }
    mx_fd = shm_open(met, O_RDWR | O_CREAT | O_EXCL, 0600);
    if (mx_fd < 0) {
        perror("shm_open: create metrics");
        goto cfail;
    }
    if (ftruncate(mx_fd, (off_t)sizeof(SharedMetrics)) != 0) {
        perror("ftruncate: metrics");
        goto cfail;
    }
    rb = mmap(NULL, sizeof(SharedRingBuffer), PROT_READ | PROT_WRITE, MAP_SHARED, ring_fd, 0);
    if (rb == MAP_FAILED) {
        rb = NULL;
        perror("mmap: ring");
        goto cfail;
    }
    mx = mmap(NULL, sizeof(SharedMetrics), PROT_READ | PROT_WRITE, MAP_SHARED, mx_fd, 0);
    if (mx == MAP_FAILED) {
        mx = NULL;
        perror("mmap: metrics");
        goto cfail;
    }
    ring_init_creator(rb, mx);
    sem_unlink(sf);
    sem_unlink(so);
    sem_free = sem_open(sf, O_CREAT | O_EXCL, 0600, (unsigned)SHM_CAPACITY);
    if (sem_free == SEM_FAILED) {
        perror("sem_open: free slots");
        goto cfail;
    }
    sem_occ = sem_open(so, O_CREAT | O_EXCL, 0600, 0u);
    if (sem_occ == SEM_FAILED) {
        perror("sem_open: occupied slots");
        goto cfail;
    }
    atomic_store_explicit(&mx->magic, SHM_MAGIC, memory_order_release);
    atomic_store_explicit(&rb->magic, SHM_MAGIC, memory_order_release);

    close(ring_fd);
    close(mx_fd);

    atomic_fetch_add_explicit(&rb->refcount, 1, memory_order_acq_rel);
    int writer_idx = -1;
    if (mx_claim_writer(mx, role, &writer_idx) != 0) {
        atomic_fetch_sub_explicit(&rb->refcount, 1, memory_order_acq_rel);
        sem_close(sem_free);
        sem_close(sem_occ);
        munmap(rb, sizeof(SharedRingBuffer));
        munmap(mx, sizeof(SharedMetrics));
        shm_unlink(ring);
        shm_unlink(met);
        sem_unlink(sf);
        sem_unlink(so);
        return -1;
    }

    h->rb = rb;
    h->mx = mx;
    h->sem_free = sem_free;
    h->sem_occ = sem_occ;
    h->writer_idx = writer_idx;
    h->created = 1;
    memcpy(h->id, id, strlen(id) + 1);
    return 0;

cfail:
    if (ring_fd >= 0)
        close(ring_fd);
    if (mx_fd >= 0)
        close(mx_fd);
    if (rb != NULL)
        munmap(rb, sizeof(SharedRingBuffer));
    if (mx != NULL)
        munmap(mx, sizeof(SharedMetrics));
    if (sem_free != SEM_FAILED) {
        sem_close(sem_free);
        sem_unlink(sf);
    }
    if (sem_occ != SEM_FAILED) {
        sem_close(sem_occ);
        sem_unlink(so);
    }
    shm_unlink(ring);
    shm_unlink(met);
    return -1;
}

void shm_leave(ShmHandle *h)
{
    if (h == NULL)
        return;
    if (h->mx != NULL && h->writer_idx >= 0) {
        MetricsWriter *w = &h->mx->w[h->writer_idx];
        uint32_t s = atomic_load_explicit(&w->seq, memory_order_relaxed);
        if (s & 1u) {
            atomic_store_explicit(&w->seq, s + 1u, memory_order_relaxed);
            s += 1u;
        }
        atomic_store_explicit(&w->seq, s + 1u, memory_order_relaxed);
        atomic_thread_fence(memory_order_release);
        uint64_t v;
        v = atomic_load_explicit(&w->produced, memory_order_relaxed);
        atomic_store_explicit(&w->produced, v, memory_order_relaxed);
        v = atomic_load_explicit(&w->consumed, memory_order_relaxed);
        atomic_store_explicit(&w->consumed, v, memory_order_relaxed);
        v = atomic_load_explicit(&w->backpressure_events, memory_order_relaxed);
        atomic_store_explicit(&w->backpressure_events, v, memory_order_relaxed);
        v = atomic_load_explicit(&w->crc_errors, memory_order_relaxed);
        atomic_store_explicit(&w->crc_errors, v, memory_order_relaxed);
        v = atomic_load_explicit(&w->seq_errors, memory_order_relaxed);
        atomic_store_explicit(&w->seq_errors, v, memory_order_relaxed);
        v = atomic_load_explicit(&w->min_latency_ns, memory_order_relaxed);
        atomic_store_explicit(&w->min_latency_ns, v, memory_order_relaxed);
        v = atomic_load_explicit(&w->max_latency_ns, memory_order_relaxed);
        atomic_store_explicit(&w->max_latency_ns, v, memory_order_relaxed);
        v = atomic_load_explicit(&w->sum_latency_ns, memory_order_relaxed);
        atomic_store_explicit(&w->sum_latency_ns, v, memory_order_relaxed);
        v = atomic_load_explicit(&w->last_ts_ns, memory_order_relaxed);
        atomic_store_explicit(&w->last_ts_ns, v, memory_order_relaxed);
        for (uint32_t b = 0; b < SHM_LAT_BUCKETS; b++) {
            v = atomic_load_explicit(&w->lat_bucket[b], memory_order_relaxed);
            atomic_store_explicit(&w->lat_bucket[b], v, memory_order_relaxed);
        }
        atomic_store_explicit(&w->seq, s + 2u, memory_order_release);
        atomic_store_explicit(&w->pid, 0, memory_order_release);
    }
    if (h->rb != NULL) {
        int32_t old = atomic_fetch_sub_explicit(&h->rb->refcount, 1, memory_order_acq_rel);
        if (old == 1) {
            char ring[SHM_NAME_MAX], met[SHM_NAME_MAX], sf[SHM_NAME_MAX], so[SHM_NAME_MAX];
            ring_name(ring, sizeof ring, h->id);
            metrics_name(met, sizeof met, h->id);
            sem_free_name(sf, sizeof sf, h->id);
            sem_occ_name(so, sizeof so, h->id);
            shm_unlink(ring);
            shm_unlink(met);
            sem_unlink(sf);
            sem_unlink(so);
        }
    }
    if (h->sem_free != NULL && h->sem_free != SEM_FAILED)
        sem_close(h->sem_free);
    if (h->sem_occ != NULL && h->sem_occ != SEM_FAILED)
        sem_close(h->sem_occ);
    if (h->rb != NULL)
        munmap(h->rb, sizeof(SharedRingBuffer));
    if (h->mx != NULL)
        munmap(h->mx, sizeof(SharedMetrics));
    h->rb = NULL;
    h->mx = NULL;
    h->sem_free = NULL;
    h->sem_occ = NULL;
    h->writer_idx = -1;
}

int shm_attach_ro(ShmRO *ro, const char *id)
{
    if (ro == NULL) {
        errno = EINVAL;
        return -1;
    }
    memset(ro, 0, sizeof *ro);
    ro->ring_fd = -1;
    ro->mx_fd = -1;
    if (shm_valid_id(id) != 0) {
        errno = EINVAL;
        return -1;
    }
    char ring[SHM_NAME_MAX], met[SHM_NAME_MAX];
    ring_name(ring, sizeof ring, id);
    metrics_name(met, sizeof met, id);
    struct stat st;

    ro->ring_fd = shm_open(ring, O_RDONLY, 0);
    if (ro->ring_fd < 0)
        return -1;
    if (fstat(ro->ring_fd, &st) != 0) {
        perror("fstat: ring");
        goto ro_fail;
    }
    if (st.st_size != (off_t)sizeof(SharedRingBuffer)) {
        errno = EAGAIN;
        goto ro_fail;
    }
    ro->mx_fd = shm_open(met, O_RDONLY, 0);
    if (ro->mx_fd < 0)
        goto ro_fail;
    if (fstat(ro->mx_fd, &st) != 0) {
        perror("fstat: metrics");
        errno = EAGAIN;
        goto ro_fail;
    }
    if (st.st_size != (off_t)sizeof(SharedMetrics)) {
        errno = EAGAIN;
        goto ro_fail;
    }
    {
        SharedRingBuffer *rb = mmap(NULL, sizeof(SharedRingBuffer), PROT_READ, MAP_SHARED,
                                    ro->ring_fd, 0);
        if (rb == MAP_FAILED) {
            perror("mmap: ring ro");
            goto ro_fail;
        }
        SharedMetrics *mx = mmap(NULL, sizeof(SharedMetrics), PROT_READ, MAP_SHARED, ro->mx_fd, 0);
        if (mx == MAP_FAILED) {
            perror("mmap: metrics ro");
            munmap(rb, sizeof(SharedRingBuffer));
            goto ro_fail;
        }
        if (atomic_load_explicit(&rb->magic, memory_order_acquire) != SHM_MAGIC ||
            atomic_load_explicit(&mx->magic, memory_order_acquire) != SHM_MAGIC ||
            rb->version != SHM_VERSION || rb->capacity != SHM_CAPACITY ||
            rb->slot_size != (uint32_t)sizeof(MessageSlot) ||
            rb->total_size != (uint32_t)sizeof(SharedRingBuffer)) {
            munmap(rb, sizeof(SharedRingBuffer));
            munmap(mx, sizeof(SharedMetrics));
            errno = EAGAIN;
            goto ro_fail;
        }
        ro->rb = rb;
        ro->mx = mx;
        return 0;
    }

ro_fail:
    if (ro->ring_fd >= 0)
        close(ro->ring_fd);
    if (ro->mx_fd >= 0)
        close(ro->mx_fd);
    ro->ring_fd = -1;
    ro->mx_fd = -1;
    return -1;
}

void shm_detach_ro(ShmRO *ro)
{
    if (ro == NULL)
        return;
    if (ro->rb != NULL)
        munmap((void *)(uintptr_t)ro->rb, sizeof(SharedRingBuffer));
    if (ro->mx != NULL)
        munmap((void *)(uintptr_t)ro->mx, sizeof(SharedMetrics));
    if (ro->ring_fd >= 0)
        close(ro->ring_fd);
    if (ro->mx_fd >= 0)
        close(ro->mx_fd);
    ro->rb = NULL;
    ro->mx = NULL;
    ro->ring_fd = -1;
    ro->mx_fd = -1;
}

int shm_ro_is_current(const ShmRO *ro, const char *id)
{
    if (ro == NULL || ro->ring_fd < 0 || ro->mx_fd < 0)
        return 0;
    if (shm_valid_id(id) != 0)
        return 0;
    char ring[SHM_NAME_MAX], met[SHM_NAME_MAX];
    ring_name(ring, sizeof ring, id);
    metrics_name(met, sizeof met, id);
    struct stat live_ring, open_ring, live_mx, open_mx;
    if (stat(ring, &live_ring) != 0)
        return 0;
    if (stat(met, &live_mx) != 0)
        return 0;
    if (fstat(ro->ring_fd, &open_ring) != 0)
        return 0;
    if (fstat(ro->mx_fd, &open_mx) != 0)
        return 0;
    return live_ring.st_dev == open_ring.st_dev && live_ring.st_ino == open_ring.st_ino &&
           live_mx.st_dev == open_mx.st_dev && live_mx.st_ino == open_mx.st_ino;
}
