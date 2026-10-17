#ifndef SHM_TELEMETRY_SHM_COMMON_H
#define SHM_TELEMETRY_SHM_COMMON_H

#include <semaphore.h>
#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>

#define SHM_MAGIC 0x53484D31u
#define SHM_VERSION 1u
#define SHM_CAPACITY 1024u
#define SHM_MASK (SHM_CAPACITY - 1u)
#define SHM_PAYLOAD_MAX 256u
#define SHM_MAX_WRITERS 64u
#define SHM_LAT_BUCKETS 5u

typedef struct {
    _Alignas(64) _Atomic uint64_t seq;
    uint64_t msg_id;
    uint64_t timestamp_ns;
    uint32_t sender_pid;
    uint32_t producer_idx;
    uint32_t payload_len;
    uint32_t crc32;
    char payload[SHM_PAYLOAD_MAX];
} MessageSlot;

typedef struct {
    _Alignas(64) _Atomic uint32_t magic;
    uint32_t version, capacity, slot_size, total_size;
    _Atomic int32_t refcount;
    _Atomic uint32_t creator_pid;
    _Alignas(64) _Atomic uint64_t enqueue_pos;
    _Alignas(64) _Atomic uint64_t dequeue_pos;
    _Alignas(64) MessageSlot slots[SHM_CAPACITY];
} SharedRingBuffer;

typedef struct {
    _Alignas(64) _Atomic uint32_t seq;
    _Atomic uint32_t pid;
    _Atomic uint32_t role;
    uint32_t _pad;
    _Atomic uint64_t produced, consumed, backpressure_events, crc_errors, seq_errors;
    _Atomic uint64_t min_latency_ns, max_latency_ns, sum_latency_ns, last_ts_ns;
    _Atomic uint64_t lat_bucket[SHM_LAT_BUCKETS];
} MetricsWriter;

typedef struct {
    _Alignas(64) _Atomic uint32_t magic;
    uint32_t version, writers;
    _Alignas(64) MetricsWriter w[SHM_MAX_WRITERS];
} SharedMetrics;

_Static_assert(ATOMIC_LLONG_LOCK_FREE == 2 && ATOMIC_INT_LOCK_FREE == 2,
               "lock-free atomics required for cross-process use");
_Static_assert(sizeof(MessageSlot) == 320, "MessageSlot must be 320 bytes");
_Static_assert(_Alignof(MessageSlot) == 64, "MessageSlot must be 64-byte aligned");
_Static_assert(offsetof(SharedRingBuffer, magic) == 0, "magic at offset 0");
_Static_assert(offsetof(SharedRingBuffer, enqueue_pos) == 64, "enqueue_pos at offset 64");
_Static_assert(offsetof(SharedRingBuffer, dequeue_pos) == 128, "dequeue_pos at offset 128");
_Static_assert(offsetof(SharedRingBuffer, slots) == 192, "slots at offset 192");
_Static_assert(sizeof(SharedRingBuffer) == 327872, "SharedRingBuffer size");
_Static_assert(sizeof(MetricsWriter) % 64 == 0, "MetricsWriter padded to 64");
_Static_assert(sizeof(MetricsWriter) == 128, "MetricsWriter size");
_Static_assert(offsetof(SharedMetrics, magic) == 0, "metrics magic at offset 0");
_Static_assert(offsetof(SharedMetrics, w) == 64, "metrics writers at offset 64");
_Static_assert(sizeof(SharedMetrics) == 8256, "SharedMetrics size");

typedef enum { SHM_ROLE_PRODUCER = 1, SHM_ROLE_CONSUMER = 2 } ShmRole;

typedef struct {
    SharedRingBuffer *rb;
    SharedMetrics *mx;
    sem_t *sem_free;
    sem_t *sem_occ;
    int writer_idx;
    int created;
    char id[17];
} ShmHandle;

typedef struct {
    const SharedRingBuffer *rb;
    const SharedMetrics *mx;
    int ring_fd;
    int mx_fd;
} ShmRO;

typedef struct {
    int idx;
    uint64_t produced, consumed, backpressure, crc_errors, seq_errors;
    uint64_t min_lat, max_lat, sum_lat;
    uint64_t lat_bucket[SHM_LAT_BUCKETS];
    uint64_t last_ts_ns;
    uint64_t base_produced, base_consumed, base_backpressure;
    uint64_t base_crc_errors, base_seq_errors;
    uint64_t base_min_lat, base_max_lat, base_sum_lat, base_last_ts;
    uint64_t base_lat_bucket[SHM_LAT_BUCKETS];
    uint64_t flush_deadline_ns;
    uint64_t unsynced;
} MxLocal;

int shm_valid_id(const char *id);
int shm_join(ShmHandle *h, const char *id, ShmRole role, unsigned timeout_ms);
void shm_leave(ShmHandle *h);
int shm_attach_ro(ShmRO *ro, const char *id);
void shm_detach_ro(ShmRO *ro);
int shm_ro_is_current(const ShmRO *ro, const char *id);
uint32_t crc32_ieee(const void *data, size_t len);
uint64_t now_ns(void);
uint64_t mix64(uint64_t x);

void mx_local_begin(MxLocal *l, SharedMetrics *mx, int writer_idx);
void mx_flush(SharedMetrics *mx, MxLocal *l);
void mx_flush_due(SharedMetrics *mx, MxLocal *l);

#endif
