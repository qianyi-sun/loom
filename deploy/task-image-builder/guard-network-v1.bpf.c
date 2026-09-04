// SPDX-License-Identifier: Apache-2.0
/* Allocation-scoped default-deny network policy for the Loom builder guard. */

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/in.h>
#include <linux/in6.h>
#include <linux/ip.h>
#include <linux/ipv6.h>
#include <linux/tcp.h>
#include <linux/udp.h>

#define SEC(name) __attribute__((section(name), used))
#define __uint(name, value) int (*name)[value]
#define __type(name, value) typeof(value) *name
#define NS_PER_SECOND 1000000000ULL
#define LOOM_AF_INET 2
#define LOOM_AF_INET6 10
#define LOOM_SOCK_STREAM 1
#define LOOM_SOCK_DGRAM 2

static void *(*bpf_map_lookup_elem)(void *map, const void *key) =
    (void *)BPF_FUNC_map_lookup_elem;
static long (*bpf_map_update_elem)(void *map, const void *key,
                                   const void *value, __u64 flags) =
    (void *)BPF_FUNC_map_update_elem;
static long (*bpf_map_delete_elem)(void *map, const void *key) =
    (void *)BPF_FUNC_map_delete_elem;
static __u64 (*bpf_ktime_get_ns)(void) = (void *)BPF_FUNC_ktime_get_ns;
static __u64 (*bpf_get_socket_cookie)(void *ctx) =
    (void *)BPF_FUNC_get_socket_cookie;
static void (*bpf_spin_lock)(struct bpf_spin_lock *lock) =
    (void *)BPF_FUNC_spin_lock;
static void (*bpf_spin_unlock)(struct bpf_spin_lock *lock) =
    (void *)BPF_FUNC_spin_unlock;

struct endpoint_v4 {
    __u32 subject;
    __u32 address;
    __u16 port;
    __u8 protocol;
    __u8 pad;
};

struct endpoint_v6 {
    __u32 subject;
    __u8 address[16];
    __u16 port;
    __u8 protocol;
    __u8 pad;
};

struct subject_limit {
    struct bpf_spin_lock lock;
    __u32 concurrent_flows;
    __u64 window_start_ns;
    __u64 ingress_bytes;
    __u64 egress_bytes;
    __u64 ingress_packets;
    __u64 egress_packets;
    __u64 new_flows;
    __u64 dns_queries;
    __u64 ingress_bytes_per_second;
    __u64 egress_bytes_per_second;
    __u64 ingress_packets_per_second;
    __u64 egress_packets_per_second;
    __u64 new_flows_per_second;
    __u64 dns_queries_per_second;
    __u32 max_concurrent_flows;
    __u32 pad;
};

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, __u32);
} scope_subject SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 4096);
    __type(key, struct endpoint_v4);
    __type(value, __u8);
} allow_v4 SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 4096);
    __type(key, struct endpoint_v6);
    __type(value, __u8);
} allow_v6 SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 16);
    __type(key, __u32);
    __type(value, struct subject_limit);
} subject_limits SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 4096);
    __type(key, __u64);
    __type(value, __u64);
} flow_sockets SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 16);
    __type(key, __u32);
    __type(value, __u64);
} drop_counters SEC(".maps");

enum drop_reason {
    DROP_SUBJECT = 0,
    DROP_POLICY = 1,
    DROP_LIMITER = 2,
    DROP_BYTES = 3,
    DROP_PACKETS = 4,
    DROP_FLOWS = 5,
    DROP_DNS = 6,
    DROP_PACKET = 7,
    DROP_RAW_SOCKET = 8,
};

static __always_inline void record_drop(__u32 reason)
{
    __u64 *counter = bpf_map_lookup_elem(&drop_counters, &reason);
    if (counter)
        *counter += 1;
}

static __always_inline int current_subject(__u32 *subject)
{
    __u32 key = 0;
    __u32 *found = bpf_map_lookup_elem(&scope_subject, &key);
    if (!found) {
        record_drop(DROP_SUBJECT);
        return 0;
    }
    *subject = *found;
    return 1;
}

static __always_inline int reset_window(struct subject_limit *limit, __u64 now)
{
    if (now < limit->window_start_ns)
        return 0;
    if (now - limit->window_start_ns >= NS_PER_SECOND) {
        limit->window_start_ns = now;
        limit->ingress_bytes = 0;
        limit->egress_bytes = 0;
        limit->ingress_packets = 0;
        limit->egress_packets = 0;
        limit->new_flows = 0;
        limit->dns_queries = 0;
    }
    return 1;
}

static __always_inline int account_packet(__u32 subject, __u64 bytes,
                                           int ingress)
{
    struct subject_limit *limit = bpf_map_lookup_elem(&subject_limits, &subject);
    __u64 now = bpf_ktime_get_ns();
    __u32 reason = DROP_BYTES;
    int allowed = 0;
    if (!limit) {
        record_drop(DROP_LIMITER);
        return 0;
    }
    bpf_spin_lock(&limit->lock);
    if (reset_window(limit, now)) {
        if (ingress) {
            if (limit->ingress_bytes <= limit->ingress_bytes_per_second &&
                bytes <= limit->ingress_bytes_per_second - limit->ingress_bytes &&
                limit->ingress_packets < limit->ingress_packets_per_second) {
                limit->ingress_bytes += bytes;
                limit->ingress_packets += 1;
                allowed = 1;
            } else if (limit->ingress_bytes <= limit->ingress_bytes_per_second &&
                       bytes <= limit->ingress_bytes_per_second -
                                    limit->ingress_bytes) {
                reason = DROP_PACKETS;
            }
        } else {
            if (limit->egress_bytes <= limit->egress_bytes_per_second &&
                bytes <= limit->egress_bytes_per_second - limit->egress_bytes &&
                limit->egress_packets < limit->egress_packets_per_second) {
                limit->egress_bytes += bytes;
                limit->egress_packets += 1;
                allowed = 1;
            } else if (limit->egress_bytes <= limit->egress_bytes_per_second &&
                       bytes <= limit->egress_bytes_per_second -
                                    limit->egress_bytes) {
                reason = DROP_PACKETS;
            }
        }
    }
    bpf_spin_unlock(&limit->lock);
    if (!allowed)
        record_drop(reason);
    return allowed;
}

static __always_inline int account_dns(__u32 subject, __u16 port)
{
    struct subject_limit *limit;
    __u64 now;
    int allowed = 0;
    if (port != __builtin_bswap16(53))
        return 1;
    limit = bpf_map_lookup_elem(&subject_limits, &subject);
    if (!limit) {
        record_drop(DROP_LIMITER);
        return 0;
    }
    now = bpf_ktime_get_ns();
    bpf_spin_lock(&limit->lock);
    if (reset_window(limit, now) &&
        limit->dns_queries < limit->dns_queries_per_second) {
        limit->dns_queries += 1;
        allowed = 1;
    }
    bpf_spin_unlock(&limit->lock);
    if (!allowed)
        record_drop(DROP_DNS);
    return allowed;
}

static __always_inline int permit_v4(__u32 subject, __u32 address,
                                     __u16 port, __u8 protocol)
{
    struct endpoint_v4 key = {
        .subject = subject,
        .address = address,
        .port = port,
        .protocol = protocol,
    };
    if (!bpf_map_lookup_elem(&allow_v4, &key)) {
        record_drop(DROP_POLICY);
        return 0;
    }
    return 1;
}

static __always_inline int permit_v6(__u32 subject, const __u32 *address,
                                     __u16 port, __u8 protocol)
{
    struct endpoint_v6 key = {
        .subject = subject,
        .port = port,
        .protocol = protocol,
    };
    __builtin_memcpy(key.address, address, sizeof(key.address));
    if (!bpf_map_lookup_elem(&allow_v6, &key)) {
        record_drop(DROP_POLICY);
        return 0;
    }
    return 1;
}

SEC("cgroup/connect4")
int guard_connect4(struct bpf_sock_addr *ctx)
{
    __u32 subject;
    if (!current_subject(&subject))
        return 0;
    if (!permit_v4(subject, ctx->user_ip4, ctx->user_port, ctx->protocol))
        return 0;
    return 1;
}

SEC("cgroup/connect6")
int guard_connect6(struct bpf_sock_addr *ctx)
{
    __u32 subject;
    if (!current_subject(&subject))
        return 0;
    if (!permit_v6(subject, ctx->user_ip6, ctx->user_port, ctx->protocol))
        return 0;
    return 1;
}

SEC("cgroup/sendmsg4")
int guard_sendmsg4(struct bpf_sock_addr *ctx)
{
    __u32 subject;
    if (!current_subject(&subject))
        return 0;
    if (!permit_v4(subject, ctx->user_ip4, ctx->user_port, ctx->protocol))
        return 0;
    return 1;
}

SEC("cgroup/sendmsg6")
int guard_sendmsg6(struct bpf_sock_addr *ctx)
{
    __u32 subject;
    if (!current_subject(&subject))
        return 0;
    if (!permit_v6(subject, ctx->user_ip6, ctx->user_port, ctx->protocol))
        return 0;
    return 1;
}

SEC("cgroup/sock_create")
int guard_sock_create(struct bpf_sock *ctx)
{
    __u32 subject;
    struct subject_limit *limit;
    __u64 cookie;
    __u64 subject_value;
    __u64 now;
    int allowed = 0;
    if ((ctx->family != LOOM_AF_INET && ctx->family != LOOM_AF_INET6) ||
        (ctx->type != LOOM_SOCK_STREAM && ctx->type != LOOM_SOCK_DGRAM)) {
        record_drop(DROP_RAW_SOCKET);
        return 0;
    }
    if (!current_subject(&subject))
        return 0;
    limit = bpf_map_lookup_elem(&subject_limits, &subject);
    if (!limit) {
        record_drop(DROP_LIMITER);
        return 0;
    }
    now = bpf_ktime_get_ns();
    bpf_spin_lock(&limit->lock);
    if (reset_window(limit, now) &&
        limit->new_flows < limit->new_flows_per_second &&
        limit->concurrent_flows < limit->max_concurrent_flows) {
        limit->new_flows += 1;
        limit->concurrent_flows += 1;
        allowed = 1;
    }
    bpf_spin_unlock(&limit->lock);
    if (!allowed) {
        record_drop(DROP_FLOWS);
        return 0;
    }
    cookie = bpf_get_socket_cookie(ctx);
    subject_value = subject;
    if (!cookie || bpf_map_update_elem(&flow_sockets, &cookie, &subject_value,
                                       BPF_NOEXIST)) {
        bpf_spin_lock(&limit->lock);
        if (limit->concurrent_flows)
            limit->concurrent_flows -= 1;
        bpf_spin_unlock(&limit->lock);
        record_drop(DROP_FLOWS);
        return 0;
    }
    return 1;
}

SEC("cgroup/sock_release")
int guard_sock_release(struct bpf_sock *ctx)
{
    __u64 cookie = bpf_get_socket_cookie(ctx);
    __u64 *subject_value = bpf_map_lookup_elem(&flow_sockets, &cookie);
    if (subject_value) {
        __u32 subject = (__u32)*subject_value;
        struct subject_limit *limit = bpf_map_lookup_elem(&subject_limits, &subject);
        if (limit) {
            bpf_spin_lock(&limit->lock);
            if (limit->concurrent_flows)
                limit->concurrent_flows -= 1;
            bpf_spin_unlock(&limit->lock);
        }
        bpf_map_delete_elem(&flow_sockets, &cookie);
    }
    return 1;
}

static __always_inline int packet_v4(struct __sk_buff *ctx, void *data,
                                     void *data_end, __u32 subject, int ingress)
{
    struct iphdr *ip = data;
    void *transport;
    __u16 port;
    __u32 address;
    if ((void *)(ip + 1) > data_end || ip->version != 4 || ip->ihl != 5 ||
        (ip->frag_off & __builtin_bswap16(0x3fff))) {
        record_drop(DROP_PACKET);
        return 0;
    }
    if (ip->protocol != IPPROTO_TCP && ip->protocol != IPPROTO_UDP) {
        record_drop(DROP_PACKET);
        return 0;
    }
    transport = (void *)ip + sizeof(*ip);
    if (ip->protocol == IPPROTO_TCP) {
        struct tcphdr *tcp = transport;
        if ((void *)(tcp + 1) > data_end)
            return 0;
        port = ingress ? tcp->source : tcp->dest;
    } else {
        struct udphdr *udp = transport;
        if ((void *)(udp + 1) > data_end)
            return 0;
        port = ingress ? udp->source : udp->dest;
    }
    address = ingress ? ip->saddr : ip->daddr;
    if (!permit_v4(subject, address, port, ip->protocol))
        return 0;
    if (!ingress && !account_dns(subject, port))
        return 0;
    return account_packet(subject, ctx->len, ingress);
}

static __always_inline int packet_v6(struct __sk_buff *ctx, void *data,
                                     void *data_end, __u32 subject, int ingress)
{
    struct ipv6hdr *ip = data;
    void *transport;
    __u16 port;
    const __u32 *address;
    if ((void *)(ip + 1) > data_end || ip->version != 6 ||
        (ip->nexthdr != IPPROTO_TCP && ip->nexthdr != IPPROTO_UDP)) {
        record_drop(DROP_PACKET);
        return 0;
    }
    transport = (void *)ip + sizeof(*ip);
    if (ip->nexthdr == IPPROTO_TCP) {
        struct tcphdr *tcp = transport;
        if ((void *)(tcp + 1) > data_end)
            return 0;
        port = ingress ? tcp->source : tcp->dest;
    } else {
        struct udphdr *udp = transport;
        if ((void *)(udp + 1) > data_end)
            return 0;
        port = ingress ? udp->source : udp->dest;
    }
    address = ingress ? ip->saddr.in6_u.u6_addr32 : ip->daddr.in6_u.u6_addr32;
    if (!permit_v6(subject, address, port, ip->nexthdr))
        return 0;
    if (!ingress && !account_dns(subject, port))
        return 0;
    return account_packet(subject, ctx->len, ingress);
}

static __always_inline int inspect_packet(struct __sk_buff *ctx, int ingress)
{
    void *data = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;
    __u32 subject;
    __u8 version;
    if (!current_subject(&subject) || data + 1 > data_end)
        return 0;
    version = *(__u8 *)data >> 4;
    if (version == 4)
        return packet_v4(ctx, data, data_end, subject, ingress);
    if (version == 6)
        return packet_v6(ctx, data, data_end, subject, ingress);
    record_drop(DROP_PACKET);
    return 0;
}

SEC("cgroup_skb/ingress")
int guard_ingress(struct __sk_buff *ctx)
{
    return inspect_packet(ctx, 1);
}

SEC("cgroup_skb/egress")
int guard_egress(struct __sk_buff *ctx)
{
    return inspect_packet(ctx, 0);
}

char LICENSE[] SEC("license") = "Apache-2.0";
