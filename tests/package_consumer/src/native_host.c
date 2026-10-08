/*
 * The native consumer's C host (docs/native-abi.md): it includes the header
 * the package ships and links the static library built from
 * native_lib.zig. package-preflight runs it; any nonzero exit fails the
 * preflight.
 */
#include <stdio.h>
#include <string.h>

#include "capnp_core.h"

static int fail(int code, const char *what) {
    fprintf(stderr, "native consumer: %s\n", what);
    return code;
}

int main(void) {
    if (capnp_core_abi_version() != CAPNP_CORE_ABI_VERSION) return fail(1, "ABI version");
    if (strcmp(capnp_core_version(), "core 0.0.0 / capnp-zig package-preflight / native-consumer") != 0)
        return fail(2, "the library root's version string did not reach capnp_core_version");
    if (strcmp(capnp_core_quic_alpn(), "capnp-rpc/1") != 0) return fail(3, "QUIC ALPN");

    /* Two connections inside the core: bootstrap, call, return, finish. */
    const char *failure = NULL;
    if (capnp_core_debug_selftest(&failure) != 0) return fail(4, failure ? failure : "selftest");

    /* One connection driven from C: a bootstrap goes out as an OUT_FRAME;
     * when the transport closes, the question ends with RETURN DISCONNECTED. */
    capnp_conn_opts opts;
    memset(&opts, 0, sizeof opts);
    opts.struct_size = sizeof opts;
    capnp_conn *conn = NULL;
    if (capnp_conn_new(&opts, 0, &conn) != CAPNP_OK || conn == NULL) return fail(5, "capnp_conn_new");
    uint32_t qid = 0;
    if (capnp_bootstrap(conn, &qid) != CAPNP_OK) return fail(6, "capnp_bootstrap");

    capnp_effect effect;
    memset(&effect, 0, sizeof effect);
    effect.struct_size = sizeof effect;
    if (capnp_conn_next_effect(conn, &effect) != 1 || effect.kind != CAPNP_EFFECT_OUT_FRAME || effect.msg_len == 0)
        return fail(7, "no OUT_FRAME for the bootstrap");
    capnp_conn_commit_effect(conn);

    capnp_conn_transport_closed(conn);
    int disconnected = 0;
    for (;;) {
        memset(&effect, 0, sizeof effect);
        effect.struct_size = sizeof effect;
        if (capnp_conn_next_effect(conn, &effect) != 1) break;
        if (effect.kind == CAPNP_EFFECT_RETURN && effect.id == qid &&
            effect.return_kind == CAPNP_RETURN_DISCONNECTED)
            disconnected += 1;
        capnp_conn_commit_effect(conn);
    }
    capnp_conn_free(conn);
    if (disconnected != 1) return fail(8, "the bootstrap did not end with one RETURN DISCONNECTED");

    puts("native consumer: ok");
    return 0;
}
