/*
** Litestream Synchronous Replication VFS Shim
**
** A SQLite loadable extension that intercepts WAL file sync operations
** to enable synchronous replication through the litestream sidecar.
**
** When PRAGMA litestream_sync = 1 is set, the next WAL xSync will POST
** to the litestream Unix socket to trigger immediate replication to S3.
** If replication fails, SQLITE_IOERR is returned and SQLite rolls back
** the transaction.
**
** Architecture note: SQLite dispatches PRAGMA xFileControl to the main
** database file handle, but xSync is called on the WAL file handle.
** These are separate LitestreamFile instances, so we use a global
** per-database state table (keyed by db path) to share the sync flag
** and socket path between them.
**
** Usage:
**   .load ./litestream_sync
**   PRAGMA litestream_socket = '/var/run/litestream.sock';  -- optional
**   PRAGMA litestream_sync = 1;
**   INSERT INTO t VALUES (1);  -- committed only if S3 upload succeeds
*/

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <pthread.h>

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

/* ---- Per-database shared state ---- */
/*
** PRAGMA litestream_sync is dispatched to the main db file's xFileControl,
** but the xSync we need to intercept fires on the WAL file — a different
** LitestreamFile instance. This table lets both handles share state by
** database path.
**
** syncCounter is an integer counter (not a boolean) so that multiple
** concurrent connections can each request synchronous replication without
** stealing each other's flag. Each PRAGMA litestream_sync=1 increments
** the counter; each WAL xSync that triggers replication decrements it.
*/
#define LS_MAX_DBS 32

typedef struct LitestreamDBState {
    char *dbPath;           /* canonical database path (owned) */
    char *socketPath;       /* per-db socket override (owned, may be NULL) */
    int syncCounter;        /* >0 = pending WAL syncs that need replication */
} LitestreamDBState;

static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;
static LitestreamDBState g_dbs[LS_MAX_DBS];
static int g_dbCount = 0;

/* Global default socket path, set at init time. */
static const char *g_socketPath = NULL;

/* The original VFS we're wrapping */
static sqlite3_vfs *g_pOrigVfs = NULL;

/*
** Find or create shared state for a database path.
** Caller must hold g_mu.
*/
static LitestreamDBState *findOrCreateDBStateLocked(const char *dbPath) {
    int i;
    if (dbPath == NULL) return NULL;

    for (i = 0; i < g_dbCount; i++) {
        if (strcmp(g_dbs[i].dbPath, dbPath) == 0) {
            return &g_dbs[i];
        }
    }

    if (g_dbCount >= LS_MAX_DBS) return NULL;

    g_dbs[g_dbCount].dbPath = sqlite3_mprintf("%s", dbPath);
    g_dbs[g_dbCount].socketPath = NULL;
    g_dbs[g_dbCount].syncCounter = 0;
    return &g_dbs[g_dbCount++];
}

static int getSyncCounter(const char *dbPath) {
    int val = 0;
    int i;
    pthread_mutex_lock(&g_mu);
    for (i = 0; i < g_dbCount; i++) {
        if (strcmp(g_dbs[i].dbPath, dbPath) == 0) {
            val = g_dbs[i].syncCounter;
            break;
        }
    }
    pthread_mutex_unlock(&g_mu);
    return val;
}

static void incSyncCounter(const char *dbPath) {
    LitestreamDBState *st;
    pthread_mutex_lock(&g_mu);
    st = findOrCreateDBStateLocked(dbPath);
    if (st) st->syncCounter++;
    pthread_mutex_unlock(&g_mu);
}

static void decSyncCounter(const char *dbPath) {
    int i;
    pthread_mutex_lock(&g_mu);
    for (i = 0; i < g_dbCount; i++) {
        if (strcmp(g_dbs[i].dbPath, dbPath) == 0) {
            if (g_dbs[i].syncCounter > 0) g_dbs[i].syncCounter--;
            break;
        }
    }
    pthread_mutex_unlock(&g_mu);
}

static const char *getSocketPath(const char *dbPath) {
    const char *path = NULL;
    int i;
    pthread_mutex_lock(&g_mu);
    for (i = 0; i < g_dbCount; i++) {
        if (strcmp(g_dbs[i].dbPath, dbPath) == 0) {
            path = g_dbs[i].socketPath;
            break;
        }
    }
    pthread_mutex_unlock(&g_mu);
    return path ? path : g_socketPath;
}

static void setSocketPath(const char *dbPath, const char *socketPath) {
    LitestreamDBState *st;
    pthread_mutex_lock(&g_mu);
    st = findOrCreateDBStateLocked(dbPath);
    if (st) {
        if (st->socketPath) sqlite3_free(st->socketPath);
        st->socketPath = sqlite3_mprintf("%s", socketPath);
    }
    pthread_mutex_unlock(&g_mu);
}

/* ---- Per-file state ---- */

typedef struct LitestreamFile {
    sqlite3_file base;          /* Must be first (SQLite convention) */
    sqlite3_file *pReal;        /* Real underlying file */
    int isWAL;                  /* 1 if this file handle is the WAL file */
    char *dbPath;               /* Database path — key into g_dbs (owned) */
} LitestreamFile;

/* Forward declarations */
static int lsOpen(sqlite3_vfs*, const char*, sqlite3_file*, int, int*);

/* Wrapped io_methods forward declarations */
static int lsClose(sqlite3_file*);
static int lsRead(sqlite3_file*, void*, int, sqlite3_int64);
static int lsWrite(sqlite3_file*, const void*, int, sqlite3_int64);
static int lsTruncate(sqlite3_file*, sqlite3_int64);
static int lsSync(sqlite3_file*, int);
static int lsFileSize(sqlite3_file*, sqlite3_int64*);
static int lsLock(sqlite3_file*, int);
static int lsUnlock(sqlite3_file*, int);
static int lsCheckReservedLock(sqlite3_file*, int*);
static int lsFileControl(sqlite3_file*, int, void*);
static int lsSectorSize(sqlite3_file*);
static int lsDeviceCharacteristics(sqlite3_file*);
/* v2 methods */
static int lsShmMap(sqlite3_file*, int, int, int, void volatile**);
static int lsShmLock(sqlite3_file*, int, int, int);
static void lsShmBarrier(sqlite3_file*);
static int lsShmUnmap(sqlite3_file*, int);
/* v3 methods */
static int lsFetch(sqlite3_file*, sqlite3_int64, int, void**);
static int lsUnfetch(sqlite3_file*, sqlite3_int64, void*);

static sqlite3_io_methods ls_io_methods_v1 = {
    1,                          /* iVersion */
    lsClose,
    lsRead,
    lsWrite,
    lsTruncate,
    lsSync,
    lsFileSize,
    lsLock,
    lsUnlock,
    lsCheckReservedLock,
    lsFileControl,
    lsSectorSize,
    lsDeviceCharacteristics,
    0, 0, 0, 0, 0, 0           /* v2/v3 slots */
};

static sqlite3_io_methods ls_io_methods_v2 = {
    2,                          /* iVersion */
    lsClose,
    lsRead,
    lsWrite,
    lsTruncate,
    lsSync,
    lsFileSize,
    lsLock,
    lsUnlock,
    lsCheckReservedLock,
    lsFileControl,
    lsSectorSize,
    lsDeviceCharacteristics,
    lsShmMap,
    lsShmLock,
    lsShmBarrier,
    lsShmUnmap,
    0, 0                        /* v3 slots */
};

static sqlite3_io_methods ls_io_methods_v3 = {
    3,                          /* iVersion */
    lsClose,
    lsRead,
    lsWrite,
    lsTruncate,
    lsSync,
    lsFileSize,
    lsLock,
    lsUnlock,
    lsCheckReservedLock,
    lsFileControl,
    lsSectorSize,
    lsDeviceCharacteristics,
    lsShmMap,
    lsShmLock,
    lsShmBarrier,
    lsShmUnmap,
    lsFetch,
    lsUnfetch
};

/*
** Derive the database path from a WAL path by stripping the "-wal" suffix.
** Returns a newly allocated string that the caller must free with sqlite3_free.
*/
static char *derive_db_path(const char *zWalPath) {
    size_t n;
    char *dbPath;
    if (zWalPath == NULL) return NULL;
    n = strlen(zWalPath);
    if (n < 4) return NULL;
    dbPath = sqlite3_malloc((int)(n - 4 + 1));
    if (dbPath == NULL) return NULL;
    memcpy(dbPath, zWalPath, n - 4);
    dbPath[n - 4] = '\0';
    return dbPath;
}

/*
** Send a synchronous replication request to the litestream sidecar.
** Returns 0 on success, -1 on failure.
*/
static int ipc_sync_replicate(const char *socketPath, const char *dbPath) {
    int fd;
    struct sockaddr_un addr;
    char body[512];
    char request[1024];
    char response[1024];
    ssize_t nread;
    int result = -1;

    if (socketPath == NULL || dbPath == NULL) return -1;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, socketPath, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }

    snprintf(body, sizeof(body), "{\"path\":\"%s\"}", dbPath);
    snprintf(request, sizeof(request),
        "POST /sync-replicate HTTP/1.0\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: %zu\r\n"
        "\r\n%s",
        strlen(body), body);

    if (write(fd, request, strlen(request)) < 0) {
        close(fd);
        return -1;
    }

    /* Shutdown write side so server sees EOF */
    shutdown(fd, SHUT_WR);

    /* Read response */
    memset(response, 0, sizeof(response));
    nread = read(fd, response, sizeof(response) - 1);
    close(fd);

    if (nread > 0 && strstr(response, "200") != NULL) {
        result = 0;
    }

    return result;
}

/*
** Select the appropriate io_methods table based on the real file's version.
*/
static const sqlite3_io_methods *ls_methods_for_version(int iVersion) {
    if (iVersion >= 3) return &ls_io_methods_v3;
    if (iVersion >= 2) return &ls_io_methods_v2;
    return &ls_io_methods_v1;
}

/* ---- VFS methods ---- */

static int lsOpen(sqlite3_vfs *pVfs, const char *zName,
                  sqlite3_file *pFile, int flags, int *pOutFlags) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    int rc;
    (void)pVfs;

    memset(p, 0, sizeof(*p));

    /* Allocate space for the real file handle */
    p->pReal = (sqlite3_file *)sqlite3_malloc(g_pOrigVfs->szOsFile);
    if (p->pReal == NULL) return SQLITE_NOMEM;
    memset(p->pReal, 0, g_pOrigVfs->szOsFile);

    rc = g_pOrigVfs->xOpen(g_pOrigVfs, zName, p->pReal, flags, pOutFlags);
    if (rc != SQLITE_OK) {
        sqlite3_free(p->pReal);
        p->pReal = NULL;
        return rc;
    }

    /* Tag WAL files for xSync interception */
    p->isWAL = (flags & SQLITE_OPEN_WAL) ? 1 : 0;

    /* Set dbPath for both main database files and WAL files so they
    ** share state through the global g_dbs table. */
    if (p->isWAL && zName != NULL) {
        p->dbPath = derive_db_path(zName);  /* strip "-wal" suffix */
    } else if ((flags & SQLITE_OPEN_MAIN_DB) && zName != NULL) {
        p->dbPath = sqlite3_mprintf("%s", zName);
    }

    /* Use the io_methods version that matches the real file */
    if (p->pReal->pMethods) {
        p->base.pMethods = ls_methods_for_version(p->pReal->pMethods->iVersion);
    } else {
        p->base.pMethods = &ls_io_methods_v1;
    }

    return SQLITE_OK;
}

static int lsDelete(sqlite3_vfs *pVfs, const char *zName, int syncDir) {
    (void)pVfs;
    return g_pOrigVfs->xDelete(g_pOrigVfs, zName, syncDir);
}

static int lsAccess(sqlite3_vfs *pVfs, const char *zName, int flags, int *pResOut) {
    (void)pVfs;
    return g_pOrigVfs->xAccess(g_pOrigVfs, zName, flags, pResOut);
}

static int lsFullPathname(sqlite3_vfs *pVfs, const char *zName, int nOut, char *zOut) {
    (void)pVfs;
    return g_pOrigVfs->xFullPathname(g_pOrigVfs, zName, nOut, zOut);
}

static void *lsDlOpen(sqlite3_vfs *pVfs, const char *zFilename) {
    (void)pVfs;
    return g_pOrigVfs->xDlOpen(g_pOrigVfs, zFilename);
}

static void lsDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg) {
    (void)pVfs;
    g_pOrigVfs->xDlError(g_pOrigVfs, nByte, zErrMsg);
}

static void (*lsDlSym(sqlite3_vfs *pVfs, void *p, const char *zSymbol))(void) {
    (void)pVfs;
    return g_pOrigVfs->xDlSym(g_pOrigVfs, p, zSymbol);
}

static void lsDlClose(sqlite3_vfs *pVfs, void *p) {
    (void)pVfs;
    g_pOrigVfs->xDlClose(g_pOrigVfs, p);
}

static int lsRandomness(sqlite3_vfs *pVfs, int nByte, char *zOut) {
    (void)pVfs;
    return g_pOrigVfs->xRandomness(g_pOrigVfs, nByte, zOut);
}

static int lsSleep(sqlite3_vfs *pVfs, int microseconds) {
    (void)pVfs;
    return g_pOrigVfs->xSleep(g_pOrigVfs, microseconds);
}

static int lsCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut) {
    (void)pVfs;
    return g_pOrigVfs->xCurrentTime(g_pOrigVfs, pTimeOut);
}

static int lsGetLastError(sqlite3_vfs *pVfs, int nByte, char *zOut) {
    (void)pVfs;
    return g_pOrigVfs->xGetLastError(g_pOrigVfs, nByte, zOut);
}

static int lsCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *pTimeOut) {
    (void)pVfs;
    return g_pOrigVfs->xCurrentTimeInt64(g_pOrigVfs, pTimeOut);
}

/* ---- File methods ---- */

static int lsClose(sqlite3_file *pFile) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    int rc = SQLITE_OK;
    if (p->pReal && p->pReal->pMethods) {
        rc = p->pReal->pMethods->xClose(p->pReal);
    }
    if (p->pReal) sqlite3_free(p->pReal);
    if (p->dbPath) sqlite3_free(p->dbPath);
    p->pReal = NULL;
    p->dbPath = NULL;
    return rc;
}

static int lsRead(sqlite3_file *pFile, void *zBuf, int iAmt, sqlite3_int64 iOfst) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    return p->pReal->pMethods->xRead(p->pReal, zBuf, iAmt, iOfst);
}

static int lsWrite(sqlite3_file *pFile, const void *zBuf, int iAmt, sqlite3_int64 iOfst) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    return p->pReal->pMethods->xWrite(p->pReal, zBuf, iAmt, iOfst);
}

static int lsTruncate(sqlite3_file *pFile, sqlite3_int64 size) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    return p->pReal->pMethods->xTruncate(p->pReal, size);
}

static int lsSync(sqlite3_file *pFile, int flags) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    int rc;
    const char *sock;

    /* Pass through for non-WAL files or when no sync is pending */
    if (!p->isWAL || !p->dbPath || getSyncCounter(p->dbPath) <= 0) {
        return p->pReal->pMethods->xSync(p->pReal, flags);
    }

    /* Look up per-db socket path (falls back to global default) */
    sock = getSocketPath(p->dbPath);

    /* 1. POST to litestream socket: /sync-replicate */
    if (ipc_sync_replicate(sock, p->dbPath) != 0) {
        /* S3 upload failed — return error so SQLite rolls back */
        decSyncCounter(p->dbPath);
        return SQLITE_IOERR;
    }

    /* 2. S3 succeeded, now do local fsync */
    rc = p->pReal->pMethods->xSync(p->pReal, flags);

    /* 3. Consume one pending sync request */
    decSyncCounter(p->dbPath);

    return rc;
}

static int lsFileSize(sqlite3_file *pFile, sqlite3_int64 *pSize) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    return p->pReal->pMethods->xFileSize(p->pReal, pSize);
}

static int lsLock(sqlite3_file *pFile, int eLock) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    return p->pReal->pMethods->xLock(p->pReal, eLock);
}

static int lsUnlock(sqlite3_file *pFile, int eLock) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    return p->pReal->pMethods->xUnlock(p->pReal, eLock);
}

static int lsCheckReservedLock(sqlite3_file *pFile, int *pResOut) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    return p->pReal->pMethods->xCheckReservedLock(p->pReal, pResOut);
}

static int lsFileControl(sqlite3_file *pFile, int op, void *pArg) {
    LitestreamFile *p = (LitestreamFile *)pFile;

    if (op == SQLITE_FCNTL_PRAGMA && p->dbPath) {
        char **azArg = (char **)pArg;
        /* azArg[1] = pragma name, azArg[2] = value (or NULL for read) */

        if (azArg[1] && strcmp(azArg[1], "litestream_sync") == 0) {
            if (azArg[2] == NULL) {
                /* Read: return current counter value */
                azArg[0] = sqlite3_mprintf("%d", getSyncCounter(p->dbPath));
                return SQLITE_OK;
            }
            /* Write: increment counter if setting to 1 (request sync) */
            if (atoi(azArg[2]) > 0) {
                incSyncCounter(p->dbPath);
            }
            azArg[0] = sqlite3_mprintf("%d", getSyncCounter(p->dbPath));
            return SQLITE_OK;
        }

        if (azArg[1] && strcmp(azArg[1], "litestream_socket") == 0) {
            if (azArg[2] == NULL) {
                /* Read: return current value from shared state */
                const char *sock = getSocketPath(p->dbPath);
                azArg[0] = sqlite3_mprintf("%s", sock ? sock : "(default)");
                return SQLITE_OK;
            }
            /* Write: set socket path in shared state */
            setSocketPath(p->dbPath, azArg[2]);
            azArg[0] = sqlite3_mprintf("ok");
            return SQLITE_OK;
        }
    }

    return p->pReal->pMethods->xFileControl(p->pReal, op, pArg);
}

static int lsSectorSize(sqlite3_file *pFile) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    return p->pReal->pMethods->xSectorSize(p->pReal);
}

static int lsDeviceCharacteristics(sqlite3_file *pFile) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    return p->pReal->pMethods->xDeviceCharacteristics(p->pReal);
}

/* v2 shared memory methods */
static int lsShmMap(sqlite3_file *pFile, int iPg, int pgsz, int bExtend, void volatile **pp) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    return p->pReal->pMethods->xShmMap(p->pReal, iPg, pgsz, bExtend, pp);
}

static int lsShmLock(sqlite3_file *pFile, int offset, int n, int flags) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    return p->pReal->pMethods->xShmLock(p->pReal, offset, n, flags);
}

static void lsShmBarrier(sqlite3_file *pFile) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    p->pReal->pMethods->xShmBarrier(p->pReal);
}

static int lsShmUnmap(sqlite3_file *pFile, int deleteFlag) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    return p->pReal->pMethods->xShmUnmap(p->pReal, deleteFlag);
}

/* v3 memory-mapped I/O methods */
static int lsFetch(sqlite3_file *pFile, sqlite3_int64 iOfst, int iAmt, void **pp) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    return p->pReal->pMethods->xFetch(p->pReal, iOfst, iAmt, pp);
}

static int lsUnfetch(sqlite3_file *pFile, sqlite3_int64 iOfst, void *pBuf) {
    LitestreamFile *p = (LitestreamFile *)pFile;
    return p->pReal->pMethods->xUnfetch(p->pReal, iOfst, pBuf);
}

/* ---- Extension entry point ---- */

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_litestreamsync_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi
) {
    static sqlite3_vfs lsVfs;
    (void)db;

    SQLITE_EXTENSION_INIT2(pApi);

    /* Find the default VFS */
    g_pOrigVfs = sqlite3_vfs_find(NULL);
    if (g_pOrigVfs == NULL) {
        if (pzErrMsg) {
            *pzErrMsg = sqlite3_mprintf("cannot find default VFS");
        }
        return SQLITE_ERROR;
    }

    /* Read default socket path from environment */
    g_socketPath = getenv("LITESTREAM_SOCKET");
    if (g_socketPath == NULL || g_socketPath[0] == '\0') {
        g_socketPath = "/var/run/litestream.sock";
    }

    /* Build our wrapper VFS */
    memset(&lsVfs, 0, sizeof(lsVfs));
    lsVfs.iVersion       = g_pOrigVfs->iVersion;
    lsVfs.szOsFile       = (int)sizeof(LitestreamFile);
    lsVfs.mxPathname     = g_pOrigVfs->mxPathname;
    lsVfs.zName          = "litestream";
    lsVfs.pAppData       = (void *)g_pOrigVfs;

    /* VFS methods */
    lsVfs.xOpen           = lsOpen;
    lsVfs.xDelete         = lsDelete;
    lsVfs.xAccess         = lsAccess;
    lsVfs.xFullPathname   = lsFullPathname;
    lsVfs.xDlOpen         = lsDlOpen;
    lsVfs.xDlError        = lsDlError;
    lsVfs.xDlSym          = lsDlSym;
    lsVfs.xDlClose        = lsDlClose;
    lsVfs.xRandomness     = lsRandomness;
    lsVfs.xSleep          = lsSleep;
    lsVfs.xCurrentTime    = lsCurrentTime;
    lsVfs.xGetLastError   = lsGetLastError;
    lsVfs.xCurrentTimeInt64 = lsCurrentTimeInt64;

    /* Register as default VFS. The shim is a transparent passthrough when
    ** syncOnCommit is 0, so there is no overhead for normal operations.
    ** Being default means all new connections automatically use the shim
    ** without requiring URI parameters or special connection setup. */
    if (sqlite3_vfs_register(&lsVfs, 1) != SQLITE_OK) {
        if (pzErrMsg) {
            *pzErrMsg = sqlite3_mprintf("failed to register litestream VFS");
        }
        return SQLITE_ERROR;
    }

    /* SQLITE_OK_LOAD_PERMANENTLY tells SQLite not to dlclose() this
    ** extension when the loading connection closes. The VFS must persist
    ** for the lifetime of the process since future connections use it. */
    return SQLITE_OK_LOAD_PERMANENTLY;
}
