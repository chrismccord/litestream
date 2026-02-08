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

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

/* Global default socket path, set at init time. */
static const char *g_socketPath = NULL;

/* Forward declarations */
static int lsOpen(sqlite3_vfs*, const char*, sqlite3_file*, int, int*);

/*
** Per-file state. Must have sqlite3_file as first member.
*/
typedef struct LitestreamFile {
    sqlite3_file base;          /* Must be first (SQLite convention) */
    sqlite3_file *pReal;        /* Real underlying file */
    sqlite3_vfs *pRealVfs;      /* Real underlying VFS */
    int isWAL;                  /* 1 if this file handle is the WAL file */
    int syncOnCommit;           /* Set by PRAGMA litestream_sync */
    char *socketPath;           /* Path to litestream Unix socket (owned) */
    char *dbPath;               /* Database path for IPC request (owned) */
} LitestreamFile;

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

/* The original VFS we're wrapping */
static sqlite3_vfs *g_pOrigVfs = NULL;

/*
** Derive the database path from a WAL path by stripping the "-wal" suffix.
** Returns a newly allocated string that the caller must free.
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

    memset(p, 0, sizeof(*p));

    /* Allocate space for the real file handle */
    p->pReal = (sqlite3_file *)sqlite3_malloc(g_pOrigVfs->szOsFile);
    if (p->pReal == NULL) return SQLITE_NOMEM;
    memset(p->pReal, 0, g_pOrigVfs->szOsFile);
    p->pRealVfs = g_pOrigVfs;

    rc = g_pOrigVfs->xOpen(g_pOrigVfs, zName, p->pReal, flags, pOutFlags);
    if (rc != SQLITE_OK) {
        sqlite3_free(p->pReal);
        p->pReal = NULL;
        return rc;
    }

    /* Tag WAL files for xSync interception */
    p->isWAL = (flags & SQLITE_OPEN_WAL) ? 1 : 0;

    /* Extract DB path from WAL path (strip "-wal" suffix) */
    if (p->isWAL && zName != NULL) {
        p->dbPath = derive_db_path(zName);
    }

    /* Set default socket path */
    if (g_socketPath != NULL) {
        p->socketPath = sqlite3_mprintf("%s", g_socketPath);
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
    return g_pOrigVfs->xDelete(g_pOrigVfs, zName, syncDir);
}

static int lsAccess(sqlite3_vfs *pVfs, const char *zName, int flags, int *pResOut) {
    return g_pOrigVfs->xAccess(g_pOrigVfs, zName, flags, pResOut);
}

static int lsFullPathname(sqlite3_vfs *pVfs, const char *zName, int nOut, char *zOut) {
    return g_pOrigVfs->xFullPathname(g_pOrigVfs, zName, nOut, zOut);
}

static void *lsDlOpen(sqlite3_vfs *pVfs, const char *zFilename) {
    return g_pOrigVfs->xDlOpen(g_pOrigVfs, zFilename);
}

static void lsDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg) {
    g_pOrigVfs->xDlError(g_pOrigVfs, nByte, zErrMsg);
}

static void (*lsDlSym(sqlite3_vfs *pVfs, void *p, const char *zSymbol))(void) {
    return g_pOrigVfs->xDlSym(g_pOrigVfs, p, zSymbol);
}

static void lsDlClose(sqlite3_vfs *pVfs, void *p) {
    g_pOrigVfs->xDlClose(g_pOrigVfs, p);
}

static int lsRandomness(sqlite3_vfs *pVfs, int nByte, char *zOut) {
    return g_pOrigVfs->xRandomness(g_pOrigVfs, nByte, zOut);
}

static int lsSleep(sqlite3_vfs *pVfs, int microseconds) {
    return g_pOrigVfs->xSleep(g_pOrigVfs, microseconds);
}

static int lsCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut) {
    return g_pOrigVfs->xCurrentTime(g_pOrigVfs, pTimeOut);
}

static int lsGetLastError(sqlite3_vfs *pVfs, int nByte, char *zOut) {
    return g_pOrigVfs->xGetLastError(g_pOrigVfs, nByte, zOut);
}

static int lsCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *pTimeOut) {
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
    if (p->socketPath) sqlite3_free(p->socketPath);
    if (p->dbPath) sqlite3_free(p->dbPath);
    p->pReal = NULL;
    p->socketPath = NULL;
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

    /* Pass through for non-WAL files or when sync not requested */
    if (!p->isWAL || !p->syncOnCommit) {
        return p->pReal->pMethods->xSync(p->pReal, flags);
    }

    /* 1. POST to litestream socket: /sync-replicate */
    if (ipc_sync_replicate(p->socketPath, p->dbPath) != 0) {
        /* S3 upload failed — return error so SQLite rolls back */
        p->syncOnCommit = 0;
        return SQLITE_IOERR;
    }

    /* 2. S3 succeeded, now do local fsync */
    int rc = p->pReal->pMethods->xSync(p->pReal, flags);

    /* 3. Auto-reset the flag */
    p->syncOnCommit = 0;

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

    if (op == SQLITE_FCNTL_PRAGMA) {
        char **azArg = (char **)pArg;
        /* azArg[1] = pragma name, azArg[2] = value (or NULL for read) */

        if (azArg[1] && strcmp(azArg[1], "litestream_sync") == 0) {
            if (azArg[2] == NULL) {
                /* Read: return current value */
                azArg[0] = sqlite3_mprintf("%d", p->syncOnCommit);
                return SQLITE_OK;
            }
            /* Write: set flag */
            p->syncOnCommit = atoi(azArg[2]);
            return SQLITE_OK;
        }

        if (azArg[1] && strcmp(azArg[1], "litestream_socket") == 0) {
            if (azArg[2] == NULL) {
                /* Read: return current value */
                azArg[0] = sqlite3_mprintf("%s",
                    p->socketPath ? p->socketPath : "(default)");
                return SQLITE_OK;
            }
            /* Write: set socket path */
            if (p->socketPath) sqlite3_free(p->socketPath);
            p->socketPath = sqlite3_mprintf("%s", azArg[2]);
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
int sqlite3_litestreamSync_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi
) {
    static sqlite3_vfs lsVfs;

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

    /* Register as non-default (applications opt-in with PRAGMA or URI) */
    return sqlite3_vfs_register(&lsVfs, 0);
}
