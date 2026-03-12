#ifndef FDP_H
#define FDP_H

#include "common/relpath.h"
#include <fcntl.h>

// add
#ifndef RWH_WRITE_LIFE_NOT_SET
#define RWH_WRITE_LIFE_NOT_SET  0
#define RWH_WRITE_LIFE_NONE     1
#define RWH_WRITE_LIFE_SHORT    2
#define RWH_WRITE_LIFE_MEDIUM   3
#define RWH_WRITE_LIFE_LONG     4
#define RWH_WRITE_LIFE_EXTREME  5
#endif

static inline void
fdp_set_fd_hint(int fd, ForkNumber forknum)
{

// #ifdef HAVE_FDP_HINTS // TODO(jhpark): add BUILD_FLAG
    uint64_t hint;

    switch (forknum)
    {
        case MAIN_FORKNUM:
            hint = RWH_WRITE_LIFE_LONG;     /* element tuple: COLD*/
            break;
        case HNSW_NBR_FORKNUM:
            hint = RWH_WRITE_LIFE_MEDIUM;   /* neighbor tuple: WARM, HOT? */
            break;
        case FSM_FORKNUM:
        case VISIBILITYMAP_FORKNUM:
        case INIT_FORKNUM:
            hint = RWH_WRITE_LIFE_SHORT;    /* VACUUM */
            break;
        default:
            hint = RWH_WRITE_LIFE_NOT_SET;
            break;
    }
    (void) fcntl(fd, F_SET_RW_HINT, &hint);  /*debug*/
//#endif
}

/* For WAL file */
static inline void
fdp_set_wal_hint(int fd)
{
//#ifdef HAVE_FDP_HINTS
    uint64_t hint = RWH_WRITE_LIFE_SHORT;
    (void) fcntl(fd, F_SET_RW_HINT, &hint);
//#endif
}

#endif /* FDP_H */
