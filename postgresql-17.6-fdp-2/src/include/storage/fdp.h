#ifndef FDP_H
#define FDP_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>

static inline uint64_t
give_write_hintno(char *datadir, const char *filepath)
{
	char absolute_path[PATH_MAX];
	char path2seg[PATH_MAX];

	if (strncmp(datadir, "/mnt/fdp/data1", 14) == 0)	/* For pgvector */
	{
		if(strncmp(filepath, "pg_wal", 6) == 0)
			return 2;	/* For WAL File */
		
		snprintf(path2seg, sizeof(path2seg), "%s/%s", datadir, filepath);

		if (realpath(path2seg, absolute_path) != NULL)
		{
			if (strncmp(absolute_path, "/mnt/fdp/pg_rel", 15) == 0)
				return 3;	/* For Relation */
			
			if (strncmp(absolute_path, "/mnt/fdp/pg_idx", 15) == 0)
			{
				if (strstr(absolute_path, "_nbr") != NULL)
					return 4;	/* For Neighbot tuple */
			
				return 5;		/* For Element tuple */
			}
		}

		return 1;	/* For others done by pgvector */
	}

	return 0;		/* not pgvector workload, it should be tpcc like oltp workload */
}


#endif
