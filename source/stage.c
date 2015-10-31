/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright © 2015 NCSA.  All rights reserved.
 *
 * Developed by:
 *
 * Storage Enabling Technologies (SET)
 *
 * Nation Center for Supercomputing Applications (NCSA)
 *
 * http://www.ncsa.illinois.edu
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the .Software.),
 * to deal with the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 *    + Redistributions of source code must retain the above copyright notice,
 *      this list of conditions and the following disclaimers.
 *
 *    + Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimers in the
 *      documentation and/or other materials provided with the distribution.
 *
 *    + Neither the names of SET, NCSA
 *      nor the names of its contributors may be used to endorse or promote
 *      products derived from this Software without specific prior written
 *      permission.
 *
 * THE SOFTWARE IS PROVIDED .AS IS., WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 * THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS WITH THE SOFTWARE.
 */

/*
 * System includes
 */
#include <sys/select.h>
#include <assert.h>
#include <stdlib.h>
#include <time.h>

/*
 * Globus includes
 */
#include <globus_list.h>

/*
 * DS3 includes
 */
#include <ds3.h>

/*
 * Local includes
 */
#include "error.h"
#include "stage.h"
#include "stat.h"
#include "path.h"
#include "gds3.h"

globus_result_t
stage_get_timeout(globus_gfs_operation_t      Operation,
                  globus_gfs_command_info_t * CommandInfo,
                  int                       * Timeout)
{
	globus_result_t result;
	char ** argv    = NULL;
	int     argc    = 0;
	int     retval  = 0;

	GlobusGFSName(stage_get_timeout);

	/* Get the command arguments. */
	result = globus_gridftp_server_query_op_info(Operation,
	                                             CommandInfo->op_info,
	                                             GLOBUS_GFS_OP_INFO_CMD_ARGS,
	                                             &argv,
	                                             &argc);

	if (result)
		return GlobusGFSErrorWrapFailed("Unable to get command args", result);

	/* Convert the timeout. */
	retval = sscanf(argv[2], "%d", Timeout);
	if (retval != 1)
		return GlobusGFSErrorGeneric("Illegal timeout value");

	return GLOBUS_SUCCESS;
}

globus_result_t
stage_file(ds3_client           * Client,
           char                 * Pathname, 
           int                    Timeout, 
           stage_file_residency * Residency)
{
	char                              * bucket_name    = NULL;
	char                              * object_name    = NULL;
	globus_result_t                     result         = GLOBUS_SUCCESS;
	ds3_bulk_response                 * bulk_response  = NULL;
	ds3_get_available_chunks_response * chunk_response = NULL;
	ds3_bulk_object_list              * object_list    = NULL;
	globus_gfs_stat_t                   gstat;
	uint64_t                            offset         = 0;
	time_t                              start_time     = time(NULL);
	int                                 i              = 0;

	GlobusGFSName(stage_file);

	// Assume it is resident
	*Residency = STAGE_FILE_RESIDENT;

	// Make sure it is a regular file
	result = stat_entry(Client, Pathname, &gstat);
	if (result)
		return result;

	if (!S_ISREG(gstat.mode))
		goto cleanup;

	path_split(Pathname, &bucket_name, &object_name);

	result = gds3_init_bulk_get(Client,
	                            bucket_name,
	                            object_name,
	                            0,
	                            gstat.size,
	                            &bulk_response);
	if (result)
		goto cleanup;

	/* Now wait for the given about of time or the file staged. */
	// Assume it is purged
	*Residency = STAGE_FILE_ARCHIVED;
	do
	{
		result = gds3_available_chunks(Client,
		                               bulk_response->job_id,
		                               &chunk_response);
		if (result)
			goto cleanup;

		if (chunk_response->object_list)
		{
			/* See if we can map the entire length of the file to available chunks. */
			for (offset = 0; offset != gstat.size; )
			{
				for (i = 0; i < chunk_response->object_list->list_size; i++)
				{
					assert(chunk_response->object_list->list[i]->size == 1);
					if (chunk_response->object_list->list[i]->list[0].offset == offset)
					{
						offset += chunk_response->object_list->list[i]->list[0].length;
						break;
					}
				}

				if (i == chunk_response->object_list->list_size)
					break;
			}

			if (offset == gstat.size)
				*Residency = STAGE_FILE_RESIDENT;
		}

		ds3_free_available_chunks_response(chunk_response);
		if (*Residency == STAGE_FILE_RESIDENT)
			break;

		// Sleep for 1.0 sec
		struct timeval tv;
		tv.tv_sec  = 1;
		tv.tv_usec = 0;
		select(0, NULL, NULL, NULL, &tv);
	} while ((time(NULL) - start_time) < Timeout);


cleanup:
	if (bulk_response)
		gds3_delete_job(Client, bulk_response->job_id);

	ds3_free_bulk_object_list(object_list);
	ds3_free_bulk_response(bulk_response);
	stat_destroy_array(&gstat, 1);
	if (bucket_name)
		free(bucket_name);
	if (object_name)
		free(object_name);

	return result;

}

void
stage(globus_gfs_operation_t      Operation,
      globus_gfs_command_info_t * CommandInfo,
      ds3_client                * Client,
      commands_callback           Callback)
{
	int                  timeout;
	char               * command_output = NULL;
	stage_file_residency residency;
	globus_result_t      result;

	/* Get the timeout. */
	result = stage_get_timeout(Operation, CommandInfo, &timeout);
	if (result)
		goto cleanup;

	result = stage_file(Client, CommandInfo->pathname, timeout, &residency);
	if (result)
		goto cleanup;


	switch (residency)
	{
	case STAGE_FILE_RESIDENT:
		command_output = globus_common_create_string(
		                                     "250 Stage of file %s succeeded.\r\n",
		                                     CommandInfo->pathname);
		goto cleanup;

	case STAGE_FILE_TAPE_ONLY:
		command_output = globus_common_create_string(
		                             "250 %s is on a tape only class of service.\r\n",
		                             CommandInfo->pathname);
		goto cleanup;

	case STAGE_FILE_ARCHIVED:
		command_output = globus_common_create_string(
		                             "450 %s: is being retrieved from the archive...\r\n",
		                             CommandInfo->pathname);
		break;
	}

cleanup:
	Callback(Operation, result, command_output);
	if (command_output)
		globus_free(command_output);
}

