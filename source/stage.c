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
_find_stage_request(ds3_client        *  Client,
                    char              *  BucketName,
                    char              *  ObjectName,
                    globus_gfs_stat_t *  Gstat,
                    ds3_bulk_response ** GetJobResponse)
{
	int                     i                 = 0;
	int                     j                 = 0;
	int                     k                 = 0;
	globus_result_t         result            = GLOBUS_SUCCESS;
	ds3_bulk_response     * bulk_response     = NULL;
	ds3_get_jobs_response * get_jobs_response = NULL;

	*GetJobResponse = NULL;

	result = gds3_get_jobs(Client, &get_jobs_response);
	if (result)
		return result;

	/* For each job .... */
	for (i = 0; get_jobs_response && i < get_jobs_response->jobs_size; i++)
	{
		/* Must be in this bucket. */
		if (strcmp(get_jobs_response->jobs[i]->bucket_name->value, BucketName))
			continue;

		/* The request must be for the entire file. */
		if (get_jobs_response->jobs[i]->original_size_in_bytes != Gstat->size)
			continue;

		/* The request must be a retrieve. */
		if (get_jobs_response->jobs[i]->request_type != GET)
			continue;

		/* Get this job's detailed information. */
		result = gds3_get_job(Client,
		                      get_jobs_response->jobs[i]->job_id->value,
		                      &bulk_response);
		if (result)
			break;

		if (bulk_response)
		{
			/* For each chunk ... */
			for (j = 0; !*GetJobResponse && j < bulk_response->list_size; j++)
			{
				/* For each portion of this chunk ... */
				for (k = 0; k < bulk_response->list_size; k++)
				{
					/* Does the name match? */
					if (strcmp(bulk_response->list[j]->list[k].name->value, ObjectName) == 0)
					{
						*GetJobResponse = bulk_response;
						goto cleanup;
					}
				}
			}
		}
		ds3_free_bulk_response(bulk_response);
		bulk_response = NULL;
	}

cleanup:
	ds3_free_get_jobs_response(get_jobs_response);

	return result;
}

/*
 * ds3_get_available_chunks() apparently can not be trusted to tell you all chunks
 * that are available. You'll need to rely on ds3_bulk_object from gds3_get_job()
 * or gds3_init_bulk_get().
 */
globus_result_t
stage_file(ds3_client           * Client,
           char                 * Pathname, 
           int                    Timeout, 
           stage_file_residency * Residency)
{
	char                              * bucket_name       = NULL;
	char                              * object_name       = NULL;
	globus_result_t                     result            = GLOBUS_SUCCESS;
	ds3_bulk_response                 * bulk_response     = NULL;
	ds3_bulk_response                 * bulk_get_response = NULL;
	ds3_bulk_response                 * get_job_response  = NULL;
	globus_gfs_stat_t                   gstat;
	time_t                              start_time        = time(NULL);
	int                                 i                 = 0;
 
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

	*Residency = STAGE_FILE_ARCHIVED;
	do
	{
		/*
		 * Try to lookup the stage request through the job interface.
		 */
		result = _find_stage_request(Client, bucket_name, object_name, &gstat, &get_job_response);
		if (result)
			goto cleanup;
		bulk_response = get_job_response;

		/*
		 * If we didn't find the stage request.
		 */
		if (!bulk_response)
		{
			/* Create the stage request. */
			result = gds3_init_bulk_get(Client,
			                            bucket_name,
			                            object_name,
			                            0,
			                            gstat.size,
			                            &bulk_get_response);
			if (result)
				goto cleanup;

			bulk_response = bulk_get_response;
		}

		*Residency = STAGE_FILE_RESIDENT;
		for (i = 0; i < bulk_response->list_size; i++)
		{
			assert(bulk_response->list[i]->size == 1);

			if (bulk_response->list[i]->list[0].in_cache == False)
			{
				*Residency = STAGE_FILE_ARCHIVED;
				break;
			}
		}

		ds3_free_bulk_response(get_job_response);
		ds3_free_bulk_response(bulk_get_response);
		get_job_response  = NULL;
		bulk_get_response = NULL;

		if (*Residency == STAGE_FILE_RESIDENT)
			break;

		// Sleep for 1.0 sec
		struct timeval tv;
		tv.tv_sec  = 1;
		tv.tv_usec = 0;
		select(0, NULL, NULL, NULL, &tv);
	} while ((time(NULL) - start_time) < Timeout);

cleanup:

	ds3_free_bulk_response(get_job_response);
	ds3_free_bulk_response(bulk_get_response);
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

