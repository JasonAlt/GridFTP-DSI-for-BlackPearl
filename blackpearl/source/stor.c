/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright  2015 NCSA.  All rights reserved.
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

#ifndef BLACKPEARL_DSI_STOR_H
#define BLACKPEARL_DSI_STOR_H

/*
 * System includes
 */
#include <pthread.h>
#include <assert.h>

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * DS3 includes
 */
#include <ds3.h>

/*
 * Local includes
 */
#include "stor.h"
#include "gds3.h"
#include "path.h"

typedef struct {
	pthread_mutex_t              Mutex;
	pthread_cond_t               Cond;
	ds3_client                 * Client;
	globus_gfs_operation_t       Operation;
	globus_gfs_transfer_info_t * TransferInfo;
	char                       * Bucket;
	char                       * Object;
	int                          Started;
	uint64_t                     Offset;

	globus_result_t              Result;
	globus_size_t                Length;
	globus_bool_t                Eof;

} stor_info_t;

void
stor_ds3_gridftp_callback(globus_gfs_operation_t Operation,
                          globus_result_t        Result,
                          globus_byte_t        * Buffer,
                          globus_size_t          Length,
                          globus_off_t           Offset,
                          globus_bool_t          Eof,
                          void                 * UserArg)
{
	stor_info_t * stor_info = UserArg;

	pthread_mutex_lock(&stor_info->Mutex);
	{
		if (!stor_info->Result)
			stor_info->Result = Result;
		if (!stor_info->Eof)
			stor_info->Eof = Eof;
		assert(stor_info->Offset == Offset);
		stor_info->Length = Length;
		pthread_cond_signal(&stor_info->Cond);
	}
	pthread_mutex_unlock(&stor_info->Mutex);
}

size_t
stor_ds3_callback(void * Buffer,
                  size_t Length,
                  size_t Nmemb,
                  void * UserArg)
{
	globus_result_t result    = GLOBUS_SUCCESS;
	stor_info_t   * stor_info = UserArg;
	int             rc        = 0;

	GlobusGFSName(stor_ds3_callback);

	if (!stor_info->Started)
		globus_gridftp_server_begin_transfer(stor_info->Operation, 0, NULL);
	stor_info->Started = 1;

	pthread_mutex_lock(&stor_info->Mutex);
	{
		if (stor_info->Eof)
		{
			result = GlobusGFSErrorGeneric("Premature end of data transfer");
			rc = -1;
			goto cleanup;
		}

		result = globus_gridftp_server_register_read(stor_info->Operation,
		                                             Buffer,
		                                             Length * Nmemb,
		                                             stor_ds3_gridftp_callback,
		                                             stor_info);
		if (result)
		{
			stor_info->Result = result;
			rc = -1;
			goto cleanup;
		}

		pthread_cond_wait(&stor_info->Cond, &stor_info->Mutex);

		if (stor_info->Result)
		{
			rc = -1;
			goto cleanup;
		}

		stor_info->Offset += stor_info->Length;
		rc = stor_info->Length;
	}
cleanup:
	pthread_mutex_unlock(&stor_info->Mutex);

	return rc;
}

void *
stor_thread(void * UserArg)
{
	globus_result_t result    = GLOBUS_SUCCESS;
	stor_info_t   * stor_info = UserArg;

	result = gds3_put_object_for_job(stor_info->Client,
	                                 stor_info->Bucket,
	                                 stor_info->Object,
	                                 0, /* Offset */
	                                 stor_info->TransferInfo->alloc_size,
	                                 NULL,
	                                 stor_ds3_callback,
	                                 stor_info);

	if (!result) result = stor_info->Result;
	globus_gridftp_server_finished_transfer(stor_info->Operation, result);
	pthread_mutex_destroy(&stor_info->Mutex);
	pthread_cond_destroy(&stor_info->Cond);
	free(stor_info->Bucket);
	free(stor_info->Object);
	free(stor_info);

	return NULL;
}

void
stor(ds3_client                 * Client, 
     globus_gfs_operation_t       Operation,
     globus_gfs_transfer_info_t * TransferInfo)
{
	globus_result_t result    = GLOBUS_SUCCESS;
	stor_info_t   * stor_info = NULL;
	char          * bucket    = NULL;
	char          * object    = NULL;
	int             rc        = 0;
	int             initted   = 0;
	pthread_t       thread;
	pthread_attr_t  attr;
// Need to make sure this isn't a restart

	GlobusGFSName(stor);

	path_split(TransferInfo->pathname, &bucket, &object);
	if (!object)
	{
		if (!bucket) free(bucket);
		result = GlobusGFSErrorGeneric("Can not store objects outside of a bucket");
		globus_gridftp_server_finished_transfer(Operation, result);
		return;
	}

	stor_info = malloc(sizeof(stor_info_t));
	if (!stor_info)
	{
		free(bucket);
		free(object);
		result = GlobusGFSErrorMemory("stor_info_t");
		globus_gridftp_server_finished_transfer(Operation, result);
		return;
	}

	memset(stor_info, 0, sizeof(stor_info_t));
	pthread_mutex_init(&stor_info->Mutex, NULL);
	pthread_cond_init(&stor_info->Cond, NULL);
	stor_info->Client       = Client;
	stor_info->Operation    = Operation;
	stor_info->TransferInfo = TransferInfo;
	stor_info->Bucket       = bucket;
	stor_info->Object       = object;

	/*
	 * Launch a detached thread.
	 */
	if ((rc = pthread_attr_init(&attr)) || !(initted = 1) ||
	    (rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED)) ||
	    (rc = pthread_create(&thread, &attr, stor_thread, stor_info)))
	{
		result = GlobusGFSErrorSystemError("Launching put object thread", rc);
		globus_gridftp_server_finished_transfer(Operation, result);

		pthread_mutex_destroy(&stor_info->Mutex);
		pthread_cond_destroy(&stor_info->Cond);
		free(stor_info->Bucket);
		free(stor_info->Object);
		free(stor_info);
	}

	if (initted) pthread_attr_destroy(&attr);
}

#endif /* BLACKPEARL_DSI_STOR_H */
