/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright � 2015 NCSA.  All rights reserved.
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
#include <pthread.h>

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
#include "retr.h"
#include "gds3.h"
#include "path.h"
#include "stat.h"
#include "markers.h"

void
retr_gridftp_callout(globus_gfs_operation_t Operation,
                     globus_result_t        Result,
                     globus_byte_t        * Buffer,
                     globus_size_t          Length,
                     void                 * UserArg)
{
	retr_info_t * retr_info = UserArg;

	pthread_mutex_lock(&retr_info->Mutex);
	{
		if (!retr_info->Result)
			retr_info->Result = Result;
		globus_list_insert(&retr_info->FreeBufferList, (char *)Buffer);
		pthread_cond_signal(&retr_info->Cond);
	}
	pthread_mutex_unlock(&retr_info->Mutex);
}

/*
 * Called locked.
 */
globus_result_t
retr_get_free_buffer(retr_info_t *  RetrInfo,
                     char        ** FreeBuffer)
{
	int all_buf_cnt  = 0;
	int free_buf_cnt = 0;

	GlobusGFSName(retr_get_free_buffer);

	/*
	 * Check for the optimal number of concurrent writes.
	 */
	if (RetrInfo->ConnChkCnt++ == 0)
		globus_gridftp_server_get_optimal_concurrency(RetrInfo->Operation,
		                                             &RetrInfo->OptConnCnt);
	if (RetrInfo->ConnChkCnt >= 100) RetrInfo->ConnChkCnt = 0;

	/*
	 * Wait for a free buffer or wait until conditions are right to create one.
	 */
	while (1)
	{
		/* Check for error first. */
		if (RetrInfo->Result)
			return RetrInfo->Result;

		all_buf_cnt  = globus_list_size(RetrInfo->AllBufferList);
		free_buf_cnt = globus_list_size(RetrInfo->FreeBufferList);

		/* If we have a free buffer... */
		if (free_buf_cnt) break;
		/* If we can create another free buffer... */
		if (all_buf_cnt < RetrInfo->OptConnCnt) break;

		pthread_cond_wait(&RetrInfo->Cond, &RetrInfo->Mutex);
	}

	if (!globus_list_empty(RetrInfo->FreeBufferList))
	{
		*FreeBuffer = globus_list_remove(&RetrInfo->FreeBufferList, RetrInfo->FreeBufferList);
		return GLOBUS_SUCCESS;
	}

	*FreeBuffer = globus_malloc(RetrInfo->BlockSize);
	if (!*FreeBuffer)
		return GlobusGFSErrorMemory("free_buffer");
	globus_list_insert(&RetrInfo->AllBufferList, *FreeBuffer);
	return GLOBUS_SUCCESS;
}

size_t
retr_ds3_callout(void * ReadyBuffer,
                 size_t Length,
                 size_t Nmemb,
                 void * UserArg)
{
	globus_result_t result      = GLOBUS_SUCCESS;
	retr_info_t   * retr_info   = UserArg;
	char          * free_buffer = NULL;
	int             rc          = Length * Nmemb;
	int             buf_offset  = 0;
	int             cpy_length  = 0;

	GlobusGFSName(retr_ds3_callout);

	pthread_mutex_lock(&retr_info->Mutex);
	{
		while (buf_offset != (Length*Nmemb))
		{
			result = retr_get_free_buffer(retr_info, &free_buffer);
			if (result)
			{
				rc = 1; /* Signal to shutdown. */
				if (!retr_info->Result) retr_info->Result = result;
				goto cleanup;
			}

			cpy_length = (Length*Nmemb) - buf_offset;
			if (cpy_length > retr_info->BlockSize)
				cpy_length = retr_info->BlockSize;

			memcpy(free_buffer, ReadyBuffer + buf_offset, cpy_length);

			result = globus_gridftp_server_register_write(retr_info->Operation,
			                                              free_buffer,
			                                              cpy_length,
			                                              retr_info->Offset,
			                                              0,
			                                              retr_gridftp_callout,
			                                              retr_info);

			if (result)
			{
				if(!retr_info->Result) retr_info->Result = result;
				rc = -1;
				goto cleanup;
			}

			/* Update perf markers */
			markers_update_perf_markers(retr_info->Operation, 
			                            retr_info->Offset,
			                            cpy_length);

			retr_info->Offset += cpy_length;
			buf_offset        += cpy_length;
		}
	}
cleanup:
	pthread_mutex_unlock(&retr_info->Mutex);

	return rc;
}

void
retr_wait_for_gridftp(retr_info_t * RetrInfo)
{
	pthread_mutex_lock(&RetrInfo->Mutex);
	{
		while (1)
		{
			if (RetrInfo->Result) break;

			if (globus_list_size(RetrInfo->AllBufferList) == globus_list_size(RetrInfo->FreeBufferList))
				break;

			pthread_cond_wait(&RetrInfo->Cond, &RetrInfo->Mutex);
		}
	}
	pthread_mutex_unlock(&RetrInfo->Mutex);
}

void
retr_destroy_info(retr_info_t * RetrInfo)
{
	if (RetrInfo)
	{
		if (RetrInfo->Object) free(RetrInfo->Object);
		if (RetrInfo->Bucket) free(RetrInfo->Bucket);
		pthread_mutex_destroy(&RetrInfo->Mutex);
		pthread_cond_destroy(&RetrInfo->Cond);
		globus_list_free(RetrInfo->FreeBufferList);
		globus_list_destroy_all(RetrInfo->AllBufferList, free);
		free(RetrInfo);
	}
}

void *
retr_thread(void * UserArg)
{
	int                 i             = 0;
	int                 last_loop     = 0;
	globus_off_t        offset        = 0;
	globus_off_t        length        = 0;
	globus_result_t     result        = GLOBUS_SUCCESS;
	retr_info_t       * retr_info     = UserArg;
	ds3_bulk_response * bulk_response = NULL;

	globus_gridftp_server_begin_transfer(retr_info->Operation, 0, NULL);

	while (!last_loop)
	{
		globus_gridftp_server_get_write_range(retr_info->Operation,
		                                      &offset,
		                                      &length);
		if (length == 0)
			break;

		if (length == -1)
		{
			globus_gfs_stat_t gfs_stat;
			result = stat_entry(retr_info->Client,
			                    retr_info->TransferInfo->pathname,
			                    &gfs_stat);
			if (result)
				break;

			length = gfs_stat.size - offset;
		
			stat_destroy(&gfs_stat);
			last_loop = 1;
		}

		/* This allows us to specify offset and length. */
		result = gds3_init_bulk_get(retr_info->Client,
		                            retr_info->Bucket,
		                            retr_info->Object,
		                            offset,
		                            length,
		                            &bulk_response);
		if (result)
			break;

		for (i = 0; i < bulk_response->list_size; i++)
		{
			assert(bulk_response->list[i]->size == 1);

/*
 * XXX bulk_response returns chunks that contain the offsets we need.
 * We must make one request per chunk that includes the ranges within
 * that chunk that we need.
 */
			retr_info->Offset = bulk_response->list[i]->list[0].offset;
			result = gds3_get_object_for_job(retr_info->Client,
			                                 retr_info->Bucket,
			                                 retr_info->Object,
			                                 bulk_response->list[i]->list[0].offset,
			                                 bulk_response->job_id->value,
			                                 retr_ds3_callout,
			                                 retr_info);
			if (result)
				break;
		}

		ds3_free_bulk_response(bulk_response);
		bulk_response = NULL;
	}

	globus_gridftp_server_finished_transfer(retr_info->Operation, result);
	ds3_free_bulk_response(bulk_response);

	return NULL;
}


void
retr(ds3_client                 * Client, 
     globus_gfs_operation_t       Operation,
     globus_gfs_transfer_info_t * TransferInfo)
{
	globus_result_t result    = GLOBUS_SUCCESS;
	retr_info_t   * retr_info = NULL;
	char          * bucket    = NULL;
	char          * object    = NULL;
	int             rc        = 0;
	int             initted   = 0;
	pthread_t       thread;
	pthread_attr_t  attr;

	GlobusGFSName(stor);

	path_split(TransferInfo->pathname, &bucket, &object);
	if (!object)
	{
		if (!bucket) free(bucket);
		result = GlobusGFSErrorGeneric("Can only retrieve objects from within buckets");
		globus_gridftp_server_finished_transfer(Operation, result);
		return;
	}

	retr_info = malloc(sizeof(retr_info_t));
	if (!retr_info)
	{
		free(bucket);
		free(object);
		result = GlobusGFSErrorMemory("retr_info_t");
		globus_gridftp_server_finished_transfer(Operation, result);
		return;
	}

	memset(retr_info, 0, sizeof(retr_info_t));
	pthread_mutex_init(&retr_info->Mutex, NULL);
	pthread_cond_init(&retr_info->Cond, NULL);
	retr_info->Client       = Client;
	retr_info->Operation    = Operation;
	retr_info->TransferInfo = TransferInfo;
	retr_info->Bucket       = bucket;
	retr_info->Object       = object;

	globus_gridftp_server_get_block_size(Operation, &retr_info->BlockSize);

	/*
	 * Launch a detached thread.
	 */
	if ((rc = pthread_attr_init(&attr)) || !(initted = 1) ||
	    (rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED)) ||
	    (rc = pthread_create(&thread, &attr, retr_thread, retr_info)))
	{
		result = GlobusGFSErrorSystemError("Launching get object thread", rc);
		globus_gridftp_server_finished_transfer(Operation, result);
		retr_destroy_info(retr_info);
	}

	if (initted) pthread_attr_destroy(&attr);
}

