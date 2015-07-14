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

void
retr_ds3_gridftp_callback(globus_gfs_operation_t Operation,
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
retr_ds3_callback(void * ReadyBuffer,
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

	GlobusGFSName(retr_ds3_callback);

	pthread_mutex_lock(&retr_info->Mutex);
	{
		if (!retr_info->Started)
			globus_gridftp_server_begin_transfer(retr_info->Operation, 0, NULL);
		retr_info->Started = 1;

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


			memcpy(free_buffer, ReadyBuffer, cpy_length);

			result = globus_gridftp_server_register_write(retr_info->Operation,
			                                              ReadyBuffer + buf_offset,
			                                              cpy_length,
			                                              retr_info->Offset + buf_offset,
			                                              0,
			                                              retr_ds3_gridftp_callback,
			                                              retr_info);

			if (result)
			{
				if(!retr_info->Result) retr_info->Result = result;
				rc = -1;
				goto cleanup;
			}

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

void *
retr_thread(void * UserArg)
{
	globus_result_t result    = GLOBUS_SUCCESS;
	retr_info_t   * retr_info = UserArg;

	result = gds3_get_object_for_job(retr_info->Client,
	                                 retr_info->Bucket,
	                                 retr_info->Object,
	                                 0,    /* Offset */
	                                 NULL, /* JobID  */
	                                 retr_ds3_callback,
	                                 retr_info);

	if (!result)
		retr_wait_for_gridftp(retr_info);

	if (!result) result = retr_info->Result;
	globus_gridftp_server_finished_transfer(retr_info->Operation, result);
	pthread_mutex_destroy(&retr_info->Mutex);
	pthread_cond_destroy(&retr_info->Cond);
	free(retr_info->Bucket);
	free(retr_info->Object);
	globus_list_free(retr_info->FreeBufferList);
	globus_list_destroy_all(retr_info->AllBufferList, free);
	free(retr_info);

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

		pthread_mutex_destroy(&retr_info->Mutex);
		pthread_cond_destroy(&retr_info->Cond);
		free(retr_info->Bucket);
		free(retr_info->Object);
		free(retr_info);
	}

	if (initted) pthread_attr_destroy(&attr);
}

