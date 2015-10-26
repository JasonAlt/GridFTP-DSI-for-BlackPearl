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
#include "markers.h"

void
stor_gridftp_callout(globus_gfs_operation_t Operation,
                     globus_result_t        Result,
                     globus_byte_t        * Buffer,
                     globus_size_t          Length,
                     globus_off_t           Offset,
                     globus_bool_t          Eof,
                     void                 * UserArg)
{
	stor_buffer_t * stor_buffer = UserArg;
	stor_info_t   * stor_info   = stor_buffer->StorInfo;
assert(stor_buffer->Buffer == (char *)Buffer);

	pthread_mutex_lock(&stor_info->Mutex);
	{
		/* Save EOF */
		if (Eof) stor_info->Eof = Eof;
		/* Save any error */
		if (Result && !stor_info->Result) stor_info->Result = Result;

// XXX BlockSize must be >= (Length*Nmemb) b/c we don't handle partial copies below.
assert(Length <= stor_info->BlockSize);

		/* Set buffer counters. */
		stor_buffer->BufferOffset   = 0;
		stor_buffer->TransferOffset = Offset;
		stor_buffer->BufferLength   = Length;

		/* Stor the buffer. */
		if (Length)
			globus_list_insert(&stor_info->ReadyBufferList, stor_buffer);
		else
			globus_list_insert(&stor_info->FreeBufferList, stor_buffer);

		/* Decrease the current connection count. */
		stor_info->CurConnCnt--;

		/* Wake the DS3 thread */
		pthread_cond_signal(&stor_info->Cond);
	}
	pthread_mutex_unlock(&stor_info->Mutex);
}

/* 1 = found, 0 = not found */
int
stor_find_buffer(void * Datum, void * Arg)
{
	struct ds3_callout * ds3_callout = Arg;
	ds3_callout->Buffer = Datum;

	if (ds3_callout->NeededOffset == (ds3_callout->Buffer->TransferOffset))
		return 1;

assert(ds3_callout->NeededOffset <  (ds3_callout->Buffer->TransferOffset) ||
       ds3_callout->NeededOffset >= (ds3_callout->Buffer->TransferOffset + ds3_callout->Buffer->BufferLength));

	return 0;
}

size_t
stor_ds3_callout(void * Buffer,
                 size_t Length,
                 size_t Nmemb,
                 void * UserArg)
{
	int             rc        = 0;
	stor_info_t   * stor_info = UserArg;
	globus_result_t result    = GLOBUS_SUCCESS;
	globus_list_t * buf_entry   = NULL;
	stor_buffer_t * stor_buffer = NULL;

	GlobusGFSName(stor_ds3_callout);

	pthread_mutex_lock(&stor_info->Mutex);
	{
// XXX BlockSize must be >= (Length*Nmemb) b/c we don't handle partial copies below.
assert((Length*Nmemb) <= stor_info->BlockSize);

		if (stor_info->Result)
		{
			result = stor_info->Result;
			goto cleanup;
		}

		/* Until we find a buffer with this offset... */
		while (1)
		{
			/*
			 * Look for a buffer containing this offset.
			 */
			stor_info->Ds3Callout.NeededOffset = stor_info->Offset;
			buf_entry = globus_list_search_pred(stor_info->ReadyBufferList,
			                                    stor_find_buffer,
			                                   &stor_info->Ds3Callout);
			if (buf_entry)
			{
				/* Copy out. */
				int copy_length = stor_info->Ds3Callout.Buffer->BufferLength;
				if (copy_length > (Length*Nmemb))
					copy_length = (Length*Nmemb);

				memcpy(Buffer,
				       stor_info->Ds3Callout.Buffer->Buffer + stor_info->Ds3Callout.Buffer->BufferOffset,
				       copy_length);

				markers_update_perf_markers(stor_info->Operation, stor_info->Offset, copy_length);

				/* Update buffer counters. */
				stor_info->Ds3Callout.Buffer->BufferOffset   += copy_length;
				stor_info->Ds3Callout.Buffer->TransferOffset += copy_length;
				stor_info->Ds3Callout.Buffer->BufferLength   -= copy_length;
				rc = copy_length;

				/* Update our sanity-check counter */
				stor_info->Offset += copy_length;

				/* If empty, move it to free. */
				if (stor_info->Ds3Callout.Buffer->BufferLength == 0)
				{
					globus_list_remove(&stor_info->ReadyBufferList, buf_entry);
					globus_list_insert(&stor_info->FreeBufferList,  stor_info->Ds3Callout.Buffer);
				}
				goto cleanup;
			}

			/* If we have an EOF then something has gone wrong. */
			if (stor_info->Eof)
			{
				result = GlobusGFSErrorGeneric("Premature end of data transfer");
				goto cleanup;
			}

			/*
			 * Check for the optimal number of concurrent writes.
			 */
			if (stor_info->ConnChkCnt++ == 0)
				globus_gridftp_server_get_optimal_concurrency(stor_info->Operation,
				                                             &stor_info->OptConnCnt);
			if (stor_info->ConnChkCnt >= 100) stor_info->ConnChkCnt = 0;

			while (stor_info->CurConnCnt < stor_info->OptConnCnt)
			{
				if (!globus_list_empty(stor_info->FreeBufferList))
				{
					/* Grab a buffer from the free list. */
					stor_buffer = globus_list_first(stor_info->FreeBufferList);
					globus_list_remove(&stor_info->FreeBufferList, stor_info->FreeBufferList);
				} else
				{
					/* Allocate a new buffer. */
					stor_buffer = globus_malloc(sizeof(stor_buffer_t));
					if (!stor_buffer)
					{
						result = GlobusGFSErrorMemory("stor_buffer_t");
						goto cleanup;
					}
					stor_buffer->Buffer = globus_malloc(stor_info->BlockSize);
					if (!stor_buffer->Buffer)
					{
						free(stor_buffer);
						result = GlobusGFSErrorMemory("stor_buffer_t");
						goto cleanup;
					}
					globus_list_insert(&stor_info->AllBufferList,  stor_buffer);
					stor_buffer->StorInfo = stor_info;
				}

				result = globus_gridftp_server_register_read(stor_info->Operation,
				                                             (globus_byte_t *)stor_buffer->Buffer,
				                                              stor_info->BlockSize,
				                                              stor_gridftp_callout,
				                                              stor_buffer);

				if (result)
					goto cleanup;

				/* Increase the current connection count. */
 				stor_info->CurConnCnt++;
            }

			pthread_cond_wait(&stor_info->Cond, &stor_info->Mutex);
			result = stor_info->Result;
			if (result) goto cleanup;
		}


cleanup:
		if (result)
		{
			if (!stor_info->Result) stor_info->Result = result;
			rc = 1; /* Signal to shutdown. */
		}
	}
	pthread_mutex_unlock(&stor_info->Mutex);

	return rc;
}

void
stor_wait_for_gridftp(stor_info_t * StorInfo)
{
	pthread_mutex_lock(&StorInfo->Mutex);
	{
		while (1)
		{
			if (StorInfo->Result) break;

			if (globus_list_size(StorInfo->AllBufferList) == globus_list_size(StorInfo->FreeBufferList))
				break;

			pthread_cond_wait(&StorInfo->Cond, &StorInfo->Mutex);
		}
	}
	pthread_mutex_unlock(&StorInfo->Mutex);
}

void
stor_destroy_info(stor_info_t * StorInfo)
{
	if (StorInfo)
	{
		if (StorInfo->Bucket) free(StorInfo->Bucket);
		if (StorInfo->Object) free(StorInfo->Object);
		pthread_mutex_destroy(&StorInfo->Mutex);
		pthread_cond_destroy(&StorInfo->Cond);
		globus_list_free(StorInfo->FreeBufferList);
		globus_list_free(StorInfo->ReadyBufferList);
		globus_list_destroy_all(StorInfo->AllBufferList, free);
		free(StorInfo);
	}
}

void *
stor_thread(void * UserArg)
{
	globus_result_t result    = GLOBUS_SUCCESS;
	stor_info_t   * stor_info = UserArg;

	globus_gridftp_server_begin_transfer(stor_info->Operation, 0, NULL);

	result = gds3_put_object(stor_info->Client,
	                         stor_info->Bucket,
	                         stor_info->Object,
	                         stor_info->TransferInfo->alloc_size,
	                         stor_ds3_callout,
	                         stor_info);

	if (!result) stor_wait_for_gridftp(stor_info);
	result = stor_info->Result;

	globus_gridftp_server_finished_transfer(stor_info->Operation, result);
	stor_destroy_info(stor_info);
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

	globus_gridftp_server_get_block_size(Operation, &stor_info->BlockSize);

	/*
	 * Launch a detached thread.
	 */
	if ((rc = pthread_attr_init(&attr)) || !(initted = 1) ||
	    (rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED)) ||
	    (rc = pthread_create(&thread, &attr, stor_thread, stor_info)))
	{
		result = GlobusGFSErrorSystemError("Launching put object thread", rc);
		globus_gridftp_server_finished_transfer(Operation, result);
		stor_destroy_info(stor_info);
	}

	if (initted) pthread_attr_destroy(&attr);
}

