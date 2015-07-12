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
#include <openssl/md5.h>
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
#include "commands.h"
#include "retr.h"
#include "gds3.h"
#include "path.h"

typedef struct {
	ds3_client                 * Client;
	globus_gfs_operation_t       Operation;
	globus_gfs_command_info_t  * CommandInfo;
	char                       * Bucket;
	char                       * Object;
	commands_callback            Callback;
	MD5_CTX                      MD5Context;
	globus_result_t              Result;
} cksm_info_t;

size_t
cksm_ds3_callback(void * Buffer,
                  size_t Length,
                  size_t Nmemb,
                  void * UserArg)
{
	cksm_info_t * cksm_info = UserArg;
	int           rc        = 0;

	GlobusGFSName(cksm_ds3_callback);

	rc = MD5_Update(&cksm_info->MD5Context, Buffer, Length*Nmemb);
	if (rc != 1)
	{
		cksm_info->Result = GlobusGFSErrorGeneric("MD5_Update() failed");
		return -1;
	}

	return Length*Nmemb;
}

void *
cksm_thread(void * UserArg)
{
	globus_result_t result    = GLOBUS_SUCCESS;
	cksm_info_t   * cksm_info = UserArg;
	int             rc;
	unsigned char   md5_digest[MD5_DIGEST_LENGTH];
	char            cksm_string[2*MD5_DIGEST_LENGTH+1];
	int             i;

	GlobusGFSName(cksm_thread);

	result = gds3_get_object_for_job(cksm_info->Client,
	                                 cksm_info->Bucket,
	                                 cksm_info->Object,
	                                 0,    /* Offset */
	                                 NULL, /* JobID  */
	                                 cksm_ds3_callback,
	                                 cksm_info);

	if (!result) result = cksm_info->Result;
	if (!result)
	{
		rc = MD5_Final(md5_digest, &cksm_info->MD5Context);
		if (rc != 1)
			result = GlobusGFSErrorGeneric("MD5_Final() failed");
	}

	if (!result)
	{
		for (i = 0; i < MD5_DIGEST_LENGTH; i++)
		{
			sprintf(&(cksm_string[i*2]), "%02x", (unsigned int)md5_digest[i]);
		}
	}

	cksm_info->Callback(cksm_info->Operation, result, result ? NULL : cksm_string);
	free(cksm_info->Bucket);
	free(cksm_info->Object);
	free(cksm_info);

	return NULL;
}

void
cksm(globus_gfs_operation_t      Operation,
     globus_gfs_command_info_t * CommandInfo,
     ds3_client                * Client,
     commands_callback           Callback)
{
	globus_result_t result    = GLOBUS_SUCCESS;
	cksm_info_t   * cksm_info = NULL;
	char          * bucket    = NULL;
	char          * object    = NULL;
	int             rc        = 0;
	int             initted   = 0;
	pthread_t       thread;
	pthread_attr_t  attr;

	GlobusGFSName(cksm);

	/* XXX Partial checksums not supported. */
	if (CommandInfo->cksm_offset != 0 || CommandInfo->cksm_length != -1)
	{
		result = GlobusGFSErrorGeneric("Partial checksums not supported");
		Callback(Operation, result, NULL);
		return;
	}

	path_split(CommandInfo->pathname, &bucket, &object);
	if (!object)
	{
		if (!bucket) free(bucket);
		result = GlobusGFSErrorGeneric("Can only checksum objects within buckets");
		Callback(Operation, result, NULL);
		return;
	}

	cksm_info = malloc(sizeof(cksm_info_t));
	if (!cksm_info)
	{
		free(bucket);
		free(object);
		result = GlobusGFSErrorMemory("cksm_info_t");
		Callback(Operation, result, NULL);
		return;
	}

	memset(cksm_info, 0, sizeof(cksm_info_t));
	cksm_info->Client       = Client;
	cksm_info->Operation    = Operation;
	cksm_info->CommandInfo  = CommandInfo;
	cksm_info->Bucket       = bucket;
	cksm_info->Object       = object;
	cksm_info->Callback     = Callback;

	rc = MD5_Init(&cksm_info->MD5Context);
	if (rc != 1)
	{
		result = GlobusGFSErrorGeneric("Failed to create MD5 context");
		Callback(Operation, result, NULL);
		free(cksm_info->Bucket);
		free(cksm_info->Object);
		free(cksm_info);
		return;
	}

	/*
	 * Launch a detached thread.
	 */
	if ((rc = pthread_attr_init(&attr)) || !(initted = 1) ||
	    (rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED)) ||
	    (rc = pthread_create(&thread, &attr, cksm_thread, cksm_info)))
	{
		result = GlobusGFSErrorSystemError("Launching cksm object thread", rc);
		Callback(Operation, result, NULL);

		free(cksm_info->Bucket);
		free(cksm_info->Object);
		free(cksm_info);
	}

	if (initted) pthread_attr_destroy(&attr);
}

