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
#include "gds3.h"
#include "error.h"

globus_result_t
gds3_get_service(ds3_client * Client, ds3_get_service_response ** Response)
{
	globus_result_t result = GLOBUS_SUCCESS;

	ds3_request * request = ds3_init_get_service();
	ds3_error * error = ds3_get_service(Client, request, Response);

	if (error)
		result = error_translate(error);

	ds3_free_request(request);
	ds3_free_error(error);

	return result;
}

globus_result_t
gds3_get_bucket(ds3_client              *  Client,
                char                    *  BucketName,
                ds3_get_bucket_response ** Response,
                char                    *  Delimiter,
                char                    *  Prefix,
                char                    *  Marker,
                uint32_t                   MaxKeys)
{
	globus_result_t result  = GLOBUS_SUCCESS;
	ds3_request   * request = ds3_init_get_bucket(BucketName);

	if (Delimiter)
		ds3_request_set_delimiter(request, Delimiter);
	if (Prefix)
		ds3_request_set_prefix(request, Prefix);
	if (Marker)
		ds3_request_set_marker(request, Marker);
	if (MaxKeys > 0)
		ds3_request_set_max_keys(request, MaxKeys);

	ds3_error * error = ds3_get_bucket(Client, request, Response);
	ds3_free_request(request);

	if (error)
	{
		result = error_translate(error);
        ds3_free_error(error);
		return result;
	}

	return GLOBUS_SUCCESS;
}

globus_result_t
gds3_put_bucket(ds3_client * Client, char * BucketName)
{
	globus_result_t   result  = GLOBUS_SUCCESS;
	ds3_request     * request = NULL;
	ds3_error       * error   = NULL;

	request = ds3_init_put_bucket(BucketName);
	error   = ds3_put_bucket(Client, request);
	result  = error_translate(error);
	ds3_free_error(error);
	ds3_free_request(request);
	return result;
}

globus_result_t
gds3_put_object_for_job(ds3_client * Client,
                        char       * BucketName,
                        char       * ObjectName,
                        uint64_t     Offset,
                        uint64_t     Length,
                        char       * JobID,
                        size_t    (* Callback)(void*, size_t, size_t, void*),
                        void       * CallbackArg)
{
	globus_result_t   result  = GLOBUS_SUCCESS;
	ds3_request     * request = NULL;
	ds3_error       * error   = NULL;

	request = ds3_init_put_object_for_job(BucketName, ObjectName, Offset, Length, JobID);
	error   = ds3_put_object(Client, request, CallbackArg, Callback);
	result  = error_translate(error);
	ds3_free_request(request);
	ds3_free_error(error);
	return result;
}

globus_result_t
gds3_get_object_for_job(ds3_client * Client,
                        char       * BucketName,
                        char       * ObjectName,
                        uint64_t     Offset,
                        char       * JobID,
                        size_t    (* Callback)(void*, size_t, size_t, void*),
                        void       * CallbackArg)
{
	globus_result_t   result  = GLOBUS_SUCCESS;
	ds3_request     * request = NULL;
	ds3_error       * error   = NULL;

	request = ds3_init_get_object_for_job(BucketName, ObjectName, Offset, JobID);
	error   = ds3_get_object(Client, request, CallbackArg, Callback);
	result  = error_translate(error);
	ds3_free_request(request);
	ds3_free_error(error);
	return result;
}

globus_result_t
gds3_delete_bucket(ds3_client * Client, char * BucketName)
{
	globus_result_t   result  = GLOBUS_SUCCESS;
	ds3_request     * request = NULL;
	ds3_error       * error   = NULL;

	request = ds3_init_delete_bucket(BucketName);
	error   = ds3_delete_bucket(Client, request);
	result  = error_translate(error);
	ds3_free_request(request);
	ds3_free_error(error);
	return result;
}

globus_result_t
gds3_delete_object(ds3_client * Client,
                   char       * BucketName, 
                   char       * ObjectName)
{
	globus_result_t   result  = GLOBUS_SUCCESS;
	ds3_request     * request = NULL;
	ds3_error       * error   = NULL;

	request = ds3_init_delete_object(BucketName, ObjectName);
	error   = ds3_delete_object(Client, request);
	result  = error_translate(error);
	ds3_free_request(request);
	ds3_free_error(error);
	return result;
}


