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

#ifndef BLACKPEARL_DSI_ERROR_H
#define BLACKPEARL_DSI_ERROR_H

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * DS3 includes
 */
#include <ds3.h>

/*
typedef struct {
    uint64_t  status_code;
    ds3_str*  status_message;
    ds3_str*  error_body;
}ds3_error_response;

typedef struct {
    ds3_error_code      code;
    ds3_str*            message;
    ds3_error_response* error;
}ds3_error;

typedef enum {
  DS3_ERROR_INVALID_XML, DS3_ERROR_CURL_HANDLE, DS3_ERROR_REQUEST_FAILED, DS3_ERROR_MISSING_ARGS, DS3_ERROR_BAD_STATUS_CODE
}ds3_error_code;
*/

globus_result_t
error_translate(ds3_error * Error)
{
	GlobusGFSName(error_translate);

	if (!Error) return GLOBUS_SUCCESS;

	switch (Error->code)
	{
	case DS3_ERROR_BAD_STATUS_CODE:
		//return GlobusGFSErrorGeneric(ds3_str_value(Error->error->status_message));
		return GlobusGFSErrorGeneric(ds3_str_value(Error->error->error_body));
	case DS3_ERROR_TOO_MANY_REDIRECTS:
	case DS3_ERROR_INVALID_XML:
	case DS3_ERROR_CURL_HANDLE:
	case DS3_ERROR_REQUEST_FAILED:
	case DS3_ERROR_MISSING_ARGS:
		break;
	}
	return GlobusGFSErrorGeneric("An unknown DS3 error has occurred");
}

#endif /* BLACKPEARL_DSI_ERROR_H */
