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
#include <stdio.h>
#include <string.h>

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * Local includes
 */
#include "access_id.h"

globus_result_t
access_id_lookup(char *  AccessIDFile,
                 char *  Username, 
                 char ** AccessID, 
                 char ** SecretKey)
{
	FILE          * fptr   = NULL;
	globus_result_t result = GLOBUS_SUCCESS;
	char            buffer[1024];

	GlobusGFSName(access_id_lookup);

	fptr = fopen(AccessIDFile, "r");
	if (!fptr)
	{
		result = GlobusGFSErrorWrapFailed("Attempting to open AccessIDFile file",
		                                  GlobusGFSErrorSystemError("fopen()", errno));
		goto cleanup;
	}

	while (fgets(buffer, sizeof(buffer), fptr) != NULL)
	{
		char * token = strtok(buffer, " ");
		if (token == NULL)
			continue;

		if (*token == '#')
			continue;

		if (strcmp(token, Username) != 0)
			continue;

		token = strtok(NULL, " ");
		if (!token)
		{
			result = GlobusGFSErrorGeneric("Could not get access ID for user");
			goto cleanup;
		}
		*AccessID = globus_libc_strdup(token);

		token = strtok(NULL, " \n");
		if (!token)
		{
			result = GlobusGFSErrorGeneric("Could not get secret key for user");
			goto cleanup;
		}
		*SecretKey = globus_libc_strdup(token);

		goto cleanup;
	}

	result = GlobusGFSErrorGeneric("AccessID not found");

cleanup:
	if (fptr != NULL)
		fclose(fptr);

	if (result != GLOBUS_SUCCESS)
	{
		if (*SecretKey) globus_free(*SecretKey);
		if (*AccessID)  globus_free(*AccessID);
		*SecretKey = NULL;
		*AccessID  = NULL;
	}
	return result;
}

