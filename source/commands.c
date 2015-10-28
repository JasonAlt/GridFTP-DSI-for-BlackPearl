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
#include "stage.h"
#include "path.h"
#include "gds3.h"
#include "cksm.h"

globus_result_t
commands_init(globus_gfs_operation_t Operation)
{
	GlobusGFSName(commands_init);

	globus_result_t result = globus_gridftp_server_add_command(
	                 Operation,
	                 "SITE STAGE",
	                 GLOBUS_GFS_HPSS_CMD_SITE_STAGE,
	                 4,
	                 4,
	                 "SITE STAGE <sp> timeout <sp> path",
	                 GLOBUS_TRUE,
	                 GFS_ACL_ACTION_READ);

	if (result != GLOBUS_SUCCESS)
		return GlobusGFSErrorWrapFailed("Failed to add custom 'SITE STAGE' command", result);

	return GLOBUS_SUCCESS;
}

void
commands_mkdir(globus_gfs_operation_t      Operation,
               globus_gfs_command_info_t * CommandInfo,
               ds3_client                * Client,
               commands_callback           Callback)
{
	globus_result_t result = GLOBUS_SUCCESS;
	char * bucket = NULL;
	char * object = NULL;
	char * subdir = NULL;

	GlobusGFSName(commands_mkdir);

	path_split(CommandInfo->pathname, &bucket, &object);

	/*
	 * Trying to mkdir '/'.
	 */
	if (!bucket)
	{
		result = GlobusGFSErrorSystemError("mkdir()", EEXIST);
		Callback(Operation, result, NULL);
		return;
	}

	/*
	 * New bucket.
	 */
	if (!object)
	{
		result = gds3_put_bucket(Client, bucket);
		Callback(Operation, result, NULL);
		free(bucket);
		return;
	}

	/*
	 * New object / subdirectory.
	 */
// XXX check if directoy exists by way of common prefix
	subdir = malloc(strlen(object) + 2);
	sprintf(subdir, "%s/", object);

	result = gds3_put_object(Client, bucket, subdir, 0, NULL, NULL, NULL, NULL);

	Callback(Operation, result, NULL);
	free(bucket);
	free(object);
	free(subdir);
}

void
commands_rmdir(globus_gfs_operation_t      Operation,
               globus_gfs_command_info_t * CommandInfo,
               ds3_client                * Client,
               commands_callback           Callback)
{
	globus_result_t result = GLOBUS_SUCCESS;
	char * bucket = NULL;
	char * folder = NULL;

	GlobusGFSName(commands_rmdir);

	path_split(CommandInfo->pathname, &bucket, &folder);

	/*
	 * Trying to rmdir '/'.
	 */
	if (!bucket)
	{
		result = GlobusGFSErrorSystemError("rmdir()", EPERM);
		Callback(Operation, result, NULL);
		return;
	}

	/*
	 * rmdir /<bucket>
	 */
	if (!folder)
	{
		result = gds3_delete_bucket(Client, bucket);
		Callback(Operation, result, NULL);
		free(bucket);
		return;
	}

	/*
	 * rmdir /<bucket>/subdirectory.
	 */
	result = gds3_delete_folder(Client, bucket, folder);
	Callback(Operation, result, NULL);
	free(bucket);
	free(folder);
}

void
commands_unlink(globus_gfs_operation_t      Operation,
                globus_gfs_command_info_t * CommandInfo,
                ds3_client                * Client,
                commands_callback           Callback)
{
	globus_result_t result = GLOBUS_SUCCESS;
	char * bucket = NULL;
	char * object = NULL;

	GlobusGFSName(commands_unlink);

	path_split(CommandInfo->pathname, &bucket, &object);

	/*
	 * Trying to unlink '/' or '/<bucket>'
	 */
	if (!bucket || !object)
	{
// XXX should check if bucket exists
		result = GlobusGFSErrorSystemError("unlink()", EISDIR);
		Callback(Operation, result, NULL);
		if (object) free(object);
		return;
	}

// XXX should check if object is a directory
	result = gds3_delete_object(Client, bucket, object);
	Callback(Operation, result, NULL);
	free(bucket);
	free(object);
}

void
commands_run(globus_gfs_operation_t      Operation,
             globus_gfs_command_info_t * CommandInfo,
             ds3_client                * Client,
             commands_callback           Callback)
{
	GlobusGFSName(commands_run);

	switch (CommandInfo->command)
	{
	case GLOBUS_GFS_CMD_MKD:
		commands_mkdir(Operation, CommandInfo, Client, Callback);
		break;
	case GLOBUS_GFS_CMD_RMD:
		commands_rmdir(Operation, CommandInfo, Client, Callback);
		break;
	case GLOBUS_GFS_CMD_DELE:
		commands_unlink(Operation, CommandInfo, Client, Callback);
		break;

	case GLOBUS_GFS_CMD_CKSM:
		cksm(Operation, CommandInfo, Client, Callback);
		break;

	case GLOBUS_GFS_HPSS_CMD_SITE_STAGE:
		stage(Operation, CommandInfo, Client, Callback);
		break;

	case GLOBUS_GFS_CMD_SITE_UTIME:       // No S3/DS3 support (need X attributes)
	case GLOBUS_GFS_CMD_RNTO:             // No S3/DS3 support
	case GLOBUS_GFS_CMD_RNFR:             // No S3/DS3 support
	case GLOBUS_GFS_CMD_SITE_CHMOD:       // No S3/DS3 support
	case GLOBUS_GFS_CMD_SITE_CHGRP:       // No S3/DS3 support
	case GLOBUS_GFS_CMD_SITE_SYMLINKFROM: // No S3/DS3 support
	case GLOBUS_GFS_CMD_SITE_SYMLINK:     // No S3/DS3 support
	case GLOBUS_GFS_CMD_SITE_AUTHZ_ASSERT:
	case GLOBUS_GFS_CMD_SITE_RDEL:
	case GLOBUS_GFS_CMD_SITE_DSI:
	case GLOBUS_GFS_CMD_SITE_SETNETSTACK:
	case GLOBUS_GFS_CMD_SITE_SETDISKSTACK:
	case GLOBUS_GFS_CMD_SITE_CLIENTINFO:
	case GLOBUS_GFS_CMD_DCSC:
	case GLOBUS_GFS_CMD_HTTP_PUT:
	case GLOBUS_GFS_CMD_HTTP_GET:
	case GLOBUS_GFS_CMD_HTTP_CONFIG:
	case GLOBUS_GFS_CMD_TRNC:
	case GLOBUS_GFS_CMD_SITE_TASKID:
	default:
		return Callback(Operation, GlobusGFSErrorGeneric("Not Supported"), NULL);
    }
}
