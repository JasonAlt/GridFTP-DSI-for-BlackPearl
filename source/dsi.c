/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright © 2012-2014 NCSA.  All rights reserved.
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
 * Local includes.
 */
#include "access_id.h"
#include "commands.h"
#include "version.h"
#include "config.h"
#include "error.h"
#include "stat.h"
#include "stor.h"
#include "retr.h"
#include "gds3.h"

/* This is used to define the debug print statements. */
GlobusDebugDefine(GLOBUS_GRIDFTP_SERVER_BLACKPEARL);

void
dsi_init(globus_gfs_operation_t      Operation,
         globus_gfs_session_info_t * SessionInfo)
{
	config_t      * config     = NULL;
	globus_result_t result     = GLOBUS_SUCCESS;
	char          * access_id  = NULL;
	char          * secret_key = NULL;
	ds3_creds     * bp_creds   = NULL;
	ds3_client    * bp_client  = NULL;

	GlobusGFSName(dsi_init);

	/* Read in the config */
	result = config_init(&config);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Lookup the access ID */
	result = access_id_lookup(config->AccessIDFile,
	                          SessionInfo->username, 
	                          &access_id,
	                          &secret_key);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	/* Create the credentials */
	bp_creds = ds3_create_creds(access_id, secret_key);
	if (!bp_creds)
	{
		result = GlobusGFSErrorMemory("ds3_create_creds");
		goto cleanup;
	}

	/* Create the client */
	bp_client = ds3_create_client(config->EndPoint, bp_creds);
	if (!bp_client)
	{
		result = GlobusGFSErrorMemory("ds3_create_client");
		goto cleanup;
	}

	/* Test the credentials with a get-service call. */
	ds3_get_service_response * response = NULL;
	result = gds3_get_service(bp_client, &response);
	ds3_free_service_response(response);
	if (result)
		goto cleanup;

	result = commands_init(Operation);

cleanup:
	/*
	 * Inform the server that we are done. If we do not pass in a username, the
	 * server will use the name we mapped to with GSI. If we do not pass in a
	 * home directory, the server will (1) look it up if we are root or
	 * (2) leave it as the unprivileged user's home directory.
	 *
	 * As far as I can tell, the server keeps a pointer to home_directory and frees
	 * it when it is done.
	 */
	globus_gridftp_server_finished_session_start(Operation,
	                                             result,
	                                             bp_client, // Session variable
	                                             NULL,      // username
	                                             "/");      // home directory

	config_destroy(config);
	if (access_id)  globus_free(access_id);
	if (secret_key) globus_free(secret_key);

	if (result != GLOBUS_SUCCESS)
	{
		ds3_free_creds(bp_creds);
		ds3_free_client(bp_client);
	}
}

void
dsi_destroy(void * Arg)
{
	ds3_client * bp_client = Arg;
	if (bp_client)
	{
		ds3_free_creds(bp_client->creds);
		ds3_free_client(bp_client);
	}
}

int
dsi_partial_transfer(globus_gfs_transfer_info_t * TransferInfo)
{
	return (TransferInfo->partial_offset  != 0 || TransferInfo->partial_length != -1);
}

int
dsi_restart_transfer(globus_gfs_transfer_info_t * TransferInfo)
{
	globus_off_t offset;
	globus_off_t length;

	if (globus_range_list_size(TransferInfo->range_list) != 1)
		return 1;

	globus_range_list_at(TransferInfo->range_list, 0, &offset, &length);
	return (offset != 0 || length != -1);
}

void
dsi_send(globus_gfs_operation_t       Operation,
         globus_gfs_transfer_info_t * TransferInfo,
         void                       * UserArg)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(dsi_send);

	if (dsi_partial_transfer(TransferInfo))
	{
		result = GlobusGFSErrorGeneric("Partial RETR is not supported");
		globus_gridftp_server_finished_transfer(Operation, result);
		return;
	}

	if (dsi_restart_transfer(TransferInfo))
	{
		result = GlobusGFSErrorGeneric("Restarts are not supported");
		globus_gridftp_server_finished_transfer(Operation, result);
		return;
	}

	retr(UserArg, Operation, TransferInfo);
}

void
dsi_recv(globus_gfs_operation_t       Operation,
         globus_gfs_transfer_info_t * TransferInfo,
         void                       * UserArg)
{
	globus_result_t result = GLOBUS_SUCCESS;

	GlobusGFSName(dsi_recv);

	if (dsi_partial_transfer(TransferInfo))
	{
		result = GlobusGFSErrorGeneric("Partial STOR is not supported");
		globus_gridftp_server_finished_transfer(Operation, result);
		return;
	}

	if (dsi_restart_transfer(TransferInfo))
	{
		result = GlobusGFSErrorGeneric("Restarts are is supported");
		globus_gridftp_server_finished_transfer(Operation, result);
		return;
	}

	stor(UserArg, Operation, TransferInfo);
}

void
dsi_command(globus_gfs_operation_t      Operation,
            globus_gfs_command_info_t * CommandInfo,
            void                      * UserArg)
{
	commands_run(Operation, CommandInfo, UserArg, globus_gridftp_server_finished_command);
}

#define STAT_ENTRIES_PER_REPLY 200

void
dsi_stat(globus_gfs_operation_t   Operation,
         globus_gfs_stat_info_t * StatInfo,
         void                   * Arg)
{
	globus_result_t   result = GLOBUS_SUCCESS;
	ds3_client      * client = Arg;
	stat_state_t      state;
	globus_gfs_stat_t gfs_stat_array[STAT_ENTRIES_PER_REPLY];
	int               stat_count;

	GlobusGFSName(dsi_stat);

	stat_init_state(&state);

	do {
		result = stat_entries(client,
		                      StatInfo->pathname,
		                      StatInfo->file_only,
		                      STAT_ENTRIES_PER_REPLY,
		                      gfs_stat_array,
		                      &stat_count,
		                      &state);

		if (stat_is_complete(&state) || result != GLOBUS_SUCCESS)
			globus_gridftp_server_finished_stat(Operation,
			                                    result,
			                                    gfs_stat_array,
			                                    stat_count);
		else
			globus_gridftp_server_finished_stat_partial(Operation,
			                                            GLOBUS_SUCCESS,
			                                            gfs_stat_array,
			                                            stat_count);

		stat_destroy_array(gfs_stat_array, stat_count);

	} while (!stat_is_complete(&state) && result == GLOBUS_SUCCESS);

	stat_destroy_state(&state);
}

globus_gfs_storage_iface_t blackpearl_dsi_iface =
{
	0,           /* Descriptor       */
	dsi_init,    /* init_func        */
	dsi_destroy, /* destroy_func     */
	NULL,        /* list_func        */
	dsi_send,    /* send_func        */
	dsi_recv,    /* recv_func        */
	NULL,        /* trev_func        */
	NULL,        /* active_func      */
	NULL,        /* passive_func     */
	NULL,        /* data_destroy     */
	dsi_command, /* command_func     */
	dsi_stat,    /* stat_func        */
	NULL,        /* set_cred_func    */
	NULL,        /* buffer_send_func */
	NULL,        /* realpath_func    */
};

static int activate(void);
static int deactivate(void);

GlobusExtensionDefineModule(globus_gridftp_server_blackpearl) =
{
	"globus_gridftp_server_blackpearl",
	activate,
	deactivate,
	GLOBUS_NULL,
	GLOBUS_NULL,
	&version
};

int
activate(void)
{
	int rc = globus_module_activate(GLOBUS_COMMON_MODULE);
	if(rc != GLOBUS_SUCCESS)
		return rc;

	globus_extension_registry_add(GLOBUS_GFS_DSI_REGISTRY,
	                              "blackpearl",
	                              GlobusExtensionMyModule(globus_gridftp_server_blackpearl),
	                              &blackpearl_dsi_iface);

	GlobusDebugInit(GLOBUS_GRIDFTP_SERVER_BLACKPEARL,
	                ERROR WARNING TRACE INTERNAL_TRACE INFO STATE INFO_VERBOSE);

	return 0;
}

int
deactivate(void)
{
	globus_extension_registry_remove(GLOBUS_GFS_DSI_REGISTRY, "blackpearl");
	return 0;
}


