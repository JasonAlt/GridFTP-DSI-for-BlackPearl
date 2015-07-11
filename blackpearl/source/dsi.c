/*
 * University of Illinois/NCSA Open Source License
 *
 * Copyright � 2012-2014 NCSA.  All rights reserved.
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
 * Local includes.
 */
#include "version.h"

/* This is used to define the debug print statements. */
GlobusDebugDefine(GLOBUS_GRIDFTP_SERVER_BLACKPEARL);

globus_gfs_storage_iface_t blackpearl_dsi_iface =
{
	0,     /* Descriptor       */
	NULL,  /* init_func        */
	NULL,  /* destroy_func     */
	NULL,  /* list_func        */
	NULL,  /* send_func        */
	NULL,  /* recv_func        */
	NULL,  /* trev_func        */
	NULL,  /* active_func      */
	NULL,  /* passive_func     */
	NULL,  /* data_destroy     */
	NULL,  /* command_func     */
	NULL,  /* stat_func        */
	NULL,  /* set_cred_func    */
	NULL,  /* buffer_send_func */
	NULL,  /* realpath_func    */
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
}


