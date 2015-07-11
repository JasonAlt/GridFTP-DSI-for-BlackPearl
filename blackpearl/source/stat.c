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
#include <libgen.h>
#include <time.h>

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * Local includes
 */
#include "stat.h"
#include "path.h"
#include "gds3.h"

globus_result_t
stat_populate(char              * Name,
              int                 Type,
              int                 LinkCount,
              uint64_t            Size,
              char              * Owner,
              char              * ModTime,
              globus_gfs_stat_t * GFSStat)
{
	GlobusGFSName(stat_populate);

	GFSStat->mode  = Type | S_IRWXU;
	GFSStat->nlink = LinkCount;
	GFSStat->ino   = 0xDEADBEEF;
//	XXX GFSStat->uid   = HpssStat->st_uid;
	GFSStat->gid   = 0; // Groups not supported
	GFSStat->dev   = 0;
	GFSStat->size  = Size;
	GFSStat->name  = globus_libc_strdup(Name);

	if (!ModTime)
	{
		GFSStat->atime = 0;
		GFSStat->mtime = 0;
		GFSStat->ctime = 0;
	} else
	{
		struct tm t;

		int ret = sscanf(ModTime, "%d-%d-%dT%d:%d:%d.", &t.tm_year,
		                                                &t.tm_mon,
		                                                &t.tm_mday,
		                                                &t.tm_hour,
		                                                &t.tm_min,
		                                                &t.tm_sec);
		if (ret != 6)
			return GlobusGFSErrorGeneric("Invalid time string");

		t.tm_year -= 1900;
		time_t time_of_day = mktime(&t);
		GFSStat->atime = time_of_day;
		GFSStat->mtime = time_of_day;
		GFSStat->ctime = time_of_day;
	}

	return GLOBUS_SUCCESS;
}

globus_result_t
stat_get_link_count(ds3_client * Client, char * Path, int * LinkCount)
{
*LinkCount = 34;
return GLOBUS_SUCCESS;
// XXX
/*
    boost::scoped_ptr<DS3::Directory> dir(new DS3::Directory(C, Path));
    ErrorCode::Prototype ec = dir->Opendir();
    if (ec) return ec;
    LinkCount = 0;
    std::string entry;
    while (!(ec = dir->ReadNextEntry(entry)) && entry.size()) {LinkCount++;}
    if (!ec)
        ec = dir->Closedir();
    return ec;
*/
}

globus_result_t
stat_object(ds3_client * Client, char * Pathname, globus_gfs_stat_t * GFSStat)
{
	globus_result_t result = GLOBUS_SUCCESS;
	char          * bucket_name = NULL;
	char          * object_name = NULL;
	int             i;
	int             link_count;
	ds3_get_service_response * get_service_response = NULL;
	ds3_get_bucket_response  * get_bucket_response  = NULL;

	GlobusGFSName(stat_object);

	/* Start by splitting the path into its two parts. */
	path_split(Pathname, &bucket_name, &object_name);

	/* Fetch the service response, we'll need it soon. */
	result = gds3_get_service(Client, &get_service_response);
	if (result != GLOBUS_SUCCESS)
		goto cleanup;

	if (!object_name)
	{
		if (!bucket_name)
		{
			result = stat_populate("/",
			                       S_IFDIR,
			                       get_service_response->num_buckets + 2,
			                       1024,
			                       ds3_str_value(get_service_response->owner->name),
			                       NULL,
			                       GFSStat);
			goto cleanup;
		}

		for (i = 0; i < get_service_response->num_buckets; i++)
		{
			if (strcmp(bucket_name, ds3_str_value(get_service_response->buckets[i].name)) == 0)
			{
				result = stat_get_link_count(Client, Pathname, &link_count);
				if (result != GLOBUS_SUCCESS)
					goto cleanup;

				result = stat_populate(bucket_name,
				                       S_IFDIR,
				                       link_count,
				                       1024,
				                       ds3_str_value(get_service_response->owner->name),
				                       ds3_str_value(get_service_response->buckets[i].creation_date),
				                       GFSStat);
				goto cleanup;
			}
		}
		
		result = GlobusGFSErrorGeneric("No such file or directory");
		goto cleanup;
	}

	char * marker = NULL;
	do {
		result = gds3_get_bucket(Client,
		                         bucket_name,
		                         &get_bucket_response,
		                         "/", /* Delimiter */
		                         object_name,
		                         marker,
		                         0);

		if (result)
			goto cleanup;

		for (i = 0; i < get_bucket_response->num_common_prefixes; i++)
        {
			if (strncmp(object_name,
			            ds3_str_value(get_bucket_response->common_prefixes[i]),
			            ds3_str_size(get_bucket_response->common_prefixes[i])-1) == 0)
			{
				result = stat_get_link_count(Client, Pathname, &link_count);
				if (result)
					goto cleanup;

				result = stat_populate(basename(object_name),
				                       S_IFDIR,
				                       link_count,
				                       1024,
				                       ds3_str_value(get_service_response->owner->name),
				                       NULL,
				                       GFSStat);
				goto cleanup;
			}
		}

		for (i = 0; i < get_bucket_response->num_objects; i++)
		{
			if (strcmp(object_name, ds3_str_value(get_bucket_response->objects[i].name)) == 0)
			{
				result = stat_populate(basename(object_name),
				                       S_IFREG,
				                       1,
				                       get_bucket_response->objects[i].size,
				                       ds3_str_value(get_bucket_response->objects[i].owner->name),
				                       ds3_str_value(get_bucket_response->objects[i].last_modified),
				                       GFSStat);
				goto cleanup;
			}
		}

		marker = NULL;
		if (get_bucket_response->next_marker)
			marker = ds3_str_value(get_bucket_response->next_marker);
	} while (marker > 0);

	result = GlobusGFSErrorGeneric("No such file or directory");

cleanup:
	ds3_free_service_response(get_service_response);
	ds3_free_bucket_response(get_bucket_response);
	if (bucket_name)
		free(bucket_name);
	if (object_name)
		free(object_name);
	return result;
}

void
stat_destroy(globus_gfs_stat_t * GFSStat)
{
	if (GFSStat)
	{
		if (GFSStat->symlink_target != NULL)
			globus_free(GFSStat->symlink_target);
		if (GFSStat->name != NULL)
			globus_free(GFSStat->name);
	}
	return;
}

void
stat_destroy_array(globus_gfs_stat_t * GFSStatArray, int Count)
{
	int i;
	for (i = 0; i < Count; i++)
	{
		stat_destroy(&(GFSStatArray[i]));
	}
}


