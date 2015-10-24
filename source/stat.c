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

	memset(GFSStat, 0, sizeof(globus_gfs_stat_t));

	GFSStat->mode  = Type | S_IRWXU;
	GFSStat->nlink = LinkCount;
// XXX Inodes not supported
	GFSStat->ino   = 0xDEADBEEF;
// XXX UIDs not supported
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

// Directory link count not supported
globus_result_t
stat_get_link_count(ds3_client * Client,
                    char       * BucketName,
                    char       * Prefix,
                    int        * LinkCount)
{
	ds3_get_bucket_response * get_bucket_response;
	globus_result_t result = GLOBUS_SUCCESS;
	char          * marker = NULL;

	*LinkCount = 0;

	do {
		result = gds3_get_bucket(Client,
		                         BucketName,
		                         &get_bucket_response,
		                         "/", /* Delimiter */
		                         Prefix,
		                         marker,
		                         0);

		if (result) return result;

		*LinkCount += get_bucket_response->num_common_prefixes + 
		              get_bucket_response->num_objects;
	} while (marker);

	ds3_free_bucket_response(get_bucket_response);

	return GLOBUS_SUCCESS;
}

void
stat_init_state(stat_state_t * State)
{
	memset(State, 0, sizeof(stat_state_t));
}

int
stat_is_complete(stat_state_t * State)
{
	return State->_complete;
}

void
stat_destroy_state(stat_state_t * State)
{
	if (State->_service_response) ds3_free_service_response(State->_service_response);
	if (State->_bucket_response)  ds3_free_bucket_response(State->_bucket_response);
	if (State->_bucket_name)      free(State->_bucket_name);
	if (State->_object_name)      free(State->_object_name);
	if (State->_marker)           free(State->_marker);
}

globus_result_t
stat_entries(ds3_client        * Client,
             char              * Path,
             int                 FileOnly,
             int                 MaxEntries,
             globus_gfs_stat_t * GFSStatArray,
             int               * CountOut,
             stat_state_t      * State)
{
	globus_result_t result = GLOBUS_SUCCESS;
	int i = 0;
	int expanding_search = 0;

	GlobusGFSName(stat_entries);

	if (!State->_bucket_name && !State->_object_name)
		path_split(Path, &State->_bucket_name, &State->_object_name);

	if (!State->_service_response)
	{
		result = gds3_get_service(Client, &State->_service_response);
		if (result != GLOBUS_SUCCESS)
			return result;
	}

	*CountOut = 0;

	/* No bucket_name means it _is_ '/' */
	if (!State->_bucket_name)
	{
		if (FileOnly)
		{
			/* Return a stat of only '/'. */
			result = stat_populate("/",
			                       S_IFDIR,
			                       State->_service_response->num_buckets + 2,
			                       1024,
			                       ds3_str_value(State->_service_response->owner->name),
			                       NULL,
			                       &GFSStatArray[(*CountOut)++]);
			State->_complete = 1;
			return result;
		} else
		{
			/* Return the contents of '/'. */
			if (State->_index++ == 0)
			{
				result = stat_populate(".",
				                       S_IFDIR,
				                       State->_service_response->num_buckets + 2,
				                       1024,
				                       ds3_str_value(State->_service_response->owner->name),
				                       NULL, // XXX no modify time
				                       &GFSStatArray[(*CountOut)++]);

				if (result != GLOBUS_SUCCESS || *CountOut == MaxEntries)
					return result;
			}

			if (State->_index++ == 1)
			{
				result = stat_populate("..",
				                       S_IFDIR,
				                       State->_service_response->num_buckets + 2,
				                       1024,
				                       ds3_str_value(State->_service_response->owner->name),
				                       NULL, // XXX no modify time
				                       &GFSStatArray[(*CountOut)++]);

				if (State->_service_response->num_buckets == 0)
					State->_complete = 1;

				if (result != GLOBUS_SUCCESS || *CountOut == MaxEntries)
					return result;
			}

			for (i = State->_index-2;
			     i < State->_service_response->num_buckets && *CountOut < MaxEntries;
			     i++)
			{
				result = stat_populate(ds3_str_value(State->_service_response->buckets[i].name),
				                       S_IFDIR,
				                       2,
				                       1024,
				                       ds3_str_value(State->_service_response->owner->name),
				                       ds3_str_value(State->_service_response->buckets[i].creation_date),
				                       &GFSStatArray[(*CountOut)++]);
				if (result != GLOBUS_SUCCESS)
					return result;
			}
			State->_complete = (i == State->_service_response->num_buckets);
			return result;
		}
	}

	/* No object_name means it is _in_ '/' */
	if (!State->_object_name && FileOnly)
	{
		for (i = 0;
		     i < State->_service_response->num_buckets;
		     i++)
		{
			if (strcmp(State->_bucket_name, ds3_str_value(State->_service_response->buckets[i].name)) == 0)
			{
				result = stat_populate(ds3_str_value(State->_service_response->buckets[i].name),
				                       S_IFDIR,
				                       2,
				                       1024,
				                       ds3_str_value(State->_service_response->owner->name),
				                       ds3_str_value(State->_service_response->buckets[i].creation_date),
				                       &GFSStatArray[(*CountOut)++]);
				State->_complete = 1;
				return result;
			}
		}

		return GlobusGFSErrorGeneric("No such file or directory");
	}

	/* Let's find this object. */
	if (State->_object_name && State->_object_name[strlen(State->_object_name-1)] != '/')
	{
		do
		{
			/* Get the next response. */
			if (!State->_bucket_response)
			{
				result = gds3_get_bucket(Client,
				                         State->_bucket_name,
				                         &State->_bucket_response,
				                         "/", /* Delimiter */
				                         State->_object_name,
				                         State->_marker,
				                         MaxEntries);
				if (result)
					return result;
			}

			/* If it is an object... */
			for (i = 0; i < State->_bucket_response->num_objects; i++)
			{
				if (strcmp(State->_object_name, ds3_str_value(State->_bucket_response->objects[i].name)) == 0)
				{
					char * last_modified = NULL;
					if (State->_bucket_response->objects[i].last_modified)
						last_modified = ds3_str_value(State->_bucket_response->objects[i].last_modified);
					result = stat_populate(basename(State->_object_name),
					                       S_IFREG,
					                       1,
					                       State->_bucket_response->objects[i].size,
					                       ds3_str_value(State->_bucket_response->objects[i].owner->name),
					                       last_modified,
					                       &GFSStatArray[(*CountOut)++]);
					State->_complete = 1;
					return result;
				}
			}

			/* If it is a directory... */
			for (i = 0; i < State->_bucket_response->num_common_prefixes; i++)
			{
				if (strncmp(State->_object_name,
				            ds3_str_value(State->_bucket_response->common_prefixes[i]),
				            ds3_str_size(State->_bucket_response->common_prefixes[i])-1) == 0 &&
				    State->_bucket_response->common_prefixes[i]->value[
				                      State->_bucket_response->common_prefixes[i]->size-1] == '/')
				{
					/* If we do not need to expand the directory... */
					if (FileOnly)
					{
						result = stat_populate(basename(State->_object_name),
						                       S_IFDIR,
						                       2,
						                       1024,
						                       ds3_str_value(State->_service_response->owner->name),
						                       NULL,
						                       &GFSStatArray[(*CountOut)++]);
						State->_complete = 1;
						return result;
					} else
					{
						char * new_object_name = malloc(strlen(State->_object_name)+2);
						sprintf(new_object_name, "%s/", State->_object_name);
						free(State->_object_name);
						State->_object_name = new_object_name;

						if (State->_bucket_response)
							ds3_free_bucket_response(State->_bucket_response);
						State->_bucket_response = NULL;
						if (State->_marker)
							free(State->_marker);
						State->_index = 0;

						expanding_search = 1;
						break;
					}
				}
			}
		} while (State->_marker);

		/* We could not find it. */
		if (!expanding_search)
			return GlobusGFSErrorGeneric("No such file or directory");
	}

	do
	{
		/* First pass. */
		if (!State->_bucket_response && !State->_index)
		{
			result = stat_populate(".",
			                       S_IFDIR,
			                       2,
			                       1024,
			                       ds3_str_value(State->_service_response->owner->name),
			                       NULL, // XXX no modify time
			                       &GFSStatArray[(*CountOut)++]);

			if (result != GLOBUS_SUCCESS)
				return result;

			result = stat_populate("..",
			                       S_IFDIR,
			                       2,
			                       1024,
			                       ds3_str_value(State->_service_response->owner->name),
			                       NULL, // XXX no modify time
			                       &GFSStatArray[(*CountOut)++]);

			if (result != GLOBUS_SUCCESS)
				return result;
		}

		/* Get the next response. */
		if (!State->_bucket_response)
		{
			result = gds3_get_bucket(Client,
			                         State->_bucket_name,
			                         &State->_bucket_response,
			                         "/", /* Delimiter */
			                         State->_object_name,
			                         State->_marker,
			                         MaxEntries);
			if (result)
				return result;

			State->_index = 0;
		}

		for (i = State->_index; i < State->_bucket_response->num_objects; i++, State->_index++)
		{
			if (State->_object_name && strcmp(State->_bucket_response->objects[i].name->value, State->_object_name) == 0)
				continue;

			char * last_modified = NULL;
			if (State->_bucket_response->objects[i].last_modified)
				last_modified = ds3_str_value(State->_bucket_response->objects[i].last_modified);
			result = stat_populate(State->_bucket_response->objects[i].name->value,
			                       S_IFREG,
			                       1,
			                       State->_bucket_response->objects[i].size,
			                       ds3_str_value(State->_bucket_response->objects[i].owner->name),
			                       last_modified,
			                       &GFSStatArray[(*CountOut)++]);

			if (result != GLOBUS_SUCCESS || *CountOut == MaxEntries)
				return result;
		}

		for (i = State->_index - State->_bucket_response->num_objects;
		     i < State->_bucket_response->num_common_prefixes; 
		     i++, State->_index++)
		{
			char * entry = malloc(State->_bucket_response->common_prefixes[i]->size);
			snprintf(entry,
			         State->_bucket_response->common_prefixes[i]->size, 
			         "%s", 
			         State->_bucket_response->common_prefixes[i]->value);
			result = stat_populate(entry,
			                       S_IFDIR,
			                       2,
			                       1024,
			                       ds3_str_value(State->_service_response->owner->name),
			                       NULL,
			                       &GFSStatArray[(*CountOut)++]);
			free(entry);
			if (result != GLOBUS_SUCCESS || *CountOut == MaxEntries)
				return result;
		}

		ds3_free_bucket_response(State->_bucket_response);
		State->_bucket_response = NULL;

	} while (State->_marker);

	State->_complete = 1;
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


