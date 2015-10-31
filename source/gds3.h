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

#ifndef BLACKPEARL_DSI_GDS3_H
#define BLACKPEARL_DSI_GDS3_H

/*
 * Globus includes
 */
#include <globus_gridftp_server.h>

/*
 * DS3 includes
 */
#include <ds3.h>

globus_result_t
gds3_get_service(ds3_client *, ds3_get_service_response **);

globus_result_t
gds3_get_bucket(ds3_client              *  Client,
                char                    *  BucketName,
                ds3_get_bucket_response ** Response,
                char                    *  Delimiter,
                char                    *  Prefix,
                char                    *  Marker,
                uint32_t                   MaxKeys);

globus_result_t
gds3_put_bucket(ds3_client * Client, char * BucketName);

globus_result_t
gds3_init_bulk_put(ds3_client         * Client,
                   char               * BucketName, 
                   char               * ObjectName, 
                   uint64_t             Length, 
                   ds3_bulk_response ** BulkResponse);

globus_result_t
gds3_allocate_chunk(ds3_client                   * Client,
                    ds3_str                      * ChunkID,
                    ds3_allocate_chunk_response ** ChunkResponse);

globus_result_t
gds3_put_object_for_job(ds3_client * Client,
                        char       * BucketName,
                        char       * ObjectName,
                        uint64_t     Offset,
                        uint64_t     Length,
                        char       * JobID,
                        size_t    (* BufferCallout)(void*, size_t, size_t, void*),
                        void       * BufferCalloutArg);

globus_result_t
gds3_init_bulk_get(ds3_client        *  Client,
                   char              *  BucketName,
                   char              *  ObjectName,
                   uint64_t             Offset,
                   uint64_t             Length,
                   ds3_bulk_response ** BulkResponse);

globus_result_t
gds3_available_chunks(ds3_client                        *  Client,
                      ds3_str                           *  JobID,
                      ds3_get_available_chunks_response ** ChunkResponse);

globus_result_t
gds3_get_object_for_job(ds3_client * Client,
                        char       * BucketName,
                        char       * ObjectName,
                        uint64_t     Offset,
                        char       * JobID,
                        size_t    (* BufferCallout)(void*, size_t, size_t, void*),
                        void       * BufferCalloutArg);

globus_result_t
gds3_delete_bucket(ds3_client * Client, char * BucketName);

globus_result_t
gds3_delete_folder(ds3_client * Client, char * BucketName, char * FolderName);

globus_result_t
gds3_delete_object(ds3_client * Client, char * BucketName, char * ObjectName);

globus_result_t
gds3_get_jobs(ds3_client * Client, ds3_get_jobs_response **);

globus_result_t
gds3_get_job(ds3_client * Client, const char * JobID, ds3_bulk_response ** Response);

globus_result_t
gds3_delete_job(ds3_client * Client, ds3_str * JobID);

#endif /* BLACKPEARL_GDSI_DS3_H */
