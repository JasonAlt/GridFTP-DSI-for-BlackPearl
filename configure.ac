dnl
dnl  University of Illinois/NCSA Open Source License
dnl
dnl  Copyright © 2012-2015, NCSA.  All rights reserved.
dnl
dnl  Developed by:
dnl
dnl  Storage Enabling Technologies (SET)
dnl
dnl  Nation Center for Supercomputing Applications (NCSA)
dnl
dnl  http://dims.ncsa.uiuc.edu/set/uberftp
dnl
dnl  Permission is hereby granted, free of charge, to any person obtaining a
dnl  copy of this software and associated documentation files (the .Software.),
dnl  to deal with the Software without restriction, including without limitation
dnl  the rights to use, copy, modify, merge, publish, distribute, sublicense,
dnl  and/or sell copies of the Software, and to permit persons to whom the
dnl  Software is furnished to do so, subject to the following conditions:
dnl
dnl     + Redistributions of source code must retain the above copyright notice,
dnl       this list of conditions and the following disclaimers.
dnl
dnl     + Redistributions in binary form must reproduce the above copyright
dnl       notice, this list of conditions and the following disclaimers in the
dnl       documentation and/or other materials provided with the distribution.
dnl
dnl     + Neither the names of SET, NCSA
dnl       nor the names of its contributors may be used to endorse or promote
dnl       products derived from this Software without specific prior written
dnl       permission.
dnl
dnl  THE SOFTWARE IS PROVIDED .AS IS., WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
dnl  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
dnl  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
dnl  THE CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
dnl  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
dnl  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
dnl  DEALINGS WITH THE SOFTWARE.
dnl

AC_INIT([GridFTP BlackPearl DSI], [0.1_BETA], [Jason Alt <jalt@ncsa.illinois.edu>])
AM_INIT_AUTOMAKE([subdir-objects foreign])
AC_PROG_LIBTOOL
AC_PROG_CXX
AC_PROG_CC
AC_CONFIG_MACRO_DIR([m4])

#
# For checksums
#
PKG_CHECK_MODULES([LIBCRYPTO], [libcrypto])
PKG_CHECK_MODULES([GLOBUS], [globus-common globus-gridftp-server >= 8.0])
PKG_CHECK_MODULES([LIBDS3], [libds3])

dnl
dnl These values are hardcoded for now, until I can get the packaging stuff working again
dnl
DIRT_TIMESTAMP=`perl -e 'print time'`
DIRT_BRANCH_ID=99999

AC_CONFIG_HEADERS([config.h])

AC_SUBST(GPT_MAJOR_VERSION, 1)
AC_SUBST(GPT_MINOR_VERSION, 1)
AC_SUBST(DIRT_TIMESTAMP)
AC_SUBST(DIRT_BRANCH_ID)

AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([source/Makefile])
AC_CONFIG_FILES([source/version.h])
AC_OUTPUT
