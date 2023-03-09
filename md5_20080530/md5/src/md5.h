/* Copyright (c) 2008 by Teradata Corporation. All Rights Reserved. */

#ifndef MD5_H
#define MD5_H

/* generate MD5 result in upper case */
#define UDF_MD5_UPPERCASE 1

/* generate same result as previous releases
 * not correct if the length of the input is 56 + 64*N (N>= 0) bytes
 */
#define UDF_MD5_COMPAT 0


void md5(const unsigned char message[], int len, unsigned char result[]);
#endif
