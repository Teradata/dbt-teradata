/* Copyright (c) 2008 by Teradata Corporation. All Rights Reserved. */

/*
 * The md5 function implements the MD5 message-digest algorithm.
 * The algorithm takes as input a message of arbitrary length and produces
 * as output a 128-bit "fingerprint" or "message digest" of the input. The
 * MD5 algorithm is intended for digital signature applications, where a
 * large file must be "compressed" in a secure manner before being
 * encrypted with a private (secret) key under a public-key cryptosystem
 * such as RSA.
 *
 *
 */


#include <stdlib.h>

#define SQL_TEXT Latin_Text

#include "sqltypes_td.h"
#include "md5.h"


#define UDF_OK "00000"


void md5_latin(VARCHAR_LATIN *message,
               CHARACTER_LATIN *digest,
               char sqlstate[])
{
    int i;
    unsigned char outbuf[16];
    CHARACTER_LATIN *ptr;


    md5((unsigned char *) message, strlen((char *) message), outbuf);

    for (i = 0, ptr = digest; i < 16; ++i, ptr += 2)
#if defined(UDF_MD5_UPPERCASE) && ((UDF_MD5_UPPERCASE) != 0)
        (void) sprintf(ptr, "%02X", outbuf[i]);
#else
        (void) sprintf(ptr, "%02x", outbuf[i]);
#endif
    (void) sprintf(sqlstate, UDF_OK);
}
