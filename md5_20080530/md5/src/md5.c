/* Copyright (c) 2008 by Teradata Corporation. All Rights Reserved. */

/* 
 * The md5 function implements the MD5 message-digest algorithm.
 * The algorithm takes as input a message of arbitrary length and produces
 * as output a 128-bit "fingerprint" or "message digest" of the input. The
 * MD5 algorithm is intended for digital signature applications, where a
 * large file must be "compressed" in a secure manner before being
 * encrypted with a private (secret) key under a public-key cryptosystem
 * such as RSA.
 */

#include <stdlib.h>
#include <string.h>

#define SQL_TEXT Latin_Text

#include "sqltypes_td.h"
#include "md5.h"

#define R1(a, b, c, d, xk, s, ti) (b + LROT((a + F(b, c, d) + xk + ti), s))
#define R2(a, b, c, d, xk, s, ti) (b + LROT((a + G(b, c, d) + xk + ti), s))
#define R3(a, b, c, d, xk, s, ti) (b + LROT((a + H(b, c, d) + xk + ti), s))
#define R4(a, b, c, d, xk, s, ti) (b + LROT((a + I(b, c, d) + xk + ti), s))

#define LROT(x, s) ((x << s) | (x >> (32 - s)))

#define F(X, Y, Z) ((X) & (Y) | (~X) & (Z))
#define G(X, Y, Z) (((X) & (Z)) | ((Y) & (~Z)))
#define H(X, Y, Z) ((X) ^ (Y) ^ (Z))
#define I(X, Y, Z) ((Y) ^ ((X) | (~Z)))

/* initial value for MD register */
#define A0 0x67452301
#define B0 0xefcdab89
#define C0 0x98badcfe
#define D0 0x10325476

#define BLOCK_SIZE 64
#define FINAL_BLOCK_SIZE (BLOCK_SIZE - 8)

static void md5_block(unsigned int register[], const unsigned int blk[]);

static unsigned int T[64] = {
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee,
    0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
    0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
    0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
    0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa,
    0xd62f105d, 0x2441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
    0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
    0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
    0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
    0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x4881d05,
    0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039,
    0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
    0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
    0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391
};


void md5(const unsigned char message[], int len, unsigned char result[])
{
    int pos = 0;
    int padded = 0;
    int remain = 0;
    unsigned int la[2];
    const unsigned char pad = 1 << 7; /* first byte of padding */
    unsigned int X[BLOCK_SIZE / sizeof(int)]; /* 512 bit block = 32 bit * 16 */
    unsigned char buf[BLOCK_SIZE]; /* for final and final-1 block */
    unsigned int r[4] = {A0, B0, C0, D0}; /* MD register */
    
    memset(buf, 0, sizeof(buf));

    /* Process Message in 16-word Blocks */
    while (len - pos >= BLOCK_SIZE) {
        memcpy(X, &message[pos], sizeof(X));
        md5_block(r, X);
        pos += BLOCK_SIZE;
    }

    remain = len - pos;
    if (remain > 0) {
        memcpy(buf, &message[pos], remain);
    }

#if !defined(UDF_MD5_COMPAT) || ((UDF_MD5_COMPAT) == 0)
    if (remain > FINAL_BLOCK_SIZE - 1) {
#else
    if (remain > FINAL_BLOCK_SIZE) {
#endif
        /* carry block: cannot put length field in final block */

        buf[remain] = pad;
        memcpy(X, buf, sizeof(buf));

        md5_block(r, X);

        padded = 1;
        memset(buf, 0, sizeof(buf));
    }

    /* Step 1: Append Padding Bits */
    if (!padded)
        buf[remain] = pad;

    /* Step 2: Append Length */
    la[0] = len << 3; /* byte to bit */
    la[1] = 0; /* assuming length < 4Gb */
    
    /* run final block */
    memcpy(buf + FINAL_BLOCK_SIZE, la, sizeof(la));
    memcpy(X, buf, sizeof(buf));

    md5_block(r, X);

    memcpy(result, r, sizeof(int) * 4);

    /* clear digester to maintain security */
    memset(r, 0, sizeof(r));
}


static void md5_block(unsigned int r[], const unsigned int blk[])
{
    unsigned int a = r[0];
    unsigned int b = r[1];
    unsigned int c = r[2];
    unsigned int d = r[3];

#ifdef DEBUG
    int i;

    printf("md5_block <");
    for (i = 0; i < 16; ++i) {
        printf(" %x", blk[i]);
    }
    printf("\n");
    printf("a, b, c, d = %x %x %x %x\n", a, b, c, d);
#endif

    a = R1(a, b, c, d, blk[0], 7, T[0]);
    d = R1(d, a, b, c, blk[1], 12, T[1]);
    c = R1(c, d, a, b, blk[2], 17, T[2]);
    b = R1(b, c, d, a, blk[3], 22, T[3]);

    a = R1(a, b, c, d, blk[4], 7, T[4]);
    d = R1(d, a, b, c, blk[5], 12, T[5]);
    c = R1(c, d, a, b, blk[6], 17, T[6]);
    b = R1(b, c, d, a, blk[7], 22, T[7]);

    a = R1(a, b, c, d, blk[8], 7, T[8]);
    d = R1(d, a, b, c, blk[9], 12, T[9]);
    c = R1(c, d, a, b, blk[10], 17, T[10]);
    b = R1(b, c, d, a, blk[11], 22, T[11]);

    a = R1(a, b, c, d, blk[12], 7, T[12]);
    d = R1(d, a, b, c, blk[13], 12, T[13]);
    c = R1(c, d, a, b, blk[14], 17, T[14]);
    b = R1(b, c, d, a, blk[15], 22, T[15]);

#ifdef DEBUG
    printf("round 1: %x %x %x %x\n", a, b, c, d);
#endif

    a = R2(a, b, c, d, blk[1], 5, T[16]);
    d = R2(d, a, b, c, blk[6], 9, T[17]);
    c = R2(c, d, a, b, blk[11], 14, T[18]);
    b = R2(b, c, d, a, blk[0], 20, T[19]);

    a = R2(a, b, c, d, blk[5], 5, T[20]);
    d = R2(d, a, b, c, blk[10], 9, T[21]);
    c = R2(c, d, a, b, blk[15], 14, T[22]);
    b = R2(b, c, d, a, blk[4], 20, T[23]);

    a = R2(a, b, c, d, blk[9], 5, T[24]);
    d = R2(d, a, b, c, blk[14], 9, T[25]);
    c = R2(c, d, a, b, blk[3], 14, T[26]);
    b = R2(b, c, d, a, blk[8], 20, T[27]);

    a = R2(a, b, c, d, blk[13], 5, T[28]);
    d = R2(d, a, b, c, blk[2], 9, T[29]);
    c = R2(c, d, a, b, blk[7], 14, T[30]);
    b = R2(b, c, d, a, blk[12], 20, T[31]);

#ifdef DEBUG
    printf("round 2: %x %x %x %x\n", a, b, c, d);
#endif

    a = R3(a, b, c, d, blk[5], 4, T[32]);
    d = R3(d, a, b, c, blk[8], 11, T[33]);
    c = R3(c, d, a, b, blk[11], 16, T[34]);
    b = R3(b, c, d, a, blk[14], 23, T[35]);

    a = R3(a, b, c, d, blk[1], 4, T[36]);
    d = R3(d, a, b, c, blk[4], 11, T[37]);
    c = R3(c, d, a, b, blk[7], 16, T[38]);
    b = R3(b, c, d, a, blk[10], 23, T[39]);

    a = R3(a, b, c, d, blk[13], 4, T[40]);
    d = R3(d, a, b, c, blk[0], 11, T[41]);
    c = R3(c, d, a, b, blk[3], 16, T[42]);
    b = R3(b, c, d, a, blk[6], 23, T[43]);

    a = R3(a, b, c, d, blk[9], 4, T[44]);
    d = R3(d, a, b, c, blk[12], 11, T[45]);
    c = R3(c, d, a, b, blk[15], 16, T[46]);
    b = R3(b, c, d, a, blk[2], 23, T[47]);

#ifdef DEBUG
    printf("round 3: %x %x %x %x\n", a, b, c, d);
#endif

    a = R4(a, b, c, d, blk[0], 6, T[48]);
    d = R4(d, a, b, c, blk[7], 10, T[49]);
    c = R4(c, d, a, b, blk[14], 15, T[50]);
    b = R4(b, c, d, a, blk[5], 21, T[51]);

    a = R4(a, b, c, d, blk[12], 6, T[52]);
    d = R4(d, a, b, c, blk[3], 10, T[53]);
    c = R4(c, d, a, b, blk[10], 15, T[54]);
    b = R4(b, c, d, a, blk[1], 21, T[55]);

    a = R4(a, b, c, d, blk[8], 6, T[56]);
    d = R4(d, a, b, c, blk[15], 10, T[57]);
    c = R4(c, d, a, b, blk[6], 15, T[58]);
    b = R4(b, c, d, a, blk[13], 21, T[59]);

    a = R4(a, b, c, d, blk[4], 6, T[60]);
    d = R4(d, a, b, c, blk[11], 10, T[61]);
    c = R4(c, d, a, b, blk[2], 15, T[62]);
    b = R4(b, c, d, a, blk[9], 21, T[63]);

#ifdef DEBUG
    printf("round 4: %x %x %x %x\n", a, b, c, d);
#endif

    r[0] += a;
    r[1] += b;
    r[2] += c;
    r[3] += d;

#ifdef DEBUG
    printf("md5_block > : %x %x %x %x\n", digester->r[0], digester->r[1],
           digester->r[2], digester->r[3]);
#endif
}
