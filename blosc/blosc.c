/*********************************************************************
  Blosc - Blocked Suffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>
  Creation date: 2009-05-20

  See LICENSES/BLOSC.txt for details about copyright and rights to use.
**********************************************************************/


#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#if defined(USING_CMAKE)
  #include "config.h"
#endif /*  USING_CMAKE */
#include "blosc.h"
#include "shuffle.h"
#include "blosclz.h"
#if defined(HAVE_LZ4)
  #include "lz4.h"
  #include "lz4hc.h"
#endif /*  HAVE_LZ4 */
#if defined(HAVE_SNAPPY)
  #include "snappy-c.h"
#endif /*  HAVE_SNAPPY */
#if defined(HAVE_ZLIB)
  #include "zlib.h"
#endif /*  HAVE_ZLIB */

#if defined(_WIN32) && !defined(__MINGW32__)
  #include <windows.h>
  
  /* stdint.h only available in VS2010 (VC++ 16.0) and newer */
  #if defined(_MSC_VER) && _MSC_VER < 1600
    #include "win32/stdint-windows.h"
  #else
    #include <stdint.h>
  #endif
  
  #include <process.h>
  #define getpid _getpid
#else
  #include <stdint.h>
  #include <unistd.h>
  #include <inttypes.h>
#endif  /* _WIN32 */

#include <stdbool.h>

/* If C11 is supported, use it's built-in aligned allocation. */
#if __STDC_VERSION__ >= 201112L
  #include <stdalign.h>
#endif

#if defined(_WIN32)
  #include "win32/pthread.h"
  #include "win32/pthread.c"
#else
  #include <pthread.h>
#endif

#include <omp.h>


/* Some useful units */
#define KB 1024
#define MB (1024*KB)

/* Minimum buffer size to be compressed */
#define MIN_BUFFERSIZE 128       /* Cannot be smaller than 66 */

/* The maximum number of splits in a block for compression */
#define MAX_SPLITS 16            /* Cannot be larger than 128 */

/* The size of L1 cache.  32 KB is quite common nowadays. */
#define L1 (32*KB)


/*
 * General helper functions
 */

/* A function for aligned malloc that is portable */
static uint8_t *blosc_malloc(size_t size)
{
  void *block = NULL;
  int res = 0;

#if __STDC_VERSION__ >= 201112L
  /* C11 aligned allocation. 'size' must be a multiple of the alignment. */
  block = aligned_alloc(16, size);
#elif defined(_WIN32)
  /* A (void *) cast needed for avoiding a warning with MINGW :-/ */
  block = (void *)_aligned_malloc(size, 16);
#elif _POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600
  /* Platform does have an implementation of posix_memalign */
  res = posix_memalign(&block, 16, size);
#elif defined __APPLE__
  /* Mac OS X guarantees 16-byte alignment in small allocs */
  block = malloc(size);
#else
  block = malloc(size);
#endif  /* _WIN32 */

  if (block == NULL || res != 0) {
    printf("Error allocating %d bytes of memory!", size);
    return NULL;
  }

  return (uint8_t *)block;
}


/* Release memory booked by blosc_malloc */
static void blosc_free(void *block)
{
#if defined(_WIN32)
    _aligned_free(block);
#else
    free(block);
#endif  /* _WIN32 */
}


/* Load signed 32-bit integer from (possibly) unaligned address,
   changing endianness if necessary. */
static int32_t loadu_int32(const uint8_t* const pa)
{
  int32_t idest;
  uint8_t *dest = (uint8_t *)&idest;
  int i = 1;                    /* for big/little endian detection */
  char *p = (char *)&i;

  if (p[0] != 1) {
    /* big endian */
    dest[0] = pa[3];
    dest[1] = pa[2];
    dest[2] = pa[1];
    dest[3] = pa[0];
  }
  else {
    /* little endian */
    dest[0] = pa[0];
    dest[1] = pa[1];
    dest[2] = pa[2];
    dest[3] = pa[3];
  }
  return idest;
}

/* Store signed 32-bit integer to (possibly) unaligned address,
   changing endianness if necessary. */
static void storeu_int32(uint8_t* const dest, const int32_t a)
{
  uint8_t *pa = (uint8_t *)&a;
  int i = 1;                    /* for big/little endian detection */
  char *p = (char *)&i;

  if (p[0] != 1) {
    /* big endian */
    dest[0] = pa[3];
    dest[1] = pa[2];
    dest[2] = pa[1];
    dest[3] = pa[0];
  }
  else {
    /* little endian */
    dest[0] = pa[0];
    dest[1] = pa[1];
    dest[2] = pa[2];
    dest[3] = pa[3];
  }
}


/*
 * Conversion routines between compressor and compression libraries
 */

/* Return the library code associated with the compressor name */
static int compname_to_clibcode(const char *compname)
{
  if (strcmp(compname, BLOSC_BLOSCLZ_COMPNAME) == 0)
    return BLOSC_BLOSCLZ_LIB;
  else if (strcmp(compname, BLOSC_LZ4_COMPNAME) == 0)
    return BLOSC_LZ4_LIB;
  else if (strcmp(compname, BLOSC_LZ4HC_COMPNAME) == 0)
    return BLOSC_LZ4_LIB;
  else if (strcmp(compname, BLOSC_SNAPPY_COMPNAME) == 0)
    return BLOSC_SNAPPY_LIB;
  else if (strcmp(compname, BLOSC_ZLIB_COMPNAME) == 0)
    return BLOSC_ZLIB_LIB;
  else
    return -1;
}

/* Return the library name associated with the compressor code */
static char *clibcode_to_clibname(int clibcode)
{
  switch (clibcode)
  {
  case BLOSC_BLOSCLZ_LIB:
    return BLOSC_BLOSCLZ_LIBNAME;
  case BLOSC_LZ4_LIB:
    return BLOSC_LZ4_LIBNAME;
  case BLOSC_SNAPPY_LIB:
    return BLOSC_SNAPPY_LIBNAME;
  case BLOSC_ZLIB_LIB:
    return BLOSC_ZLIB_LIBNAME;
  default:
    /* should never happen */
    return NULL;
  }
}


/*
 * Conversion routines between compressor names and compressor codes
 */

/* Get the compressor name associated with the compressor code */
int blosc_compcode_to_compname(const int compcode, const char** const compname)
{
  /* Map the compressor code */
  switch (compcode)
  {
  case BLOSC_BLOSCLZ:
    *compname = BLOSC_BLOSCLZ_COMPNAME;
    break;
  case BLOSC_LZ4:
    *compname = BLOSC_LZ4_COMPNAME;
    break;
  case BLOSC_LZ4HC:
    *compname = BLOSC_LZ4HC_COMPNAME;
    break;
  case BLOSC_SNAPPY:
    *compname = BLOSC_SNAPPY_COMPNAME;
    break;
  case BLOSC_ZLIB:
    *compname = BLOSC_ZLIB_COMPNAME;
    break;
  default:
    /* Invalid compressor code */
    *compname = NULL;
  }

  /* Guess if there is support for this code */
  switch (compcode)
  {
  case BLOSC_BLOSCLZ:
    return BLOSC_BLOSCLZ;
#if defined(HAVE_LZ4)
  case BLOSC_LZ4:
    return BLOSC_LZ4;
  case BLOSC_LZ4HC:
    return BLOSC_LZ4HC;
#endif /*  HAVE_LZ4 */
#if defined(HAVE_SNAPPY)
  case BLOSC_SNAPPY:
    return BLOSC_SNAPPY;
#endif /*  HAVE_SNAPPY */
#if defined(HAVE_ZLIB)
  case BLOSC_ZLIB:
    return BLOSC_ZLIB;
#endif /*  HAVE_ZLIB */
  default:
    /* -1 indicates an invalid or unsupported compressor. */
    return -1;
  }
}

/* Get the compressor code for the compressor name. -1 if it is not available */
int blosc_compname_to_compcode(const char *compname)
{
  if (strcmp(compname, BLOSC_BLOSCLZ_COMPNAME) == 0) {
    return BLOSC_BLOSCLZ;
  }
#if defined(HAVE_LZ4)
  else if (strcmp(compname, BLOSC_LZ4_COMPNAME) == 0) {
    return BLOSC_LZ4;
  }
  else if (strcmp(compname, BLOSC_LZ4HC_COMPNAME) == 0) {
    return BLOSC_LZ4HC;
  }
#endif /*  HAVE_LZ4 */
#if defined(HAVE_SNAPPY)
  else if (strcmp(compname, BLOSC_SNAPPY_COMPNAME) == 0) {
    return BLOSC_SNAPPY;
  }
#endif /*  HAVE_SNAPPY */
#if defined(HAVE_ZLIB)
  else if (strcmp(compname, BLOSC_ZLIB_COMPNAME) == 0) {
    return BLOSC_ZLIB;
  }
#endif /*  HAVE_ZLIB */
  else {
    /* -1 means non-existent compressor code */
    return -1;
  }
}


#if defined(HAVE_LZ4)
static int lz4_wrap_compress(const char* input, size_t input_length,
                             char* output, size_t maxout)
{
  int cbytes;
  cbytes = LZ4_compress_limitedOutput(input, output, (int)input_length,
                                      (int)maxout);
  return cbytes;
}

static int lz4hc_wrap_compress(const char* input, size_t input_length,
                               char* output, size_t maxout, int clevel)
{
  int cbytes;
  if (input_length > (size_t)(2<<30))
    return -1;   /* input larger than 1 GB is not supported */
  /* clevel for lz4hc goes up to 16, at least in LZ4 1.1.3 */
  cbytes = LZ4_compressHC2_limitedOutput(input, output, (int)input_length,
					 (int)maxout, clevel*2-1);
  return cbytes;
}

static int lz4_wrap_decompress(const char* input, size_t compressed_length,
                               char* output, size_t maxout)
{
  size_t cbytes;
  cbytes = LZ4_decompress_fast(input, output, (int)maxout);
  if (cbytes != compressed_length) {
    return 0;
  }
  return (int)maxout;
}

#endif /* HAVE_LZ4 */

#if defined(HAVE_SNAPPY)
static int snappy_wrap_compress(const char* input, size_t input_length,
                                char* output, size_t maxout)
{
  snappy_status status;
  size_t cl = maxout;
  status = snappy_compress(input, input_length, output, &cl);
  if (status != SNAPPY_OK){
    return 0;
  }
  return (int)cl;
}

static int snappy_wrap_decompress(const char* input, size_t compressed_length,
                                  char* output, size_t maxout)
{
  snappy_status status;
  size_t ul = maxout;
  status = snappy_uncompress(input, compressed_length, output, &ul);
  if (status != SNAPPY_OK){
    return 0;
  }
  return (int)ul;
}
#endif /* HAVE_SNAPPY */

#if defined(HAVE_ZLIB)
/* zlib is not very respectful with sharing name space with others.
 Fortunately, its names do not collide with those already in blosc. */
static int zlib_wrap_compress(const char* input, size_t input_length,
                              char* output, size_t maxout, int clevel)
{
  int status;
  uLongf cl = maxout;
  status = compress2(
	     (Bytef*)output, &cl, (Bytef*)input, (uLong)input_length, clevel);
  if (status != Z_OK){
    return 0;
  }
  return (int)cl;
}

static int zlib_wrap_decompress(const char* input, size_t compressed_length,
                                char* output, size_t maxout)
{
  int status;
  uLongf ul = maxout;
  status = uncompress(
             (Bytef*)output, &ul, (Bytef*)input, (uLong)compressed_length);
  if (status != Z_OK){
    return 0;
  }
  return (int)ul;
}

#endif /*  HAVE_ZLIB */

static int32_t compute_blocksize(
  const int32_t compressor_code,
  const int32_t clevel,
  const int32_t typesize,
  const int32_t nbytes,
  const int32_t forced_blocksize)
{
  int32_t blocksize;

  /* Protection against very small buffers */
  if (nbytes < (int32_t)typesize) {
    return 1;
  }

  blocksize = nbytes;           /* Start by a whole buffer as blocksize */

  if (forced_blocksize) {
    blocksize = forced_blocksize;
    /* Check that forced blocksize is not too small */
    if (blocksize < MIN_BUFFERSIZE) {
      blocksize = MIN_BUFFERSIZE;
    }
  }
  else if (nbytes >= L1*4) {
    blocksize = L1 * 4;

    /* For certain compressors, increase the block sizes by a factor
       of 8; they are designed for compressing large blocks and show
       sizable overheads when compressing small ones. */
    switch (compressor_code)
    {
    case BLOSC_ZLIB:
    case BLOSC_LZ4HC:
      blocksize *= 8;
      break;
    default:
      /* Use the default block size. */
      break;
    }

    /* Increase or decrease the block size based on the compression level. */
    switch (clevel)
    {
    case 0:
      blocksize /= 16;
      break;
    case 1:
    case 2:
    case 3:
      blocksize /= 8;
      break;
    case 4:
    case 5:
      blocksize /= 4;
      break;
    case 6:
      blocksize /= 2;
      break;
    case 7:
    case 8:
      blocksize *= 1;
      break;
    /* clevel >= 9 */
    case 9:
    default:
      blocksize *= 2;
      break;
    }
  }
  else if (nbytes > (16 * 16))  {
    /* align to typesize to make use of vectorized shuffles */
    switch (typesize)
    {
    case 2:
    case 4:
    case 8:
    case 16:
      blocksize -= blocksize % (16 * typesize);
      break;
    default:
      /* Use the default block size. */
      break;
    }
  }

  /* Check that blocksize is not too large */
  if (blocksize > (int32_t)nbytes) {
    blocksize = nbytes;
  }

  /* blocksize must be a multiple of the typesize */
  if (blocksize > typesize) {
    blocksize = blocksize / typesize * typesize;
  }

  /* blocksize must not exceed (64 KB * typesize) in order to allow
     BloscLZ to achieve better compression ratios (the ultimate reason
     for this is that hash_log in BloscLZ cannot be larger than 15) */
  if ((compressor_code == BLOSC_BLOSCLZ) && (blocksize / typesize) > 64*KB) {
    blocksize = 64 * KB * typesize;
  }

  return blocksize;
}


char* blosc_list_compressors(void)
{
  static int compressors_list_done = 0;
  static char ret[256];

  if (compressors_list_done) return ret;
  ret[0] = '\0';
  strcat(ret, BLOSC_BLOSCLZ_COMPNAME);
#if defined(HAVE_LZ4)
  strcat(ret, ","); strcat(ret, BLOSC_LZ4_COMPNAME);
  strcat(ret, ","); strcat(ret, BLOSC_LZ4HC_COMPNAME);
#endif /*  HAVE_LZ4 */
#if defined(HAVE_SNAPPY)
  strcat(ret, ","); strcat(ret, BLOSC_SNAPPY_COMPNAME);
#endif /*  HAVE_SNAPPY */
#if defined(HAVE_ZLIB)
  strcat(ret, ","); strcat(ret, BLOSC_ZLIB_COMPNAME);
#endif /*  HAVE_ZLIB */
  compressors_list_done = 1;
  return ret;
}


int blosc_get_complib_info(const char* const compname, char** const complib, char** const version)
{
  int clibcode;
  char *clibname;
  char *clibversion = "unknown";

  #if (defined(HAVE_LZ4) && defined(LZ4_VERSION_MAJOR)) || (defined(HAVE_SNAPPY) && defined(SNAPPY_VERSION))
  char sbuffer[256];
  #endif

  clibcode = compname_to_clibcode(compname);
  clibname = clibcode_to_clibname(clibcode);

  /* complib version */
  switch (clibcode)
  {
  case BLOSC_BLOSCLZ_LIB:
    clibversion = BLOSCLZ_VERSION_STRING;
    break;
#if defined(HAVE_LZ4)
  case BLOSC_LZ4_LIB:
#if defined(LZ4_VERSION_MAJOR)
    sprintf(sbuffer, "%d.%d.%d",
            LZ4_VERSION_MAJOR, LZ4_VERSION_MINOR, LZ4_VERSION_RELEASE);
    clibversion = sbuffer;
#endif /*  LZ4_VERSION_MAJOR */
    break;
#endif /*  HAVE_LZ4 */
#if defined(HAVE_SNAPPY)
  case BLOSC_SNAPPY_LIB:
#if defined(SNAPPY_VERSION)
    sprintf(sbuffer, "%d.%d.%d", SNAPPY_MAJOR, SNAPPY_MINOR, SNAPPY_PATCHLEVEL);
    clibversion = sbuffer;
#endif /*  SNAPPY_VERSION */
    break;
#endif /*  HAVE_SNAPPY */
#if defined(HAVE_ZLIB)
  case BLOSC_ZLIB_LIB:
    clibversion = ZLIB_VERSION;
    break;
#endif /*  HAVE_ZLIB */
  }
  
  *complib = strdup(clibname);
  *version = strdup(clibversion);
  return clibcode;
}


/* Return `nbytes`, `cbytes` and `blocksize` from a compressed buffer. */
void blosc_cbuffer_sizes(
  const uint8_t* const cbuffer,
  size_t* const nbytes,
  size_t* const cbytes,
  size_t* const blocksize)
{
  uint8_t *_src = (uint8_t *)(cbuffer);    /* current pos for source buffer */
  uint8_t version, versionlz;              /* versions for compressed header */

  /* Read the version info (could be useful in the future) */
  version = _src[0];                       /* blosc format version */
  versionlz = _src[1];                     /* blosclz format version */

  version += 0;                            /* shut up compiler warning */
  versionlz += 0;                          /* shut up compiler warning */

  /* Read the interesting values */
  *nbytes = (size_t)loadu_int32(_src + 4);       /* uncompressed buffer size */
  *blocksize = (size_t)loadu_int32(_src + 8);    /* block size */
  *cbytes = (size_t)loadu_int32(_src + 12);      /* compressed buffer size */
}


/* Return `typesize` and `flags` from a compressed buffer. */
void blosc_cbuffer_metainfo(
  const uint8_t* const cbuffer,
  size_t* const typesize,
  int* const flags)
{
  uint8_t *_src = (uint8_t *)(cbuffer);  /* current pos for source buffer */
  uint8_t version, versionlz;            /* versions for compressed header */

  /* Read the version info (could be useful in the future) */
  version = _src[0];                     /* blosc format version */
  versionlz = _src[1];                   /* blosclz format version */

  version += 0;                             /* shut up compiler warning */
  versionlz += 0;                           /* shut up compiler warning */

  /* Read the interesting values */
  *flags = (int)_src[2];                 /* flags */
  *typesize = (size_t)_src[3];           /* typesize */
}


/* Return version information from a compressed buffer. */
void blosc_cbuffer_versions(
  const uint8_t* const cbuffer,
  int* const version,
  int* const versionlz)
{
  uint8_t *_src = (uint8_t *)(cbuffer);  /* current pos for source buffer */

  /* Read the version info */
  *version = (int)_src[0];         /* blosc format version */
  *versionlz = (int)_src[1];       /* Lempel-Ziv compressor format version */
}


/* Return the compressor library/format used in a compressed buffer. */
char *blosc_cbuffer_complib(const uint8_t* const cbuffer)
{
  uint8_t *_src = (uint8_t *)(cbuffer);  /* current pos for source buffer */
  int clibcode;
  char *complib;

  /* Read the compressor format/library info */
  clibcode = (_src[2] & 0xe0) >> 5;
  complib = clibcode_to_clibname(clibcode);
  return complib;
}


/*
 * Synchronization variables
 */

struct blosc_context {
  const uint8_t* src;
  uint8_t* dest;                  /* The current pos in the destination buffer */
  /* TODO : Is this really meant to be a pointer? If so, can it be changed to uintptr_t? */
  uint8_t* header_flags;          /* Flags for header.  Currently booked:
                                    - 0: shuffled?
                                    - 1: memcpy'ed? */
  size_t sourcesize;              /* Number of bytes in source buffer (or uncompressed bytes in compressed file) */
  size_t destsize;                /* Maximum size for destination buffer */
  
  size_t blocksize;               /* Length of the block in bytes */
  uint32_t typesize;              /* Type size */
  
  uintptr_t nblocks;              /* Number of total blocks in buffer */
  ptrdiff_t leftover;             /* Extra bytes at end of buffer */
  
  /* TODO : Maybe make this a ptrdiff_t */
  uint8_t* bstarts;               /* Start of the buffer past header info */
  
  bool compress;                  /* true if we are doing compression, false if decompress */
  /* TODO : If necessary/useful for alignment, change this to uint8_t or uint16_t */
  int32_t compcode;               /* Compressor code to use */
  uint8_t clevel;                 /* Compression level (1-9) */
  uint8_t numthreads;             /* The number of threads to use when (de)compressing. */

  /*
   * Fields below hold mutable state for (de)compression.
   * TODO : It may improve speed a bit to insert padding here so the fields below
   *        sit on their own cache line, so the fields above aren't repeatedly evicted from the cache
   *        due to the fields below being written to.
   */

  size_t num_output_bytes;        /* Counter for the number of output bytes */
  int32_t thread_giveup_code;     /* error code when give up */
  int32_t thread_nblock;          /* number of processed blocks: 0 <= thread_nblock <= nblocks */
};

struct thread_context {
  struct blosc_context* parent_context;
  int32_t tid;
  uint8_t* tmp;
  uint8_t* tmp2;
  int32_t tmpblocksize; /* Used to keep track of how big the temporary buffers are */
};


int initialize_context_compression(
  struct blosc_context* const context,
  const uint8_t clevel,
  const bool doshuffle,
  const uint32_t typesize,
  const size_t sourcesize,
  const uint8_t* const src,
  uint8_t* const dest,
  const size_t destsize,
  const int32_t compressor,
  const int32_t blocksize,
  const uint8_t numthreads)
{
  /* Set parameters */
  context->compress = 1;
  context->src = (const uint8_t*)src;
  context->dest = (uint8_t *)(dest);
  context->num_output_bytes = 0;
  context->destsize = (int32_t)destsize;
  context->sourcesize = sourcesize;
  context->typesize = typesize;
  context->compcode = compressor;
  context->numthreads = numthreads;
  context->clevel = clevel;

  /* Check buffer size limits */
  if (sourcesize > BLOSC_MAX_BUFFERSIZE) {
    /* If buffer is too large, give up. */
    fprintf(stderr, "Input buffer size cannot exceed %d bytes\n",
            BLOSC_MAX_BUFFERSIZE);
    return -1;
  }

  /* Compression level */
  if (clevel < 0 || clevel > 9) {
    /* If clevel not in 0..9, print an error */
    fprintf(stderr, "`clevel` parameter must be between 0 and 9!\n");
    return -10;
  }

  /* Shuffle */
  if (doshuffle != 0 && doshuffle != 1) {
    fprintf(stderr, "`shuffle` parameter must be either 0 or 1!\n");
    return -10;
  }

  /* Check typesize limits */
  if (context->typesize > BLOSC_MAX_TYPESIZE) {
    /* If typesize is too large, treat buffer as an 1-byte stream. */
    context->typesize = 1;
  }

  /* Get the blocksize */
  context->blocksize = compute_blocksize(context->compcode, clevel, (int32_t)context->typesize, context->sourcesize, blocksize);

  /* Compute number of blocks in buffer */
  ldiv_t blocks_and_leftover = ldiv(context->sourcesize, context->blocksize);
  context->leftover = blocks_and_leftover.rem;
  context->nblocks = (blocks_and_leftover.rem > 0) ? (blocks_and_leftover.quot + 1) : blocks_and_leftover.quot;

  return 1;
}


int write_compression_header(struct blosc_context* const context, const int clevel, const bool doshuffle)
{
  int32_t compcode;

  /* Write version header for this block */
  context->dest[0] = BLOSC_VERSION_FORMAT;              /* blosc format version */

  /* Write compressor format */
  compcode = -1;
  switch (context->compcode)
  {
  case BLOSC_BLOSCLZ:
    compcode = BLOSC_BLOSCLZ_FORMAT;
    context->dest[1] = BLOSC_BLOSCLZ_VERSION_FORMAT; /* blosclz format version */
    break;

#if defined(HAVE_LZ4)
  case BLOSC_LZ4:
    compcode = BLOSC_LZ4_FORMAT;
    context->dest[1] = BLOSC_LZ4_VERSION_FORMAT;  /* lz4 format version */
    break;
  case BLOSC_LZ4HC:
    compcode = BLOSC_LZ4HC_FORMAT;
    context->dest[1] = BLOSC_LZ4HC_VERSION_FORMAT; /* lz4hc is the same as lz4 */
    break;
#endif /*  HAVE_LZ4 */

#if defined(HAVE_SNAPPY)
  case BLOSC_SNAPPY:
    compcode = BLOSC_SNAPPY_FORMAT;
    context->dest[1] = BLOSC_SNAPPY_VERSION_FORMAT;    /* snappy format version */
    break;
#endif /*  HAVE_SNAPPY */

#if defined(HAVE_ZLIB)
  case BLOSC_ZLIB:
    compcode = BLOSC_ZLIB_FORMAT;
    context->dest[1] = BLOSC_ZLIB_VERSION_FORMAT;      /* zlib format version */
    break;
#endif /*  HAVE_ZLIB */

  default:
  {
    char *compname;
    blosc_compcode_to_compname(compcode, &compname);
    fprintf(stderr, "Blosc has not been compiled with '%s' ", compname);
    fprintf(stderr, "compression support.  Please use one having it.");
    return -5;    /* signals no compression support */
    break;
  }
  }

  context->header_flags = context->dest+2;                       /* flags */
  context->dest[2] = 0;                                          /* zeroes flags */
  context->dest[3] = (uint8_t)context->typesize;                 /* type size */
  storeu_int32(context->dest + 4, context->sourcesize);           /* size of the buffer */
  storeu_int32(context->dest + 8, context->blocksize);            /* block size */
  context->bstarts = context->dest + 16;                         /* starts for every block */
  context->num_output_bytes = 16 + sizeof(int32_t)*context->nblocks;  /* space for header and pointers */

  if (context->clevel == 0) {
    /* Compression level 0 means buffer to be memcpy'ed */
    *(context->header_flags) |= BLOSC_MEMCPYED;
  }

  if (context->sourcesize < MIN_BUFFERSIZE) {
    /* Buffer is too small.  Try memcpy'ing. */
    *(context->header_flags) |= BLOSC_MEMCPYED;
  }

  if (doshuffle == 1) {
    /* Shuffle is active */
    *(context->header_flags) |= BLOSC_DOSHUFFLE;          /* bit 0 set to one in flags */
  }

  *(context->header_flags) |= compcode << 5;              /* compressor format start at bit 5 */

  return 1;
}


/* Shuffle & compress a single block */
static size_t blosc_c(
  const struct blosc_context* const context,
  const size_t blocksize,
  const bool leftoverblock,
  const size_t ntbytes,
  const size_t maxbytes,
  const uint8_t* const src,
  uint8_t* const dest,
  uint8_t* const tmp)
{
  int32_t split_idx;
  int32_t neblock, nsplits;
  int32_t cbytes;                   /* number of compressed bytes in split */
  int32_t ctbytes = 0;              /* number of compressed bytes in block */
  int32_t maxout;
  int32_t typesize = context->typesize;
  const uint8_t *_tmp;
  uint8_t* dest_current = dest;
  size_t ntbytes_current = ntbytes;

  if ((*(context->header_flags) & BLOSC_DOSHUFFLE) && (typesize > 1)) {
    /* Shuffle this block (this makes sense only if typesize > 1) */
    shuffle(typesize, blocksize, src, tmp);
    _tmp = tmp;
  }
  else {
    _tmp = src;
  }

  /* Compress for each shuffled slice split for this block. */
  /* If typesize is too large, neblock is too small or we are in a
     leftover block, do not split at all. */
  if ((typesize <= MAX_SPLITS) && (blocksize/typesize) >= MIN_BUFFERSIZE &&
      (!leftoverblock)) {
    nsplits = typesize;
  }
  else {
    nsplits = 1;
  }
  neblock = blocksize / nsplits;
  for (split_idx = 0; split_idx < nsplits; split_idx++) {
    dest_current += sizeof(int32_t);
    ntbytes_current += (int32_t)sizeof(int32_t);
    ctbytes += (int32_t)sizeof(int32_t);
    maxout = neblock;
    #if defined(HAVE_SNAPPY)
    if (context->compcode == BLOSC_SNAPPY) {
      /* TODO perhaps refactor this to keep the value stashed somewhere */
      maxout = snappy_max_compressed_length(neblock);
    }
    #endif /*  HAVE_SNAPPY */
    if (ntbytes_current+maxout > maxbytes) {
      maxout = maxbytes - ntbytes_current;   /* avoid buffer overrun */
      if (maxout <= 0) {
        return 0;                  /* non-compressible block */
      }
    }
    
    switch (context->compcode)
    {
    case BLOSC_BLOSCLZ:
      cbytes = blosclz_compress(context->clevel, _tmp+split_idx*neblock, neblock,
                                dest_current, maxout);
      break;
    #if defined(HAVE_LZ4)
    case BLOSC_LZ4:
      cbytes = lz4_wrap_compress((char *)_tmp+split_idx*neblock, (size_t)neblock,
                                 (char *)dest_current, (size_t)maxout);
      break;
    case BLOSC_LZ4HC:
      cbytes = lz4hc_wrap_compress((char *)_tmp+split_idx*neblock, (size_t)neblock,
                                   (char *)dest_current, (size_t)maxout, context->clevel);
      break;
    #endif /*  HAVE_LZ4 */
    #if defined(HAVE_SNAPPY)
    case BLOSC_SNAPPY:
      cbytes = snappy_wrap_compress((char *)_tmp+split_idx*neblock, (size_t)neblock,
                                    (char *)dest_current, (size_t)maxout);
      break;
    #endif /*  HAVE_SNAPPY */
    #if defined(HAVE_ZLIB)
    case BLOSC_ZLIB:
      cbytes = zlib_wrap_compress((char *)_tmp+split_idx*neblock, (size_t)neblock,
                                  (char *)dest_current, (size_t)maxout, context->clevel);
      break;
    #endif /*  HAVE_ZLIB */
    default:
    {
      char *compname;
      blosc_compcode_to_compname(context->compcode, &compname);
      fprintf(stderr, "Blosc has not been compiled with '%s' ", compname);
      fprintf(stderr, "compression support.  Please use one having it.");
      return -5;    /* signals no compression support */
    }
    }

    if (cbytes > maxout) {
      /* Buffer overrun caused by compression (should never happen) */
      return -1;
    }
    else if (cbytes < 0) {
      /* cbytes should never be negative */
      return -2;
    }
    else if (cbytes == 0 || cbytes == neblock) {
      /* The compressor has been unable to compress data at all. */
      /* Before doing the copy, check that we are not running into a
         buffer overflow. */
      if ((ntbytes_current+neblock) > maxbytes) {
        return 0;    /* Non-compressible data */
      }
      memcpy(dest_current, _tmp+split_idx*neblock, neblock);
      cbytes = neblock;
    }
    storeu_int32(dest_current - 4, cbytes);
    dest_current += cbytes;
    ntbytes_current += cbytes;
    ctbytes += cbytes;
  }  /* Closes block_idx < nsplits */

  return ctbytes;
}


/* Decompress & unshuffle a single block */
static size_t blosc_d(
  struct blosc_context* const context,
  const size_t blocksize,
  const bool leftoverblock,
  const uint8_t* const src,
  uint8_t* const dest,
  uint8_t* const tmp,
  uint8_t* const tmp2)
{
  int32_t split_idx;
  int32_t neblock, nsplits;
  int32_t nbytes;                /* number of decompressed bytes in split */
  int32_t cbytes;                /* number of compressed bytes in split */
  int32_t ctbytes = 0;           /* number of compressed bytes in block */
  int32_t ntbytes = 0;           /* number of uncompressed bytes in block */
  uint8_t *_tmp;
  int32_t typesize = context->typesize;
  int32_t compcode;
  uint8_t* src_current = src;

  if ((*(context->header_flags) & BLOSC_DOSHUFFLE) && (typesize > 1)) {
    _tmp = tmp;
  }
  else {
    _tmp = dest;
  }

  compcode = (*(context->header_flags) & 0xe0) >> 5;

  /* Compress for each shuffled slice split for this block. */
  if ((typesize <= MAX_SPLITS) && (blocksize/typesize) >= MIN_BUFFERSIZE &&
      (!leftoverblock)) {
    nsplits = typesize;
  }
  else {
    nsplits = 1;
  }
  neblock = blocksize / nsplits;
  for (split_idx = 0; split_idx < nsplits; split_idx++) {
    cbytes = loadu_int32(src_current);      /* amount of compressed bytes */
    src_current += sizeof(int32_t);
    ctbytes += (int32_t)sizeof(int32_t);
    /* Uncompress */
    if (cbytes == neblock) {
      memcpy(_tmp, src_current, neblock);
      nbytes = neblock;
    }
    else {
      switch (compcode)
      {
      case BLOSC_BLOSCLZ_FORMAT:
        nbytes = blosclz_decompress(src_current, cbytes, _tmp, neblock);
        break;
      #if defined(HAVE_LZ4)
      case BLOSC_LZ4_FORMAT:
        nbytes = lz4_wrap_decompress((char *)src_current, (size_t)cbytes,
                                     (char*)_tmp, (size_t)neblock);
        break;
      #endif /*  HAVE_LZ4 */
      #if defined(HAVE_SNAPPY)
      case BLOSC_SNAPPY_FORMAT:
        nbytes = snappy_wrap_decompress((char *)src_current, (size_t)cbytes,
                                        (char*)_tmp, (size_t)neblock);
        break;
      #endif /*  HAVE_SNAPPY */
      #if defined(HAVE_ZLIB)
      case BLOSC_ZLIB_FORMAT:
        nbytes = zlib_wrap_decompress((char *)src_current, (size_t)cbytes,
                                      (char*)_tmp, (size_t)neblock);
        break;
      #endif /*  HAVE_ZLIB */
      default:
      {
        char *compname;
        blosc_compcode_to_compname(compcode, &compname);
        fprintf(stderr,
                "Blosc has not been compiled with decompression "
                "support for '%s' format. ", compname);
        fprintf(stderr, "Please recompile for adding this support.\n");
        return -5;    /* signals no decompression support */
      }
      }

      /* Check that decompressed bytes number is correct */
      if (nbytes != neblock) {
          return -2;
      }

    }
    src_current += cbytes;
    ctbytes += cbytes;
    _tmp += nbytes;
    ntbytes += nbytes;
  } /* Closes split_idx < nsplits */

  if ((*(context->header_flags) & BLOSC_DOSHUFFLE) && (typesize > 1)) {
    if ((uintptr_t)dest % 16 == 0) {
      /* 16-bytes aligned dest.  SSE2 unshuffle will work. */
      unshuffle(typesize, blocksize, tmp, dest);
    }
    else {
      /* dest is not aligned.  Use tmp2, which is aligned, and copy. */
      unshuffle(typesize, blocksize, tmp, tmp2);
      if (tmp2 != dest) {
        /* Copy only when dest is not tmp2 (e.g. not blosc_getitem())  */
        memcpy(dest, tmp2, blocksize);
      }
    }
  }

  /* Return the number of uncompressed bytes */
  return ntbytes;
}


/* Serial version for compression/decompression */
static size_t serial_blosc(struct blosc_context* const context)
{
  int32_t block_idx;
  int32_t cbytes;

  const size_t ebsize = context->blocksize + (size_t)context->typesize * sizeof(int32_t);
  int32_t ntbytes = context->num_output_bytes;

  uint8_t* const tmp = blosc_malloc(context->blocksize);
  uint8_t* const tmp2 = blosc_malloc(ebsize);

  for (block_idx = 0; block_idx < context->nblocks; block_idx++) {
    if (context->compress && !(*(context->header_flags) & BLOSC_MEMCPYED)) {
      storeu_int32(context->bstarts + block_idx * sizeof(int32_t), ntbytes);
    }
    
    const bool leftoverblock = (block_idx == context->nblocks - 1) && (context->leftover > 0);
    const int32_t bsize = leftoverblock ? context->leftover : context->blocksize;
    
    if (context->compress) {
      if (*(context->header_flags) & BLOSC_MEMCPYED) {
        /* We want to memcpy only */
        memcpy(context->dest+BLOSC_MAX_OVERHEAD+block_idx*context->blocksize,
                context->src+block_idx*context->blocksize,
                bsize);
        cbytes = bsize;
      }
      else {
        /* Regular compression */
        cbytes = blosc_c(context, bsize, leftoverblock, ntbytes, context->destsize,
                         context->src+block_idx*context->blocksize, context->dest+ntbytes, tmp);
        if (cbytes == 0) {
          ntbytes = 0;              /* uncompressible data */
          break;
        }
      }
    }
    else {
      if (*(context->header_flags) & BLOSC_MEMCPYED) {
        /* We want to memcpy only */
        memcpy(context->dest+block_idx*context->blocksize,
                context->src+BLOSC_MAX_OVERHEAD+block_idx*context->blocksize,
                bsize);
        cbytes = bsize;
      }
      else {
        /* Regular decompression */
        cbytes = blosc_d(context, bsize, leftoverblock,
                          context->src + loadu_int32(context->bstarts + block_idx * sizeof(int32_t)),
                          context->dest+block_idx*context->blocksize, tmp, tmp2);
      }
    }
    if (cbytes < 0) {
      ntbytes = cbytes;         /* error in blosc_c or blosc_d */
      break;
    }
    ntbytes += cbytes;
  }

  // Free temporaries
  blosc_free(tmp);
  blosc_free(tmp2);

  return ntbytes;
}


/* OpenMP-based threaded version for compression/decompression. */
static size_t openmp_blosc(struct blosc_context* const context, const uint8_t numthreads)
{
  int32_t block_idx;
  
  const int32_t nblocks = context->nblocks;

  const size_t ebsize = context->blocksize + (size_t)context->typesize * sizeof(int32_t);
  
  int32_t thread_giveup_code = 1; /* zero or negative indicates an error */
  int32_t num_output_bytes = 0; /* number of bytes processed by all threads */
  
  #pragma omp parallel num_threads(numthreads) shared(thread_giveup_code, num_output_bytes) private(block_idx)
  {
    /* Number of bytes processed by this thread. */
    int32_t ntbytes = 0;
  
    /* Temporary buffers needed during (de)compression. */
    uint8_t* const tmp = blosc_malloc(context->blocksize);
    uint8_t* const tmp2 = blosc_malloc(ebsize);
    
    /* Compressing or decompressing? */
    if (context->compress) {
      #pragma omp parallel for schedule(static, 1) ordered
      for (block_idx = 0; block_idx < nblocks; block_idx++) {
        /* If an error was encountered by any thread, all threads short-circuit
           to quickly reach the end of the loop. */
        #pragma omp flush (thread_giveup_code)
        if (thread_giveup_code <= 0) { continue; }
        
        const bool leftoverblock = (block_idx == nblocks - 1) && (context->leftover > 0);
        const int32_t bsize = leftoverblock ? context->leftover : context->blocksize;

        int32_t cbytes;
        if (*(context->header_flags) & BLOSC_MEMCPYED) {
          /* We want to memcpy only */
          memcpy(
            context->dest + BLOSC_MAX_OVERHEAD + block_idx * context->blocksize,
            context->src + block_idx * context->blocksize,
            bsize);
          cbytes = bsize;
          
          /* Update counter for this thread */
          ntbytes += cbytes;
        }
        else {
          /* Regular compression */
          cbytes = blosc_c(context, bsize, leftoverblock, 0, ebsize,
                           context->src + block_idx * context->blocksize, tmp2, tmp);
          if (cbytes <= 0) {
            thread_giveup_code = cbytes;    /* uncompressible data or error in blosc_c */
            #pragma omp flush (thread_giveup_code)
          }
          else {
            int32_t ntdest;
          
            /* Begin ordered critical section */
            #pragma omp ordered
            {
              /* TEMP : Debugging/diagnostics */
              const int thread_number = omp_get_thread_num();
              const int threads_in_team = omp_get_num_threads();
              fprintf(stderr, "%d,%d,%d,%d,%d\r\n", thread_number, threads_in_team, block_idx, ntbytes, num_output_bytes);
            
              /* Save the current value of num_output_bytes before modifying it. */
              #pragma omp flush (num_output_bytes)
              ntdest = num_output_bytes;
              
              storeu_int32(context->bstarts + block_idx * sizeof(int32_t), ntdest); /* update block start counter */
              if ( (cbytes == 0) || (ntdest + cbytes > context->destsize) ) {
                thread_giveup_code = 0;  /* uncompressible buffer */
                #pragma omp flush (thread_giveup_code)
                /* 'break' out of the loop by quickly skipping the remaining iterations.
                    Can't use 'continue' here to break out of the critical section, so
                    the control flow below is carefully structured so if an error occured
                    the execution will be the same as if 'continue' was used to skip the
                    rest of the loop body. */
              }
              else {
                num_output_bytes += cbytes;           /* update return bytes counter */
              }
            } /* End of ordered critical section */
            
            /* If no error, copy the compressed buffer to destination. */
            if (thread_giveup_code > 0) {
              memcpy(context->dest + ntdest, tmp2, cbytes);
            }
          } /* cbytes <= 0 */
        } /* flags & BLOSC_MEMCPYED */
      } /* for(block_idx) */
    }
    else {
      /* Decompression can happen using any order.
         For now, we don't specify a particular order, and threads process the blocks
         one-by-one. The original parallel blosc code divided the blocks into contiguous
         segments, each thread processing one contiguous segment. This behavior could
         be restored by calculating the segment size and passing it to the OpenMP schedule directive. */
      #pragma omp parallel for schedule(static)
      for (block_idx = 0; block_idx < nblocks; block_idx++) {
        /* If an error was encountered by any thread, all threads short-circuit
           to quickly reach the end of the loop. */
        #pragma omp flush (thread_giveup_code)
        if (thread_giveup_code <= 0) { continue; }

        const bool leftoverblock = (block_idx == nblocks - 1) && (context->leftover > 0);
        const int32_t bsize = leftoverblock ? context->leftover : context->blocksize;

        int32_t cbytes;
        if (*(context->header_flags) & BLOSC_MEMCPYED) {
          /* We want to memcpy only */
          memcpy(
            context->dest + block_idx * context->blocksize,
            context->src + BLOSC_MAX_OVERHEAD + block_idx * context->blocksize,
            bsize);
          cbytes = bsize;
        }
        else {
          /* Regular decompression */
          cbytes = blosc_d(context, bsize, leftoverblock,
                            context->src + loadu_int32(context->bstarts + block_idx * sizeof(int32_t)),
                            context->dest + block_idx * context->blocksize, tmp, tmp2);
        }

        if (cbytes < 0) {
          thread_giveup_code = cbytes;         /* error in blosc_d */
          #pragma omp flush (thread_giveup_code)
          /* 'break' out of the loop by quickly skipping the remaining iterations */
          continue;
        }
        
        /* Update counter for this thread */
        ntbytes += cbytes;
      } /* for(block_idx) */
    }

    /* Free temporary (de)compression buffers. */
    blosc_free(tmp2);
    blosc_free(tmp);
    
    /* Sum up all the bytes decompressed */
    if ((!context->compress || (*(context->header_flags) & BLOSC_MEMCPYED)) && thread_giveup_code > 0) {
      /* Update global counter for all threads (decompression only) */
      #pragma omp atomic
      num_output_bytes += ntbytes;
    }
  }
  
  /* If thread_giveup_code was set to an error value, return that instead of ntbytes */
  return thread_giveup_code <= 0 ? thread_giveup_code : num_output_bytes;
}


/* Do the compression or decompression of the buffer depending on the
   global params. */
static int do_job(struct blosc_context* const context)
{
  int32_t ntbytes;

  /* Run the serial version when nthreads is 1 or when the buffers are
     not much larger than blocksize */
  assert(context->numthreads > 0);
  const uint8_t thread_count =
    (context->sourcesize <= context->blocksize) ? 1 : context->numthreads;
    
  if (thread_count > 1) {
    ntbytes = openmp_blosc(context, thread_count);
  }
  else {
    ntbytes = serial_blosc(context);
  }

  return ntbytes;
}


int blosc_compress_context(struct blosc_context* const context)
{
  int32_t ntbytes = 0;

  if (!(*(context->header_flags) & BLOSC_MEMCPYED)) {
    /* Do the actual compression */
    ntbytes = do_job(context);
    if (ntbytes < 0) {
      return -1;
    }
    if ((ntbytes == 0) && (context->sourcesize+BLOSC_MAX_OVERHEAD <= context->destsize)) {
      /* Last chance for fitting `src` buffer in `dest`.  Update flags
       and do a memcpy later on. */
      *(context->header_flags) |= BLOSC_MEMCPYED;
    }
  }
  
  if ((*(context->header_flags) & BLOSC_MEMCPYED)) {
    if (context->sourcesize + BLOSC_MAX_OVERHEAD > context->destsize) {
      /* We are exceeding maximum output size */
      ntbytes = 0;
    }
    else if (((context->sourcesize % L1) == 0) || (context->numthreads > 1)) {
      /* More effective with large buffers that are multiples of the
       cache size or multi-cores */
      context->num_output_bytes = BLOSC_MAX_OVERHEAD;
      ntbytes = do_job(context);
      if (ntbytes < 0) {
        return -1;
      }
    }
    else {
      memcpy(context->dest+BLOSC_MAX_OVERHEAD, context->src, context->sourcesize);
      ntbytes = context->sourcesize + BLOSC_MAX_OVERHEAD;
    }
  }

  /* Set the number of compressed bytes in header */
  storeu_int32(context->dest + 12, ntbytes);

  assert(ntbytes <= context->destsize);
  return ntbytes;
}


/* The public routine for compression with context. */
int blosc_compress_ctx(
  const uint8_t clevel,
  const bool doshuffle,
  const size_t typesize,
  const size_t nbytes,
  const uint8_t* const src,
  uint8_t* const dest,
  const size_t destsize,
  const char* const compressor,
  const size_t blocksize,
  const uint8_t numinternalthreads)
{
  int error, result;

  struct blosc_context context;
  error = initialize_context_compression(&context, clevel, doshuffle, typesize, nbytes,
                                  src, dest, destsize, blosc_compname_to_compcode(compressor),
                                  blocksize, numinternalthreads);
  if (error < 0) { return error; }

  error = write_compression_header(&context, clevel, doshuffle);
  if (error < 0) { return error; }

  result = blosc_compress_context(&context);

  return result;
}


int blosc_run_decompression_with_context(
  struct blosc_context* const context,
  const uint8_t* const src,
  uint8_t* const dest,
  const size_t destsize,
  const uint8_t numinternalthreads)
{
  uint8_t version;
  uint8_t versionlz;
  uint32_t ctbytes;
  int32_t ntbytes;

  context->compress = 0;
  context->src = (const uint8_t*)src;
  context->dest = (uint8_t*)dest;
  context->destsize = destsize;
  context->num_output_bytes = 0;
  context->numthreads = numinternalthreads;

  /* Read the header block */
  version = context->src[0];                        /* blosc format version */
  versionlz = context->src[1];                      /* blosclz format version */

  context->header_flags = (uint8_t*)(context->src + 2);           /* flags */
  context->typesize = (int32_t)context->src[3];      /* typesize */
  context->sourcesize = loadu_int32(context->src + 4);     /* buffer size */
  context->blocksize = loadu_int32(context->src + 8);      /* block size */
  ctbytes = loadu_int32(context->src + 12);               /* compressed buffer size */

  /* Unused values */
  version += 0;                             /* shut up compiler warning */
  versionlz += 0;                           /* shut up compiler warning */
  ctbytes += 0;                             /* shut up compiler warning */

  context->bstarts = (uint8_t*)(context->src + 16);
  /* Compute some params */
  /* Total blocks */
  
  ldiv_t blocks_and_leftover = ldiv(context->sourcesize, context->blocksize);
  context->leftover = blocks_and_leftover.rem;
  context->nblocks = (blocks_and_leftover.rem > 0) ? blocks_and_leftover.quot + 1 : blocks_and_leftover.quot;

  /* Check that we have enough space to decompress */
  if (context->sourcesize > (int32_t)destsize) {
    return -1;
  }

  /* Check whether this buffer is memcpy'ed */
  if (*(context->header_flags) & BLOSC_MEMCPYED) {
    if (((context->sourcesize % L1) == 0) || (context->numthreads > 1)) {
      /* More effective with large buffers that are multiples of the
       cache size or multi-cores */
      ntbytes = do_job(context);
      if (ntbytes < 0) {
        return -1;
      }
    }
    else {
      memcpy(dest, (uint8_t *)src+BLOSC_MAX_OVERHEAD, context->sourcesize);
      ntbytes = context->sourcesize;
    }
  }
  else {
    /* Do the actual decompression */
    ntbytes = do_job(context);
    if (ntbytes < 0) {
      return -1;
    }
  }

  assert(ntbytes <= (int32_t)destsize);
  return ntbytes;
}


/* The public routine for decompression with context. */
int blosc_decompress_ctx(
  const void* const src,
  void* const dest,
  const size_t destsize,
	const uint8_t numinternalthreads)
{
  struct blosc_context context;
  return blosc_run_decompression_with_context(&context, src, dest, destsize, numinternalthreads);
}


/* Specific routine optimized for decompression a small number of
   items out of a compressed chunk.  This does not use threads because
   it would affect negatively to performance. */
int blosc_getitem(const uint8_t* const src, const int start, const int nitems, uint8_t* const dest)
{
  uint8_t *_src=NULL;               /* current pos for source buffer */
  uint8_t version, versionlz;       /* versions for compressed header */
  uint8_t flags;                    /* flags for header */
  size_t ntbytes = 0;               /* the number of uncompressed bytes */
  int32_t nblocks;                  /* number of total blocks in buffer */
  ptrdiff_t leftover;               /* extra bytes at end of buffer */
  uint8_t *bstarts;                 /* start pointers for each block */
  int tmp_init = 0;
  int32_t typesize, blocksize, nbytes, ctbytes;
  int32_t j, bsize, bsize2, leftoverblock;
  int32_t cbytes, startb, stopb;
  int stop = start + nitems;
  uint8_t *tmp;
  uint8_t *tmp2;
  int32_t ebsize;

  _src = (uint8_t *)(src);

  /* Read the header block */
  version = _src[0];                        /* blosc format version */
  versionlz = _src[1];                      /* blosclz format version */
  flags = _src[2];                          /* flags */
  typesize = (int32_t)_src[3];              /* typesize */
  nbytes = loadu_int32(_src + 4);                 /* buffer size */
  blocksize = loadu_int32(_src + 8);              /* block size */
  ctbytes = loadu_int32(_src + 12);               /* compressed buffer size */

  ebsize = blocksize + typesize * (int32_t)sizeof(int32_t);
  tmp = blosc_malloc(blocksize);     /* tmp for thread 0 */
  tmp2 = blosc_malloc(ebsize);                /* tmp2 for thread 0 */

  version += 0;                             /* shut up compiler warning */
  versionlz += 0;                           /* shut up compiler warning */
  ctbytes += 0;                             /* shut up compiler warning */

  _src += 16;
  bstarts = _src;
  /* Compute some params */
  /* Total blocks */
  div_t blocks_and_leftover = div(nbytes, blocksize);
  leftover = blocks_and_leftover.rem;
  nblocks = (leftover>0)? blocks_and_leftover.quot+1 : blocks_and_leftover.quot;
  _src += sizeof(int32_t)*nblocks;

  /* Check region boundaries */
  if ((start < 0) || (start*typesize > nbytes)) {
    fprintf(stderr, "`start` out of bounds");
    return -1;
  }

  if ((stop < 0) || (stop*typesize > nbytes)) {
    fprintf(stderr, "`start`+`nitems` out of bounds");
    return -1;
  }

  for (j = 0; j < nblocks; j++) {
    bsize = blocksize;
    leftoverblock = 0;
    if ((j == nblocks - 1) && (leftover > 0)) {
      bsize = leftover;
      leftoverblock = 1;
    }

    /* Compute start & stop for each block */
    startb = start * typesize - j * blocksize;
    stopb = stop * typesize - j * blocksize;
    if ((startb >= (int)blocksize) || (stopb <= 0)) {
      continue;
    }
    if (startb < 0) {
      startb = 0;
    }
    if (stopb > (int)blocksize) {
      stopb = blocksize;
    }
    bsize2 = stopb - startb;

    /* Do the actual data copy */
    if (flags & BLOSC_MEMCPYED) {
      /* We want to memcpy only */
      memcpy((uint8_t *)dest + ntbytes,
          (uint8_t *)src + BLOSC_MAX_OVERHEAD + j*blocksize + startb,
             bsize2);
      cbytes = bsize2;
    }
    else {
      struct blosc_context context;
      /* blosc_d only uses typesize and flags */
      context.typesize = typesize;
      context.header_flags = &flags;

      /* Regular decompression.  Put results in tmp2. */
      cbytes = blosc_d(&context, bsize, leftoverblock,
                       (uint8_t *)src + loadu_int32(bstarts + j * 4),
                       tmp2, tmp, tmp2);
      if (cbytes < 0) {
        ntbytes = cbytes;
        break;
      }
      /* Copy to destination */
      memcpy((uint8_t *)dest + ntbytes, tmp2 + startb, bsize2);
      cbytes = bsize2;
    }
    ntbytes += cbytes;
  }

  blosc_free(tmp);
  blosc_free(tmp2);

  return ntbytes;
}


/*
 * Deprecated functions which use a global blosc_context.
 * It is recommended to move to the new functions which explicitly require
 * a blosc_context be passed in.
 */
 
/* Global context for non-contextual API */
static struct blosc_context* g_global_context;
static pthread_mutex_t global_comp_mutex;
static int32_t g_force_blocksize = 0;
static int32_t g_initlib = 0;


int blosc_set_nthreads(const int nthreads_new)
{
  int ret = g_global_context->numthreads;

  /* Check if should initialize (implementing previous 1.2.3 behaviour,
     where calling blosc_set_nthreads was enough) */
  if (!g_initlib) blosc_init();

  g_global_context->numthreads = nthreads_new;

  return ret;
}


int blosc_set_compressor(const char* const compname)
{
  int code = blosc_compname_to_compcode(compname);

  g_global_context->compcode = code;

  /* Check if should initialize (implementing previous 1.2.3 behaviour,
     where calling blosc_set_nthreads was enough) */
  if (!g_initlib) blosc_init();

  return code;
}


/* Force the use of a specific blocksize.  If 0, an automatic
   blocksize will be used (the default). */
void blosc_set_blocksize(const size_t size)
{
  g_force_blocksize = (int32_t)size;
}


void blosc_init(void)
{
  pthread_mutex_init(&global_comp_mutex, NULL);
  g_global_context = (struct blosc_context*)blosc_malloc(sizeof(struct blosc_context));
  g_global_context->numthreads = 1;
  g_global_context->compcode = BLOSC_BLOSCLZ; /* the compressor to use by default */
  g_initlib = 1;
  
  fprintf(stderr, "ThreadNumber,ThreadsInTeam,BlockNumber,ThreadBytes,TotalBytes\r\n");
}


void blosc_destroy(void)
{
  g_initlib = 0;
  blosc_free(g_global_context);
  pthread_mutex_destroy(&global_comp_mutex);
}


/* The public routine for compression.  See blosc.h for docstrings. */
int blosc_compress(
  const uint8_t clevel,
  const bool doshuffle,
  const size_t typesize,
  const size_t nbytes,
  const void* const src,
  void* const dest,
  const size_t destsize)
{
  int error;
  int result;
  
  if (!g_initlib) blosc_init();

  pthread_mutex_lock(&global_comp_mutex);

  error = initialize_context_compression(g_global_context, clevel, doshuffle, typesize, nbytes,
                                  src, dest, destsize, g_global_context->compcode, g_force_blocksize, g_global_context->numthreads);
  if (error < 0) { return error; }

  error = write_compression_header(g_global_context, clevel, doshuffle);
  if (error < 0) { return error; }

  result = blosc_compress_context(g_global_context);

  pthread_mutex_unlock(&global_comp_mutex);

  return result;
}


/* The public routine for decompression.  See blosc.h for docstrings. */
int blosc_decompress(
  const uint8_t* const src,
  uint8_t* const dest,
  const size_t destsize)
{
  return blosc_run_decompression_with_context(g_global_context, src, dest, destsize, g_global_context->numthreads);
}

int blosc_free_resources()
{
  /* Function doesn't do anything now */
  return 0;
}