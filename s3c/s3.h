/*
 * s3c/s3.h: interface for S3 filesystem
 *
 * Community edition
 *
 * Copyright and bla-bla-bla
 */

#ifndef PROBACKUP_S3_H
#define PROBACKUP_S3_H

/* list of predefined constants */
/* default options */


/* list of error codes */


/* list of typedefs */

/* some S3 flags */
/* I bet we'll need many enums */
/*typedef enum S3_flags {
} S3_flags;*/

/* s3 main configure structure with access parameters */
/* I guess here we store parameters from commandline */
typedef struct S3_config
{
	char *access_key;
	char *secret_access_key;
	char *bucket_name;
	char *region;
	/*
	 * -- bucket size?? protocol put_object supports up to 5GB
	 * -- retry?
	 * -- timeout?
	 * -- backup type? different behavior for FULL and incremental ones
	 * -- what to do in case of error (store in local machine or
	 * drop?)
	 * -- S3 specific conditions (if-modified-since, if-match...)
	 * (do we need them???)
	 * -- some enum flags
	 */
} S3_config;


extern int S3_put_files (parray *files_list , S3_config *config);


#endif /* PROBACKUP_S3_H */
