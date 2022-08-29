/*
 * s3c/s3.c: all functions of S3 filesystem module
 *
 * Community edition
 */


#include <curl/curl.h>
#include <string.h>
#include <time.h>
#include <stdio.h>
#include <ctype.h>
#include <stdarg.h>

/* libs for archiving... */

#include "pg_probackup.h"

#if PG_VERSION_NUM >= 140000
#include "common/hmac.h" /* for hmac-sha256 */
/*TODO !!!fix paths in Makefile and replase it */
#include "../../src/common/sha2_int.h"
#else
#include "common/scram-common.h"
#endif

#include "s3.h"


/* list of defined constants */
/* function error codes? */

#define S3_PUT_SUCCESS 0
#define CURL_INIT_SUCCESS 0
#define CURL_PERFORM_SUCCESS 0
#define ERROR_OPENING_FILE 1
#define ERROR_CURL_EASY_INIT 2
#define ERROR_CURL_HEADERS_APPEND 3
#define ERROR_REDING_FILE 4
#define ERROR_CURL_EASY_PERFORM 5


#define MAX_DATE_HEADER_LEN 35
#define MAX_SIGNED_HEADERS_LEN 900


typedef enum Request_type
{
	PUT,
	GET,
	POST
} Request_type;

/*
// structure for current query params, such as file size (and maybe other settings)
typedef struct Query_params
{
	Request_type	request_type;
	FILE			*current_file;
	char			*filename;
	size_t			content_length;
	struct			tm tm;
	char			*url;

	parray			*headers;
	parray			*contents;
} Query_params;


// S3 special structure that will be inherited from basic Query_params
typedef struct S3_params
{
	Query_params	*params;

	char			content_sha256[PG_SHA256_DIGEST_LENGTH * 2 + 1];
	char			*host;
	parray			*lower_headers;
} S3_params;
*/

/* structure for current query params, such as file size (and maybe other settings) */
typedef struct S3_query_params
{
	Request_type	request_type;
	FILE			*current_file;
	char			*filename;
	size_t			content_length;
	struct			tm tm;
	char			content_sha256[PG_SHA256_DIGEST_LENGTH * 2 + 1]; /* in hexadecimal format */

	char			*host;
	char			*url;
	parray			*headers; /* list of all headers: Host, Date, x-amz-...  */
	parray			*contents; /* list of header contents: url of host, date in http format etc... */
	parray			*lower_headers; /* list of header contents: url of host, date in http format etc... */

	/* according to documentation, signature is valid for 7 days ???? */
	/* https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html */
} S3_query_params;


static void
params_cleanup(S3_query_params *params)
{
	int		i = 0;

	for (i = 0; i < parray_num(params->headers); i++)
	{
		char *elem = (char*)parray_get(params->headers, i);
		elog(LOG, "Header: %s", elem);
		pfree(elem);

		elem = (char*)parray_get(params->contents, i);
		elog(LOG, "Content: %s", elem);
		pfree(elem);

		elem = (char*)parray_get(params->lower_headers, i);
		elog(LOG, "Lower header: %s", elem);
		if (elem) /* NULL for Authorization header */
			pfree(elem);
	}
	parray_free(params->headers);
	parray_free(params->contents);
}


/*
 * concatenates variadic number of strings
 * ATTENTION!!! This function pallocs memory
 */
static char*
concatenate_multiple_strings(int argno, ...)
{
	char		*res = (char*)palloc(1);
	size_t		size = 0;
	size_t		capacity = 1;
	va_list		args;
	int			i = 0;
	char		*str;

	va_start(args, argno);
	res[0] = 0;
	for (i = 0; i < argno; i++)
	{
		str = va_arg(args, char*);
		size += strlen(str);
		if (size >= capacity)
		{
			capacity = size * 2;
			res = (char*)repalloc(res, capacity);
		}

		strcat(res, str);
	}
	va_end(args);

	res = (char*)repalloc(res, size + 1);
	res[size] = 0;
	return res;
}


/* Thu, 11 Aug 2022 09:07:00 GMT+04:00 */
/* https://stackoverflow.com/questions/7548759/generate-a-date-string-in-http-response-date-format-in-c */
/* ЭТОТ ГАД ВЫДАЕТ ВРЕМЯ ПО ГРИНВИЧУ !!!!!!! */
static void
S3_get_date_for_header(char **out, S3_query_params *params)
{
	*out = (char*)palloc(MAX_DATE_HEADER_LEN + 1);

	memset(*out, 0, MAX_DATE_HEADER_LEN + 1);
	strftime(*out, MAX_DATE_HEADER_LEN + 1, "%a, %d %b %Y %H:%M:%S %Z", &(params->tm));
	elog(LOG, "Time is: %s\n", *out);
	*out = (char*)repalloc(*out, strlen(*out) + 1);
}

/* for make headers lowercase */
static void
to_lowecase(char* dst, const char *src)
{
	/* len(dst) >= len(src) */
	int		i = 0;

	while (src[i])
	{
		dst[i] = tolower(src[i]);
		i++;
	}
	dst[i] = 0;
}

static void
translate_checksum_to_hexadecimal(char *dst, char *src)
{
	/* size of dst >= PG_SHA256_DIGEST_LENGTH * 2 + 1 */
	int		i = 0;

	for (i = 0; i < PG_SHA256_DIGEST_LENGTH; i++)
	{
		char ch = src[i];
		sprintf(dst + i * 2, "%x\n", (ch >> 4) & 0x0f); /* first 4 bits */
		sprintf(dst + i * 2 + 1,"%x\n", ch & 0x0f); /* second 4 bits */
	}
	dst[PG_SHA256_DIGEST_LENGTH * 2] = 0;
}

static void
S3_get_SHA256(char *out, const char *data, size_t len)
{
	/* size of out >= PG_SHA256_DIGEST_LENGTH + 1 */

#if PG_VERSION_NUM < 140000
	pg_sha256_ctx			sha256_ctx;
	pg_sha256_init(&sha256_ctx);
	pg_sha256_update(&sha256_ctx, (uint8 *)data, len);
	pg_sha256_final(&sha256_ctx, (uint8 *)out);
#else
	pg_cryptohash_ctx		*ctx = pg_cryptohash_create(PG_SHA256);
	pg_cryptohash_init(ctx);
	pg_cryptohash_update(ctx, (uint8 *)data, len);
	pg_cryptohash_final(ctx, (uint8 *)out, PG_SHA256_DIGEST_LENGTH);
#endif
	out[PG_SHA256_DIGEST_LENGTH] = 0;
}


static void
S3_get_HMAC_SHA256(char *out, const char *key, size_t keylen, const char *data, size_t datalen)
{
	/* size of out >= PG_SHA256_DIGEST_LENGTH + 1 */
#if PG_VERSION_NUM < 140000
	scram_HMAC_ctx		hmac_ctx;
	scram_HMAC_init(&hmac_ctx, (uint8 *)key, keylen);
	scram_HMAC_update(&hmac_ctx, data, datalen);
	scram_HMAC_final((uint8 *)out, &hmac_ctx);
#else
	pg_hmac_ctx 		*hmac_ctx = pg_hmac_create(PG_SHA256);
	pg_hmac_init(hmac_ctx, (uint8 *)key, keylen);
	pg_hmac_update(hmac_ctx,  (uint8 *)data, datalen);
	pg_hmac_final(hmac_ctx, (uint8 *)out, PG_SHA256_DIGEST_LENGTH);
#endif
	out[PG_SHA256_DIGEST_LENGTH] = 0;
}


static void
binary_hmac_sha256(char *out, const char *key, size_t keylen, const char *data, size_t datalen)
{
	/* size of out >= PG_SHA256_DIGEST_LENGTH * 2 + 1 */
	char		hmac_buffer[PG_SHA256_DIGEST_LENGTH + 1];
	int			i = 0;

	S3_get_HMAC_SHA256(hmac_buffer, key, keylen, data, datalen);

	for (i = 0; i < PG_SHA256_DIGEST_LENGTH; i++)
	{
		char ch = hmac_buffer[i];
		out[i * 2] = (ch >> 4) & 0x0f;
		out[i * 2 + 1] = ch & 0x0f;
	}
	out[PG_SHA256_DIGEST_LENGTH * 2] = 0;
}



static char*
S3_create_string_to_sign(const char *scope, const char *canonical_request, S3_query_params *params)
{
	char		*retptr = NULL;
	/* 20150915T124500Z */
	char		time_buf[18];
	char		checksum[PG_SHA256_DIGEST_LENGTH + 1];
	char		hex_checksum[PG_SHA256_DIGEST_LENGTH * 2 + 1];

	strftime(time_buf, 17, "%Y%m%dT%H%M%SZ", &(params->tm));
	time_buf[17] = 0;

	S3_get_SHA256(checksum, canonical_request, strlen(canonical_request));
	translate_checksum_to_hexadecimal(hex_checksum, checksum);

	retptr = concatenate_multiple_strings(6, "AWS4-HMAC-SHA256\n", time_buf, "\n", scope, "\n", hex_checksum);
	elog(LOG, "String to sign: %s", retptr);

	return retptr;
}


static void
S3_get_signed_headers(char *out, S3_query_params *params)
{
	int		i = 0;

	/* out is a buffer with enough free space */
	out[0] = 0;
	params->lower_headers = parray_new();
	for (i = 0; i < parray_num(params->headers); i++)
	{
		char *elem = (char*)parray_get(params->headers, i);
		char *header_lowercase = (char*)palloc((strlen(elem) + 1));
		to_lowecase(header_lowercase, elem);
		header_lowercase[strlen(elem)] = 0;
		parray_append(params->lower_headers, header_lowercase);

		out = strcat(out, header_lowercase);
		out = strcat(out, ";");
	}

	out[strlen(out)-1] = 0; /* remove finishing ';' */
}


static char*
S3_get_canonical_headers(S3_query_params *params)
{
	size_t		size = 0;
	size_t		capacity = 1;
	int			i = 0;
	char		*res = (char*)palloc(1);

	res[0] = 0;
	for (i = 0; i < parray_num(params->headers); i++)
	{
		char *header_lowercase = (char*)parray_get(params->lower_headers, i);
		/* TODO: Trim function -- delete trailing whitespaces, flatten many spaces to 1 */
		/* Trim(content) */
		char *content = (char*)parray_get(params->contents, i);

		size += strlen(header_lowercase) + strlen(content) + 2;
		if (size >= capacity)
		{
			capacity = size * 2;
			res = (char*)repalloc(res, capacity);
		}

		res = strcat(res, header_lowercase);
		res = strcat(res, ":");
		res = strcat(res, content);
		res = strcat(res, "\n");
	}
	res = repalloc(res, size + 1);

	return res;
}


/*
 * Returns index of first symbol after canonical url
 * (for future extracting of canonical query string)
 */
static size_t
get_canonical_url(char **out, char *url)
{
	size_t		size = 0;
	size_t		capacity = 1;

	*out = (char*)palloc(1);
	while(*url)
	{
		if (url[0] == '?')
			break;

		size += 1;
		if (size >= capacity)
		{
			capacity = size * 2;
			*out = (char*)repalloc(*out, capacity);
		}

		(*out)[size-1] = url[0];

		url += 1;
	}

	*out = (char*)repalloc((*out), size + 1);
	(*out)[size] = 0;
	elog(LOG, "Canonical URL: %s", *out);

	return size;
}


static char*
S3_create_canonical_request(const char *signed_headers, S3_query_params *params)
{
	char		*retptr = NULL;
	char		*canonical_headers = NULL;
	char		query[6];
	/*
	 * canonical_url is a part of url starting with the "/" that follows
	 * the domain name and up to the end of the string or to the '?' sign
	 */
	char		*canonical_url = NULL;
	size_t		query_begins = 0;
	/* query string is a part of url that follows '?' sign, excluding the '?' */
	char		*canonical_query_string = NULL;
	size_t		canonical_qs_size = 0;


	memset(query, 0, 5);

	/* realization only for PutObject now */
	switch(params->request_type)
	{
		case PUT:
			strcpy(query, "PUT");
			break;
		case GET:
			strcpy(query, "GET");
			break;
		case POST:
			strcpy(query, "POST");
			break;
	}

	query_begins = get_canonical_url(&canonical_url, params->url + strlen(params->host));
	canonical_qs_size = strlen(params->url + strlen(params->host) + query_begins);
	if (canonical_qs_size == 0)
	{
		canonical_query_string = (char*)palloc(1);
		canonical_query_string[0] = 0;
	}
	else
	{
		canonical_query_string = (char*)palloc(canonical_qs_size);
		strcpy(canonical_query_string, params->url + query_begins);
	}
	elog(LOG, "Canonical query string: %s", canonical_query_string);

	/* canonical headers divided by \n */
	canonical_headers = S3_get_canonical_headers(params);
	elog(LOG, "Canonical headers: %s", canonical_headers);
	/* canonical headers finished */

	retptr = concatenate_multiple_strings(11, query, "\n", canonical_url, "\n", canonical_query_string, "\n",
										  canonical_headers, "\n", signed_headers, "\n", params->content_sha256);

	/* cleanup */
	pfree(canonical_headers);
	pfree(canonical_query_string);

	elog(LOG, "Canonical request: %s", retptr);

	return retptr;
}


static void
get_content_sha256(S3_query_params *params)
{
	char		*buf = (char*)palloc(params->content_length + 1);
	char		hashed_payload[PG_SHA256_DIGEST_LENGTH + 1];
	char		hex_hashed_payload[PG_SHA256_DIGEST_LENGTH * 2 + 1];
	size_t		read_bytes = 0;

	/* read all file content to buf */
	read_bytes = fread(buf, sizeof(char), params->content_length, params->current_file);
	if (read_bytes != params->content_length)
	{
		pfree(buf);
		elog(ERROR, "error reading file: %s", params->filename); /* TODO: format output */
	}
	S3_get_SHA256(hashed_payload, buf, read_bytes);
	translate_checksum_to_hexadecimal(hex_hashed_payload, hashed_payload);
	/* set field content_sha256 in params to re-use in header x-amz-checksum-sha256 */
	memcpy(params->content_sha256, hex_hashed_payload, PG_SHA256_DIGEST_LENGTH * 2);
	params->content_sha256[PG_SHA256_DIGEST_LENGTH * 2] = 0;
	pfree(buf);
}


/*
 * S3_get_authorization_string
 *
 * Algorhytm: AWS4-HMAC-SHA256
 * https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html
 */
static char*
S3_get_authorization_string(S3_query_params *params, S3_config *config)
{
	char		*retptr = NULL;
	char		date_ptr[10];
	char		*credential_scope = NULL;
	char		*canonical_request = NULL;
	char		*string_to_sign = NULL;
	char		*buf = NULL;
	char		signed_headers[MAX_SIGNED_HEADERS_LEN + 1];
	char		hmac_buffer[PG_SHA256_DIGEST_LENGTH * 2 + 1];
	char		signing_key[PG_SHA256_DIGEST_LENGTH * 2 + 1];
	char		signature[PG_SHA256_DIGEST_LENGTH * 2 + 1];

	char		*tmp = NULL;

	retptr = concatenate_multiple_strings(1, "AWS4-HMAC-SHA256 ");

	/* Credential */
	date_ptr[9] = 0;
	strftime(date_ptr,9, "%Y%m%d", &(params->tm));

	/* create credential scope */
	credential_scope = concatenate_multiple_strings(4, date_ptr, "/", config->region, "/s3/aws4_request");

	tmp = retptr;
	retptr = concatenate_multiple_strings(6, retptr, "Credential=", config->access_key, "/", credential_scope, ", ");
	pfree(tmp);
	/* Credential finished */

	/* SignedHeaders */
	S3_get_signed_headers(signed_headers, params);

	tmp = retptr;
	retptr = concatenate_multiple_strings(4, retptr, "SignedHeaders=", signed_headers, ", ");
	pfree(tmp);
	/* SignedHeaders finished */

	/* signature */
	/* let's count AWS4-HMAC-SHA256 params */

	/* create canonical request */
	canonical_request = S3_create_canonical_request(signed_headers, params);
	/* create string-to-sign */
	string_to_sign = S3_create_string_to_sign(credential_scope, canonical_request, params);

	/* calculate params */
	/* signing key = HMAC(HMAC(HMAC(HMAC("AWS4" + kSecret,"20150830"),"us-east-1"),"iam"),"aws4_request") -- binary (numbers) of size 64 */
	/* signature = HexEncode(HMAC(derived signing key, string to sign)) -- hexadecimal (symbols) of size 64 */
	buf = concatenate_multiple_strings(2, "AWS4", config->secret_access_key);

	binary_hmac_sha256(hmac_buffer, buf, strlen(buf), date_ptr, strlen(date_ptr)); /* kDate = HMAC("AWS4" + kSecret,"20150830") */
	binary_hmac_sha256(hmac_buffer, hmac_buffer, PG_SHA256_DIGEST_LENGTH * 2, config->region, strlen(config->region)); /* kRegion = HMAC(kDate, Region) */
	binary_hmac_sha256(hmac_buffer, hmac_buffer, PG_SHA256_DIGEST_LENGTH * 2, "s3", 2); /* kService = HMAC(kRegion, Service) */
	binary_hmac_sha256(signing_key, hmac_buffer, PG_SHA256_DIGEST_LENGTH * 2, "aws4_request", 12); /* kSigning = HMAC(kService, "aws4_request") */
	/* we got the signing key */

	/* final step to obtain a signature */
	S3_get_HMAC_SHA256(hmac_buffer, signing_key, PG_SHA256_DIGEST_LENGTH * 2, string_to_sign, strlen(string_to_sign));
	translate_checksum_to_hexadecimal(signature, hmac_buffer);

	tmp = retptr;
	retptr = concatenate_multiple_strings(3, retptr, "Signature=", signature);
	pfree(tmp);
	/* params finished */

	/* cleanup */
	pfree(credential_scope);
	pfree(canonical_request);
	pfree(string_to_sign);
	pfree(buf);
	return retptr;
}

static void
headers_append(struct curl_slist **headers, const char *header, const char *content)
{
	/* +2 for ": ", +1 for finishing 0 */
	char				*header_string = concatenate_multiple_strings(3, header, ": ", content);
	struct curl_slist	*temp = NULL;

	/* curl_slist_append COPIES the string */
	temp = curl_slist_append((*headers), header_string);
	if (!temp)
		elog(ERROR, "Error in curl_slist_append. Aborting");
	
	(*headers) = temp;
	pfree(header_string);
}


/* In future it will be HOOK function */
/* Initialize S3 specific headers: x-amz-content-sha256, maybe x-amz-acl */
static void
S3_headers_init(S3_query_params *params, S3_config *config)
{
	char		*tmp_str = NULL;

	/* header: x-amz-content-sha256 */
	/* Calculate content hash */
	/* Required */
	/* 24022437d7c046b585290c87a37cad2b88ff9d4bf55174f40f1ccb96f664aa38 */
	tmp_str = (char*)palloc(21);
	stpcpy(tmp_str, "x-amz-content-sha256");
	tmp_str[21] = 0;
	parray_append(params->headers, tmp_str);
	tmp_str = (char*)palloc(PG_SHA256_DIGEST_LENGTH * 2 + 1);
	tmp_str[PG_SHA256_DIGEST_LENGTH * 2] = 0;
	get_content_sha256(params);
	sprintf(tmp_str, "%s", params->content_sha256);
	parray_append(params->contents, tmp_str);
	tmp_str = NULL;
}


/* HOOK function for creating S3 url */
static void
S3_create_url(S3_query_params *params, S3_config *config)
{
	char		*host = NULL;
	char		*url = NULL;

	host = concatenate_multiple_strings(5, "http://", config->bucket_name, ".s3.", config->region, ".s3.amazonaws.com");
	url = concatenate_multiple_strings(3, host, "/", params->filename);

	elog(LOG, "host: %s", host);
	elog(LOG, "url: %s", url);

	params->url = url;
	params->host = host;
}


/*
 * headers_init
 * Initialize curl variables and http headers.
 * Initialize headers common to all clouds: Host(url), Authorization, Date, Content-Length, ?Content-Type?
 */

/* ONLY FOR S3 PUT OBJECT NOW */
/* Initialize put_object REST/S3 operation */
static void
headers_init(CURL *curl, struct curl_slist **headers, S3_query_params *params, S3_config *config)
{
	char		*authorization_string = NULL;
	time_t		t = time(0);
	struct		tm tm = *gmtime(&t);
	int			i = 0;

	char		*tmp_str = NULL;

	params->tm = tm;

	elog(LOG, "Starting headers_init function");

	switch (params->request_type)
	{
		case PUT:
			curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
			break;

		case GET:
			curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
			break;

		/* https://curl.se/libcurl/c/CURLOPT_HTTPPOST.html */
		case POST:
			curl_easy_setopt(curl, CURLOPT_HTTPPOST, 1L);
			break;
	}

	/* TODO: https protocol */
	curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);

	params->headers = parray_new();
	params->contents = parray_new();

	params->tm = *gmtime(&t);;

	/*
	 * First set the URL that is about to receive our POST. This URL can
	 * just as well be a https:// URL if that is what should receive the
	 * data.
	 */

	/* Host: http://myBucket.s3.myRegion.amazonaws.com */
	/* Full URL: http://s3.amazonaws.com/examplebucket/myphoto.jpg */
	/* S3 url style: Virtual host */
	/* https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAPI.html */

	/* create url, save to params */
	S3_create_url(params, config);
	curl_easy_setopt(curl, CURLOPT_URL, params->url);

	/* Curl automatically provides header Host, based on url */
	/* We can replace it if we will */

	/*header: Date */
	tmp_str = (char*)palloc(5);
	stpcpy(tmp_str, "Date");
	tmp_str[4] = 0;
	parray_append(params->headers, tmp_str);
	tmp_str = NULL;
	S3_get_date_for_header(&tmp_str, params);
	parray_append(params->contents, tmp_str);
	tmp_str = NULL;

	/* header: Expect ???????? */

	/* some optional headers to append */
	/* Content-type ? */
	/* x-amz-meta-author ??? */

	/* header: Content-Length */
	tmp_str = (char*)palloc(15);
	stpcpy(tmp_str, "Content-Length");
	tmp_str[14] = 0;
	parray_append(params->headers, tmp_str);
	tmp_str = (char*)palloc(100);
	memset(tmp_str, 0, 100);
	sprintf(tmp_str, "%lu", params->content_length);
	tmp_str = repalloc(tmp_str, strlen(tmp_str) + 1);
	parray_append(params->contents, tmp_str);
	tmp_str = NULL;

	/* initialize all other headers before computing authorization string */
	S3_headers_init(params, config);

	/*header: Authorization*/
	/*
	 * MUST BE CALCULATED AFTER ALL HEADERS ARE SET
	 * because we calculate params with used headers and canonical request
	 */

	tmp_str = (char*)palloc(14);
	stpcpy(tmp_str, "Authorization");
	tmp_str[13] = 0;
	authorization_string = S3_get_authorization_string(params, config);
	parray_append(params->headers, tmp_str);
	parray_append(params->contents, authorization_string);
	parray_append(params->lower_headers, NULL);
	tmp_str = NULL;


	/* ??? Shall we add basic headers like Host, Date, Authorization before others ??? */

	/* now pass all headers to curl */
	for (i = 0; i < parray_num(params->headers); i++)
	{
		char *header = (char*)parray_get(params->headers, i);
		char *content = (char*)parray_get(params->contents, i);

		headers_append(headers, header, content);
	}

	/* cleanup */
	params_cleanup(params);
}



static int
put_object(FILE *fd, S3_query_params *params, S3_config *config)
{
	CURL				*curl;
	CURLcode			res;
	struct curl_slist	*headers = NULL;
	/*long http_response_code;*/

	curl = curl_easy_init();

	if (!curl)
		return ERROR_CURL_EASY_INIT;

	headers_init(curl, &headers, params, config);
	/* Сразу всей пачкой, https://curl.se/libcurl/c/CURLOPT_HTTPHEADER.html*/
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	curl_easy_setopt(curl, CURLOPT_READDATA, fd); // set file we are about to put


	/* output of all params */

	/* Perform the request, res will get the return code */
	/*res = curl_easy_perform(curl);*/

	/* check for errors*/
	if (res != CURLE_OK)
	{
		elog(LOG, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
		return ERROR_CURL_EASY_PERFORM;
	}
	/* or */
	/*curl_easy_getinfo (curl, CURLINFO_RESPONSE_CODE, &http_response_code);
	if (http_response_code == 200 && res != CURLE_ABORTED_BY_CALLBACK)
	{  success }
	else
	{ display return code, exit with error }*/

	/* RETRY ????*/

	/* cleanup */
	curl_easy_cleanup(curl);
	curl_slist_free_all(headers);
	return CURL_PERFORM_SUCCESS;
}


/*
 * S3_put_files
 *
 * Prepares files for sending, calls for put_object
 */
int
S3_put_files(parray *files_list/* global backup_files_list from backup.c */, S3_config *config)
{
	int		i = 0;

	elog(LOG, "This is S3_put_object function");

	/* initializing something */

	/* In windows, this will init the winsock stuff */
	curl_global_init(CURL_GLOBAL_ALL);

	/* Code for sending files */

	/* for proper cycle ask Daria */
	for (i = 0; i < parray_num(files_list); i++)
	{ /* cycle start*/
		pgFile				*elem = (pgFile*)parray_get(files_list, i);
		S3_query_params		*params = NULL;
		FILE				*fd;
		int					err = 0;

		elog(LOG, "Processing file: %s", elem->name);
		fd = fopen(elem->name, "rb");

		if (!fd)
			return ERROR_OPENING_FILE;

		params = (S3_query_params*)palloc(sizeof(S3_query_params));
		params->request_type = PUT;
		params->filename = elem->name;
		params->current_file = fd;

		params->content_length = elem->size;

		err = put_object(fd, params, config);
		if (err)
			return err;

		pfree(params);
	} /*cycle end*/

	curl_global_cleanup();
	return S3_PUT_SUCCESS; /* 0 */
}
