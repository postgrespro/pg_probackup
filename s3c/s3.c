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

/* ?!?!?!?! где взять эту хрень */
#include "utils/file.h"


/* list of defined constants */
/* function error codes? */

#define S3_SUCCESS 0
/*#define CURL_INIT_SUCCESS 0
#define CURL_PERFORM_SUCCESS 0
#define ERROR_OPENING_FILE 1*/
#define ERROR_CURL_EASY_INIT 2
/*#define ERROR_CURL_HEADERS_APPEND 3
#define ERROR_REDING_FILE 4*/
#define ERROR_CURL_EASY_PERFORM 5 


#define MAX_DATE_HEADER_LEN 35
#define MAX_SIGNED_HEADERS_LEN 900

#define S3_CHUNK_SIZE 50 * 1024 * 1024 /* 50 MB */

fobj_error_cstr_key(curlError);



S3_config *config; /* global config for AWS/VK S3 */

typedef enum Request_type
{
	PUT,
	GET,
	POST
} Request_type;


typedef struct pioCloudFile
{
	const char		*path;

	/* buffer for gathering file to write in WriteFlush */
	char			*filebuf;
	size_t			buflen;
	size_t			capacity;

	/* for pioRead */
	size_t			current_pos;

	S3_config		*config;
} pioCloudFile;

#define kls__pioCloudFile	mth(pioRead, pioWrite, pioFlush)
fobj_klass(pioCloudFile);


typedef struct pioCloudDrive
{
} pioCloudDrive;

/* ??????? */
#define kls__pioCloudDrive	iface__pioDrive, iface(pioDrive)
fobj_klass(pioCloudDrive);

/* structure for current query params, such as file size (and maybe other settings) */
typedef struct S3_query_params
{
	Request_type	request_type;
	const char		*filename;
	struct			tm tm;
	char			content_sha256[PG_SHA256_DIGEST_LENGTH * 2 + 1]; /* in hexadecimal format */
	char			*buf;
	size_t			start_pos; /* != 0 only for reading */
	size_t			content_length;

	char			*protocol;
	char			*host;
	/* !!! query_string == Canonical query string, please !!! */
	/* p.3 in https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html */
	char			*query_string; /* do not transfer auth parameters in query string, they are in Authorization header */
	char			*url;
	char			*canonical_url;
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
		free(elem);

		elem = (char*)parray_get(params->contents, i);
		elog(LOG, "Content: %s", elem);
		free(elem);

		elem = (char*)parray_get(params->lower_headers, i);
		elog(LOG, "Lower header: %s", elem);
		if (elem) /* NULL for Authorization header */
			free(elem);
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
	char		*res = (char*)pgut_malloc(1);
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
			res = (char*)pgut_realloc(res, capacity);
		}

		strcat(res, str);
	}
	va_end(args);

	res = (char*)pgut_realloc(res, size + 1);
	res[size] = 0;
	return res;
}


/* Thu, 11 Aug 2022 09:07:00 GMT+04:00 */
/* https://stackoverflow.com/questions/7548759/generate-a-date-string-in-http-response-date-format-in-c */
/* ЭТОТ ГАД ВЫДАЕТ ВРЕМЯ ПО ГРИНВИЧУ !!!!!!! */
static void
S3_get_date_for_header(char **out, S3_query_params *params)
{
	*out = (char*)pgut_malloc0(MAX_DATE_HEADER_LEN + 1);

	/*strftime(*out, MAX_DATE_HEADER_LEN + 1, "%a, %d %b %Y %H:%M:%S %Z", &(params->tm));*/
	strftime(*out, 17, "%Y%m%dT%H%M%SZ", &(params->tm));
	elog(LOG, "Time is: %s\n", *out);
	*out = (char*)pgut_realloc(*out, strlen(*out) + 1);
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
		sprintf(dst + i * 2, "%x", (ch >> 4) & 0x0f); /* first 4 bits */
		sprintf(dst + i * 2 + 1,"%x", ch & 0x0f); /* second 4 bits */
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
		char *header_lowercase = (char*)pgut_malloc((strlen(elem) + 1));
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
	char		*res = (char*)pgut_malloc(1);

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
			res = (char*)pgut_realloc(res, capacity);
		}

		res = strcat(res, header_lowercase);
		res = strcat(res, ":");
		res = strcat(res, content);
		res = strcat(res, "\n");
	}
	res = pgut_realloc(res, size + 1);

	return res;
}


/*static void
get_canonical_url(char **out, char *url)
{
	size_t		size = 0;
	size_t		capacity = 1;

	*out = (char*)pgut_malloc(1);
	while(*url)
	{
		if (url[0] == '?')
			break;

		size += 1;
		if (size >= capacity)
		{
			capacity = size * 2;
			*out = (char*)pgut_realloc(*out, capacity);
		}

		(*out)[size-1] = url[0];

		url += 1;
	}

	*out = (char*)pgut_realloc((*out), size + 1);
	(*out)[size] = 0;
	elog(LOG, "Canonical URL: %s", *out);
}*/


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
	/*char		*canonical_url = NULL;*/
	/* query string is a part of url that follows '?' sign, excluding the '?' */
	char		*canonical_query_string = NULL;


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

	/*get_canonical_url(&canonical_url, params->url + strlen(params->protocol) + strlen(params->host));*/
	if (params->query_string) /* no query string in request */
	{
		canonical_query_string = (char*)pgut_malloc(strlen(params->query_string));
		strcpy(canonical_query_string, params->query_string + 1);
	}
	else
	{
		canonical_query_string = (char*)pgut_malloc(1);
		canonical_query_string[0] = 0;
	}
	elog(LOG, "Canonical query string: %s", canonical_query_string);

	/* canonical headers divided by \n */
	canonical_headers = S3_get_canonical_headers(params);
	elog(LOG, "Canonical headers: %s", canonical_headers);
	/* canonical headers finished */

	retptr = concatenate_multiple_strings(11, query, "\n", params->canonical_url, "\n", canonical_query_string, "\n",
										  canonical_headers, "\n", signed_headers, "\n", params->content_sha256);

	/* cleanup */
	free(canonical_headers);
	free(canonical_query_string);

	elog(LOG, "Canonical request: %s", retptr);

	return retptr;
}


static void
get_content_sha256(S3_query_params *params)
{
	char		hashed_payload[PG_SHA256_DIGEST_LENGTH + 1];
	char		hex_hashed_payload[PG_SHA256_DIGEST_LENGTH * 2 + 1];

	if ((params->request_type == PUT) && (params->content_length > 0))
		S3_get_SHA256(hashed_payload, params->buf, params->content_length);
	else
		S3_get_SHA256(hashed_payload, "", 0);

	translate_checksum_to_hexadecimal(hex_hashed_payload, hashed_payload);
	elog(LOG, "hex_hashed_payload: %s", hex_hashed_payload);
	/* set field content_sha256 in params to re-use in header x-amz-checksum-sha256 */
	memcpy(params->content_sha256, hex_hashed_payload, PG_SHA256_DIGEST_LENGTH * 2);
	params->content_sha256[PG_SHA256_DIGEST_LENGTH * 2] = 0;
}


static void
binary_print(char *buf)
{
	/* sizeof buf >= PG_SHA256_DIGEST_LENGTH */
	int			i = 0;
	char		strbuf[PG_SHA256_DIGEST_LENGTH * 2 + 1];

	for (i = 0; i < PG_SHA256_DIGEST_LENGTH; i++)
	{
		char ch = buf[i];
		sprintf(strbuf + i * 2, "%x", (ch >> 4) & 0x0f); /* first 4 bits */
		sprintf(strbuf + i * 2 + 1,"%x", ch & 0x0f); /* second 4 bits */
	}

	elog(LOG, "binary_print: %s", strbuf);
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
	char		hmac_buffer[PG_SHA256_DIGEST_LENGTH + 1];
	char		signing_key[PG_SHA256_DIGEST_LENGTH + 1];
	char		signature[PG_SHA256_DIGEST_LENGTH * 2 + 1];

	char		*tmp = NULL;

	retptr = concatenate_multiple_strings(1, "AWS4-HMAC-SHA256 ");

	/* Credential */
	date_ptr[9] = 0;
	strftime(date_ptr, 9, "%Y%m%d", &(params->tm));
	//strcpy(date_ptr, "20150830");

	/* create credential scope */
	credential_scope = concatenate_multiple_strings(4, date_ptr, "/", config->region, "/s3/aws4_request");

	tmp = retptr;
	retptr = concatenate_multiple_strings(6, retptr, "Credential=", config->access_key, "/", credential_scope, ", ");
	free(tmp);
	/* Credential finished */

	/* SignedHeaders */
	S3_get_signed_headers(signed_headers, params);

	tmp = retptr;
	retptr = concatenate_multiple_strings(4, retptr, "SignedHeaders=", signed_headers, ", ");
	free(tmp);
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
	//buf = concatenate_multiple_strings(2, "AWS4", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY");

	S3_get_HMAC_SHA256(hmac_buffer, buf, strlen(buf), date_ptr, strlen(date_ptr)); /* kDate = HMAC("AWS4" + kSecret,"20150830") */
	/*elog(LOG, "kDate");
	binary_print(hmac_buffer);*/
	S3_get_HMAC_SHA256(hmac_buffer, hmac_buffer, PG_SHA256_DIGEST_LENGTH, config->region, strlen(config->region)); /* kRegion = HMAC(kDate, Region) */
	/*elog(LOG, "kRegion");
	binary_print(hmac_buffer);*/
	S3_get_HMAC_SHA256(hmac_buffer, hmac_buffer, PG_SHA256_DIGEST_LENGTH, "s3", 2); /* kService = HMAC(kRegion, Service) */
	/*elog(LOG, "kService");
	binary_print(hmac_buffer);*/
	S3_get_HMAC_SHA256(signing_key, hmac_buffer, PG_SHA256_DIGEST_LENGTH, "aws4_request", 12); /* kSigning = HMAC(kService, "aws4_request") */
	/*elog(LOG, "signing_key");
	binary_print(hmac_buffer);*/
	/* we got the signing key */

	/* final step to obtain a signature */
	//char		string_to_sign[] = "AWS4-HMAC-SHA256\n20150830T123600Z\n20150830/us-east-1/iam/aws4_request\nf536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59";
	//S3_get_HMAC_SHA256(hmac_buffer, signing_key, PG_SHA256_DIGEST_LENGTH * 2, string_to_sign, strlen(string_to_sign));
	S3_get_HMAC_SHA256(hmac_buffer, signing_key, PG_SHA256_DIGEST_LENGTH, string_to_sign, strlen(string_to_sign));
	elog(LOG, "signature");
	binary_print(hmac_buffer);

	translate_checksum_to_hexadecimal(signature, hmac_buffer);
	elog(LOG, "final signature: %s", signature);

	tmp = retptr;
	retptr = concatenate_multiple_strings(3, retptr, "Signature=", signature);
	free(tmp);
	/* params finished */

	/* cleanup */
	free(credential_scope);
	free(canonical_request);
	free(string_to_sign);
	free(buf);
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

	elog(LOG, "headers_append header_string: %s", header_string);
	(*headers) = temp;
	free(header_string);
}


/* In future it will be HOOK function */
/* Initialize S3 specific headers: x-amz-content-sha256, maybe x-amz-acl */
static void
S3_headers_init(S3_query_params *params, S3_config *config)
{
	char		*tmp_str = NULL;

	/* we must calculate content SHA256 for future Authorization header */
	get_content_sha256(params);

	/* header: x-amz-content-sha256 */
	/* Calculate content hash */
	/* Required for PUT */
	if ((params->request_type == PUT) && (params->content_length > 0))
	{
		tmp_str = (char*)pgut_malloc(21);
		stpcpy(tmp_str, "x-amz-content-sha256");
		tmp_str[20] = 0;
		parray_append(params->headers, tmp_str);
		tmp_str = (char*)pgut_malloc(PG_SHA256_DIGEST_LENGTH * 2 + 1);
		tmp_str[PG_SHA256_DIGEST_LENGTH * 2] = 0;
		sprintf(tmp_str, "%s", params->content_sha256);
		parray_append(params->contents, tmp_str);
		tmp_str = NULL;
	}
}


/* HOOK function for creating S3 url */
/*  */
static void
S3_create_url(S3_query_params *params, S3_config *config)
{
	char		*host = NULL;
	char		*url = NULL;
	size_t		len;

	/* SWITCH for vault type (AWS, VK, Minio) */
	/* This is Minio variant */
	if (!config->endpoint_url) /* by default we call to Amazon S3 service */
	{
		ft_assert(config->bucket_name && config->region);
		host = concatenate_multiple_strings(4, config->bucket_name, ".s3.", config->region, ".s3.amazonaws.com");
	}
	else /* user's service (like minio or !!! VK !!!) */
	/* !!!!!!!!!!! ВОТ ЗДЕСЬ НАПИСАНА ХРЕНЬ !!!!!!!!!! */
	{
		host = concatenate_multiple_strings(1, config->endpoint_url);
		/*host = concatenate_multiple_strings(3, "http://", config->bucket_name, ".", config->endpoint_url);*/
	}

	/* !!!!! */
	/* MINIO variant */
	/* !!!!! Важно помнить, что у нас не всегда есть даже bucket_name. В простейшем случае у нас только ключи */
	url = concatenate_multiple_strings(4, params->protocol, host, "/", config->bucket_name);
	/* TODO: AWS and VK variants */
	len = strlen(params->protocol) + strlen(host) + 2;
	if (params->filename)
	{
		len += strlen(params->filename) + 1;
		url = (char*)pgut_realloc(url, len);
		strcat(url, "/");
		strcat(url, params->filename);
	}

	params->canonical_url = (char*)pgut_malloc0(strlen(url) - strlen(host) - strlen(params->protocol) + 1);
	strcpy(params->canonical_url, url + strlen(host) + strlen(params->protocol));

	if (params->query_string)
	{
		len += strlen(params->query_string);
		url = (char*)pgut_realloc(url, len);
		strcat(url, params->query_string);
	}

	elog(LOG, "host: %s", host);
	elog(LOG, "url: %s", url);
	elog(LOG, "canonical_url: %s", params->canonical_url);

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

	/* header: Host */
	/* Придется добавить руками, чтобы он попал в Signed Headers */
	tmp_str = (char*)pgut_malloc0(5);
	stpcpy(tmp_str, "Host");
	parray_append(params->headers, tmp_str);
	tmp_str = (char*)pgut_malloc0(strlen(params->host) + 1);;
	strcpy(tmp_str, params->host);
	parray_append(params->contents, tmp_str);
	tmp_str = NULL;

	/* header: Date */
	tmp_str = (char*)pgut_malloc0(11);
	stpcpy(tmp_str, "X-Amz-Date");
	parray_append(params->headers, tmp_str);
	tmp_str = NULL;
	S3_get_date_for_header(&tmp_str, params);
	parray_append(params->contents, tmp_str);
	tmp_str = NULL;

	/* header: Expect ???????? */

	/* some optional headers to append */
	/* Content-type ? */
	/* x-amz-meta-author ??? */

	if (params->content_length > 0)
	{
		if (params->request_type == GET) /* GET */
		{
			/* header: Range */
			tmp_str = (char*)pgut_malloc0(6);
			stpcpy(tmp_str, "Range");
			parray_append(params->headers, tmp_str);
			tmp_str = (char*)pgut_malloc0(100);
			strcat(tmp_str, "bytes=");
			sprintf(tmp_str, "%lu", params->start_pos);
			strcat(tmp_str, "-");
			sprintf(tmp_str, "%lu", params->start_pos + params->content_length);
			tmp_str = pgut_realloc(tmp_str, strlen(tmp_str) + 1);
			parray_append(params->contents, tmp_str);
			tmp_str = NULL;
		}
		else /* PUT */
		{
			/* header: Content-Length */
			tmp_str = (char*)pgut_malloc0(15);
			stpcpy(tmp_str, "Content-Length");
			parray_append(params->headers, tmp_str);
			tmp_str = (char*)pgut_malloc0(100);
			sprintf(tmp_str, "%lu", params->content_length);
			tmp_str = pgut_realloc(tmp_str, strlen(tmp_str) + 1);
			parray_append(params->contents, tmp_str);
			tmp_str = NULL;
		}
	}

	/* initialize all other headers before computing authorization string */
	S3_headers_init(params, config);

	/*header: Authorization*/
	/*
	 * MUST BE CALCULATED AFTER ALL HEADERS ARE SET
	 * because we calculate params with used headers and canonical request
	 */

	tmp_str = (char*)pgut_malloc0(14);
	stpcpy(tmp_str, "Authorization");
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

/* Здесь нет выделения памяти, только чтение. Как в fread */
/* По факту здесь будет копирование данных из структуры pio_data_buffer. Даные уже считаны через pioRead */
/* !!! Сюда передавать stream, который можно портить, т.к. делаем += к указателю, чтобы отметить прочитанные байты !!! */
static size_t
read_callback(char *dest, size_t blcksize, size_t count, ft_bytes_t *src)
{
	/* Вместо EOF */
	size_t readlen = blcksize * count > src->len ? src->len : blcksize * count;
	/*
	 * memcpy always returns pointer to destination -- dest.
	 * If memcpy was not successful, it will result in segfault.
	 * In all other cases read_callback is successful so we return number of copied bytes.
	 */
	memcpy(dest, src->ptr, readlen);
	src->len -= readlen;
	src->ptr += readlen;

	return readlen;
}


static err_i
put_object(S3_query_params *params, S3_config *config)
{
	CURL				*curl;
	CURLcode			res;
	struct curl_slist	*headers = NULL;
	ft_bytes_t			*readbuf;
	/*long http_response_code;*/

	curl = curl_easy_init();

	if (!curl)
	{
		elog(LOG, "curl_easy_init() failed");
		return $err(RT, "curl_easy_init() failed");
	}

	headers_init(curl, &headers, params, config);
	/* Сразу всей пачкой, https://curl.se/libcurl/c/CURLOPT_HTTPHEADER.html*/
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	/* TODO: https protocol */
	curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);

	readbuf = (ft_bytes_t*)pgut_malloc(sizeof(ft_bytes_t));
	readbuf->ptr = params->buf;
	readbuf->len = params->content_length;
	/* I hope this f*ckin sh*t will work. */
	curl_easy_setopt(curl, CURLOPT_READFUNCTION, read_callback);
	curl_easy_setopt(curl, CURLOPT_READDATA, readbuf); // set file we are about to put

	/* Perform the request, res will get the return code */
	/*res = curl_easy_perform(curl);*/

	/* check for errors*/
	if (res != CURLE_OK)
	{
		/* error format in $err ??? */
		elog(LOG, "curl_easy_perform() failed: %s", curl_easy_strerror(res));
		return $err(RT, "curl_easy_perform() failed: {curlError}", curlError(curl_easy_strerror(res)));
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
	return $noerr();
}


/* !!! Передавать указатель dest, который можно портить (все по тем же причинам) !!! */
/* Вот эта хрень должна реаллоцировать память в буфере (т.к. мы не знаем, файл какого размера пришлет нам curl) */
/* ???? Или память выделена уровнями выше ???? */
static size_t
write_callback(char *src, size_t blcksize, size_t count, ft_bytes_t *dest)
{
	/* Не читать больше, чем надо */
	size_t writelen = blcksize * count > dest->len ? dest->len : blcksize * count;
	/*
	 * memcpy always returns pointer to destination -- dest.
	 * If memcpy was not successful, it will result in segfault.
	 * In all other cases read_callback is successful so we return number of copied bytes.
	 */
	memcpy(dest->ptr, src, writelen);
	dest->len -= writelen;
	dest->ptr += writelen;

	return writelen;
}


/* TODO: many code duplicates, optimize put_object and get_object */
/* (in the same way like headers_init and other S3 functions) */
static err_i
get_object(S3_query_params *params, S3_config *config)
{
	CURL				*curl;
	CURLcode			res;
	struct curl_slist	*headers = NULL;
	ft_bytes_t			*writebuf;
	/*long http_response_code;*/

	curl = curl_easy_init();

	if (!curl)
	{
		elog(LOG, "curl_easy_init() failed");
		return $err(RT, "curl_easy_init() failed");
	}

	headers_init(curl, &headers, params, config);
	/* Сразу всей пачкой, https://curl.se/libcurl/c/CURLOPT_HTTPHEADER.html*/
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	/* TODO: https protocol */
	curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);

	writebuf = (ft_bytes_t*)pgut_malloc(sizeof(ft_bytes_t));
	writebuf->ptr = params->buf;
	writebuf->len = params->content_length;
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, writebuf); // set buffer for writing received object

	/* Perform the request, res will get the return code */
	/*res = curl_easy_perform(curl);*/

	/* check for errors*/
	if (res != CURLE_OK)
	{
		/* error format in $err ??? */
		elog(LOG, "curl_easy_perform() failed: %s", curl_easy_strerror(res));
		return $err(RT, "curl_easy_perform() failed: {curlError}", curlError(curl_easy_strerror(res)));
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
	return $noerr();
}

/*
 * pioCloudFile_pioFlush
 * 
 * Send buffer contents to cloud
 */
static err_i
pioCloudFile_pioFlush(VSelf)
{
	Self(pioCloudFile); /* file name from here */
	err_i 				put_err = $noerr();
	S3_query_params		*params = NULL;
	/*long http_response_code;*/


	elog(LOG, "This is S3_put_files function");

	if (self->buflen == 0)
	{
		elog(LOG, "Empty buffer in pioCloudFile_pioFlush");
		return $noerr();
	}

	/* initializing something */

	/* In windows, this will init the winsock stuff */
	curl_global_init(CURL_GLOBAL_ALL);

	/* TODO: проверки из push_file_internal */
	config = self->config;

	params = (S3_query_params*)pgut_malloc(sizeof(S3_query_params));
	params->request_type = PUT;
	params->protocol = "http://";
	params->filename = self->path; /* нужен ли нам полный путь с именем хоста базы данных ??? */
	params->query_string = NULL;
	params->buf = self->filebuf;
	params->start_pos = 0;
	params->content_length = self->buflen;

	put_err = put_object(params, self->config);
	if ($haserr(put_err))
	{
		elog(ERROR, "S3 put_object error: %s", $errmsg(put_err));
		return put_err;
	}

	/* cleanup */
	free(params);
	curl_global_cleanup();

    return $noerr();
}


/*
 * pioCloudFile_pioWrite
 *
 * In first version of S3 we write files to cloud in "as-is" way, so in
 * pioWrite we only save file to buffer
 */
/* TODO: send part of file sized S3_chunk_size by multipart upload */
/* https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html */
/* https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html */
/* https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html */
static size_t
pioCloudFile_pioWrite(VSelf, ft_bytes_t buf, err_i *err)
{
	Self(pioCloudFile);

	fobj_reset_err(err);

	if (buf.len == 0)
		return 0;

	if (!self->filebuf)
	{
		self->capacity = buf.len;
		self->filebuf = (char*)pgut_malloc(buf.len);
	}

	if (self->capacity <= (self->buflen + buf.len))
	{
		self->capacity *= 2;
		self->filebuf = (char*)pgut_realloc(self->filebuf, buf.len);
	}

	memcpy(self->filebuf, buf.ptr, buf.len);
	self->buflen += buf.len;

	return buf.len;
}


/*
 * pioCloudFile_pioRead
 *
 * Read specified file in specified byte range. Perform GetObject with curl
 * 
 * https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
 */
static size_t
pioCloudFile_pioRead(VSelf, ft_bytes_t buf, err_i *err)
{
	Self(pioCloudFile);
	err_i 				get_err = $noerr();
	S3_query_params		*params = NULL;

	fobj_reset_err(err);

	if (buf.len == 0)
		return 0;


	elog(LOG, "This is S3_get_files function");

	/* In windows, this will init the winsock stuff */
	curl_global_init(CURL_GLOBAL_ALL);

	config = self->config;

	params = (S3_query_params*)pgut_malloc(sizeof(S3_query_params));
	params->request_type = GET;
	params->filename = self->path; /* нужен ли нам полный путь с именем хоста базы данных ??? */
	params->query_string = NULL;
	params->buf = NULL;
	params->start_pos = self->current_pos;
	params->content_length = buf.len; /* bytes to read starting from params->start_pos */

	get_err = get_object(params, self->config);
	if ($haserr(get_err))
	{
		elog(ERROR, "S3 put_object error: %s", $errmsg(get_err));
		*err = get_err;
		return 0;
	}

	/* cleanup */
	free(params);
	curl_global_cleanup();

	return buf.len;
}


/* S3_pre_start_check
 *
 *
 * Before starting backup operations, we check if S3 remote bucket with
 * specified paramaters is available by performing request GetBucketAcl
 * (https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html).
 * If user made a mistake in config, he may fix it quickly.
 */
int
S3_pre_start_check(S3_config *config)
{
	CURL				*curl;
	CURLcode			res;
	struct curl_slist	*headers = NULL;
	long				http_response_code;
	S3_query_params		*params = NULL;

/*	char				aws4_str[] = "AWS4:Amz:us-east-1:s3";
	char				cred[] = "minioadmin:minioadmin";*/

	curl_global_init(CURL_GLOBAL_ALL);

	elog(LOG, "S3_pre_start_check in progress");

	params = (S3_query_params*)pgut_malloc(sizeof(S3_query_params));
	params->request_type = GET;
	params->protocol = "http://";
	params->filename = NULL;
	params->query_string = "?acl=";
	//params->query_string = NULL;
	params->buf = NULL;
	params->content_length = 0;

	curl = curl_easy_init();

	if (!curl)
		return ERROR_CURL_EASY_INIT;

	/* available since libcurl 7.75.0, а у меня в не сильно старой системе 7.64.0 */
	/*curl_easy_setopt(c, CURLOPT_AWS_SIGV4, aws4_str);
	curl_easy_setopt(c, CURLOPT_USERPWD, cred);*/

	headers_init(curl, &headers, params, config);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

	/* Perform the request, res will get the return code */
	res = curl_easy_perform(curl);

	/*
	 * TODO!!!
	 * Read received ACLs from xml response
	 */
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_response_code);
	elog(LOG, "curl_easy_perform returned: %ld", http_response_code);
	if (http_response_code == 200 && res != CURLE_ABORTED_BY_CALLBACK)
	{
		elog(LOG, "S3 pre-check successful, continue the operation");
	}
	else
	{
		elog(LOG, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
		return ERROR_CURL_EASY_PERFORM;
	}

	/* RETRY ????*/

	/* cleanup */
	free(params);
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);
	curl_global_cleanup();

	return S3_SUCCESS;
}


/* TODO */
/*
 * S3_permissions_check
 *
 * Check if user has permissions for writing or reading certain file.
 * Needed for correct error reporting. Call for GetObjectAcl
 * https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html
 *
 * PARSE XML
 */
static int
S3_permissions_check(S3_config *config, char *filename, int permissions)
{
	return S3_SUCCESS;
}


/*
 * pioCloudDrive_pioOpen
 *
 * Create pioFile object with specified path and empty buffer.
 * Call S3_permissions_check
 * !!! We need global variable S3_config config
 */

static pioFile_i
pioCloudDrive_pioOpen(VSelf, path_t path, int flags, int permissions, err_i *err)
{
	fobj_t	file;
	int		s3_err;

	fobj_reset_err(err);

	/* TODO: check file permissions during S3_pre_start_check */
	/*
	if (permissions == 0)
		fd = open(path, flags, FILE_PERMISSION);
	else
		fd = open(path, flags, permissions);
	*/

	ft_assert(config->access_key && config->secret_access_key && config->bucket_name, "one of keys o bucket name not provided");

	s3_err = S3_pre_start_check(config);

	if (s3_err != S3_SUCCESS)
	{
		*err = $err(RT, "S3_pre_start_check failed, aborting backp operations");
		return (pioFile_i){NULL};
	}

	file = $alloc(pioCloudFile, .path = ft_cstrdup(path), .config = config, .buflen = 0, .capacity = 0, .filebuf = NULL );

	return bind_pioFile(file);
}


/*
 * pioCloudFile_pioClose
 *
 * Call pioWriteFlush here
 *
 * TODO: if multipart upload hasn't finished, abort it
 */
static err_i
pioCloudFile_pioClose(VSelf, bool sync)
{
	Self(pioCloudFile);
	err_i err = $noerr();

	if (sync)
		err = pioCloudFile_pioFlush(self);

	if (self->filebuf && (self->buflen != 0))
		free(self->filebuf);

	/* ??????? */
	/*free(self->path);*/

	return fobj_err_combine($noerr(), err);
}

fobj_klass_handle(pioCloudFile);
