/*-------------------------------------------------------------------------
 *
 * main.c: proxy main
 *
 * To allow linking pg_probackup.c with tests we have to have proxy `main`
 * in separate file to call real `pbk_main` function
 *
 * Copyright (c) 2018-2022, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#include "pg_probackup.h"

int
main(int argc, char** argv)
{
	return pbk_main(argc, argv);
}