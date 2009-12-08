/*-------------------------------------------------------------------------
 *
 * pgut-port.c
 *
 * Copyright (c) 2009, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "c.h"
#include "pgut-port.h"

#ifdef WIN32

#include <winioctl.h>

int
uname(struct utsname *buf)
{
	OSVERSIONINFO	os = { sizeof(OSVERSIONINFO) };
	SYSTEM_INFO		sys = { { sizeof(SYSTEM_INFO) } };
	DWORD			bufsize;

	GetVersionEx(&os);
	GetSystemInfo(&sys);

	/* sysname */
	strcpy(buf->sysname, "Windows");

	/* nodename */
	bufsize = lengthof(buf->nodename);
	GetComputerName(buf->nodename, &bufsize);

	/* release: major.minor */
	snprintf(buf->release, lengthof(buf->release), "%ld.%ld",
		os.dwMajorVersion, os.dwMinorVersion);

	/* version */
	strcpy(buf->sysname, os.szCSDVersion);

	/* machine */
	switch (sys.wProcessorArchitecture)
	{
	case PROCESSOR_ARCHITECTURE_INTEL:
		strcpy(buf->machine, "x86");
		break;
	case PROCESSOR_ARCHITECTURE_IA64:
		strcpy(buf->machine, "IA64");
		break;
	case PROCESSOR_ARCHITECTURE_AMD64:
		strcpy(buf->machine, "x86_64");
		break;
	case PROCESSOR_ARCHITECTURE_IA32_ON_WIN64:
		strcpy(buf->machine, "x86_on_win64");
		break;
	default:
		strcpy(buf->machine, "unknown");
		break;
	}

	return 0;
}

#define NTFS_BLOCK_SIZE		512

int
statfs(const char *path, struct statfs *buf)
{
	ULARGE_INTEGER	availBytes;
	ULARGE_INTEGER	totalBytes;
	ULARGE_INTEGER	freeBytes;

	if (!GetDiskFreeSpaceEx(path, &availBytes, &totalBytes, &freeBytes))
	{
		_dosmaperr(GetLastError());
		return -1;
	}

	memset(buf, 0, sizeof(struct statfs));
	buf->f_type = NTFS_SB_MAGIC;
	buf->f_bsize = NTFS_BLOCK_SIZE;
	buf->f_blocks = (long) (totalBytes.QuadPart / NTFS_BLOCK_SIZE);
	buf->f_bfree = (long) (freeBytes.QuadPart / NTFS_BLOCK_SIZE);
	buf->f_bavail = (long) (availBytes.QuadPart / NTFS_BLOCK_SIZE);
	buf->f_namelen = MAX_PATH;

	return 0;
}

#define REPARSE_DATA_SIZE		1024

/* same layout as REPARSE_DATA_BUFFER, which is defined only in old winnt.h */
typedef struct REPARSE_DATA
{
	ULONG	ReparseTag;
	WORD	ReparseDataLength;
	WORD	Reserved;
	union
	{
		struct
		{
			WORD	SubstituteNameOffset;
			WORD	SubstituteNameLength;
			WORD	PrintNameOffset;
			WORD	PrintNameLength;
			ULONG	Flags;
			WCHAR	PathBuffer[1];
		} Symlink;
		struct
		{
			WORD	SubstituteNameOffset;
			WORD	SubstituteNameLength;
			WORD	PrintNameOffset;
			WORD	PrintNameLength;
			WCHAR	PathBuffer[1];
		} Mount;
		struct
		{
			BYTE  DataBuffer[REPARSE_DATA_SIZE];
		} Generic;
	};
} REPARSE_DATA;

ssize_t
readlink(const char *path, char *target, size_t size)
{
    HANDLE			handle; 
 	DWORD			attr;
	REPARSE_DATA	data;
 	DWORD			datasize;
	PCWSTR			wpath;
	int				wlen;
	int				r;

	attr = GetFileAttributes(path);
	if (attr == INVALID_FILE_ATTRIBUTES)
	{
		_dosmaperr(GetLastError());
        return -1; 
    } 
	if ((attr & FILE_ATTRIBUTE_REPARSE_POINT) == 0)
	{
		errno = EINVAL;	/* not a symlink */
        return -1; 
	}

    handle = CreateFileA(path, 0,
		FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE, NULL,
		OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
	if (handle == INVALID_HANDLE_VALUE)
	{
		_dosmaperr(GetLastError());
        return -1; 
    }
 
	wpath = NULL;
	if (DeviceIoControl(handle, FSCTL_GET_REPARSE_POINT, NULL, 0,
        &data, sizeof(data), &datasize, NULL))
	{
		switch (data.ReparseTag)
		{
			case IO_REPARSE_TAG_MOUNT_POINT:
			{
				wpath = data.Mount.PathBuffer + data.Mount.SubstituteNameOffset;
				wlen = data.Mount.SubstituteNameLength;
				break;
			}
			case IO_REPARSE_TAG_SYMLINK:
			{
				wpath = data.Symlink.PathBuffer + data.Symlink.SubstituteNameOffset;
				wlen = data.Symlink.SubstituteNameLength;
				break;
			}
		}
	}

	if (wpath == NULL)
		r = -1;
	else
	{
		if (wcsncmp(wpath, L"\\??\\", 4) == 0 ||
			wcsncmp(wpath, L"\\\\?\\", 4) == 0)
		{
			wpath += 4;
			wlen -= 4;
		}
		r = WideCharToMultiByte(CP_ACP, 0, wpath, wlen, target, size, NULL, NULL);
	}

	CloseHandle(handle);
	return r;
}

char *
blkid_devno_to_devname(dev_t devno)
{
	static char	devname[4];
	char		letter = 'A' + devno;
	if ('A' <= letter && letter <= 'Z')
	{
		snprintf(devname, lengthof(devname), "%c:\\", 'A' + devno);
		return devname;
	}
	else
		return NULL;
}

int
flock(int fd, int operation)
{
	BOOL	ret;
	HANDLE	handle = (HANDLE) _get_osfhandle(fd);
	DWORD	lo = 0;
	DWORD	hi = 1;

	if (operation & LOCK_UN)
	{
		ret = UnlockFileEx(handle, 0, lo, hi, NULL);
	}
	else
	{
		DWORD	flags = 0;
		if (operation & LOCK_EX)
			flags |= LOCKFILE_EXCLUSIVE_LOCK;
		if (operation & LOCK_NB)
			flags |= LOCKFILE_FAIL_IMMEDIATELY;
		ret = LockFileEx(handle, flags, 0, lo, hi, NULL);
	}

	if (!ret)
	{
		_dosmaperr(GetLastError());
		return -1;
	}

	return 0;
}

#endif
