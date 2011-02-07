#!/bin/sh
#############################################################################
#  Copyright (c) 2009-2010, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
#############################################################################

CMD_SUDO="/usr/bin/sudo"
CMD_LVCREATE="${CMD_SUDO} /usr/sbin/lvcreate"
CMD_LVREMOVE="${CMD_SUDO} /usr/sbin/lvremove"
CMD_MOUNT="${CMD_SUDO} /bin/mount"
CMD_UMOUNT="${CMD_SUDO} /bin/umount"

INFO=-2
WARNING=-1
ERROR=0

#
# Please set the information necessary for the snapshot acquisition
# to each array.
# 
# The following arrays are one sets.
# 
# SNAPSHOT_NAME .... name for snapshot volume
# SNAPSHOT_SIZE .... size to allocate for snapshot volume
# SNAPSHOT_VG ...... volume group to which logical volume to create snapshot belongs
# SNAPSHOT_LV ...... logical volume to create snapshot
# SNAPSHOT_MOUNT ... mount point to file-system of snapshot volume
# 
# increase and decrease the slot in proportion to the number of acquisition
# of snapshots.
# 
# example:
#
# SNAPSHOT_NAME[0]="snap00"
# SNAPSHOT_SIZE[0]="2G"
# SNAPSHOT_VG[0]="/dev/VolumeGroup00" 
# SNAPSHOT_LV[0]="/dev/VolumeGroup00/LogicalVolume00"
# SNAPSHOT_MOUNT[0]="/mnt/pgdata"
#
# SNAPSHOT_NAME[1]="snap01"
# SNAPSHOT_SIZE[1]="2G"
# SNAPSHOT_VG[1]="/dev/VolumeGroup00" 
# SNAPSHOT_LV[1]="/dev/VolumeGroup00/LogicalVolume01"
# SNAPSHOT_MOUNT[1]="/mnt/tblspc/account"
#
#SNAPSHOT_NAME[0]=""
#SNAPSHOT_SIZE[0]=""
#SNAPSHOT_VG[0]="" 
#SNAPSHOT_LV[0]=""
#SNAPSHOT_MOUNT[0]=""

#
# Please set the tablespace name and tablespace storage path in snapshot 
# to each array.
# 
# The following arrays are one sets.
# 
# SNAPSHOT_TBLSPC ........ name of tablespace in snapshot volume
# SNAPSHOT_TBLSPC_DIR .... stored directory of tablespace in snapshot volume
#
# Note: set 'PG-DATA' to SNAPSHOT_TBLSPC when PGDATA in snapshot volume.
#
# increase and decrease the slot in proportion to the number of acquisition
# of tablespace.
# 
# example:
#
# SNAPSHOT_TBLSPC[0]="PG-DATA"
# SNAPSHOT_TBLSPC_DIR[0]="/mnt/pgdata/pgdata"
# SNAPSHOT_TBLSPC[1]="custom"
# SNAPSHOT_TBLSPC_DIR[1]="/mnt/tblspc_custom/tblspc/custom"
# SNAPSHOT_TBLSPC[2]="account"
# SNAPSHOT_TBLSPC_DIR[2]="/mnt/tblspc_account/tblspc/account"
#
#SNAPSHOT_TBLSPC[0]=""
#SNAPSHOT_TBLSPC_DIR[0]=""

#
# argument of the command.
# this variables are set by set_args().
#
ARGS_SS_NAME=""  # SNAPSHOT_NAME[N]
ARGS_SS_SIZE=""  # SNAPSHOT_SIZE[N]
ARGS_SS_VG=""    # SNAPSHOT_VG[N]
ARGS_SS_LV=""    # SNAPSHOT_LV[N]
ARGS_SS_MOUNT="" # SNAPSHOT_MOUNT[N]


#
# implement of interface 'freeze'.
# don't remove this function even if there is no necessity.
#
function freeze()
{
	# nothing to do
	return
}

#
# implement of interface 'unfreeze'.
# don't remove this function even if there is no necessity.
#
function unfreeze()
{
	# nothing to do
	return
}

#
# implement of interface 'split'.
# create a snapshot volume from the setting of the specified slot.
# don't remove this function even if there is no necessity.
# 
function split()
{
	local i=0
	
	for ss_name in "${SNAPSHOT_NAME[@]}"
	do
		set_args "${ss_name}" "${SNAPSHOT_SIZE[${i}]}" "" "${SNAPSHOT_LV[${i}]}" ""
		execute_split
		i=$(expr ${i} + 1)
	done
	
	# print tablespace name
	i=0
	for tblspc in "${SNAPSHOT_TBLSPC[@]}"
	do
		local tblspc="${SNAPSHOT_TBLSPC[${i}]}"
		
		echo "${tblspc}"
		i=$(expr ${i} + 1)
	done
	return
}

#
# implement of interface 'resync'.
# remove a snapshot volume from the setting of the specified slot.
# don't remove this function even if there is no necessity.
#
function resync()
{
	local i=0
	
	for ss_name in "${SNAPSHOT_NAME[@]}"
	do
		set_args "${ss_name}" "" "${SNAPSHOT_VG[${i}]}" "" ""
		execute_resync
		i=$(expr ${i} + 1)
	done
	return
}

#
# implement of interface 'mount'.
# create mount point of the snapshot volume to the file-system.
# don't remove this function even if there is no necessity.
#
function mount()
{
	local i=0
	
	for ss_name in "${SNAPSHOT_NAME[@]}"
	do
		set_args "${ss_name}" "" "${SNAPSHOT_VG[${i}]}" "" "${SNAPSHOT_MOUNT[${i}]}"
		execute_mount
		i=$(expr ${i} + 1)
	done
	
	# print tablespace name and stored directory
	i=0
	for tblspc in "${SNAPSHOT_TBLSPC[@]}"
	do
		local tblspc_mp="${SNAPSHOT_TBLSPC_DIR[${i}]}"
		
		echo "${tblspc}=${tblspc_mp}"
		i=$(expr ${i} + 1)
	done
	return
}

#
# implement of interface 'umount'.
# remove mount point of the snapshot volume from the file-system.
# don't remove this function even if there is no necessity.
#
function umount()
{
	for ss_mp in "${SNAPSHOT_MOUNT[@]}"
	do
		set_args "" "" "" "" "${ss_mp}"
		execute_umount
	done
	return
}

#
# create the snapshot volume.
#
function execute_split()
{
	${CMD_LVCREATE} --snapshot --size=${ARGS_SS_SIZE} --name="${ARGS_SS_NAME}" "${ARGS_SS_LV}" > /dev/null
	[ ${?} -ne 0 ] && \
		print_log ${ERROR} "${CMD_LVCREATE} command failed: ${ARGS_SS_LV}"
}

#
# remove the snapshot volume.
#
function execute_resync()
{
	${CMD_LVREMOVE} -f "${ARGS_SS_VG}/${ARGS_SS_NAME}" > /dev/null
	[ ${?} -ne 0 ] && \
		print_log ${ERROR} "${CMD_LVREMOVE} command failed: ${ARGS_SS_VG}/${ARGS_SS_NAME}"
}

#
# mount the snapshot volume to file-system.
#
function execute_mount()
{
	${CMD_MOUNT} "${ARGS_SS_VG}/${ARGS_SS_NAME}" "${ARGS_SS_MOUNT}" > /dev/null
	[ ${?} -ne 0 ] && \
		print_log ${ERROR} "${CMD_MOUNT} command failed: ${ARGS_SS_MOUNT}"
}

#
# unmount the directory from file-system in snapshot volume.
#
function execute_umount()
{
	${CMD_UMOUNT} "${ARGS_SS_MOUNT}" > /dev/null
	[ ${?} -ne 0 ] && \
		print_log ${ERROR} "${CMD_UMOUNT} command failed: ${ARGS_SS_MOUNT}"
}

#
# set argument of command to execute.
#
set_args()
{
	ARGS_SS_NAME="${1}"
	ARGS_SS_SIZE="${2}"
	ARGS_SS_VG="${3}"
	ARGS_SS_LV="${4}"
	ARGS_SS_MOUNT="${5}"
}

#
# output the log message and abort the script when level <= ERROR
#
function print_log()
{
	local level="${1}"
	local message="${2}"
	
	# if cleanup enable change ERROR to WARNING
	[ -n "${cleanup}" -a ${level} -ge 0 ] && \
		level=${WARNING}
	
	case "${level}" in
		${INFO} ) # INFO
			echo "INFO: ${message}" 1>&2
		;;
		${WARNING} ) # WARNING
			echo "WARNING: ${message}" 1>&2
		;;
		${ERROR} ) # ERROR
			echo "ERROR: ${message}" 1>&2
		;;
	esac
	[ ${level} -ge 0 ] && exit
}

#
# main
#
command="${1}"
cleanup="${2}"

case "${command}" in
	"freeze" )
		freeze
	;;
	"unfreeze" )
		unfreeze
	;;
	"split" )
		split
	;;
	"resync" )
		resync
	;;
	"mount" )
		mount
	;;
	"umount" )
		umount
	;;
	* )
		print_log ${ERROR} "specified invalid command: ${command} (internal error)"
	;;
esac

echo "SUCCESS"
exit
