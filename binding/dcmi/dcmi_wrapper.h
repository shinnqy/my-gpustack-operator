#ifndef DCMI_WRAPPER_H
#define DCMI_WRAPPER_H

#include "dcmi_interface_api.h"
// V2 头文件复用 V1 中定义的 DCMI_VERSION_2 和数据类型，因此必须先包含 V1。
#include "dcmi_interface_api_v2.h"

#ifdef __cplusplus
extern "C" {
#endif

#define	SUCCESS                         0
#define	ERROR_INVALID_PARAMETER         -8001
#define	ERROR_MEM_OPERATE_FAIL          -8003
#define	ERROR_INVALID_DEVICE_ID         -8007
#define	ERROR_DEVICE_NOT_EXIST          -8008
#define	ERROR_CONFIG_INFO_NOT_EXIST     -8023
#define	ERROR_OPER_NOT_PERMITTED        -8002
#define	ERROR_NOT_SUPPORT_IN_CONTAINER  -8013
#define	ERROR_NOT_SUPPORT               -8255
#define	ERROR_TIME_OUT                  -8006
#define	ERROR_NOT_REDAY                 -8012
#define	ERROR_IS_UPGRADING              -8017
#define	ERROR_RESOURCE_OCCUPIED         -8020
#define	ERROR_SECURE_FUN_FAIL           -8004
#define	ERROR_INNER_ERR                 -8005
#define	ERROR_IOCTL_FAIL                -8009
#define	ERROR_SEND_MSG_FAIL             -8010
#define	ERROR_RECV_MSG_FAIL             -8011
#define	ERROR_RESET_FAIL                -8015
#define	ERROR_ABORT_OPERATE             -8016
#define	ERROR_FUNCTION_NOT_FOUND        -99997
#define	ERROR_LIBRARY_NOT_FOUND         -99998
#define	ERROR_UNKNOWN                   -99999

// 记录驱动实际接受的 API 代际。V2 可能仍导出所有 V1 入口却拒绝调用，需要依据哪一个初始化成功来选择代际。
#define API_VERSION_UNKNOWN             0
#define API_VERSION_V1                  1
#define API_VERSION_V2                  2

int w_dcmi_init(const char *path);
int w_dcmi_shutdown(void);
const char* w_dcmi_last_error(void);

// w_dcmi_api_version 返回 API_VERSION_* 中表示当前代际的值。API_VERSION_UNKNOWN 表示还没有成功初始化。库是进程级共享状态，调用方按需读取，避免缓存过期。
int w_dcmi_api_version(void);

#define DECL_API(ret, name, decl_args, call_args) ret w_##name decl_args;

#include "dcmi_wrapper_api.def"

DCMI_API_LIST(DECL_API)
DCMI_V2_API_LIST(DECL_API)

#undef DECL_API

#ifdef __cplusplus
}
#endif

#endif
