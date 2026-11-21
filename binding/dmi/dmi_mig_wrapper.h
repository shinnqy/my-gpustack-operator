#ifndef DMI_MIG_WRAPPER_H
#define DMI_MIG_WRAPPER_H

#include "dmi_mig.h"

#ifdef __cplusplus
extern "C" {
#endif

// 厂商库导出 nvmlDeviceGetUtilizationRates，但头文件没有原型，这里补充包装层调用需要的签名。
nvmlReturn_t nvmlDeviceGetUtilizationRates(nvmlDevice_t device,
                                           nvmlUtilization_t *utilization);

#include "dmi_mig_wrapper_api.def"

// w_dmi_mig_init 从给定路径打开库并解析多实例 API。无法打开或记录路径时返回 NVML_ERROR_LIBRARY_NOT_FOUND，未导出任何所需符号时返回 NVML_ERROR_FUNCTION_NOT_FOUND。重复初始化相同库会复用现有句柄。
nvmlReturn_t w_dmi_mig_init(const char *path);

// w_dmi_mig_shutdown 卸载共享库并清空指针，只能在没有其他使用者时调用。
nvmlReturn_t w_dmi_mig_shutdown(void);

// w_dmi_mig_last_error 返回当前线程最近一次加载失败的说明，成功时为空字符串。
const char *w_dmi_mig_last_error(void);

#define DECL_API(ret, name, decl_args, call_args) ret w_##name decl_args;
DMI_MIG_API_LIST(DECL_API)
#undef DECL_API

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // DMI_MIG_WRAPPER_H
