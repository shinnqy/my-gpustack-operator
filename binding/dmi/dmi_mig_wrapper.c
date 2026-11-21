#include "dmi_mig_wrapper.h"
#include <dlfcn.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>

// 海光多实例库导出同名的 nvml* 符号，但返回结构与 NVIDIA 的布局不同。通过 RTLD_LOCAL 打开海光库，再用该句柄 dlsym，防止两套驱动在同一进程中互相解析到对方的入口。

static void *migLib = NULL;

// 缓存已打开库的路径。同一路径再次初始化时复用现有句柄，避免让其他调用方在用的函数指针失效。
static char migLibPath[1024];

// 互斥锁保护库句柄、路径和加载过程，防止两个线程同时首次打开。
static pthread_mutex_t migInitMutex = PTHREAD_MUTEX_INITIALIZER;

// 在线程局部缓冲区保留加载错误，使同线程稍后的诊断调用读到本次失败的原因。
static __thread char mig_last_err[1024];

static void clear_last_error(void) { mig_last_err[0] = '\0'; }

static void set_last_error(const char *msg) {
    if (!msg || msg[0] == '\0') {
        mig_last_err[0] = '\0';
        return;
    }
    snprintf(mig_last_err, sizeof(mig_last_err), "%s", msg);
}

static void set_last_errorf(const char *prefix, const char *msg) {
    if (!msg) msg = "unknown dynamic loader error";
    if (!prefix) prefix = "dmi_mig";
    snprintf(mig_last_err, sizeof(mig_last_err), "%s: %s", prefix, msg);
}

#define DECL_FUNC_PTR(ret, name, decl_args, call_args) \
    static ret (*name##_func) decl_args = NULL;

DMI_MIG_API_LIST(DECL_FUNC_PTR)

#undef DECL_FUNC_PTR

// 未导出的函数保留空指针，其包装函数返回 NVML_ERROR_FUNCTION_NOT_FOUND。
#define IMPL_WRAPPER(ret, name, decl_args, call_args)  \
    ret w_##name decl_args {                           \
        if (!name##_func) {                            \
            return NVML_ERROR_FUNCTION_NOT_FOUND;      \
        }                                              \
        return name##_func call_args;                  \
    }

DMI_MIG_API_LIST(IMPL_WRAPPER)

#undef IMPL_WRAPPER

nvmlReturn_t w_dmi_mig_init(const char *path) {
    const char *err = NULL;
    nvmlReturn_t rc = NVML_ERROR_UNKNOWN;

    clear_last_error();

    if (!path || path[0] == '\0') {
        set_last_error("invalid library path");
        return NVML_ERROR_LIBRARY_NOT_FOUND;
    }

    pthread_mutex_lock(&migInitMutex);

    // 同一个库已打开，直接返回成功，不触碰进行中的调用。
    if (migLib != NULL && strcmp(migLibPath, path) == 0) {
        rc = NVML_SUCCESS;
        goto out;
    }

    // 选择新库之前先释放原有句柄。
    if (migLib) {
        dlclose(migLib);
        migLib = NULL;
    }
    migLibPath[0] = '\0';

#define RESET_API_PTR(ret, name, decl_args, call_args) name##_func = NULL;
    DMI_MIG_API_LIST(RESET_API_PTR)
#undef RESET_API_PTR

    // 使用 RTLD_LOCAL 隔离厂商的 nvml* 符号，避免它们进入全局作用域并覆盖 NVIDIA 实现。
    dlerror();
    migLib = dlopen(path, RTLD_LAZY | RTLD_LOCAL);
    err = dlerror();
    if (!migLib) {
        set_last_errorf("dlopen", err);
        rc = NVML_ERROR_LIBRARY_NOT_FOUND;
        goto out;
    }

    // 先保存已打开的库路径。若路径无法完整记录，拒绝保留该句柄，避免下次比较导致意外重载。
    if (snprintf(migLibPath, sizeof(migLibPath), "%s", path) >= (int)sizeof(migLibPath)) {
        set_last_error("library path is too long to track");
        migLibPath[0] = '\0';
        dlclose(migLib);
        migLib = NULL;
        rc = NVML_ERROR_LIBRARY_NOT_FOUND;
        goto out;
    }

#define LOAD_API(ret, name, decl_args, call_args) name##_func = dlsym(migLib, #name);
    DMI_MIG_API_LIST(LOAD_API)
#undef LOAD_API

    // 至少要解析到一个所需符号才保留此库。不同驱动提供的接口范围不同，缺少个别可选函数时由对应调用单独返回错误。
    if (!nvmlGetSystemMigMode_func && !nvmlDeviceGetCount_func) {
        set_last_error("library exports neither nvmlGetSystemMigMode nor nvmlDeviceGetCount");
        dlclose(migLib);
        migLib = NULL;
        migLibPath[0] = '\0';
        rc = NVML_ERROR_FUNCTION_NOT_FOUND;
        goto out;
    }

    rc = NVML_SUCCESS;

out:
    pthread_mutex_unlock(&migInitMutex);
    return rc;
}

// w_dmi_mig_shutdown 卸载库并清空缓存，只能在没有其他持有者时调用。Go 层暂不公开它。对外提供前应在初始化锁内维护引用计数，最后一个调用方释放后再真正关闭。
nvmlReturn_t w_dmi_mig_shutdown(void) {
    nvmlReturn_t rc = NVML_SUCCESS;

    clear_last_error();

    pthread_mutex_lock(&migInitMutex);

    if (!migLib) {
        goto out;
    }

    if (dlclose(migLib) != 0) {
        set_last_errorf("dlclose", dlerror());
        rc = NVML_ERROR_UNKNOWN;
        goto out;
    }

    migLib = NULL;
    migLibPath[0] = '\0';

#define RESET_API_PTR(ret, name, decl_args, call_args) name##_func = NULL;
    DMI_MIG_API_LIST(RESET_API_PTR)
#undef RESET_API_PTR

out:
    pthread_mutex_unlock(&migInitMutex);
    return rc;
}

const char *w_dmi_mig_last_error(void) {
    return mig_last_err[0] ? mig_last_err : "";
}
