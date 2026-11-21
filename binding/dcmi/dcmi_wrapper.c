#include "dcmi_wrapper.h"
#include <dlfcn.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

static void *dcmiLib = NULL;

// 保存当前 DCMI 动态库的路径及初始化状态，使进程内的不同调用方能独立复用同一个已成功打开的库。
static char dcmiLibPath[1024];
static int dcmiReady = 0;

// 互斥锁保护库句柄、路径、就绪状态及首次加载。两个并发初始化不能重复打开，也不能在另一个调用方使用函数指针时卸载。
static pthread_mutex_t dcmiInitMutex = PTHREAD_MUTEX_INITIALIZER;

// 记录当前驱动成功初始化的 API 代际。初始化锁内写入，查询热路径通过原子读取得到一致的值。
static _Atomic int dcmiApiVersion = API_VERSION_UNKNOWN;

// 使用线程局部缓冲区保存本次加载的错误，避免混入其他调用的 dlerror 消息。
static __thread char dcmi_last_err[1024];

static void clear_last_error(void) {
    dcmi_last_err[0] = '\0';
}

static void set_last_error(const char *msg) {
    if (!msg || msg[0] == '\0') {
        dcmi_last_err[0] = '\0';
        return;
    }
    snprintf(dcmi_last_err, sizeof(dcmi_last_err), "%s", msg);
}

static void set_last_errorf(const char *prefix, const char *msg) {
    if (!msg) msg = "unknown dynamic loader error";
    if (!prefix) prefix = "dcmi";
    snprintf(dcmi_last_err, sizeof(dcmi_last_err), "%s: %s", prefix, msg);
}

#define DECL_FUNC_PTR(ret, name, decl_args, call_args) \
    static ret (*name##_func) decl_args = NULL;

DCMI_API_LIST(DECL_FUNC_PTR)
DCMI_V2_API_LIST(DECL_FUNC_PTR)

#undef DECL_FUNC_PTR

// 两代初始化函数单独解析，由下面的流程统一确定使用哪一代。
static int (*dcmi_init_func) (void) = NULL;
static int (*dcmiv2_init_func) (void) = NULL;

// vendor_init 先调用 V1，仅在返回码明确拒绝该代接口时尝试 V2。它是 dcmiApiVersion 的唯一写入者。
// V2 驱动可能仍导出 dcmi_init 却返回 ERROR_NOT_SUPPORT，因此不能只看符号是否存在。V1 临时故障也不能当成切换代际的依据。
static int vendor_init(void)
{
    int rc = dcmi_init_func ? dcmi_init_func() : ERROR_FUNCTION_NOT_FOUND;
    int v2rc = SUCCESS;
    char msg[128];

    if (rc == SUCCESS) {
        dcmiApiVersion = API_VERSION_V1;
        return rc;
    }
    if (rc != ERROR_NOT_SUPPORT && rc != ERROR_FUNCTION_NOT_FOUND) {
        return rc;
    }

    v2rc = dcmiv2_init_func ? dcmiv2_init_func() : ERROR_FUNCTION_NOT_FOUND;
    if (v2rc == SUCCESS) {
        dcmiApiVersion = API_VERSION_V2;
        return v2rc;
    }

    // 两代初始化都失败时返回 V1 错误码，在诊断字符串中同时保留 V2 的返回值。
    snprintf(msg, sizeof(msg), "dcmi_init: %d, dcmiv2_init: %d", rc, v2rc);
    set_last_error(msg);

    return rc;
}

#define IMPL_WRAPPER(ret, name, decl_args, call_args) \
    ret w_##name decl_args { \
        if (!name##_func) { \
            return ERROR_FUNCTION_NOT_FOUND; \
        } \
        return name##_func call_args; \
    }

DCMI_API_LIST(IMPL_WRAPPER)
DCMI_V2_API_LIST(IMPL_WRAPPER)

#undef IMPL_WRAPPER

int w_dcmi_init(const char *path)
{
    const char *err = NULL;
    int rc = ERROR_UNKNOWN;

    clear_last_error();

    if (!path || path[0] == '\0') {
        set_last_error("invalid library path");
        return ERROR_LIBRARY_NOT_FOUND;
    }

    pthread_mutex_lock(&dcmiInitMutex);

    // 请求的正是当前持有的库。复用已成功的初始化；若还未就绪则在原句柄上重试，避免影响其他调用方。
    if (dcmiLib != NULL && strcmp(dcmiLibPath, path) == 0) {
        rc = SUCCESS;
        if (!dcmiReady) {
            rc = vendor_init();
            if (rc == SUCCESS) {
                dcmiReady = 1;
            }
        }
        goto out;
    }

    // 更换库路径前释放旧句柄，避免泄漏。
    if (dcmiLib) {
        dlclose(dcmiLib);
        dcmiLib = NULL;
    }
    dcmiReady = 0;
    dcmiLibPath[0] = '\0';
    dcmiApiVersion = API_VERSION_UNKNOWN;

    // 加载前清空旧的函数指针。
    #define RESET_API_PTR(ret, name, decl_args, call_args) \
        name##_func = NULL;
    DCMI_API_LIST(RESET_API_PTR)
    DCMI_V2_API_LIST(RESET_API_PTR)
    #undef RESET_API_PTR
    dcmi_init_func = NULL;
    dcmiv2_init_func = NULL;

    // 打开候选动态库。
    dlerror();
    dcmiLib = dlopen(path, RTLD_LAZY | RTLD_GLOBAL);
    err = dlerror();
    if (!dcmiLib) {
        set_last_errorf("dlopen", err);
        rc = ERROR_LIBRARY_NOT_FOUND;
        goto out;
    }

    // 分别查找两代初始化入口，至少需要其中一个。V2 库可能省略 dcmi_init，较早的 V1 库则可能没有 V2 入口。
    dlerror();
    dcmi_init_func = dlsym(dcmiLib, "dcmi_init");
    dcmiv2_init_func = dlsym(dcmiLib, "dcmiv2_init");
    err = dlerror();
    if (!dcmi_init_func && !dcmiv2_init_func) {
        set_last_errorf("dlsym(dcmi_init, dcmiv2_init)", err);
        dlclose(dcmiLib);
        dcmiLib = NULL;
        dcmi_init_func = NULL;
        dcmiv2_init_func = NULL;
        rc = ERROR_FUNCTION_NOT_FOUND;
        goto out;
    }

    // 库打开后先记录路径。路径过长会导致下一次无法正确比较，必须在持有不明句柄之前报告失败。
    if (snprintf(dcmiLibPath, sizeof(dcmiLibPath), "%s", path) >= (int)sizeof(dcmiLibPath)) {
        set_last_error("library path is too long to track");
        dcmiLibPath[0] = '\0';
        dlclose(dcmiLib);
        dcmiLib = NULL;
        dcmi_init_func = NULL;
        dcmiv2_init_func = NULL;
        rc = ERROR_LIBRARY_NOT_FOUND;
        goto out;
    }

    // 逐项解析 API 符号。驱动不提供的代际会留下空指针，由包装函数返回相应的不可用错误。
    #define LOAD_API(ret, name, decl_args, call_args) \
        name##_func = dlsym(dcmiLib, #name);
    DCMI_API_LIST(LOAD_API)
    DCMI_V2_API_LIST(LOAD_API)
    #undef LOAD_API

    rc = vendor_init();
    if (rc == SUCCESS) {
        dcmiReady = 1;
    }

out:
    pthread_mutex_unlock(&dcmiInitMutex);
    return rc;
}

// w_dcmi_shutdown 卸载共享库并清空全部函数指针，只能在确认进程中没有其他持有者时调用。对外提供卸载前，应在锁内对成功加载和复用进行引用计数，最后一个持有者退出时再关闭。
int w_dcmi_shutdown(void)
{
    int rc = SUCCESS;

    clear_last_error();

    pthread_mutex_lock(&dcmiInitMutex);

    if (!dcmiLib) {
        goto out;
    }

    if (dlclose(dcmiLib) != 0) {
        set_last_errorf("dlclose", dlerror());
        rc = ERROR_UNKNOWN;
        goto out;
    }

    dcmiLib = NULL;
    dcmiReady = 0;
    dcmiLibPath[0] = '\0';
    dcmiApiVersion = API_VERSION_UNKNOWN;
    dcmi_init_func = NULL;
    dcmiv2_init_func = NULL;

    #define RESET_API_PTR(ret, name, decl_args, call_args) \
        name##_func = NULL;
    DCMI_API_LIST(RESET_API_PTR)
    DCMI_V2_API_LIST(RESET_API_PTR)
    #undef RESET_API_PTR

out:
    pthread_mutex_unlock(&dcmiInitMutex);
    return rc;
}

int w_dcmi_api_version(void)
{
    // 每次查询需要知道当前 API 代际，使用原子读以免每次都争用初始化锁。
    return dcmiApiVersion;
}

const char* w_dcmi_last_error(void) {
    return dcmi_last_err[0] ? dcmi_last_err : "";
}
