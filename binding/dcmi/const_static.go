// SPDX-FileCopyrightText: 2026 GPUStack, Inc.
// SPDX-License-Identifier: Apache-2.0

package dcmi

// ERROR_LIST_TRUNCATED 表示返回条目超过本次读取可容纳的数量。这个本地错误码避开厂商使用的 -80xx 区间。
// 某些列表接口只输出数量，无法传入缓冲区长度，必须由绑定层识别截断。
const ERROR_LIST_TRUNCATED Return = -9001

// DDIE 的值为 2，A5 使用 DDie ID 作为芯片 UUID。V2 头文件复用只列到 VDIE 的旧枚举，
// 厂商实现还接受 NDIE 和 DDIE。仅在 V2 查询中传这个值。
const DDIE DieType = 2

// MAIN_CMD_CHIP_INF 和 CINF_SUB_CMD_GET_SPOD_INFO 是设备信息接口查询超节点的主命令和子命令。
// 头文件未列出它们，这里取 ascend-common 在两代 API 都使用的值 12 和 1。
const (
	MAIN_CMD_CHIP_INF          = 12
	CINF_SUB_CMD_GET_SPOD_INFO = 1
)
