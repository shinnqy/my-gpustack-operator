package deviceplugin

import ()

// SlicedAllocateGateEnv 在设置为 "false"时会关闭逻辑切片分配拒绝机制：此时逻辑切片 kubelet 会分配没有剩余
// 空间的卡片，而账本会将该卡片的剩余量设为零，这与拒绝机制存在之前的情况一致。
const SlicedAllocateGateEnv = "GPUSTACK_DEVICE_PLUGIN_SLICED_ALLOCATE_GATE"
