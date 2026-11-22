// SPDX-FileCopyrightText: 2026 GPUStack, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package v1 是 worker.gpustack.ai 的版本化 API。
//
// +k8s:openapi-gen=true
// +k8s:openapi-model-package=ai.gpustack.worker.v1
// +k8s:deepcopy-gen=package
// +k8s:protobuf-gen=package
// +k8s:apireg-gen:service
// +k8s:conversion-gen=gpustack.ai/gpustack/api/worker/v1alpha1
// +k8s:conversion-gen-external-types=gpustack.ai/gpustack/api/worker/v1
// +domain=gpustack.ai
// +groupName=worker.gpustack.ai
// +versionName=v1
package v1
