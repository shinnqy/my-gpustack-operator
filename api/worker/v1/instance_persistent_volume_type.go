package v1

import (
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// InstancePersistentVolumeType 是 worker.gpustack.ai 的资源 schema。
//
// 在底层，InstancePersistentVolumeType 对应一个 Kubernetes StorageClass，
// 且 InstancePersistentVolumeType 的名称与该 Kubernetes StorageClass 的名称相同。
//
// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:apireg-gen:resource:scope="Cluster",categories=["gpustack"]
type InstancePersistentVolumeType struct {
	meta.TypeMeta   `json:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec InstancePersistentVolumeTypeSpec `json:"spec" protobuf:"bytes,2,name=spec"`
}

var _ runtime.Object = (*InstancePersistentVolumeType)(nil)

// InstancePersistentVolumeTypeSpec 定义 InstancePersistentVolumeType 的期望状态。
type InstancePersistentVolumeTypeSpec struct {
	// DisplayName 是 InstancePersistentVolumeType 的显示名称。
	//
	// +k8s:validation:maxLength=64
	DisplayName string `json:"displayName,omitempty" protobuf:"bytes,1,opt,name=displayName"`

	// Description 是 InstancePersistentVolumeType 的描述信息。
	//
	// +k8s:validation:maxLength=1024
	Description string `json:"description,omitempty" protobuf:"bytes,2,opt,name=description"`

	// InstancePersistentVolumeSource 是 InstancePersistentVolumeType 的存储源配置。
	InstancePersistentVolumeSource `json:",inline" protobuf:"bytes,3,opt,name=instancePersistentVolumeSource"`
}

// InstancePersistentVolumeSource 定义 InstancePersistentVolumeType 的存储源。
type InstancePersistentVolumeSource struct {
	// NFS 表示由 NFS 服务器管理的卷存储源。
	NFS *NFSInstancePersistentVolumeSource `json:"nfs,omitempty" protobuf:"bytes,1,opt,name=nfs"`

	// S3 表示由兼容 S3 的对象存储服务管理的卷存储源。
	S3 *S3InstancePersistentVolumeSource `json:"s3,omitempty" protobuf:"bytes,2,opt,name=s3"`
}

// NFSInstancePersistentVolumeSource 定义 NFS 存储源配置。
type NFSInstancePersistentVolumeSource struct {
	// Server 是 NFS 服务器的主机名或 IP 地址。
	//
	// 创建后不可变。
	//
	// +required
	Server string `json:"server" protobuf:"bytes,1,name=server"`

	// Share 是服务器上的 NFS 共享路径。
	// 对于每个 InstancePersistentVolume，
	// 都会在该 NFS 共享路径下创建对应的子路径。
	//
	// 创建后不可变。
	//
	// +default="/"
	// +k8s:validation:pattern="^(/[^/]+)+$"
	// +k8s:validation:maxLength=1024
	Share string `json:"share,omitempty" protobuf:"bytes,2,opt,name=share"`

	// SubDirectory 是 NFS 共享路径下为每个 InstancePersistentVolume 创建的子目录。
	// 若为空，则子目录名称与 InstancePersistentVolume 的卷 ID 相同。
	//
	// 支持指定固定字符串或以下模板变量：
	// - `${pvc.metadata.name}`：对应 PersistentVolumeClaim 的名称。
	// - `${pvc.metadata.namespace}`：对应 PersistentVolumeClaim 的命名空间。
	// - `${pv.metadata.name}`：对应 Kubernetes PersistentVolume 的名称。
	//
	// 例如，指定 `${pvc.metadata.namespace}/${pvc.metadata.name}` 可在 NFS 共享路径下
	// 为每个 InstancePersistentVolume 创建以其对应 PersistentVolumeClaim 命名空间和名称命名的子目录。
	//
	// 创建后不可变。
	SubDirectory string `json:"subDirectory,omitempty" protobuf:"bytes,3,opt,name=subDirectory"`

	// MountPermissions 是挂载目录的权限设置。
	// 若为非零值，将在挂载完成后使用指定权限执行 chmod。
	MountPermissions string `json:"mountPermissions,omitempty" protobuf:"bytes,4,opt,name=mountPermissions"`

	// MountOptions 是 NFS 共享的挂载选项。
	//
	// +default=["hard","vers=4","rsize=1048576","wsize=1048576","noatime","nodiratime"]
	MountOptions []string `json:"mountOptions,omitempty" protobuf:"bytes,5,opt,name=mountOptions"`
}

// S3InstancePersistentVolumeSource 定义 S3 存储源配置。
type S3InstancePersistentVolumeSource struct {
	// Endpoint 是兼容 S3 的对象存储服务端点地址。
	//
	// +required
	Endpoint string `json:"endpoint" protobuf:"bytes,1,name=endpoint"`

	// Region 是兼容 S3 的对象存储服务所属区域。
	// 对于 AWS，生成兼容 S3 对象存储服务的预签名 URL 时必须提供该区域。
	Region string `json:"region,omitempty" protobuf:"bytes,2,opt,name=region"`

	// Insecure 表示是否使用非安全连接访问兼容 S3 的对象存储服务。
	// 若为 true，连接时将跳过验证该对象存储服务的 TLS 证书。
	Insecure bool `json:"insecure,omitempty" protobuf:"varint,3,opt,name=insecure"`

	// AccessKey 是兼容 S3 的对象存储服务的访问密钥（Access Key）。
	//
	// 该字段为只写输入，在创建或更新操作中必填。
	AccessKey string `json:"accessKey,omitempty" protobuf:"bytes,4,opt,name=accessKey"`

	// SecretKey 是兼容 S3 的对象存储服务的私钥（Secret Key）。
	//
	// 该字段为只写输入，在创建或更新操作中必填。
	SecretKey string `json:"secretKey,omitempty" protobuf:"bytes,5,opt,name=secretKey"`

	// Bucket 是兼容 S3 的对象存储服务中的存储桶（Bucket）名称。
	// 若为空，对于每个 InstancePersistentVolume，
	// 将在兼容 S3 的对象存储服务中创建一个与该 InstancePersistentVolume 卷 ID 同名的存储桶。
	//
	// 创建后不可变。
	Bucket string `json:"bucket,omitempty" protobuf:"bytes,6,opt,name=bucket"`

	// Prefix 是存储桶中为每个 InstancePersistentVolume 分配的前缀。
	// 若为空，则前缀与 InstancePersistentVolume 的卷 ID 相同。
	//
	// 支持指定固定字符串或以下模板变量：
	// - `${pvc.metadata.name}`：对应 PersistentVolumeClaim 的名称。
	// - `${pvc.metadata.namespace}`：对应 PersistentVolumeClaim 的命名空间。
	// - `${pv.metadata.name}`：对应 Kubernetes PersistentVolume 的名称。
	//
	// 例如，指定 `${pvc.metadata.namespace}/${pvc.metadata.name}` 可在存储桶中
	// 为每个 InstancePersistentVolume 创建以其对应 PersistentVolumeClaim 命名空间和名称组成的前缀。
	//
	// 创建后不可变。
	Prefix string `json:"prefix,omitempty" protobuf:"bytes,7,opt,name=prefix"`

	// Mounter 是兼容 S3 的对象存储服务的挂载器。
	// 用于为 [GeeseFS](https://github.com/yandex-cloud/geesefs) 指定挂载器。
	//
	// 创建后不可变。
	//
	// +default="geesefs"
	//
	Mounter string `json:"mounter,omitempty" protobuf:"bytes,8,opt,name=mounter"`

	// MountOptions 是 [GeeseFS](https://github.com/yandex-cloud/geesefs) 的挂载选项。
	//
	// 创建后不可变。
	//
	// 大文件高并发写入场景：
	//   关闭校验和以降低 CPU 开销、降低刷新频率、最大化并发度，
	//   并减小分片大小以提升大文件写入性能。
	//   ["--no-checksum","--memory-limit=4000","--max-flushers=32","--max-parallel-parts=32","--part-sizes=25"]
	//
	// 大文件顺序读取场景：
	//   增大预读窗口和预读并发度，
	//   并提高内存缓存上限以提升大文件读取性能。
	//   ["--read-ahead-large=200000"," --large-read-cutoff=10240","--read-ahead-parallel=40000","--memory-limit=8000"]
	//
	// 小文件随机读取场景：
	//   减小预读大小、延长元数据缓存 TTL，
	//   并提高条目上限以提升小文件读取性能。
	//   ["--read-ahead-small=64","--small-read-cutoff=64","--read-ahead=1024","--stat-cache-ttl=300s","--entry-limit=200000"]
	//
	// 写入高可用场景：
	//   增加重试次数并在关闭文件时启用 fsync，以提高写入数据的持久性。
	//   ["--sdk-max-retries=10","--read-retry-attempts=5","--fsync-on-close","--cache=/mnt/disk-cache]
	//
	// 非 Yandex 的兼容 S3 对象存储服务：
	//   ["--list-type=2","--no-specials"]
	//
	// +default=["--no-checksum","--memory-limit=4000","--max-flushers=32","--max-parallel-parts=32","--part-sizes=25","--list-type=2","--no-specials"]
	MountOptions []string `json:"mountOptions,omitempty" protobuf:"bytes,9,name=mountOptions"`
}

// InstancePersistentVolumeTypeList 包含 InstancePersistentVolumeType 列表。
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InstancePersistentVolumeTypeList struct {
	meta.TypeMeta `json:",inline"`
	meta.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,name=metadata"`

	Items []InstancePersistentVolumeType `json:"items" protobuf:"bytes,2,name=items"`
}

var _ runtime.Object = (*InstancePersistentVolumeTypeList)(nil)
