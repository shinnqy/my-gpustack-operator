// Package gc 将节点的模型缓存控制在高低水位线之间：只清理未被任何 Pod 引用的内容，
// 按最久未使用顺序回收，并在新下载开始前预留空间。
package gc

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"gpustack.ai/gpustack/pkg/modelmanager/download"
	"gpustack.ai/gpustack/pkg/modelmanager/metrics"
	"gpustack.ai/gpustack/pkg/modelmanager/store"
)

// 垃圾回收的默认时间参数。
const (
	// DefaultGrace 在目录树最后一条引用移除后保留一段宽限期，便于重建中的 Pod 直接命中缓存。
	DefaultGrace = 10 * time.Minute
	// DefaultPartialTTL 决定无人请求的半成品尝试目录在磁盘上保留多久以供断点续传。
	DefaultPartialTTL = 24 * time.Hour
)

// Usage 表示文件系统的总容量与已使用字节数。
type Usage struct {
	Total uint64
	Used  uint64
}

// Percent 返回已用空间占总容量的百分比。
func (u Usage) Percent() float64 {
	if u.Total == 0 {
		return 0
	}
	return float64(u.Used) / float64(u.Total) * 100
}

// Collector 负责回收缓存中允许删除的内容。
type Collector struct {
	Store *store.Store
	// Usage 读取缓存所在文件系统的当前使用情况。
	Usage func() (Usage, error)
	// Watermarks 返回当前生效的高低水位百分比，或无法执行回收的原因：尚未应用配置，
	// 或节点配置校验未通过。两种情况下均跳过回收，避免按未生效的水位线误删数据。
	Watermarks func() (high, low int32, unavailable string)
	// Referenced 和 Downloading 返回必须保留的摘要集合。引用读取失败时必须返回错误而非空集合：
	// 返回空集合会导致所有正在挂载的目录树被视为可回收对象。
	Referenced  func() (map[string]bool, error)
	Downloading func() map[string]bool
	// Pinned 返回节点配置要求固定保留的摘要集合：永不作为回收候选，
	// 但仍计入文件系统使用率，以真实反映磁盘饱和状态。为 nil 时表示无固定项。
	Pinned func() map[string]bool
	Now    func() time.Time
	Grace  time.Duration
	// PartialTTL 指定无人请求的尝试目录保留用于续传的时长。
	PartialTTL time.Duration

	// mu 保护 reservations；collectMu 防止多轮回收同时重叠执行。
	mu        sync.Mutex
	collectMu sync.Mutex
	// reservations 记录所有正在运行下载的磁盘空间预留。
	reservations map[*reservation]struct{}
}

// reservation 表示一个进行中下载对文件系统的空间预留：总量为 size，其中 written 已落盘并计入文件系统使用量。
type reservation struct {
	size    int64
	written func() int64
}

type reserveCheck struct {
	release  func()
	bytes    int64
	incoming int64
	usage    Usage
	high     int32
}

// Result 记录单轮垃圾回收的结果。
type Result struct {
	RemovedBytes int64
	// CapacityLow 表示使用率仍高于高水位且已无可回收内容。
	CapacityLow bool
	Usage       Usage
	// Skipped 记录本轮未考量已发布目录树的原因，正常扫描时为空字符串。
	Skipped string
}

// Collect 清理过期的半成品尝试目录；当使用率超过高水位时，按最久未使用顺序移除已过宽限期的无引用目录树，
// 直到使用率降至低水位或以下。绝不会删除仍有引用的目录树或正在运行尝试的文件。
func (c *Collector) Collect(ctx context.Context) (Result, error) {
	c.collectMu.Lock()
	defer c.collectMu.Unlock()

	c.mu.Lock()
	incoming := c.inFlight()
	c.mu.Unlock()

	return c.collect(ctx, incoming)
}

// Reserve 为总大小为 sizeBytes、当前已在磁盘写入 written() 字节的下载预留空间，
// 并持续占用该预留直到调用方调用返回的 release。本次下载尚未写入的剩余字节连同其他所有运行中预留的剩余字节
// 必须整体落在高水位之下：避免多个单独能放下但叠加超限的下载同时获准启动。
// 若当前放不下则会触发一次回收；回收后仍不足则返回 InsufficientCapacity 错误。
func (c *Collector) Reserve(ctx context.Context, hex string, sizeBytes int64, written func() int64) (func(), error) {
	check, err := c.tryReserve(sizeBytes, written)
	if err != nil || check.release != nil {
		return check.release, err
	}

	c.collectMu.Lock()
	defer c.collectMu.Unlock()
	check, err = c.tryReserve(sizeBytes, written)
	if err != nil || check.release != nil {
		return check.release, err
	}
	res, err := c.collect(ctx, check.incoming)
	if err != nil {
		return nil, err
	}

	check, err = c.tryReserve(sizeBytes, written)
	if err != nil || check.release != nil {
		return check.release, err
	}
	// 节点自身的容量数据不涉及任何租户信息，整条消息可作为公开 Detail。
	if res.Skipped != "" {
		return nil, nodeCapacityError(fmt.Sprintf("%d bytes of sha256:%s do not fit under the high watermark of %d%%, "+
			"and collection was skipped: %s", check.bytes, hex[:12], check.high, res.Skipped))
	}
	return nil, nodeCapacityError(fmt.Sprintf("%d bytes of sha256:%s do not fit under the high watermark of %d%%: "+
		"the cache's filesystem is %.1f%% used of %d bytes with %d more reserved by running downloads, "+
		"and nothing unreferenced is left to remove",
		check.bytes, hex[:12], check.high, check.usage.Percent(), check.usage.Total, check.incoming-check.bytes))
}

// tryReserve 在持有 c.mu 时检查容量，空间充足则立即登记预留。
func (c *Collector) tryReserve(sizeBytes int64, written func() int64) (reserveCheck, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var check reserveCheck
	var unavailable string
	check.high, _, unavailable = c.Watermarks()
	if unavailable != "" {
		return check, &download.Error{Reason: download.ReasonInvalidRequest, Message: unavailable, Detail: unavailable}
	}
	check.bytes = max(sizeBytes-written(), 0)
	check.incoming = check.bytes + c.inFlight()
	var err error
	check.usage, err = c.Usage()
	if err != nil {
		return check, err
	}
	if fits(check.usage, check.incoming, check.high) {
		check.release = c.addReservation(sizeBytes, written)
	}

	return check, nil
}

// addReservation 在持有 c.mu 时登记一条空间预留。
func (c *Collector) addReservation(sizeBytes int64, written func() int64) func() {
	r := &reservation{size: sizeBytes, written: written}
	if c.reservations == nil {
		c.reservations = map[*reservation]struct{}{}
	}
	c.reservations[r] = struct{}{}

	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.reservations, r)
	}
}

// inFlight 计算所有运行中预留尚未写入磁盘的字节总和。调用方须已持有 c.mu。
func (c *Collector) inFlight() int64 {
	var n int64
	for r := range c.reservations {
		n += max(r.size-r.written(), 0)
	}

	return n
}

func nodeCapacityError(msg string) error {
	return &download.Error{Reason: download.ReasonInsufficientCapacity, Message: msg, Detail: msg}
}

func fits(u Usage, bytes int64, high int32) bool {
	if u.Total == 0 {
		return false
	}

	return float64(u.Used+uint64(max(bytes, 0)))/float64(u.Total)*100 <= float64(high) // nolint: gosec
}

func (c *Collector) collect(ctx context.Context, incoming int64) (Result, error) {
	var res Result
	now := c.Now()

	// 没有生效水位线时不删除任何内容，连过期半成品也不删：决定节点保留策略的配置尚未生效。
	high, low, unavailable := c.Watermarks()
	if unavailable != "" {
		u, err := c.Usage()
		if err != nil {
			return res, err
		}
		res.Usage, res.Skipped = u, unavailable
		return c.done(res, 0), nil
	}

	partials, err := c.Store.Partials()
	if err != nil {
		return res, err
	}
	// 在列出半成品后以及每次删除前都重新读取正在下载的集合，避免把期间新启动的尝试当成闲置目录删掉。
	for _, p := range partials {
		if now.Sub(p.ModTime) < c.partialTTL() || c.Downloading()[p.Hex] {
			continue
		}
		if err := c.Store.RemovePartial(p.Hex, p.Attempt); err != nil {
			return res, err
		}
		c.removed(&res, p.SizeBytes)
	}

	if err := c.pruneRecords(now); err != nil {
		return res, err
	}

	u, err := c.Usage()
	if err != nil {
		return res, err
	}
	res.Usage = u
	if fits(u, incoming, high) {
		return c.done(res, high), nil
	}

	// 引用无法读取时跳过本轮回收而非令整体失败：状态上报仍可执行，并说明未清理任何内容的原因。
	referenced, refErr := c.Referenced()
	if res.Skipped = unreadableReferences(refErr); res.Skipped != "" {
		return c.done(res, high), nil
	}
	candidates, err := c.candidates(now, referenced)
	if err != nil {
		return res, err
	}
	for _, cand := range candidates {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if fits(res.Usage, incoming, low) {
			break
		}
		if c.Downloading()[cand.hex] || (c.Pinned != nil && c.Pinned()[cand.hex]) {
			continue
		}
		rec, err := c.Store.ReadDigest(cand.hex)
		if err != nil {
			return res, err
		}
		lastUsed := rec.LastUsedTime
		if lastUsed.IsZero() {
			lastUsed = cand.lastUsed
		}
		if c.Now().Sub(lastUsed) < c.grace() {
			continue
		}
		// Store 在与挂载 Claim 相同的锁下复核实时引用，避免误删刚被挂载的目录树。
		removed, refErr := c.Store.RemoveUnreferenced(cand.hex)
		if res.Skipped = unreadableReferences(refErr); res.Skipped != "" {
			break
		}
		if !removed {
			continue
		}
		c.removed(&res, cand.size)
		if res.Usage, err = c.Usage(); err != nil {
			return res, err
		}
	}

	return c.done(res, high), nil
}

// pruneRecords 清理不再对应节点上任何内容的失败记录：摘要既未发布、磁盘上也无半成品、也没有在途下载，
// 且最近一次重试时间已超过半成品 TTL。若不清理，历史上每次失败的摘要都会永久挂在 Failed 列表中。
func (c *Collector) pruneRecords(now time.Time) error {
	records, err := c.Store.Digests()
	if err != nil {
		return err
	}
	partials, err := c.Store.Partials()
	if err != nil {
		return err
	}
	onDisk := map[string]bool{}
	for _, p := range partials {
		onDisk[p.Hex] = true
	}
	for hex, rec := range records {
		if rec.Reason == "" || now.Sub(rec.RetryTime) < c.partialTTL() || onDisk[hex] || c.Store.IsPublished(hex) ||
			c.Downloading()[hex] {
			continue
		}
		if err := c.Store.RemoveDigest(hex); err != nil {
			return err
		}
	}

	return nil
}

// unreadableReferences 在无法读取引用时返回跳过回收的原因说明，无错误时返回 ""。
func unreadableReferences(err error) string {
	if err == nil {
		return ""
	}
	return "the references cannot be read, so no tree is known to be unmounted: " + err.Error()
}

func (c *Collector) done(res Result, high int32) Result {
	res.CapacityLow = res.Skipped == "" && res.Usage.Percent() > float64(high)

	return res
}

// removed 在删除发生时立即累计回收字节数，即使本轮后续出错也能保留已发生的统计。
func (*Collector) removed(res *Result, bytes int64) {
	res.RemovedBytes += bytes
	metrics.GCRemovedBytes.Add(float64(bytes))
}

type candidate struct {
	hex      string
	size     int64
	lastUsed time.Time
}

// candidates 筛出无 Pod 引用、未被 pin、已超过宽限期的已发布目录树，并按最近使用时间从旧到新排序。
func (c *Collector) candidates(now time.Time, referenced map[string]bool) ([]candidate, error) {
	markers, err := c.Store.Published()
	if err != nil {
		return nil, err
	}
	downloading := c.Downloading()
	var pinned map[string]bool
	if c.Pinned != nil {
		pinned = c.Pinned()
	}
	var out []candidate
	for _, m := range markers {
		hex := store.HexOf(m.Digest)
		if hex == "" || referenced[hex] || downloading[hex] || pinned[hex] {
			continue
		}
		rec, err := c.Store.ReadDigest(hex)
		if err != nil {
			return nil, err
		}
		lastUsed := rec.LastUsedTime
		if lastUsed.IsZero() {
			lastUsed = m.PublishedTime
		}
		if now.Sub(lastUsed) < c.grace() {
			continue
		}
		out = append(out, candidate{hex: hex, size: m.SizeBytes, lastUsed: lastUsed})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].lastUsed.Before(out[j].lastUsed) })

	return out, nil
}

func (c *Collector) grace() time.Duration {
	if c.Grace > 0 {
		return c.Grace
	}
	return DefaultGrace
}

func (c *Collector) partialTTL() time.Duration {
	if c.PartialTTL > 0 {
		return c.PartialTTL
	}
	return DefaultPartialTTL
}
