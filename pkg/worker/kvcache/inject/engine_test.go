package inject

// testConnectionFor 是 testConnection 的变体，其传输方式符合推理引擎存储后端的要求。
//
// 将推理引擎与它拒绝处理的传输方式组合在一起，并非较弱的情况，而是不可能的配置：Render 会拒绝这种组合，因此
// 基于这种配置构建的案例，所测量的内容只是对该拒绝行为的反映，而非原本想要测量的内容。所有改变推理引擎的案
// 例，其连接方式都来自这里。
//
// 传输数据是从表中读取出来的，而不是作为字面量来写的，这是有意为之的，且出于一个值得提及的原因是安全的：使
// 用此辅助函数时，不会对传输规则进行任何断言。它们会断言车辆、连接器名称、角色和租户信息，并且需要一种不会
// 自我拒绝的连接方式。该规则在 TestCheckTransport 中被绑定在字面量上，若从表中推导出来，则会导致检查出现循环。
func testConnectionFor(engine Engine) Connection {
	conn := testConnection()
	if required := engineTransportConstraint[engine].Required; required != "" {
		conn.Protocol = required
	}
	return conn
}
