# EventStream 学习笔记

> 位置: `packages/ai/src/utils/event-stream.ts`

## 为什么存在

LLM API 返回的是**流式响应**（SSE/WebSocket），一个 token 一个 token 地到。消费者有两种需求：

1. **逐事件处理** — TUI 要实时渲染每个 delta、tool call、thinking chunk
2. **只要最终结果** — agent loop 在 tool loop 里只需要 `await` 拿到完整消息，不关心中间过程

原生 `AsyncIterable` 只能满足第一种。如果你只想拿最终结果，得自己遍历完整个流再拼出来。`EventStream` 把两种消费方式统一在一个对象上。

## 双接口

```ts
const stream = provider.stream(model, context);

// 接口 1: 逐事件消费（AsyncIterable）
for await (const event of stream) {
  render(event);  // TUI 实时渲染
}

// 接口 2: 直接拿最终结果（Promise）
const message = await stream.result();  // agent loop 用这个
```

## 两种输出对比

### 接口 1：逐事件（for await）

收到一系列 `AssistantMessageEvent`，按时间顺序：

```ts
// 1. 流开始
{ type: "start", partial: { role: "assistant", content: [], ... } }

// 2. 开始一段文本
{ type: "text_start", contentIndex: 0, partial: { ..., content: [] } }

// 3~5. 文本分块到达（每个 delta 是一小段）
{ type: "text_delta", contentIndex: 0, delta: "Hello", partial: { ..., content: [{ type: "text", text: "Hello" }] } }
{ type: "text_delta", contentIndex: 0, delta: ", I'll", partial: { ..., content: [{ type: "text", text: "Hello, I'll" }] } }
{ type: "text_delta", contentIndex: 0, delta: " help you.", partial: { ..., content: [{ type: "text", text: "Hello, I'll help you." }] } }

// 6. 文本结束
{ type: "text_end", contentIndex: 0, content: "Hello, I'll help you.", partial: { ... } }

// 7. 流结束
{ type: "done", reason: "stop", message: { role: "assistant", content: [...], usage: {...}, stopReason: "stop" } }
```

`partial` 是**截至当前的累积快照**，TUI 用它来实时渲染。

### 接口 2：最终结果（.result()）

一个完整的 `AssistantMessage`：

```ts
{
  role: "assistant",
  content: [{ type: "text", text: "Hello, I'll help you." }],
  usage: { inputTokens: 42, outputTokens: 8, ... },
  stopReason: "stop",
  timestamp: 1748822400000,
  model: "claude-sonnet-4-20250514",
  provider: "anthropic",
  api: "anthropic",
}
```

这就是 `done` 事件里的 `message` 字段 — 所有 delta 已经合并好的完整消息。

### 对比总结

| | 接口 1 (for await) | 接口 2 (.result()) |
|---|---|---|
| **拿到什么** | 多个事件，逐个到达 | 一个 AssistantMessage |
| **什么时候拿到** | 实时，每个事件立刻到 | 流结束后一次性 |
| **谁用** | TUI（实时渲染 UI） | Agent loop（只关心最终回复） |
| **内容** | delta、partial 快照 | 拼好的完整 content |

## 实现拆解

### 泛型参数

```ts
class EventStream<T, R = T>
```

- `T` = 迭代产出的事件类型（如 `AssistantMessageEvent`）
- `R` = 最终结果类型（如 `AssistantMessage`），默认等于 `T`

### 核心数据流

```
生产者 push(event)          消费者 for-await
      |                          |
      v                          v
  有 waiter 在等？           队列有数据？
   ├── 是 → 直接给 waiter      ├── 是 → yield 出去
   └── 否 → 入 queue           ├── done？→ return
                               └── 否 → 注册 waiter，等 push 唤醒
```

- **queue**: 缓冲队列，生产快于消费时暂存数据
- **waiting**: 等待回调列表，消费快于生产时 consumer 挂起

### finalResultPromise

构造时创建一个 Promise，把 `resolve` 存到 `resolveFinalResult`：

```ts
constructor() {
  this.finalResultPromise = new Promise((resolve) => {
    this.resolveFinalResult = resolve;  // 只是存储，还没调用
  });
}
```

`resolve` 在两个时机被调用：

1. `push(event)` 收到终止事件（`isComplete(event)` 为 true）→ 用 `extractResult(event)` 提取结果后 resolve
2. `end(result)` 外部直接传入结果 → resolve

**时序图**：

```
[构造]              [push(delta)]  [push(delta)]  [push(done)]
  │                      │              │              │
  ▼                      ▼              ▼              ▼
创建 Promise          普通事件        普通事件      终止事件
存 resolve            入队列          入队列        isComplete=true
Promise pending       给消费者        给消费者        ↓
                                              调用 resolveFinalResult(result)
                                                ↓
                                              Promise fulfilled
                                                ↓
                                        await stream.result() 拿到值
```

### isComplete + extractResult — 策略注入

通过构造函数注入两个函数，让 `EventStream<T, R>` 不关心具体事件类型：

```ts
// AssistantMessageEventStream 的具体策略：
isComplete:    event.type === "done" || event.type === "error"
extractResult: done → event.message,  error → event.error
```

### 注意点

- `result()` 是自定义方法，不是 JS 内置的
- `end()` 如果不传 `result`（即 `undefined`），`resolveFinalResult` 不会被调用，`.result()` 的 Promise 会永远 pending — 这是有意的设计：`end()` 只是关闭迭代器，不保证有最终结果
- `resolveFinalResult` 本质就是 Promise 的 `resolve`，但它 resolve 的值经过 `extractResult` 提取，语义上是"流的最终结果"

## 使用场景

- **Provider 层**: 每个 provider（Anthropic、OpenAI 等）创建 `AssistantMessageEventStream`，调用 `push()` 推送解析后的事件
- **Agent loop**: `await response.result()` 拿最终 `AssistantMessage` 做 tool loop
- **Compaction**: `return stream.result()` 直接返回 Promise
- **TUI**: `for await (const event of stream)` 逐事件渲染 UI
