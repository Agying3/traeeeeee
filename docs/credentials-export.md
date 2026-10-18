# Agent2API 账号批量导出格式

## 默认导出

注册机「凭证库 → 导出全部账号（单个JSON）」现在下载 **Agent2API 原生批量账号文件**。接口为 `GET /api/credentials/export?format=agent2api`；不带参数同样返回原生格式。导出只读取本地凭证，不调用网关、不刷新令牌、不修改账号源文件。

```json
{
  "version": 2,
  "exportedAt": 1800000000000,
  "customProviders": [],
  "accounts": [
    {
      "id": "trae-solo-ACCOUNT_SHA256_PLACEHOLDER",
      "provider": "trae",
      "uid": "UID_PLACEHOLDER",
      "nickname": "NICKNAME_PLACEHOLDER",
      "enterpriseId": "ENTERPRISE_ID_PLACEHOLDER",
      "name": "NICKNAME_PLACEHOLDER",
      "accessToken": "ACCESS_TOKEN_PLACEHOLDER",
      "refreshToken": "REFRESH_TOKEN_PLACEHOLDER",
      "expiresAt": 1800000000000,
      "domain": "DOMAIN_PLACEHOLDER",
      "apiHost": "API_HOST_PLACEHOLDER",
      "machineId": "MACHINE_ID_PLACEHOLDER",
      "deviceId": "DEVICE_ID_PLACEHOLDER",
      "devicePublicKey": "PUBLIC_KEY_PLACEHOLDER",
      "devicePrivateKey": "PRIVATE_KEY_PLACEHOLDER",
      "authClientId": "CLIENT_ID_PLACEHOLDER",
      "variant": "solo",
      "enabled": true,
      "desktop": false,
      "priority": 1,
      "proxy": null,
      "source": "import"
    }
  ],
  "count": 1,
  "summary": {"sourceFiles": 1, "duplicatesRemoved": 0, "skippedFiles": []}
}
```

`version`、`exportedAt`、`customProviders`、`accounts` 与 Agent2API 的文件导出结构一致。`count` 和 `summary` 是注册机附加的导出统计；当前导入器忽略这些额外外层字段。

## 单账号与批量文件入口

Agent2API 的单账号添加 `POST /api/accounts` 和批量文件导入 `POST /api/accounts/import` 是不同协议：

- 单账号添加的 Trae 解析器接受 `{type, provider, auth:{...}, account:{...}}`。
- 批量文件导入要求外层是含 `accounts` 数组的对象；每条直接读取 `id`、`provider`、`uid`、`accessToken` / `refreshToken` 等平铺字段，不会先用单账号解析器解开 `auth`。
- 仅添加外层 `accounts` 仍不够：没有账号级 `id` 会逐条报缺少 id；只有 `auth.accessToken` 仍会逐条报缺少 Token。

## 字段与去重

- 原文件 `auth` 的 Token、设备标识、公私钥、OAuth ClientID、端点字段全部映射到账号外层；`account.uid/nickname/enterpriseId` 同样平铺。未知业务字段保留，不导出内部下划线字段或 `rateLimits`。
- `id` 使用与 Agent2API 添加路径一致的 `trae-{variant}-{sha256(variant + '-' + uid)}`，重复导出稳定不变。
- 去重按 `provider + variant + uid`；同一 UID 的 `solo` / `cn` 两个谱系不会被误合并。
- 到期时刻统一成 Unix 毫秒；兼容旧文件的秒值后再比较新旧凭证。选择到期更晚的完整凭证，平局按修改时间和文件名；不会拼接不同设备的 Token 或密钥。
- 异常文件列在 `summary.skippedFiles`；没有任何可导出账号时返回 404。JSON 内容校验不等同于在线令牌有效性验证。
- 导入使用“账号导入/导出 → 导入账号”文件入口，不是单账号“填写凭证”的 accessToken 文本框。

## 旧格式保留

`GET /api/credentials/export?format=nested` 仍返回原有 `trae-credentials` v1 对象，账号保留 `auth/account` 嵌套，仅供原有自定义接收端使用。默认按钮不再生成这种文件。

文档示例使用占位值，不附带账号导出文件。默认导出为 Agent2API 原生平铺批量格式。

## 数据保存

输出包含访问令牌与设备私钥，应私人保存，不纳入公开源码。Trae 刷新令牌会轮换，两个程序同时接管刷新可能导致旧令牌失效。
