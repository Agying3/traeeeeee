# 第三方组件与许可复核

本候选只附带 `vendor/slider` 的现有源文件与完整现有许可；其他组件均作为依赖／接口使用，不分发其安装目录、可执行程序、浏览器缓存、模型或数据库。

| 组件 | 本次查证 | 候选处理／待办 |
| --- | --- | --- |
| Agent2API | 本地 LICENSE 27–32、55–64：MIT 加追加限制，非纯 MIT | 排除源码与二进制；外部 HTTP 适配。辅助库与其实现的来源关系待确认 |
| vendor/slider | 现有 LICENSE 包含 MIT 全文及末尾研究／使用提示 | 原样保留全部文件与提示；确认上游归属和末尾提示的效力 |
| cloakbrowser 0.6.2 wrapper | 已安装 dist-info 随包 LICENSE：MIT | 不附带包装库源码；保留依赖声明 |
| CloakBrowser binary | 官方 BINARY-LICENSE v1.3 July 2026，为单独条款 | 排除二进制／缓存；自动账号创建用途限制及 OEM/SaaS 条件待确认 |
| FastAPI 0.143.0 | 随包 LICENSE：MIT | 仅依赖声明；如后续分发依赖，保留通知 |
| Uvicorn 0.54.0 | 随包 LICENSE.md：BSD-3-Clause | 同上 |
| HTTPX 0.28.1 | 随包 LICENSE.md：BSD-3-Clause | 同上 |
| 其余 requirements 依赖、模型、GeoIP 数据 | 本次尚未逐项完成全依赖树许可复核 | 发布最终清单确定后继续复核，不能视作已整体放行 |

CloakBrowser 官方条款：<https://github.com/CloakHQ/CloakBrowser/blob/main/BINARY-LICENSE.md>。本次核对版本 v1.3（July 2026），Git blob `3a3acbfc9d0081ad4077575a21001361d383c881`。其 Restrictions 涉及浏览器二进制再分发和打包；Cloud／第三方使用段区分用户自行下载依赖与对客户提供浏览器服务；Acceptable Use 包含 `automated account creation` 限制。Python wrapper 的 MIT 不能替代二进制条款。

本项目主许可证尚未指定，第三方许可不自动授予整个项目相同许可。豪猪模块的移植来源、协议辅助库参考关系和自有代码贡献权属仍需确认。这里不重写或删减任何现有第三方版权／许可。
