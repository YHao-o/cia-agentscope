# OpenSandbox 集群部署手册 v3.0

> 基于 v2.0 修订：镜像已预推送至内网仓库 `registry.ccdc.cn:5000`，部署文件统一放在 `/app/docker/nfs/opensandbox`，补充 imagePullSecret 关联的关键坑点与完整验证流程。

---

## 1. 架构说明

OpenSandbox 0.2.x 集群版至少包含以下组件：

```text
┌─────────────┐     ┌─────────────────────┐     ┌──────────────┐
│   Client    │────▶│  OpenSandbox Server │────▶│  BatchSandbox│
│  (SDK/curl) │     │   (API Gateway)     │     │     CRD      │
└─────────────┘     └─────────────────────┘     └──────┬───────┘
                                                       │
                              ┌───────────────────────┘
                              ▼
                    ┌─────────────────────┐
                    │ Controller Manager  │────▶ 创建/管理沙箱 Pod
                    └─────────────────────┘
```

| 组件 | 是否必须 | 说明 |
| --- | --- | --- |
| CRDs | 是 | `batchsandboxes`、`pools`、`sandboxsnapshots` |
| Server | 是 | API 网关，接收 SDK/客户端请求 |
| Controller Manager | 是 | 监听 CRD，创建实际沙箱 Pod |
| Ingress Gateway | 否 | 直连模式需要，Server Proxy 模式不需要 |
| BatchSandbox 模板 | 是 | Server 创建 CR 时使用 |

---

## 2. 前置条件

- Kubernetes >= 1.24
- kubectl 已配置集群访问
- 节点可用内存 >= 4Gi（每个沙箱 Pod 128Mi–512Mi）
- 内网镜像仓库 `registry.ccdc.cn:5000` 可访问，且镜像已推送完毕
- 镜像仓库认证信息（`admin` / `<password>`）

---

## 3. 镜像仓库说明

### 3.1 镜像已预推送

本次部署使用的镜像**已全部推送至内网仓库 `registry.ccdc.cn:5000`**，无需再执行拉取/推送操作。完整清单如下：

| 镜像地址 | 用途 | 大小 | 备注 |
| --- | --- | --- | --- |
| `registry.ccdc.cn:5000/opensandbox/server:v0.2.2` | Server API 网关 | ~200MB | 必须 |
| `registry.ccdc.cn:5000/opensandbox/controller:v0.2.0` | Controller Manager | ~150MB | 必须 |
| `registry.ccdc.cn:5000/opensandbox/ingress:v1.0.10` | Ingress Gateway | ~100MB | 可选 |
| `registry.ccdc.cn:5000/opensandbox/execd:v1.0.21` | 沙箱内执行代理 | ~50MB | 必须 |
| `registry.ccdc.cn:5000/opensandbox/egress:v1.1.5` | 出口网关 | ~100MB | 必须 |
| `registry.ccdc.cn:5000/opensandbox/image-committer:v0.1.1` | 镜像提交 Job | ~100MB | 必须 |
| `registry.ccdc.cn:5000/opensandbox/code-interpreter:v1.1.0` | Code Interpreter | ~7GB | 必须 |
| `registry.ccdc.cn:5000/library/python:3.13-slim` | 沙箱基础镜像 | ~150MB | 必须 |

### 3.2 验证镜像在仓库中存在（可选）

```bash
# 登录仓库
docker login registry.ccdc.cn:5000 -u admin -p <password>

# 拉取一个镜像验证连通性与认证
docker pull registry.ccdc.cn:5000/opensandbox/server:v0.2.2
```

> **注意**：在节点上手动 `docker login` + `docker pull` 成功，**不代表** kubelet 能拉取镜像。kubelet 通过 CRI（containerd）拉镜像，**不读取** docker 的凭据，只认 Pod/ServiceAccount 中声明的 `imagePullSecrets`。详见第 4.4 节。

---

## 4. K8s 部署

### 4.1 部署文件目录

所有部署 YAML 文件统一放在 `/app/docker/nfs/opensandbox`：

```bash
cd /app/docker/nfs/opensandbox
ls -l
```

预期文件：

```text
crds.yaml
namespace.yaml
rbac.yaml
api-key-secret.yaml
server-config.yaml
controller-manager-deployment.yaml
ingress-gateway-service.yaml
ingress-gateway-deployment.yaml
server-deployment.yaml
server-nodeport-service.yaml
install.sh
```

### 4.2 创建命名空间

```bash
kubectl apply -f /app/docker/nfs/opensandbox/namespace.yaml
```

### 4.3 安装 CRDs

```bash
kubectl apply -f /app/docker/nfs/opensandbox/crds.yaml
```

验证：

```bash
kubectl get crd | grep sandbox.opensandbox.io
```

预期输出包含：

```text
batchsandboxes.sandbox.opensandbox.io
pools.sandbox.opensandbox.io
sandboxsnapshots.sandbox.opensandbox.io
```

### 4.4 配置 API Key

API Key 已在 `api-key-secret.yaml` 中预填，直接 apply：

```bash
kubectl apply -f /app/docker/nfs/opensandbox/api-key-secret.yaml
```

### 4.5 配置 imagePullSecret（私有仓库必须）

#### 4.5.1 创建 Secret

```bash
kubectl create secret docker-registry opensandbox-image-pull-secret \
  --docker-server=registry.ccdc.cn:5000 \
  --docker-username=admin \
  --docker-password=<password> \
  -n opensandbox-system
```

#### 4.5.2 关键说明：Secret 不会自动生效

`kubectl create secret docker-registry` **只是创建了一个 Secret 对象**，不会自动关联到任何 Pod。必须显式声明 `imagePullSecrets`，Pod 才能用它拉镜像。

本交付包的 `rbac.yaml` 已在三个 ServiceAccount 中预挂载 `opensandbox-image-pull-secret`：

```yaml
# rbac.yaml 中已包含（无需修改）
apiVersion: v1
kind: ServiceAccount
metadata:
  name: opensandbox-controller-manager
  namespace: opensandbox-system
imagePullSecrets:
- name: opensandbox-image-pull-secret
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: opensandbox-ingress-gateway
  namespace: opensandbox-system
imagePullSecrets:
- name: opensandbox-image-pull-secret
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: opensandbox-server
  namespace: opensandbox-system
imagePullSecrets:
- name: opensandbox-image-pull-secret
```

三个 Deployment 分别使用对应的 ServiceAccount：

| Deployment | ServiceAccount |
| --- | --- |
| `opensandbox-controller-manager` | `opensandbox-controller-manager` |
| `opensandbox-ingress-gateway` | `opensandbox-ingress-gateway` |
| `opensandbox-server` | `opensandbox-server` |

因此，**只要创建了名为 `opensandbox-image-pull-secret` 的 Secret，三个组件的 Pod 都会自动继承并使用它拉镜像**，无需在每个 Deployment 中单独配置。

> **注意事项**：
> - Secret 名称**必须**是 `opensandbox-image-pull-secret`，与 `rbac.yaml` 中三个 SA 的 `imagePullSecrets.name` 一致。
> - `--docker-server` 必须是 `registry.ccdc.cn:5000`（带端口），与镜像地址中的 registry 完全一致。
> - 若镜像仓库为公开仓库，不创建此 Secret 不影响拉取。

#### 4.5.3 验证 Secret 与 ServiceAccount 关联

```bash
# 验证 Secret 存在且内容正确
kubectl get secret opensandbox-image-pull-secret -n opensandbox-system -o jsonpath='{.data.\.dockerconfigjson}' | base64 -d
# 预期 .auths 中包含 "registry.ccdc.cn:5000"

# 验证三个 ServiceAccount 都挂载了该 Secret
kubectl get sa -n opensandbox-system -o custom-columns='NAME:.metadata.name,IMAGE_PULL_SECRETS:.imagePullSecrets[*].name'
# 预期三个 SA 的 IMAGE_PULL_SECRETS 列均为 opensandbox-image-pull-secret
```

### 4.6 配置 Server ConfigMap

```bash
kubectl apply -f /app/docker/nfs/opensandbox/server-config.yaml
```

`server-config.yaml` 中关键配置（已配置为内网仓库地址）：

```toml
[runtime]
execd_image = "registry.ccdc.cn:5000/opensandbox/execd:v1.0.21"

[egress]
image = "registry.ccdc.cn:5000/opensandbox/egress:v1.1.5"

[ingress]
mode = "gateway"
gateway.address = "opensandbox-ingress-gateway.opensandbox-system.svc"
gateway.route.mode = "header"
```

> **注意**：`gateway.address` 不可为空，否则 Server 启动会因校验失败而崩溃。

### 4.7 配置 RBAC

```bash
kubectl apply -f /app/docker/nfs/opensandbox/rbac.yaml
```

> **注意**：CRD 的 apiGroup 是 `sandbox.opensandbox.io`，不是 `opensandbox.io`。

### 4.8 部署组件

```bash
kubectl apply -f /app/docker/nfs/opensandbox/controller-manager-deployment.yaml
kubectl apply -f /app/docker/nfs/opensandbox/ingress-gateway-service.yaml
kubectl apply -f /app/docker/nfs/opensandbox/ingress-gateway-deployment.yaml
kubectl apply -f /app/docker/nfs/opensandbox/server-deployment.yaml
kubectl apply -f /app/docker/nfs/opensandbox/server-nodeport-service.yaml
```

各 Deployment 中的镜像地址已配置为 `registry.ccdc.cn:5000`。

### 4.9 一键安装

也可以使用 `install.sh` 一键完成上述所有步骤：

```bash
cd /app/docker/nfs/opensandbox
bash install.sh
```

`install.sh` 内容：

```bash
#!/bin/bash
set -e

cd /app/docker/nfs/opensandbox

kubectl apply -f crds.yaml
kubectl apply -f namespace.yaml
kubectl apply -f rbac.yaml
kubectl apply -f api-key-secret.yaml
kubectl apply -f server-config.yaml
kubectl apply -f controller-manager-deployment.yaml
kubectl apply -f ingress-gateway-service.yaml
kubectl apply -f ingress-gateway-deployment.yaml
kubectl apply -f server-deployment.yaml
kubectl apply -f server-nodeport-service.yaml

echo ""
echo "==> 等待组件就绪..."
kubectl rollout status deployment/opensandbox-controller-manager -n opensandbox-system --timeout=300s
kubectl rollout status deployment/opensandbox-server -n opensandbox-system --timeout=300s

echo ""
echo "==> OpenSandbox 部署完成"
```

---

## 5. 验证

### 5.1 组件状态检查

```bash
# 查看所有系统组件 Pod 状态
kubectl get pods -n opensandbox-system
```

**预期**：三个 Pod 均为 `Running` 且 `READY 1/1`。

```text
NAME                                            READY   STATUS    RESTARTS   AGE
opensandbox-controller-manager-xxxxxxxxx-xxxxx   1/1     Running   0          2m
opensandbox-ingress-gateway-xxxxxxxxx-xxxxx      1/1     Running   0          2m
opensandbox-server-xxxxxxxxx-xxxxx               1/1     Running   0          2m
```

### 5.2 验证 imagePullSecrets 已关联

部署完成后，确认每个 Pod 实际挂载的 imagePullSecrets：

```bash
# 查看每个 Deployment 的 imagePullSecrets
kubectl get deploy -n opensandbox-system -o custom-columns='NAME:.metadata.name,SA:.spec.template.spec.serviceAccountName,IPS:.spec.template.spec.imagePullSecrets[*].name'
```

**预期**：
- SA 列分别为 `opensandbox-controller-manager`、`opensandbox-ingress-gateway`、`opensandbox-server`
- IPS 列为空（因为通过 ServiceAccount 继承，不需要在 Deployment 中显式声明）

或者直接查看 Pod 实际生效的 imagePullSecrets：

```bash
kubectl get pod -n opensandbox-system -o custom-columns='NAME:.metadata.name,IPS:.spec.imagePullSecrets[*].name'
```

**预期**：每个 Pod 的 IPS 列均为 `opensandbox-image-pull-secret`。

### 5.3 CRD 检查

```bash
kubectl get crd | grep sandbox.opensandbox.io
```

预期输出包含三个 CRD。

### 5.4 Server 健康检查

通过 NodePort 30097 直接访问（无需 port-forward）：

```bash
# 获取节点 IP
hostname -I

# 用节点 IP 访问（不要用 localhost）
curl http://<节点IP>:30097/health
```

**预期**：`{"status":"healthy"}`

> **注意**：NodePort 默认不监听 `127.0.0.1`，必须用节点的实际 IP 访问。`hostname -I` 可获取当前节点 IP。

### 5.5 SDK 创建沙箱验证

```bash
export OPENSANDBOX_DOMAIN="opensandbox-server.opensandbox-system.svc:80"
export OPENSANDBOX_API_KEY="<api-key-secret.yaml 中配置的 API Key>"
export OPENSANDBOX_PROTOCOL="http"
export OPENSANDBOX_USE_SERVER_PROXY="true"
export OPENSANDBOX_IMAGE="registry.ccdc.cn:5000/library/python:3.13-slim"
```

```bash
python -c "
import asyncio
from opensandbox import AsyncSandbox
from opensandbox import ConnectionConfig

async def main():
    config = ConnectionConfig(
        domain='opensandbox-server.opensandbox-system.svc:80',
        api_key='<api-key-secret.yaml 中配置的 API Key>',
        use_server_proxy=True
    )
    sbx = await AsyncSandbox.create(config=config)
    print('sandbox id:', sbx.sandbox_id)
    result = await sbx.commands.run('env')
    print(result.stdout)
    await sbx.kill()

asyncio.run(main())
"
```

成功输出环境变量即表示 OpenSandbox 可正常使用。
