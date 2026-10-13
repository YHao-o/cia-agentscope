# cia-agentscope Helm Chart

该 Chart 只部署 `cia-agentscope` 应用，不创建 MySQL、Redis 或 OpenSandbox。应用通过 `ClusterIP` 暴露 `7010` 端口，默认单副本，并把所有节点共同挂载的 `/app/dify/sfs` 挂到容器内 `/data/session-files`。

## 前置条件

- 内网部署机已配置 `kubectl` 和 Helm 3，并可访问目标 Kubernetes 集群；
- 应用镜像已推送到 `registry.ccdc.cn:5000`；
- 集群可以解析并访问现有 MySQL、Redis；
- MySQL 账号可以连接目标库，并具备应用启动时初始化表所需的 DDL/DML 权限；
- 所有可能调度应用 Pod 的节点都已把同一个 SFS 挂载到 `/app/dify/sfs`；
- 集群支持 `startupProbe`；
- 已准备包含 `MYSQL_PASSWORD` 和 `JWT_SECRET` 的 Kubernetes Secret。

OpenSandbox 不属于本 Chart。部署前保持 `opensandbox.poolSize: 0`，依赖沙箱的功能要等 OpenSandbox 独立部署并配置连接地址后才能使用。

## 1. 构建和推送应用镜像

在包含 Dockerfile 和离线依赖包的项目根目录执行：

```bash
export APP_IMAGE=registry.ccdc.cn:5000/cia-agentscope:20261013-1

docker build -t "$APP_IMAGE" .
docker push "$APP_IMAGE"
```

镜像 tag 应使用不可变版本，不建议生产环境使用 `latest`。把实际 tag 同步写入 `examples/values-prod-hostpath.yaml` 的 `image.tag`。

如果仓库需要认证，提前在目标命名空间创建 `imagePullSecret`，并填入 values 的 `imagePullSecrets`：

```yaml
imagePullSecrets:
  - name: registry-credentials
```

## 2. 检查每个节点的 SFS

在每个可能调度应用的 Kubernetes 节点执行：

```bash
mountpoint /app/dify/sfs
findmnt -T /app/dify/sfs -o SOURCE,FSTYPE,TARGET,OPTIONS
stat -c '%U:%G %a %n' /app/dify/sfs
```

确认：

- `mountpoint` 返回成功；
- 每个节点看到的 `SOURCE` 和文件系统类型一致；
- 目录不是节点本地的空目录；
- 实际应用镜像用户对目录有读写权限。

跨节点验证可以在节点 A 创建唯一标记文件，然后立即从其他节点读取同一文件：

```bash
marker=".cia-agentscope-sfs-check-$(date +%s)-$(hostname)"
printf '%s\n' "$(hostname)" > "/app/dify/sfs/$marker"
echo "$marker"
```

在其他节点执行 `cat /app/dify/sfs/<marker>`。所有节点都能读到后删除标记文件。若只有部分节点通过检查，应给这些节点增加标签，并在 values 的 `nodeSelector` 中限制调度。

当前 Dockerfile 没有声明非 root 用户。后续镜像改为固定 UID/GID 时，必须重新验证该 UID/GID 对 SFS 的写权限，并在 values 中显式启用 `runAsNonRoot`。

## 3. 准备命名空间和 Secret

先创建命名空间：

```bash
kubectl create namespace cia-agentscope --dry-run=client -o yaml | kubectl apply -f -
```

推荐把敏感值分别保存为权限受控的临时文件，再创建 Secret，避免把真实凭证写入 values：

```bash
kubectl -n cia-agentscope create secret generic cia-agentscope-secrets \
  --from-file=MYSQL_PASSWORD=/secure/path/mysql-password \
  --from-file=JWT_SECRET=/secure/path/jwt-secret \
  --from-file=CONFIG_ENCRYPTION_KEY=/secure/path/config-encryption-key \
  --dry-run=client -o yaml | kubectl apply -f -
```

`CONFIG_ENCRYPTION_KEY` 仅在配置中使用 `ENC(...)` 密文时需要。OpenSandbox 部署后，可向同一个 Secret 增加 `OPENSANDBOX_API_KEY`。不要把 Secret YAML、密码或密钥提交到 Git。

默认 Secret key 映射如下，可通过 values 的 `existingSecret.keys` 修改：

```text
MYSQL_PASSWORD
JWT_SECRET
CONFIG_ENCRYPTION_KEY
OPENSANDBOX_API_KEY
```

## 4. 修改生产 values

复制示例文件后修改：

```bash
cp examples/values-prod-hostpath.yaml values-prod.yaml
```

至少检查：

- `image.tag`：已推送的应用镜像 tag；
- `imagePullSecrets`：内网仓库需要认证时填写；
- `externalMysql.host/port/user/database`；
- `externalRedis.url`；
- `existingSecret.name`；
- `storage.hostPath: /app/dify/sfs`；
- `storage.mountPath: /data/session-files`；
- `nodeSelector`：仅部分节点验证通过 SFS 时填写；
- `resources`：结合压测结果调整。

默认 `replicaCount: 1`。当前应用仍有进程内的取消标志、反馈状态和后台任务，尚未完成多副本状态外置，不要直接扩容。

## 5. 在内网校验 Chart

从 Chart 源目录校验：

```bash
helm lint ./cia-agentscope -f ./cia-agentscope/examples/values-prod-hostpath.yaml --strict

helm template cia-agentscope ./cia-agentscope \
  -n cia-agentscope \
  -f ./cia-agentscope/examples/values-prod-hostpath.yaml \
  > /tmp/cia-agentscope-rendered.yaml
```

重点确认：

```bash
grep -nE 'kind: Deployment|kind: Service|type: ClusterIP|/app/dify/sfs|/data/session-files' /tmp/cia-agentscope-rendered.yaml

if grep -nE 'docker.io|quay.io|ghcr.io|kind: Secret|MYSQL_PASSWORD:|JWT_SECRET:' /tmp/cia-agentscope-rendered.yaml; then
  echo 'ERROR: 渲染结果包含公共镜像仓库或明文敏感信息' >&2
  exit 1
fi
```

如果内网已有 `kubeconform`，可进一步执行 Kubernetes Schema 校验：

```bash
kubeconform -strict -summary /tmp/cia-agentscope-rendered.yaml
```

## 6. 安装

使用 Chart 源目录：

```bash
helm upgrade --install cia-agentscope ./cia-agentscope \
  --namespace cia-agentscope \
  --create-namespace \
  --values ./values-prod.yaml \
  --atomic \
  --timeout 10m
```

使用离线 `.tgz` 包：

```bash
sha256sum -c cia-agentscope-0.1.0.tgz.sha256
helm show chart cia-agentscope-0.1.0.tgz

helm upgrade --install cia-agentscope ./cia-agentscope-0.1.0.tgz \
  --namespace cia-agentscope \
  --create-namespace \
  --values ./values-prod.yaml \
  --atomic \
  --timeout 10m
```

## 7. 部署后检查

```bash
kubectl -n cia-agentscope get deployment,pod,service
kubectl -n cia-agentscope rollout status deployment/cia-agentscope --timeout=10m
kubectl -n cia-agentscope logs deployment/cia-agentscope --tail=200

kubectl -n cia-agentscope port-forward service/cia-agentscope 7010:7010
curl http://127.0.0.1:7010/health
```

如果 Release 名不是 `cia-agentscope`，资源名可能带有 Release 前缀，请以 `helm status` 和 `kubectl get` 输出为准。

应用启动时会连接 MySQL 并初始化表。`/health` 成功只表示应用进程和基础初始化已完成，不代表尚未部署的 OpenSandbox 功能可用。

## 8. 升级与回滚

推送新镜像并更新 `values-prod.yaml` 的 `image.tag` 后执行：

```bash
helm upgrade cia-agentscope ./cia-agentscope-0.1.0.tgz \
  --namespace cia-agentscope \
  --values ./values-prod.yaml \
  --atomic \
  --timeout 10m
```

查看历史和回滚：

```bash
helm -n cia-agentscope history cia-agentscope
helm -n cia-agentscope rollback cia-agentscope <REVISION> --wait --timeout 10m
```

滚动升级会终止旧 Pod 上仍在进行的 SSE 连接；客户端应支持断线重连。`terminationGracePeriodSeconds` 默认 60 秒，可根据实际长任务调整。

## 9. OpenSandbox 后续接入

OpenSandbox 独立部署完成后，更新 values：

```yaml
opensandbox:
  domain: opensandbox-server.opensandbox.svc.cluster.local:8080
  protocol: http
  poolSize: 0
```

如启用了 API Key，同时在已有 Secret 中加入 `OPENSANDBOX_API_KEY`，然后执行 `helm upgrade`。本 Chart 不包含 OpenSandbox CRD、Controller、Server 或 RBAC。

## 10. 切换到已有 PVC

未来若集群提供正式的 StorageClass/PVC，可改为：

```yaml
storage:
  type: existingClaim
  existingClaim: cia-agentscope-shared-files
  mountPath: /data/session-files
```

PVC 模式不会渲染 hostPath。

## 常见排查

- `ImagePullBackOff`：检查镜像 tag、仓库连通性和 `imagePullSecrets`；
- `CreateContainerConfigError`：检查 `existingSecret.name` 及 Secret key 名；
- `FailedMount`：逐节点检查 `/app/dify/sfs` 是否存在并已挂载，Chart 使用 `Directory`，不会自动创建本地目录；
- Pod 启动失败且日志显示 MySQL 错误：检查 DNS、端口、账号、密码、库名和建表权限；
- Redis 错误：检查 `externalRedis.url`、网络策略和 Redis ACL；
- readiness/liveness 失败：查看应用日志以及 `/health` 响应；
- Pod 一直 Pending：检查资源 requests、nodeSelector、污点容忍和节点 SFS 标签；
- 沙箱相关接口失败：OpenSandbox 尚未部署或 `opensandbox.domain/API Key` 配置不正确。
