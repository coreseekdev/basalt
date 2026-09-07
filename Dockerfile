# Basalt broker 运行时镜像：直接装载主机构建的 release 二进制（同架构 glibc 兼容）
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY target/release/basalt-server /usr/local/bin/basalt-server
EXPOSE 9092 9093
ENTRYPOINT ["/usr/local/bin/basalt-server"]
