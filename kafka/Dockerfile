FROM bitnami/kafka:latest

USER root

RUN set -x \
 && groupadd -r foo -g 1001 \
 && useradd -r -g foo -u 1001 -m -d /build -s /bin/bash foo \
 && adduser foo sudo

RUN set -x \
  chown foo:foo -R /build

RUN set -x \
  && mkdir -p /etc/sudoers.d/ \
  && echo "foo ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/foo


RUN apt-get update && apt-get install -y --no-install-recommends \
    kafkacat \
    sudo \
    unzip \
 && rm -rf /var/lib/apt/lists/*

COPY create_topic.sh /usr/local/bin/create_topic.sh

COPY entrypoint.sh /usr/local/bin/entrypoint.sh

RUN chmod 755 /usr/local/bin/entrypoint.sh /usr/local/bin/create_topic.sh

ENV PATH=$PATH:/usr/local/bin

USER foo


ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
