# worker (lab-unknown / 後続の emscripten 版) が繋ぐ DB (Cloudflare Containers)。
# postgres + PgBouncer (transaction mode) を 1 つの Container に入れる。
#
# 「トランザクションごとにサーバー側のコネクションが使い回される」条件 (Supabase のプーラー 6543 と同じ)
# を作るため、Worker は postgres ではなく PgBouncer (6432) にだけ繋ぐ。Container へ届くのは
# Durable Object (`LabDb`) の getTcpPort() 経由だけで、外からは届かない。
#
# ディスクは揮発 (sleep 後の再起動で image の状態に戻る) ので、毎回の起動で空の DB から
# role `bench` と表 `items` (1000 行) を作る (start.sh)。build context はこのディレクトリ。
FROM postgres:16

RUN apt-get update \
    && apt-get install -y --no-install-recommends pgbouncer \
    && rm -rf /var/lib/apt/lists/*

# 認証は trust。postgres も PgBouncer も Container の外へは出ず、到達経路は LabDb の
# getTcpPort() だけなので、パスワードを持たない。
ENV POSTGRES_HOST_AUTH_METHOD=trust
# VOLUME (/var/lib/postgresql/data) に頼らず、揮発のディスク上の普通のディレクトリに置く
ENV PGDATA=/tmp/lab-pgdata

COPY pgbouncer.ini /etc/pgbouncer/pgbouncer.ini
COPY start.sh /lab/start.sh
RUN chmod +x /lab/start.sh && chown -R postgres:postgres /lab /etc/pgbouncer

# start.sh が root なら gosu で postgres に降りる (どちらのユーザーで起動されても動く)
USER postgres
EXPOSE 6432
CMD ["/lab/start.sh"]
