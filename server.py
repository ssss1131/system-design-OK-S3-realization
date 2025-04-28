import os, hashlib, uuid
from datetime import datetime, timezone

from cassandra.policies import DCAwareRoundRobinPolicy
from flask import Flask, request, abort, jsonify, make_response
from cassandra.cluster import Cluster

STORAGE_DIR = "obs_data"
os.makedirs(STORAGE_DIR, exist_ok=True)

app = Flask(__name__)

cluster = Cluster(
    contact_points=['127.0.0.1'],
    port=9042,
    load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
    connect_timeout=20
)


admin_session = cluster.connect()
admin_session.execute("""
    CREATE KEYSPACE IF NOT EXISTS ok_s3
      WITH replication = {'class':'SimpleStrategy','replication_factor':1};
""")
admin_session.set_keyspace("ok_s3")

table_statements = [
    """
    CREATE TABLE IF NOT EXISTS block_status (
        block  blob,
        status varchar,
        ts     timestamp,
        PRIMARY KEY ((block))
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS locks (
        parent text,
        name   text,
        PRIMARY KEY ((parent), name)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS block_references (
        block   blob,
        bucket  text,
        name    text,
        version timeuuid,
        part    int,
        PRIMARY KEY ((block), bucket, name, version, part)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS object_folders (
        parent text,
        child  text,
        ts     timestamp,
        PRIMARY KEY ((parent), child)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS version_folders (
        parent text,
        child  text,
        ts     timestamp,
        PRIMARY KEY ((parent), child)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS upload_folders (
        parent text,
        child  text,
        ts     timestamp,
        PRIMARY KEY ((parent), child)
    );
    """
]

for stmt in table_statements:
    admin_session.execute(stmt)

session = cluster.connect("ok_s3")


def block_id(data: bytes) -> bytes:
    sha_digest = hashlib.sha256(data).digest()
    length_bytes = len(data).to_bytes(4, byteorder="big")
    return sha_digest + length_bytes


def full_object_key(parent: str, name: str) -> str:
    return name if not parent else f"{parent.rstrip('/')}/{name}"


@app.route("/<bucket>/<path:parent>/<name>", methods=["POST"])
def initiate_multipart_upload(bucket, parent, name):
    if "uploads" in request.args:
        upload_id = str(uuid.uuid1())
        key = full_object_key(parent, name)
        response_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult>
  <Bucket>{bucket}</Bucket>
  <Key>{key}</Key>
  <UploadId>{upload_id}</UploadId>
</InitiateMultipartUploadResult>"""
        return response_xml, 200, {"Content-Type": "application/xml"}
    elif "uploadId" in request.args:
        # upload_id = request.args.get("uploadId")
        key = full_object_key(parent, name)
        response_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
    <CompleteMultipartUploadResult>
      <Location>http://localhost:5000/{bucket}/{key}</Location>
      <Bucket>{bucket}</Bucket>
      <Key>{key}</Key>
      <ETag>"dummy-etag"</ETag>
    </CompleteMultipartUploadResult>"""
        return response_xml, 200, {"Content-Type": "application/xml"}

    else:
        abort(405)


@app.route("/<bucket>/<path:parent>/<name>", methods=["PUT"])
def upload_object(bucket, parent, name):
    upload_id = request.args.get("uploadId")
    part_num = request.args.get("partNumber", type=int)
    data = request.get_data()
    if not upload_id or part_num is None or not data:
        abort(400, description="Не заданы uploadId, partNumber или данные пусты.")

    now = datetime.now(timezone.utc)
    full_key = full_object_key(parent, name)

    # check_q = "SELECT child FROM object_folders WHERE parent=%s AND child=%s"
    # exists = session.execute(check_q, (parent, name)).one()
    # if exists is not None and part_num == 1:
    #     abort(409)

    lock_q = "INSERT INTO locks (parent, name) VALUES (%s, %s) IF NOT EXISTS"
    lock_result = session.execute(lock_q, (parent, name))
    if not lock_result[0].applied:
        abort(409, description="Объект уже занят другой операцией (lock exists).")

    try:
        bid = block_id(data)
        bid_hex = bid.hex()

        status_q = "INSERT INTO block_status (block, status) VALUES (%s, %s) IF NOT EXISTS"
        session.execute(status_q, (bid, "UPLOADING"))

        storage_path = os.path.join(STORAGE_DIR, bid_hex)
        if not os.path.exists(storage_path):
            with open(storage_path, "wb") as f:
                f.write(data)

        ref_q = ("INSERT INTO block_references (block, bucket, name, version, part) "
                 "VALUES (%s, %s, %s, %s, %s)")
        version_uuid = uuid.UUID(upload_id)
        session.execute(ref_q, (bid, bucket, full_key, version_uuid, part_num))

        uf_q = "INSERT INTO upload_folders (parent, child, ts) VALUES (%s, %s, %s)"
        session.execute(uf_q, (parent, name, now))
        vf_q = "INSERT INTO version_folders (parent, child, ts) VALUES (%s, %s, %s)"
        of_q = "INSERT INTO object_folders (parent, child, ts) VALUES (%s, %s, %s)"
        session.execute(vf_q, (parent, name, now))
        session.execute(of_q, (parent, name, now))

        update_status_q = "UPDATE block_status SET status=%s WHERE block=%s IF status=%s"
        session.execute(update_status_q, ("ONLINE", bid, "UPLOADING"))
    finally:
        del_lock_q = "DELETE FROM locks WHERE parent=%s AND name=%s"
        del_upload_q = "DELETE FROM version_folders where parent=%s AND child=%s"
        session.execute(del_upload_q, (parent, name))
        session.execute(del_lock_q, (parent, name))

    response = make_response('', 200)
    response.headers["ETag"] = f"\"{bid_hex}\""
    return response


@app.route("/files/<bucket>/<path:parent>", methods=["GET"])
def list_files(bucket, parent):
    list_q = "SELECT child, ts FROM object_folders WHERE parent=%s"
    rows = session.execute(list_q, (parent,))
    files = []
    for r in rows:
        files.append({
            "name": r.child,
            "last_modified": r.ts.isoformat() if r.ts else None
        })
    return jsonify({"parent": parent, "files": files})


@app.route("/<bucket>/<path:parent>/<name>", methods=["GET"])
def download_object(bucket, parent, name):
    full_key = full_object_key(parent, name)
    sel_q = "SELECT block, part FROM block_references WHERE bucket=%s AND name=%s ALLOW FILTERING"
    rows = list(session.execute(sel_q, (bucket, full_key)))
    if not rows:
        abort(404, description="Файл не найден.")

    rows.sort(key=lambda r: r.part)
    file_content = b""
    for r in rows:
        block_hex = r.block.hex()
        fpath = os.path.join(STORAGE_DIR, block_hex)
        if not os.path.exists(fpath):
            abort(500, description=f"Отсутствует блок {block_hex} в хранилище.")
        with open(fpath, "rb") as f:
            file_content += f.read()

    response = make_response(file_content)

    response.headers["Content-Disposition"] = f'attachment; filename="{name}"'
    return response


@app.route("/<bucket>/<path:parent>/<name>", methods=["DELETE"])
def delete_object(bucket, parent, name):
    full_key = full_object_key(parent, name)

    sel_q = "SELECT block, version, part FROM block_references WHERE bucket=%s AND name=%s ALLOW FILTERING"
    rows = session.execute(sel_q, (bucket, full_key))
    found = False
    for r in rows:
        found = True
        del_ref_q = ("DELETE FROM block_references WHERE block=%s AND bucket=%s "
                     "AND name=%s AND version=%s AND part=%s")
        session.execute(del_ref_q, (r.block, bucket, full_key, r.version, r.part))

        count_q = "SELECT COUNT(*) as cnt FROM block_references WHERE block=%s ALLOW FILTERING"
        cnt_row = session.execute(count_q, (r.block,)).one()
        cnt = cnt_row.cnt if cnt_row else 0
        if cnt == 0:
            upd_status_q = "UPDATE block_status SET status=%s WHERE block=%s IF status=%s"
            session.execute(upd_status_q, ("REMOVING", r.block, "ONLINE"))
            bid_hex = r.block.hex()
            fpath = os.path.join(STORAGE_DIR, bid_hex)
            if os.path.exists(fpath):
                os.remove(fpath)
            del_status_q = "DELETE FROM block_status WHERE block=%s"
            session.execute(del_status_q, (r.block,))

    if not found:
        abort(404, description="Файл не найден.")

    del_of_q = "DELETE FROM object_folders WHERE parent=%s AND child=%s"
    del_vf_q = "DELETE FROM version_folders WHERE parent=%s AND child=%s"
    del_uf_q = "DELETE FROM upload_folders WHERE parent=%s AND child=%s"
    for q in (del_of_q, del_vf_q, del_uf_q):
        session.execute(q, (parent, name))

    return "", 204


if __name__ == "__main__":
    app.run(port=5000, debug=True)
