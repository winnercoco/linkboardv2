from flask import Flask, request, Response, abort
import requests
from urllib.parse import urljoin

app = Flask(__name__)

BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
}

session = requests.Session()
session.headers.update(BASE_HEADERS)


def build_headers(referer: str | None):
    headers = dict(BASE_HEADERS)
    if referer:
        headers["Referer"] = referer
    return headers


def is_m3u8(content_type: str, url: str) -> bool:
    ct = (content_type or "").lower()
    u = (url or "").lower()
    return (
        "application/vnd.apple.mpegurl" in ct
        or "application/x-mpegurl" in ct
        or ".m3u8" in u
    )


def rewrite_m3u8(text: str, playlist_url: str, referer: str | None) -> str:
    lines = text.splitlines()
    out = []

    for line in lines:
        stripped = line.strip()

        if not stripped or stripped.startswith("#"):
            if stripped.startswith("#EXT-X-KEY:") and 'URI="' in line:
                prefix, suffix = line.split('URI="', 1)
                original_uri, rest = suffix.split('"', 1)
                abs_uri = urljoin(playlist_url, original_uri)
                proxied = f'/proxy?url={abs_uri}'
                if referer:
                    proxied += f"&referer={referer}"
                line = f'{prefix}URI="{proxied}"{rest}'
            out.append(line)
            continue

        abs_url = urljoin(playlist_url, stripped)
        proxied = f"/proxy?url={abs_url}"
        if referer:
            proxied += f"&referer={referer}"
        out.append(proxied)

    return "\n".join(out)


@app.route("/proxy")
def proxy():
    target = request.args.get("url")
    referer = request.args.get("referer")

    if not target:
        abort(400, "Missing url")

    try:
        upstream = session.get(
            target,
            headers=build_headers(referer),
            stream=True,
            timeout=30,
            allow_redirects=True,
        )
    except Exception as e:
        return Response(f"Upstream request failed: {e}", status=500)

    content_type = upstream.headers.get("content-type", "")

    if is_m3u8(content_type, target):
        try:
            text = upstream.text
            rewritten = rewrite_m3u8(text, upstream.url, referer or target)
            return Response(
                rewritten,
                status=upstream.status_code,
                content_type="application/vnd.apple.mpegurl",
            )
        except Exception as e:
            return Response(f"Failed to rewrite m3u8: {e}", status=500)

    excluded = {"content-encoding", "content-length", "transfer-encoding", "connection"}
    response_headers = [
        (k, v)
        for k, v in upstream.headers.items()
        if k.lower() not in excluded
    ]

    return Response(
        upstream.iter_content(chunk_size=8192),
        status=upstream.status_code,
        headers=response_headers,
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5001, debug=True)
