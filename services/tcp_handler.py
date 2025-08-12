from arc.db import validate_token
from arc.redis_helpers import push_request_to_queue

# ==== TCP HANDLER ====
async def handle_client(reader, writer):
    try:
        token = None
        headers = {}

        first_line = await reader.readuntil(b"\r\n")
        if not first_line:
            writer.close()
            return

        line = first_line.decode().strip()

        # === 2. Read headers ===
        while True:
            header_line = await reader.readuntil(b"\r\n")
            if header_line in (b"\r\n", b"\n", b""):
                break
            key, value = header_line.decode().strip().split(":", 1)
            headers[key.lower()] = value.strip()

        if "proxy-authorization" not in headers:
            writer.write(b"HTTP/1.1 407 Proxy Authentication Required\r\n")
            writer.write(b'Proxy-Authenticate: Basic realm="Access to proxy"\r\n')
            writer.write(b"Content-Length: 0\r\n\r\n")
            await writer.drain()
            writer.close()
            return
        # Extract token from Proxy-
        if key == "Proxy-Authorization" or key.lower() == "proxy-authorization":
            import base64

            if "basic" in value.lower():
                b64token = value[6:].strip()
                try:
                    decoded = base64.b64decode(b64token).decode()
                    token = decoded.split(":", 1)[0]  # Just token, ignore password part
                except Exception as e:
                    print(f"❌ Token decode error: {e}")

        # === 3. Validate the token ===
        if not token or not await validate_token(token):
            writer.write(b"HTTP/1.1 401 Unauthorized\r\n\r\n")
            await writer.drain()
            writer.close()
            return

        method, path, *_ = line.split()

        # HTTPS Tunneling
        if method == "CONNECT":
            target_host, target_port = path.split(":")
            target_port = int(target_port)

            # Push request to Redis queue
            request = {
                "method": method,
                "target_host": target_host,
                "target_port": target_port,
                "reader": reader,
                "writer": writer,
            }
            push_request_to_queue(request)
            
            while True:
                data = await reader.read(4096)
                if not data:
                    break

            return

        else:
            # HTTP forwarding
            headers = {}
            while True:
                h = await reader.readuntil(b"\r\n")
                if h in (b"\r\n", b"\n", b""):
                    break
                key, value = h.decode().strip().split(":", 1)
                headers[key.lower()] = value.strip()

            host = headers.get("host")
            url = f"http://{host}{path}"

            request = {
                "method": method,
                "url": url,
                "headers": headers,
                "reader": reader,
                "writer": writer,
            }
            push_request_to_queue(request)

    except Exception as e:
        print("🔴 Fatal proxy error:", e)
        writer.close()
