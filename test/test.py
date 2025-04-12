import ssl
import socket

hostname = "apis.data.go.kr"
port = 443

context = ssl.create_default_context()
try:
    with socket.create_connection((hostname, port)) as sock:
        with context.wrap_socket(sock, server_hostname=hostname) as ssock:
            print(f"✅ 연결 성공: {ssock.version()}")
except Exception as e:
    print(f"❌ 연결 실패: {e}")


