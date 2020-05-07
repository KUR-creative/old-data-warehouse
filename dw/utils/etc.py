import uuid
import socket

import funcy as F

def host_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

def uuid4strs(length):
    return list(F.repeatedly(
        lambda: str(uuid.uuid4()), length
    ))
