import base64
import json
import os
import urllib.request


def json_request(url, body=None, method=None, headers=None, timeout=3):
    request = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None,
        method=method, headers={'Content-Type': 'application/json', **(headers or {})})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        data = response.read()
        return json.loads(data) if data else None


def supervisor(path, body=None):
    return json_request(os.getenv('SUPERVISOR_URL', 'http://localhost:8001') + path, body)


def broker(path, body=None, method=None):
    credentials = (os.getenv('BROKER_USER', 'kitchen') + ':' + os.getenv('BROKER_PASSWORD', 'kitchen')).encode()
    return json_request(os.getenv('BROKER_HTTP', 'http://localhost:15672') + path, body, method,
                        {'Authorization': 'Basic ' + base64.b64encode(credentials).decode()})


def install_dead_letters():
    # Policy keeps existing v0 work queues compatible; only the three demo queues match.
    return broker('/api/policies/%2F/pizza-demo-work', {
        'pattern': r'^pizza\.(prepare|bake|pack)$', 'priority': 10, 'apply-to': 'queues',
        'definition': {'dead-letter-exchange': '', 'dead-letter-routing-key': 'pizza.dead'},
    }, 'PUT')
