"""Explicit live capability probe. Uses an existing disposable account; deletes its own container.
Run: python3 azure_probe.py SUBSCRIPTION RESOURCE_GROUP ACCOUNT
CLI credentials retrieve an account key in memory; neither credentials nor signed URLs are logged.
No HTTP retries. This is a capability experiment, not a replication adapter.
"""
import base64
import datetime
import hashlib
import hmac
import json
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid

subscription, group, account = sys.argv[1:]
key = base64.b64decode(subprocess.check_output([
    'az', 'storage', 'account', 'keys', 'list', '--subscription', subscription,
    '-g', group, '-n', account, '--query', '[0].value', '-o', 'tsv'], text=True).strip())
container = 'm1-' + uuid.uuid4().hex
results = []


def request(method, blob='', query=None, body=b'', headers=None, expected=200):
    path = '/' + container + ('/' + blob if blob else '')
    query = query or {}
    headers = {k.lower(): v for k, v in (headers or {}).items()}
    headers['x-ms-date'] = datetime.datetime.now(datetime.timezone.utc).strftime('%a, %d %b %Y %H:%M:%S GMT')
    headers['x-ms-version'] = '2023-11-03'
    if method == 'PUT':
        headers['content-length'] = str(len(body))
        headers.setdefault('content-type', 'application/octet-stream')
    standard = [method, '', '', str(len(body)) if body else '', '', headers.get('content-type', ''), '',
                headers.get('if-modified-since', ''), headers.get('if-match', ''),
                headers.get('if-none-match', ''), headers.get('if-unmodified-since', ''), headers.get('range', '')]
    canonical_headers = ''.join(k + ':' + headers[k] + '\n' for k in sorted(headers) if k.startswith('x-ms-'))
    resource = '/' + account + path + ''.join('\n' + k.lower() + ':' + query[k] for k in sorted(query))
    signature = base64.b64encode(hmac.new(key, ('\n'.join(standard) + '\n' + canonical_headers + resource).encode(), hashlib.sha256).digest()).decode()
    headers['authorization'] = 'SharedKey ' + account + ':' + signature
    url = 'https://' + account + '.blob.core.windows.net' + path
    if query:
        url += '?' + urllib.parse.urlencode(query)
    req = urllib.request.Request(url, data=body if method == 'PUT' else None, headers=headers, method=method)
    try:
        response = urllib.request.urlopen(req, timeout=30)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        status, response_headers, content = response.status, response.headers, response.read()
    results.append({'method': method, 'blob': blob or '<container>', 'operation': query.get('comp', ''), 'status': status})
    assert status == expected, (method, blob, status, response_headers.get('x-ms-error-code'))
    return response_headers, content


def stage(blob, content):
    block = base64.b64encode(uuid.uuid4().bytes).decode()
    request('PUT', blob, {'comp': 'block', 'blockid': block}, content, expected=201)
    return block


def commit(blob, blocks, token=None, term='1', next_key=None, expected=201):
    headers = {'content-type': 'application/xml', 'x-ms-meta-btdb_format': '1', 'x-ms-meta-btdb_term': term}
    headers['if-match' if token else 'if-none-match'] = token or '*'
    if next_key:
        headers['x-ms-meta-btdb_next'] = next_key
        headers['x-ms-meta-btdb_next_id'] = '3'
    xml = ('<BlockList>' + ''.join('<Latest>' + b + '</Latest>' for b in blocks) + '</BlockList>').encode()
    return request('PUT', blob, {'comp': 'blocklist'}, xml, headers, expected)[0].get('ETag')


created = False
try:
    request('PUT', query={'restype': 'container'}, expected=201)
    created = True
    a = stage('tail', b'native-prefix')
    e1 = commit('tail', [a])
    b = stage('tail', b'-suffix')
    h, data = request('GET', 'tail')
    assert data == b'native-prefix' and h['ETag'] == e1
    e2 = commit('tail', [a, b], e1)  # Discard the successful write response at the protocol layer.
    h, data = request('GET', 'tail')
    assert data == b'native-prefix-suffix' and h['x-ms-meta-btdb_term'] == '1' and h['ETag'] != e1
    # Same bytes + newer authority in the same CAS object fence a previously dispatched old-ETag append.
    e3 = commit('tail', [a, b], e2, term='2')
    assert e3 != e2
    c = stage('tail', b'-stale')
    commit('tail', [a, b, c], e2, expected=412)
    # A staged successor is invisible until predecessor metadata selects its immutable key.
    successor = stage('successor', b'next-native-file')
    commit('successor', [successor], term='2')
    commit('tail', [a, b], e3, term='2', next_key='successor')
    h, data = request('GET', 'tail')
    assert data == b'native-prefix-suffix' and h['x-ms-meta-btdb_next'] == 'successor'
    # Lease is per-object; it does not fence the TRL through leader.json.
    h, _ = request('PUT', 'leader.json', body=b'{}', headers={'x-ms-blob-type': 'BlockBlob'}, expected=201)
    leader_etag = h['ETag']
    lease_id, new_id = str(uuid.uuid4()), str(uuid.uuid4())
    request('PUT', 'leader.json', {'comp': 'lease'}, headers={'x-ms-lease-action': 'acquire',
            'x-ms-proposed-lease-id': lease_id, 'x-ms-lease-duration': '15'}, expected=201)
    request('PUT', 'leader.json', body=b'bad', headers={'x-ms-blob-type': 'BlockBlob'}, expected=412)
    request('PUT', 'unleased', body=b'ok', headers={'x-ms-blob-type': 'BlockBlob'}, expected=201)
    request('PUT', 'leader.json', {'comp': 'lease'}, headers={'x-ms-lease-action': 'change',
            'x-ms-lease-id': lease_id, 'x-ms-proposed-lease-id': new_id}, expected=200)
    request('PUT', 'leader.json', {'comp': 'lease'}, headers={'x-ms-lease-action': 'renew', 'x-ms-lease-id': lease_id}, expected=409)
    request('PUT', 'leader.json', {'comp': 'lease'}, headers={'x-ms-lease-action': 'renew', 'x-ms-lease-id': new_id}, expected=200)
    h, _ = request('GET', 'leader.json')
    assert h['ETag'] == leader_etag
    request('PUT', 'leader.json', {'comp': 'lease'}, headers={'x-ms-lease-action': 'release', 'x-ms-lease-id': new_id}, expected=200)
finally:
    if created:
        request('DELETE', query={'restype': 'container'}, expected=202)
    print(json.dumps({'requests': results, 'container_deleted': created and results[-1]['method'] == 'DELETE' and results[-1]['status'] == 202}, indent=2))
