import m3u8
import requests
import shutil
import tempfile
import argparse
import os
import sys
import posixpath
import urllib.parse
import re
from multiprocessing import Lock
from multiprocessing.sharedctypes import RawValue
import concurrent.futures
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend


def is_url(uri):
    return re.match(r'https?://', uri) is not None


class Counter(object):
    def __init__(self, value=0):
        self.val = RawValue('i', value)
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def value(self):
        with self.lock:
            return self.val.value


def highest_bandwidth(m3u8_obj, location):
    highest_bw_playlist = None
    for playlist in m3u8_obj.playlists:
        if not highest_bw_playlist:
            highest_bw_playlist = playlist
        elif playlist.stream_info.bandwidth > highest_bw_playlist.stream_info.bandwidth:
            highest_bw_playlist = playlist
    if not is_url(highest_bw_playlist.uri):
        highest_bw_playlist.base_uri = location
    return highest_bw_playlist


def merge_files(filelist, source, destination, name):
    with open(os.path.join(destination, name), 'wb') as outfile:
        for file in filelist:
            if is_url(file.uri):
                uri = os.path.basename(urllib.parse.urlparse(file.uri).path)
            else:
                uri = file.uri
            with open(os.path.join(source, uri), 'rb') as readfile:
                shutil.copyfileobj(readfile, outfile)


def m3u8_load(uri):
    r = requests.get(uri)
    m3u8_obj = m3u8.M3U8(r.text)
    return m3u8_obj


def download_file(download_location, remote_file, base_uri, m3u8_playlist, counter, total):
    if not is_url(remote_file.uri):
        m3u8_playlist.base_uri = base_uri
    if m3u8_playlist.base_uri:
        url = m3u8_playlist.base_uri + "/" + remote_file.uri
    else:
        url = remote_file.uri
    filename = os.path.basename(urllib.parse.urlparse(url).path)
    if remote_file.key:
        backend = default_backend()
        r = requests.get(remote_file.key.uri)
        key = r.content
        cipher = Cipher(algorithms.AES(key), modes.CBC(bytes.fromhex(remote_file.key.iv[2:])), backend=backend)
        decrypter = cipher.decryptor()
    r = requests.get(url, stream=True)
    with open(os.path.join(download_location, filename), 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                if remote_file.key:
                    f.write(decrypter.update(chunk))
                else:
                    f.write(chunk)
    counter.increment()
    print(" {0:.2f}%".format((counter.value() / total) * 100), end='\r')
    sys.stdout.flush()


def hls_fetch(playlist_location, storage_location, name="video.ts", threads=5):
    with tempfile.TemporaryDirectory() as download_location:
        playlist = m3u8_load(playlist_location)
        high_bw = highest_bandwidth(playlist, playlist_location)
        playlist = m3u8_load(high_bw.absolute_uri)
        parsed_url = urllib.parse.urlparse(playlist_location)
        prefix = parsed_url.scheme + '://' + parsed_url.netloc
        base_path = posixpath.normpath(parsed_url.path + '/..')
        base_uri = urllib.parse.urljoin(prefix, base_path)
        thread_safe_counter = Counter()
        total = len(playlist.segments)
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(download_file, download_location, file, base_uri, playlist,
                                       thread_safe_counter, total)
                       for file in playlist.segments]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except:
                    exit(900)
        merge_files(playlist.segments, download_location, storage_location, name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="URL to HLS playlist")
    parser.add_argument("-f", "--file", help="specify filename. Defaults to video.ts")
    parser.add_argument("-n", '--threads', help="specify the amount of download threads. Defaults to 5")
    args = parser.parse_args()
    cwd = os.getcwd()
    if args.file and args.threads:
        hls_fetch(args.url, cwd, name=args.file, threads=args.threads)
    if args.file:
        hls_fetch(args.url, cwd, name=args.file)
    if args.threads:
        hls_fetch(args.url, cwd, threads=args.threads)
    else:
        hls_fetch(args.url, cwd)
