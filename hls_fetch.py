import threading
import queue
import m3u8
import requests
import shutil
import tempfile
import argparse
import os
import posixpath
import urllib.parse
import re
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend


def is_url(uri):
    return re.match(r'https?://', uri) is not None


class DownloadSegment(threading.Thread):
    def __init__(self, downloadqueue, location):
        threading.Thread.__init__(self)
        self.downloadQueue = downloadqueue
        self.location = location

    def run(self):
        while True:
            item = self.downloadQueue.get()
            if item is None:
                break
            self.execute(item)
            self.downloadQueue.task_done()

    def execute(self, item):
        if item[1]:
            url = item[1] + "/" + item[2]
        else:
            url = item[2]
            item[2] = os.path.basename(urllib.parse.urlparse(url).path)
        if item[3]:
            backend = default_backend()
            r = requests.get(item[3].uri)
            key = r.content
            cipher = Cipher(algorithms.AES(key), modes.CBC(bytes.fromhex(item[3].iv[2:])), backend=backend)
            decryptor = cipher.decryptor()
        r = requests.get(url, stream=True)
        with open(os.path.join(self.location, item[2]), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    if item[3]:
                        f.write(decryptor.update(chunk))
                    else:
                        f.write(chunk)


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


def hls_fetch(playlist_location, storage_location, name="video.ts", threads=5):
    download_queue = queue.Queue()
    with tempfile.TemporaryDirectory() as download_location:
        num_worker_threads = threads
        playlist = m3u8_load(playlist_location)
        high_bw = highest_bandwidth(playlist, playlist_location)
        playlist = m3u8_load(high_bw.absolute_uri)
        parsed_url = urllib.parse.urlparse(playlist_location)
        prefix = parsed_url.scheme + '://' + parsed_url.netloc
        base_path = posixpath.normpath(parsed_url.path + '/..')
        base_uri = urllib.parse.urljoin(prefix, base_path)
        pool = list()
        for number, file in enumerate(playlist.segments):
            if not is_url(file.uri):
                playlist.base_uri = base_uri
            download_queue.put([number, playlist.base_uri, file.uri, file.key])
        for i in range(num_worker_threads):
            thread = DownloadSegment(download_queue, download_location)
            thread.daemon = True
            thread.start()
            pool.append(thread)
        download_queue.join()
        for i in range(num_worker_threads):
            download_queue.put(None)
        for thread in pool:
            thread.join()
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
