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
        r = requests.get(item[1]+"/"+item[2], stream=True)
        with open(os.path.join(self.location, item[2]), 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)


def highest_bandwidth(m3u8_obj):
    highest_bw_playlist = {}
    for playlist in m3u8_obj.playlists:
        if not highest_bw_playlist:
            highest_bw_playlist = playlist
        elif playlist.stream_info.bandwidth > highest_bw_playlist.stream_info.bandwidth:
            highest_bw_playlist = playlist
    return highest_bw_playlist


def merge_files(filelist, source, destination, name):
    with open(os.path.join(destination, name), 'wb') as outfile:
        for file in filelist:
            with open(os.path.join(source, file), 'rb') as readfile:
                shutil.copyfileobj(readfile, outfile)


def m3u8_load(uri):
    r = requests.get(uri)
    parsed_url = urllib.parse.urlparse(uri)
    prefix = parsed_url.scheme + '://' + parsed_url.netloc
    base_path = posixpath.normpath(parsed_url.path + '/..')
    m3u8_obj = m3u8.M3U8(r.text, base_uri=urllib.parse.urljoin(prefix, base_path))
    return m3u8_obj


def hls_fetch(playlist_location, storage_location, name="video.ts", threads=5):
    download_queue = queue.Queue()
    with tempfile.TemporaryDirectory() as download_location:
        num_worker_threads = threads
        playlist = m3u8_load(playlist_location)
        high_bw = highest_bandwidth(playlist)
        playlist = m3u8_load(high_bw.absolute_uri)
        pool = list()
        for number, file in enumerate(playlist.files):
            download_queue.put([number, playlist.base_uri, file])
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
        merge_files(playlist.files, download_location, storage_location, name)


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
