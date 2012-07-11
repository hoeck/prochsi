#!/usr/bin/env python

import re
import os
import select
import SocketServer
import socket
import traceback
import Queue
import threading
import argparse
from urlparse import urlparse

# constants

BUFSIZE = 2**16

# utils

def data_len(d):
    """Given a list of strings, return its total len."""
    return sum(len(s) for s in d)

# proxy server

class SocketReader(object):

    def __init__(self, socket):
        self._s = socket
        self._buf = ""

    def get_clear_buf(self):
        b = self._buf
        self._buf = ""
        return b

    def read(self, max_bytes=-1):
        res = []

        remaining = max_bytes if max_bytes > -1 else 2**62

        # clear buf
        if len(self._buf) > remaining:
            d = self._buf[:remaining]
            self._buf = self._buf[remaining:]
            return d
        else:
            res.appen(self._buf)
            remaining -= len(self._buf)
            self._buf = ""

        while remaining > 0:
            r,w,x = select.select([self._s], [], [])
            if r:
                d = self._s.recv(min(remaining, BUFSIZE))
                if not d:
                    return ''.join(res)
                res.append(d)
                remaining -= len(d)

        return ''.join(res)

    def readline(self):
        """Read a line from the socket.

        Return the line chars with the whitespace stripped off!
        """
        strings = []

        # clear buf
        if self._buf:
            pos = self._buf.find("\n")
            if pos > -1:
                res = self._buf[:pos+1]
                self._buf = self._buf[pos+1:]
                return res.strip()
            else:
                strings.append(self._buf)
                self._buf = ""

        while True:
            r,w,x = select.select([self._s], [], [])
            d = self._s.recv(BUFSIZE)
            if not d:
                return ''.join(strings)
            pos = d.find("\n")
            if pos > -1:
                strings.append(d[:pos+1])
                self._buf = d[pos+1:]
                return ''.join(strings).strip()
            else:
                strings.append(d)

    def forward(self, dest, max_bytes=-1):
        """Forward max_bytes or ALL the data (-1) to socket dest.

        Return the forwarded data as a list of strings.
        """
        # src dest being two sockets
        remaining = max_bytes if max_bytes > -1 else 2**62

        sdata = []

        # clear buf
        if self._buf:
            if len(self._buf) > remaining:
                sdata.append(self._buf[:remaining])
                dest.sendall(self._buf[:remaining])
                self._buf = self._buf[remaining:]
                remaining = 0
            else:
                sdata.append(self._buf)
                dest.sendall(self._buf)
                remaining -= len(self._buf)
                self._buf = ""

        while remaining > 0:
            r,w,x = select.select([self._s], [], [])
            if r:
                d = self._s.recv(min(remaining, BUFSIZE))
                if not d:
                    break
                sdata.append(d)
                dest.sendall(d)
                remaining -= len(d)

        return sdata


class HTTPProxyHandler(SocketServer.BaseRequestHandler):
    """handles a connection from the client, can handle multiple requests"""

    @classmethod
    def configure(self, store_response=None):
        """Set the store_response predicate function."""
        self.store_response = store_response or (lambda handler, response_line, response_header, response_data: None)

    def parse_request(self, line):
        """parse a request line"""
        method, url, version = line.split(" ")
        return method, url, version

    def read_header(self, rdr):
        """read the httpheaders from the file like f into a dictionary"""
        headers = {}
        while True:
            line = rdr.readline().strip()
            if line:
                key, value = line.split(": ", 1)
                headers.setdefault(key, []).append(value.strip())
            else:
                return headers

    def write_headers(self, s, headers):
        res = []
        for name, values in headers.items():
            if not name.startswith('Proxy-'):
                for v in values:
                    res.append("%s: %s\r\n" % (name, v))
        res.append("\r\n")
        s.sendall(''.join(res))

    def handle_connect(self, url, header):
        """Handle the CONNET proxy tunnel method."""
        host, port = url.split(":")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((host, int(port)))

        # proxy connection established
        self.request.sendall("HTTP/1.1 200 Connection established\r\n\r\n")

        # move data between self.request and s

        # firs empty the client reader buf
        b = self.client_rdr.get_clear_buf()
        if b:
            s.sendall(b)

        while True:
            r, w, x = select.select([self.request, s], [], [])
            if r[0] is self.request:
                # client > server
                data = self.request.recv(BUFSIZE)
                if data:
                    s.sendall(data)
                else:
                    s.close()
                    return

            elif r[0] is s:
                # server -> client
                data = s.recv(BUFSIZE)
                if data:
                    self.request.sendall(data)
                else:
                    s.close()
                    return

            else:
                # ????
                self.request.close()
                self.server_socket.close()
                return True
        else:
            # no content-len -> close connection
            self.request.close()
            self.server_socket.close()
            return True

    def handle_request(self):
        # handle a single proxy request
        self.client_rdr = SocketReader(self.request)

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        connected = False
        close = False

        while True: # keep connection

            request_line = self.client_rdr.readline()
            if not request_line:
                return

            method, url, version = self.parse_request(request_line)
            header = self.read_header(self.client_rdr)

            print "  %s %s" % (method, url)

            if header.get('Connection') == 'close':
                close = True

            if method == 'CONNECT':
                self.handle_connect(url, header)
                return
            else:
                u = urlparse(url)
                if not connected:
                    self.server_socket.connect((u.hostname, int(u.port or 80)))
                    self.server_rdr = SocketReader(self.server_socket)
                    connected = True

                # forward request to server
                request = "%s %s%s %s\r\n" % (method, u.path or "/", u.query and "?" + u.query or "", version)
                self.server_socket.sendall(request)

                # forward headers to server
                self.write_headers(self.server_socket, header)

                # forward the request body data (if any) to the server
                if method in ('POST', 'PUT'):
                    if "Content-Length" in header:
                        cl = int(header["Content-Length"][0])
                        self.client_rdr.forward(self.server_socket, cl)
                        #s.shutdown(socket.SHUT_WR) # ?????
                    else:
                        # no content-length
                        self.client_rdr.forward(self.server_socket)
                        self.server_socket.shutdown(socket.SHUT_WR)
                        close = True

                ## SERVER RESPONSE

                # read & forward response to the client
                response_line = self.server_rdr.readline() + "\r\n"
                self.request.sendall(response_line)

                # read and forward header to the client
                response_header = self.read_header(self.server_rdr)
                self.write_headers(self.request, response_header)

                # body -> client
                response_data = []
                if 'Content-Length' in response_header:
                    cl = int(response_header['Content-Length'][0])
                    if cl:
                        response_data = self.server_rdr.forward(self.request, cl)
                    if response_header.get('Connection') == ['close']:
                        close = True
                else:
                    # forward all the data
                    response_data = self.server_rdr.forward(self.request)
                    close = True

                # call the store hook
                if response_data:
                    self.store_response(response_line, response_header, response_data)

                if header.get("Proxy-Connection") != ["keep-alive"] or close:
                    self.server_socket.shutdown(socket.SHUT_RDWR) # or close?
                    self.server_socket.close()
                    return
                else:
                    pass

    def handle(self):
        try:
            self.handle_request()
        except:
            traceback.print_exc()


class HttpProxyServer(SocketServer.ThreadingMixIn,
                      SocketServer.TCPServer):
    allow_reuse_address = True
    daemon_threads = True
    timeout = 90
    request_queue_size = 64

    def __init__(self, addr, handler=HTTPProxyHandler):
        SocketServer.TCPServer.__init__(self, addr, handler)

    def handle_error(self, request, addr):
        pass

# writing contents to disk

def get_max_filename_idx(path):
    n = 0
    for f in os.listdir(path):
        m = re.match("^prochsi_([0-9]+)$", f)
        if m:
            n = max(n, int(m.group(1)))
    return n

def file_write_worker(queue, path):
    idx = get_max_filename_idx(path)
    while True:
        fdata = queue.get()

        # write the file
        idx += 1
        fname = os.path.join(path, "prochsi_%i" % idx)
        bytes = 0
        with open(fname, "w") as f:
            print "    writing:", fname
            for s in fdata:
                bytes += len(s)
                f.write(s)
            print "    done writing %skb" % (bytes / 1000,)

# content filter

def create_store_response_f(queue, args):
    """Create and return a store_response function for use with the HttpProxyHandler."""

    min_size = int(args.min) * 1000
    max_size = int(args.max) * 1000

    def store_response(handler, response_line, response_header, response_data):
        if args.content_type in response_header.get('Content-Type',[''])[0].lower():
            dlen = data_len(response_data)
            if min_size < dlen and dlen < max_size:
                queue.put(response_data)

    return store_response

# main

def main():
    args = parse_args()

    if not os.path.exists(args.path):
        os.makedirs(args.path)

    queue = Queue.Queue()

    fw = threading.Thread(target=lambda: file_write_worker(queue, args.path))
    fw.setDaemon(True)
    fw.start()

    HTTPProxyHandler.configure(create_store_response_f(queue, args))
    server = HttpProxyServer(addr=("localhost", 8080), handler=HTTPProxyHandler)

    print "prochsi listening on localhost:8080"
    server.serve_forever()


def parse_args():
    p = argparse.ArgumentParser(description="Start a proxy on localhost and capture interesting data to PATH")
    p.add_argument('-t', '--content-type', help="if this substring is contained in a responses 'Content-Type' header, store the content body in PATH (defaults to 'audio').", default='audio')
    p.add_argument('--min', help="Min content size in kbytes, everything smaller is not stored (defaults to 200).", default="200")
    p.add_argument('--max', help="Max content size in kbytes, everything larger is not stored (defaults to 200000).", default="200000")
    p.add_argument('path', metavar='PATH', help="path to store the contents")

    return p.parse_args()

if __name__ == "__main__":
    main()
