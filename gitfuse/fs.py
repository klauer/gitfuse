import os
import sys
import signal
import threading
import errno
import stat

from fusell import FUSELL
from .directory_entry import (DirectoryEntry, ReadableString)


class FileSystem(FUSELL):
    def __init__(self, *args, **kwargs):
        self.lock = threading.RLock()
        super().__init__(*args, **kwargs)

    def create_ino(self):
        with self.lock:
            self.ino += 1
            return self.ino

    def create_ino_range(self, num):
        with self.lock:
            start_inode = self.ino + 1
            self.ino = start_inode + num
            return range(start_inode, self.ino + 1)

    def init(self, userdata, conn):
        self.ino = 0
        self.inode_entries = {}
        self.root = DirectoryEntry(self, parent_inode=1)

    forget = None

    def getattr(self, req, ino, fi):
        try:
            entry = self.inode_entries[ino]
        except KeyError:
            self.reply_err(req, errno.ENOENT)
        else:
            self.reply_attr(req, entry.attr, 1.0)

    def lookup(self, req, parent_inode, name):
        parent = self.inode_entries[parent_inode].obj
        name = name.decode('utf-8')

        try:
            entry = parent[name]
        except (KeyError, TypeError):
            self.reply_err(req, errno.ENOENT)
        else:
            entry = dict(ino=entry.inode,
                         attr=entry.attr,
                         attr_timeout=1.0,
                         entry_timeout=1.0)
            self.reply_entry(req, entry)

    def readdir(self, req, ino, size, off, fi):
        tree = self.inode_entries[ino].obj
        entries = tree.get_entries()
        self.reply_readdir(req, size, off, entries)

    def read(self, req, ino, size, offset, fi):
        print('read:', ino, size, offset)
        try:
            obj = self.inode_entries[ino].obj
        except (KeyError, AttributeError):
            self.reply_err(req, errno.EIO)
        else:
            try:
                if obj is None:
                    buf = b''
                else:
                    buf = obj.read(size, offset)
            except Exception:
                self.reply_err(req, errno.EIO)
                return

            self.reply_buf(req, buf)

    def readlink(self, req, ino):
        print('readlink', req, ino)
        try:
            entry = self.inode_entries[ino]
        except (KeyError, AttributeError):
            self.reply_err(req, errno.ENOENT)
        else:
            attr = entry.attr
            if (attr['st_mode'] & stat.S_IFLNK) == stat.S_IFLNK:
                reply = entry.obj.read(2048, 0)
                self.reply_readlink(req, reply)
            else:
                self.reply_err(req, errno.ENOENT)

    # def mkdir(self, req, parent, name, mode):
    #     print('mkdir:', parent, name)
    #     ino = self.create_ino()
    #     ctx = self.req_ctx(req)
    #     now = time.time()
    #     attr = dict(st_ino=ino,
    #                 st_mode=S_IFDIR | mode,
    #                 st_nlink=2,
    #                 st_uid=ctx['uid'],
    #                 st_gid=ctx['gid'],
    #                 st_atime=now,
    #                 st_mtime=now,
    #                 st_ctime=now,
    #                 st_rdev=None,
    #                 )

    #     self.attr[ino] = attr
    #     self.attr[parent]['st_nlink'] += 1
    #     self.parent[ino] = parent
    #     self.children[parent][name] = ino

    #     entry = dict(ino=ino, attr=attr, attr_timeout=1.0,
    #     entry_timeout=1.0)
    #     self.reply_entry(req, entry)

    # def mknod(self, req, parent, name, mode, rdev):
    #    print('mknod:', parent, name)
    #    ino = self.create_ino()
    #    ctx = self.req_ctx(req)
    #    now = time.time()
    #    attr = dict(st_ino=ino,
    #                st_mode=mode,
    #                st_nlink=1,
    #                st_uid=ctx['uid'],
    #                st_gid=ctx['gid'],
    #                st_rdev=rdev,
    #                st_atime=now,
    #                st_mtime=now,
    #                st_ctime=now,
    #                )

    #    self.attr[ino] = attr
    #    self.attr[parent]['st_nlink'] += 1
    #    self.children[parent][name] = ino

    #    entry = dict(ino=ino, attr=attr, attr_timeout=1.0,
    #                 entry_timeout=1.0)
    #    self.reply_entry(req, entry)

    # def open(self, req, ino, fi):
    #     print('open:', ino)
    #     self.reply_open(req, fi)

    # def rename(self, req, parent, name, newparent, newname):
    #     print('rename:', parent, name, newparent, newname)
    #     ino = self.children[parent].pop(name)
    #     self.children[newparent][newname] = ino
    #     self.parent[ino] = newparent
    #     self.reply_err(req, 0)

    # def setattr(self, req, ino, attr, to_set, fi):
    #     print('setattr:', ino, to_set)
    #     a = self.attr[ino]
    #     for key in to_set:
    #         if key == 'st_mode':
    #             # Keep the old file type bit fields
    #             a['st_mode'] = S_IFMT(a['st_mode']) |
    #                                   S_IMODE(attr['st_mode'])
    #         else:
    #             a[key] = attr[key]
    #     self.attr[ino] = a
    #     self.reply_attr(req, a, 1.0)

    # def write(self, req, ino, buf, off, fi):
    #     print('write:', ino, off, len(buf))
    #     self.data[ino] = self.data[ino][:off] + buf
    #     self.attr[ino]['st_size'] = len(self.data[ino])
    #     self.reply_write(req, len(buf))

class TestFileSystem(FileSystem):
    def init(self, userdata, conn):
        super().init(userdata, conn)

        tree = self.root
        tree.add_file('file1', obj=ReadableString('file1\n'))
        tree.add_file('file2', obj=ReadableString('file2\n'))
        tree.add_file('file3', obj=ReadableString('file3\n'))
        tree.add_dir('dir1')
        tree.add_dir('dir2')
        tree.add_dir('dir3')
        tree.add_link('link1', 'file1')
        tree.add_link('link2', '/opt')

    def mkdir(self, req, parent, name, mode):
        if parent == self.root.inode and name.decode('utf-8') == 'exit':
            os.kill(os.getpid(), signal.SIGHUP)

        self.reply_err(req, errno.EIO)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('usage: %s <mountpoint>' % sys.argv[0])
        sys.exit(1)

    fuse = TestFileSystem(sys.argv[1])
