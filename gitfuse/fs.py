import os
import sys
import time
import threading
import errno

from stat import S_IFDIR, S_IFREG  # S_IFMT, S_IMODE

from fusell import FUSELL
from .path_tree import (PathTree, ReadableString)


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
            start_inode = self.ino
            self.ino += num
            end_inode = self.ino
            return start_inode + 1, end_inode + 1

    def init(self, userdata, conn):
        self.ino = 0
        self.trees = {}
        self.ino_owners = {}

        tree = PathTree(self, parent_inode=1)
        tree.add_file('file1', obj=ReadableString('file1\n'))
        tree.add_file('file2', obj=ReadableString('file2\n'))
        tree.add_file('file3', obj=ReadableString('file3\n'))
        tree.add_dir('dir1')
        tree.add_dir('dir2')
        tree.add_dir('dir3')
        # self.attr[1] = dict(st_ino=1,
        #                     st_mode=S_IFDIR | 0o777,
        #                     st_nlink=2)

    forget = None

    def find_owner(self, inode):
        # TODO missing data structure
        if inode <= self.ino:
            for tree_inode, tree in self.trees.items():
                if inode in tree:
                    return tree

        raise ValueError('Unknown inode')

    def getattr(self, req, ino, fi):
        print('getattr:', ino)
        if ino in self.trees:
            tree = self.trees[ino]
            self.reply_attr(req, tree.attr, 1.0)
            return
        else:
            try:
                tree = self.find_owner(ino)
            except ValueError:
                pass
            else:
                entry = tree.inode_to_entry[ino]
                self.reply_attr(req, entry.attr, 1.0)
                return

        self.reply_err(req, errno.ENOENT)

    def lookup(self, req, parent_inode, name):
        parent = self.trees[parent_inode]
        try:
            entry = parent.entries[name.decode('utf-8')]
        except KeyError:
            self.reply_err(req, errno.ENOENT)
        else:
            entry = dict(ino=entry.inode,
                         attr=entry.attr,
                         atttr_timeout=1.0,
                         entry_timeout=.0)
            self.reply_entry(req, entry)

    def readdir(self, req, ino, size, off, fi):
        entries = self.trees[ino].get_entries()
        self.reply_readdir(req, size, off, entries)

    def read(self, req, ino, size, offset, fi):
        print('read:', ino, size, offset)
        try:
            tree = self.find_owner(ino)
        except ValueError:
            self.reply_err(req, errno.EIO)
        else:
            self.reply_buf(req, tree.read(ino, size, offset))

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

    #     entry = dict(ino=ino, attr=attr, atttr_timeout=1.0,
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

    #    entry = dict(ino=ino, attr=attr, atttr_timeout=1.0,
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

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('usage: %s <mountpoint>' % sys.argv[0])
        sys.exit(1)
    fuse = FileSystem(sys.argv[1])
