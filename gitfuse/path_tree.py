import os
import time

from stat import S_IFDIR, S_IFREG  # S_IFMT, S_IMODE
from .util import EntryInfo


class ReadableString(str):
    def read(self, size, offset):
        return self.encode('utf-8')[offset:offset + size]


class PathTree:
    def __init__(self, fuse, parent_inode, *, file_attr=None, dir_attr=None,
                 timestamp=None):
        self.fuse = fuse
        self.inode = fuse.create_ino()
        self.fuse.trees[self.inode] = self
        self.parent_inode = parent_inode
        self.entries = {}
        self.inode_to_entry = {}

        if timestamp is None:
            timestamp = time.time()

        if dir_attr is None:
            dir_attr = dict(st_mode=S_IFDIR | 0o555,
                            st_nlink=2,
                            st_uid=os.getuid(),
                            st_gid=os.getgid(),
                            # st_rdev=None,
                            st_atime=timestamp,
                            st_mtime=timestamp,
                            st_ctime=timestamp,
                            )

        if file_attr is None:
            file_attr = dict(st_mode=S_IFREG | 0o555,
                             st_nlink=1,
                             st_uid=os.getuid(),
                             st_gid=os.getgid(),
                             # st_rdev=None,
                             st_atime=timestamp,
                             st_mtime=timestamp,
                             st_ctime=timestamp,
                             st_size=0,
                             )
        self.file_attr = file_attr
        self.dir_attr = dir_attr
        self.attr = dict(dir_attr, inode=self.inode,
                         st_nlink=2)

    def get_entries(self):
        entries = [('.', self.attr),
                   ('..', {'st_ino': self.parent_inode, 'st_mode': S_IFDIR})
                   ]

        for fn, info in self.entries.items():
            entries.append((fn, info.attr))

        return entries

    def add_dir(self, dirname, *, tree=None):
        if tree is None:
            tree = PathTree(self.fuse, self.inode,
                            file_attr=dict(self.file_attr),
                            dir_attr=dict(self.dir_attr),
                            )

        attr = dict(self.dir_attr)
        attr['st_ino'] = tree.inode
        self.attr['st_nlink'] += 1

        entry = EntryInfo(type_='dir', inode=tree.inode, attr=attr,
                          name=dirname, obj=tree)
        self.entries[dirname] = entry
        self.inode_to_entry[tree.inode] = entry
        return entry

    def add_file(self, fn, *, obj=None):
        inode = self.fuse.create_ino()
        attr = dict(self.file_attr)
        attr['st_ino'] = inode
        attr['st_size'] = len(obj)

        entry = EntryInfo(type_='file', inode=inode, attr=attr, name=fn,
                          obj=obj)
        self.entries[fn] = entry
        self.inode_to_entry[inode] = entry
        return entry

    def get_attr(self, inode):
        ret = dict(self.file_attr)
        ret.update(st_ino=inode)
        return ret

    def get_dir_attr(self, inode):
        ret = dict(self.file_attr)
        ret.update(st_ino=inode)
        return ret

    def __contains__(self, inode):
        return (inode in self.inode_to_entry)

    def read(self, inode, size, offset):
        obj = self.inode_to_entry[inode].obj
        if obj is None:
            return b''

        return obj.read(size, offset)
