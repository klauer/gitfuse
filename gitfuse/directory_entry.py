import os
import time

import stat
from .util import EntryInfo


class ReadableString(str):
    def read(self, size, offset):
        return self.encode('utf-8')[offset:offset + size]


class DirectoryEntry:
    def __init__(self, fuse, parent_inode, *, file_attr=None, dir_attr=None,
                 timestamp=None):
        self.inode = fuse.create_ino()
        self.fuse = fuse
        self.parent_inode = parent_inode
        self.entry_by_name = {}
        self.inodes = []

        if timestamp is None:
            timestamp = time.time()

        if dir_attr is None:
            dir_attr = dict(st_mode=stat.S_IFDIR | 0o555,
                            st_nlink=2,
                            st_uid=os.getuid(),
                            st_gid=os.getgid(),
                            # st_rdev=None,
                            st_atime=timestamp,
                            st_mtime=timestamp,
                            st_ctime=timestamp,
                            )

        if file_attr is None:
            file_attr = dict(st_mode=stat.S_IFREG | 0o555,
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
        self.entry = EntryInfo(type_='direntry', inode=self.inode,
                               attr=self.attr, name='', obj=self)

        fuse.inode_entries[self.inode] = self.entry

    def __getitem__(self, name):
        return self.entry_by_name[name]

    def get_entries(self):
        entries = [('.', self.attr),
                   ('..', dict(st_ino=self.parent_inode, st_mode=stat.S_IFDIR))
                   ]

        for fn, info in self.entry_by_name.items():
            entries.append((fn, info.attr))

        return entries

    def add_dir(self, dirname, *, dirobj=None):
        if dirobj is None:
            dirobj = DirectoryEntry(self.fuse, self.inode,
                                    file_attr=dict(self.file_attr),
                                    dir_attr=dict(self.dir_attr),
                                    )

        attr = dirobj.attr
        attr['st_ino'] = dirobj.inode
        self.attr['st_nlink'] += 1

        entry = EntryInfo(type_='dir', inode=dirobj.inode, attr=attr,
                          name=dirname, obj=dirobj)
        self.entry_by_name[dirname] = entry
        self.fuse.inode_entries[dirobj.inode] = entry
        return entry

    def add_file(self, fn, *, obj=None, inode=None):
        if inode is None:
            inode = self.fuse.create_ino()

        attr = dict(self.file_attr)
        attr['st_ino'] = inode
        attr['st_size'] = len(obj)

        entry = EntryInfo(type_='file', inode=inode, attr=attr, name=fn,
                          obj=obj)
        self.entry_by_name[fn] = entry
        self.fuse.inode_entries[inode] = entry
        return entry

    def add_link(self, fn, dest, *, obj=None, inode=None):
        if inode is None:
            inode = self.fuse.create_ino()

        attr = dict(self.file_attr)
        attr['st_mode'] = (stat.S_IFLNK | 0o777)
        attr['st_ino'] = inode
        attr['st_size'] = len(dest) + 1
        attr['st_nlink'] = 1

        entry = EntryInfo(type_='link', inode=inode, attr=attr, name=fn,
                          obj=ReadableString(dest))
        self.entry_by_name[fn] = entry
        self.fuse.inode_entries[inode] = entry
        return entry

    def add_files(self, filenames):
        inodes = self.fuse.create_ino_range(filenames)
        return {fn: self.add_file(fn, inode=ino)
                for ino, fn in zip(filenames, inodes)
                }

    def get_attr(self, inode):
        return dict(self.file_attr, st_ino=inode)

    def get_dir_attr(self, inode):
        return dict(self.dir_attr, st_ino=inode)
