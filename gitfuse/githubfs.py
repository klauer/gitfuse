'''

Usage:
  githubfs.py [-v] <mount_point> [--users=<list>] [--orgs=<list>]
              [--update-rate=<rate>]

Options:
  --users=<users>         comma-delimited set of users.
  --orgs=<orgs>           comma-delimited set of organizations.
  --update-rate=<rate>    update rate in seconds [default: 60.0].
'''

import os
import signal
import time
import asyncio
import logging
import functools
import threading
import errno

from datetime import datetime

from .fs import FileSystem
from .ghclient import (get_org_repos, get_user_repos, get_tags, get_branches,
                       get_branch_info, get_commit_info)
from .directory_entry import DirectoryEntry


logger = logging.getLogger(__name__)


def iso8601_string_to_posix(string_ts):
    dt = datetime.strptime(string_ts, "%Y-%m-%dT%H:%M:%SZ")
    return time.mktime(dt.timetuple())


def require_initialization(fcn):
    @functools.wraps(fcn)
    def inner(self, *args, **kwargs):
        if not self._initialized:
            self._initialized = True
            self.update()

        return fcn(self, *args, **kwargs)
    return inner


class RepoMetadataDirectory(DirectoryEntry):
    def __init__(self, *args, **kwargs):
        self.repo_owner = kwargs.pop('repo_owner')
        self.repo_name = kwargs.pop('repo_name')
        self._initialized = False
        super().__init__(*args, **kwargs)

    @property
    def loop(self):
        return self.fuse.loop

    @require_initialization
    def __getitem__(self, key):
        return super().__getitem__(key)

    @require_initialization
    def get_entries(self):
        return super().get_entries()


class RepoTagDirectory(RepoMetadataDirectory):
    def update(self):
        _, tags = self.loop.run_until_complete(get_tags(self.repo_owner,
                                                        self.repo_name))

        tags = [(tag['name'], tag['commit']['sha']) for tag in tags]

        futures = [get_commit_info(self.repo_owner, self.repo_name, sha)
                   for tag_name, sha in tags]

        gather_fut = asyncio.gather(*futures)
        tag_info = self.loop.run_until_complete(gather_fut)

        for (tag_name, sha), (_, info) in zip(tags, tag_info):
            try:
                entry = self[tag_name]
            except KeyError:
                entry = self.add_dir(tag_name)

            attr = entry.attr
            try:
                ts = info['author']['date']
            except KeyError:
                print(info, list(info.keys()))
            else:
                mtime = iso8601_string_to_posix(ts)
                if attr['st_ctime'] != mtime:
                    attr['st_ctime'] = attr['st_mtime'] = mtime
                    logger.debug('%s/%s tag %s updated at %s', self.repo_owner,
                                 self.repo_name, tag_name, ts)

        # TODO remove entries that are no longer there


class RepoBranchDirectory(RepoMetadataDirectory):
    def update(self):
        fut = get_branches(self.repo_owner, self.repo_name)
        _, branches = self.loop.run_until_complete(fut)

        branch_names = [branch['name'] for branch in branches]

        futures = [get_branch_info(self.repo_owner, self.repo_name,
                                   branch_name)
                   for branch_name in branch_names]

        gather_fut = asyncio.gather(*futures)
        branch_info = self.loop.run_until_complete(gather_fut)

        for branch_name, (_, info) in zip(branch_names, branch_info):
            try:
                entry = self[branch_name]
            except KeyError:
                entry = self.add_dir(branch_name)

            attr = entry.attr
            try:
                ts = info['commit']['commit']['author']['date']
            except KeyError:
                print(info, list(info.keys()))
            else:
                mtime = iso8601_string_to_posix(ts)
                if attr['st_ctime'] != mtime:
                    attr['st_ctime'] = attr['st_mtime'] = mtime
                    logger.debug('%s/%s branch %s updated at %s',
                                 self.repo_owner, self.repo_name, branch_name,
                                 ts)


class GithubFileSystem(FileSystem):
    def __init__(self, mount_point, users=None, organizations=None,
                 update_rate=60.0, **kwargs):
        if users is None:
            users = []
        if organizations is None:
            organizations = []

        self.loop = asyncio.get_event_loop()
        self.monitoring = dict(users=list(users),
                               organizations=list(organizations),
                               )
        self.update_rate = update_rate
        super().__init__(mount_point, **kwargs)

    def init(self, userdata, conn):
        super().init(userdata, conn)

        root = self.root
        self.users = root.add_dir('users').obj
        self.orgs = root.add_dir('orgs').obj
        self._update_thread = threading.Thread(target=self.update_loop)
        self._update_thread.daemon = True
        self._update_thread.start()

    def update_loop(self):
        asyncio.set_event_loop(self.loop)
        while True:
            self.update()
            time.sleep(self.update_rate)

    def update(self):
        self.update_users()
        self.update_organizations()

    def update_users(self):
        for user in self.monitoring['users']:
            try:
                entry = self.users[user]
            except KeyError:
                entry = self.users.add_dir(user)

            user_dir = entry.obj
            _, repos = self.loop.run_until_complete(get_user_repos(user))
            logger.debug('-- User: %s --', user)

            for repo in repos:
                self.update_repo(user_dir, repo)

    def update_organizations(self):
        for org in self.monitoring['organizations']:
            try:
                entry = self.orgs[org]
            except KeyError:
                entry = self.orgs.add_dir(org)

            org_dir = entry.obj
            _, repos = self.loop.run_until_complete(get_org_repos(org))
            logger.debug('-- Organization: %s --', org)
            for repo in repos:
                self.update_repo(org_dir, repo)

    def update_repo(self, parent_obj, repo):
        repo_name = repo['name']

        try:
            entry = parent_obj[repo_name]
        except KeyError:
            entry = parent_obj.add_dir(repo_name)

        attr, repo_dir = entry.attr, entry.obj

        updated_at = iso8601_string_to_posix(repo['updated_at'])
        repo_owner = repo['owner']['login']
        if attr['st_mtime'] != updated_at:
            logger.debug('Repo %s/%s updated at: %s', repo_owner, repo_name,
                         repo['updated_at'])
            attr['st_mtime'] = updated_at
            attr['st_ctime'] = iso8601_string_to_posix(repo['created_at'])

            self.update_tags(repo_dir, repo_owner, repo_name)
        else:
            logger.debug('Repo %s unmodified', repo_name)

        self.update_branches(repo_dir, repo_owner, repo_name)

    def update_tags(self, repo_dir, repo_owner, repo_name):
        try:
            entry = repo_dir['tags']
        except KeyError:
            tag_dir = RepoTagDirectory(self, repo_dir.inode,
                                       repo_owner=repo_owner,
                                       repo_name=repo_name)
            entry = repo_dir.add_dir('tags', dirobj=tag_dir)
        else:
            tag_dir = entry.obj
            tag_dir.update()

    def update_branches(self, repo_dir, repo_owner, repo_name):
        try:
            entry = repo_dir['branches']
        except KeyError:
            branch_dir = RepoBranchDirectory(self, repo_dir.inode,
                                             repo_owner=repo_owner,
                                             repo_name=repo_name)
            entry = repo_dir.add_dir('branches', dirobj=branch_dir)
        else:
            branch_dir = entry.obj
            branch_dir.update()

    def mkdir(self, req, parent, name, mode):
        if parent == self.root.inode and name.decode('utf-8') == 'exit':
            os.kill(os.getpid(), signal.SIGHUP)

        self.reply_err(req, errno.EIO)


def main(mount_point, users, orgs, update_rate):
    GithubFileSystem(mount_point, users=users, organizations=orgs,
                     update_rate=update_rate)


if __name__ == "__main__":
    from docopt import docopt
    args = docopt(__doc__, version='0.1')

    if args['-v']:
        for loggername in ('gitfuse', '__main__'):
            logging.getLogger(loggername).setLevel(logging.DEBUG)
        logging.basicConfig()

    main(mount_point=args['<mount_point>'],
         users=args['--users'].split(','),
         orgs=args['--orgs'].split(','),
         update_rate=float(args['--update-rate']))
