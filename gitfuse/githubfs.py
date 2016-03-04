'''

Usage:
    githubfs.py [-v] <mount_point> [--users=<users>] [--orgs=<orgs>]
'''

import time
import asyncio
import logging

from datetime import datetime

from .fs import FileSystem
from .ghclient import (get_org_repos, get_user_repos, get_tags, get_branches,
                       get_branch_info, get_commit_info)
from .directory_entry import DirectoryEntry


logger = logging.getLogger(__name__)


def iso8601_string_to_posix(string_ts):
    dt = datetime.strptime(string_ts, "%Y-%m-%dT%H:%M:%SZ")
    return time.mktime(dt.timetuple())


class RepoMetadataDirectory(DirectoryEntry):
    def __init__(self, *args, **kwargs):
        self.repo_owner = kwargs.pop('repo_owner')
        self.repo_name = kwargs.pop('repo_name')
        self._initialized = False
        super().__init__(*args, **kwargs)

    @property
    def loop(self):
        return self.fuse.loop

    def _initialize(self):
        pass

    def get_entries(self):
        if not self._initialized:
            try:
                self._initialize()
            finally:
                self._initialized = True

        return super().get_entries()


class RepoTagDirectory(RepoMetadataDirectory):
    def _initialize(self):
        _, tags = self.loop.run_until_complete(get_tags(self.repo_owner,
                                                        self.repo_name))

        tags = [(tag['name'], tag['commit']['sha']) for tag in tags]

        futures = [get_commit_info(self.repo_owner, self.repo_name, sha)
                   for tag_name, sha in tags]

        gather_fut = asyncio.gather(*futures)
        tag_info = self.loop.run_until_complete(gather_fut)

        for (tag_name, sha), (_, info) in zip(tags, tag_info):
            entry = self.add_dir(tag_name)
            attr = entry.attr
            try:
                ts = info['author']['date']
            except KeyError:
                print(info, list(info.keys()))
            else:
                mtime = iso8601_string_to_posix(ts)
                attr['st_ctime'] = attr['st_mtime'] = mtime


class RepoBranchDirectory(RepoMetadataDirectory):
    def _initialize(self):
        fut = get_branches(self.repo_owner, self.repo_name)
        _, branches = self.loop.run_until_complete(fut)

        branch_names = [branch['name'] for branch in branches]

        futures = [get_branch_info(self.repo_owner, self.repo_name,
                                   branch_name)
                   for branch_name in branch_names]

        gather_fut = asyncio.gather(*futures)
        branch_info = self.loop.run_until_complete(gather_fut)

        for branch_name, (_, info) in zip(branch_names, branch_info):
            entry = self.add_dir(branch_name)
            attr = entry.attr
            try:
                ts = info['commit']['commit']['author']['date']
            except KeyError:
                print(info, list(info.keys()))
            else:
                mtime = iso8601_string_to_posix(ts)
                attr['st_ctime'] = attr['st_mtime'] = mtime


class GithubFileSystem(FileSystem):
    def __init__(self, *args, **kwargs):
        self.monitoring = dict(users=kwargs.pop('users', []),
                               organizations=kwargs.pop('organizations', []),
                               )

        super().__init__(*args, **kwargs)

    def init(self, userdata, conn):
        super().init(userdata, conn)

        tree = self.root
        self.loop = asyncio.get_event_loop()

        self.users = tree.add_dir('users').obj
        self.orgs = tree.add_dir('orgs').obj
        for user in self.monitoring['users']:
            user_tree = self.users.add_dir(user).obj
            _, repos = self.loop.run_until_complete(get_user_repos(user))
            logger.debug('-- User: %s --', user)

            for repo in repos:
                self._init_repo(user_tree, repo)

        for org in self.monitoring['organizations']:
            org_tree = self.orgs.add_dir(org).obj
            _, repos = self.loop.run_until_complete(get_org_repos(org))
            logger.debug('-- Organization: %s --', user)

            for repo in repos:
                self._init_repo(org_tree, repo)

    def _init_repo(self, tree, repo):
        repo_name = repo['name']
        logger.debug('Repo %s updated at: %s', repo_name, repo['updated_at'])

        entry = tree.add_dir(repo['name'])
        attr, repo_tree = entry.attr, entry.obj
        attr['st_mtime'] = iso8601_string_to_posix(repo['updated_at'])
        attr['st_ctime'] = iso8601_string_to_posix(repo['created_at'])


        repo_owner = repo['owner']['login']
        tag_tree = RepoTagDirectory(self, tree.inode, repo_owner=repo_owner,
                                    repo_name=repo_name)
        repo_tree.add_dir('tags', tree=tag_tree)

        branch_tree = RepoBranchDirectory(self, tree.inode,
                                          repo_owner=repo_owner,
                                          repo_name=repo_name)
        repo_tree.add_dir('branches', tree=branch_tree)

    def update_repo(self, user, repo_name):
        pass


def main(mount_point, users, orgs):
    loop = asyncio.get_event_loop()
    GithubFileSystem(mount_point, users=users, organizations=orgs)


if __name__ == "__main__":
    from docopt import docopt
    args = docopt(__doc__, version='0.1')

    if args['-v']:
        for loggername in ('gitfuse', '__main__'):
            logging.getLogger(loggername).setLevel(logging.DEBUG)
        logging.basicConfig()

    print(args)
    main(mount_point=args['<mount_point>'],
         users=args['--users'].split(','),
         orgs=args['--orgs'].split(','))
