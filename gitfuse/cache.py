import os
import logging
import weakref
import atexit
import json


logger = logging.getLogger(__name__)


class TaggedCache(dict):
    def __init__(self, fn):
        self.tags = {}
        self.fn = fn

        atexit.register(weakref.WeakMethod(self.save))

        if os.path.exists(fn):
            try:
                self.load(fn)
            except Exception as ex:
                logger.warning('Corrupt tag cache', exc_info=ex)
            else:
                logger.debug('Loaded tag cache from %s', fn)

    def load(self, fn):
        with open(fn, 'rt') as f:
            data = json.load(f)
        self.__setstate__(data)

    def __getstate__(self):
        return {'data': dict(self),
                'tags': self.tags}

    def __setstate__(self, state):
        assert 'tags' in state
        assert 'data' in state

        self.tags.clear()
        self.clear()
        self.tags.update(state['tags'])
        self.update(state['data'])

    def save(self, fn=None):
        if fn is None:
            fn = self.fn

        with open(fn, 'wt') as f:
            json.dump(self.__getstate__(), f)

    def set_with_tag(self, key, tag, value):
        self.tags[key] = tag
        self[key] = value


caches = {'user-repo': TaggedCache('user_repo_cache.json'),
          'org-repo': TaggedCache('org_repo_cache.json'),
          'tags': TaggedCache('tag_cache.json'),
          'branches': TaggedCache('branch_cache.json'),
          'commits': TaggedCache('commit_cache.json'),
          }


def _cache_cleanup():
    for name, cache in caches.items():
        if cache.fn is None:
            continue

        try:
            cache.save()
        except Exception as ex:
            logger.error('Failed to save cache: %s', name,
                         exc_info=ex)

atexit.register(_cache_cleanup)
