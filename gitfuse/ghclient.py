import os
import time
import asyncio
import logging

import aiohttp

from .cache import caches

access_token = os.environ.get('OAUTH_TOKEN', None)
logger = logging.getLogger(__name__)


class GitResponse:
    def __init__(self, response, json):
        self.response = response
        self.json = json

        headers = response.headers
        self.timestamp = headers['DATE']
        self.etag = headers.get('ETAG', None)
        self.rate_limit_remaining = int(headers['X-RATELIMIT-REMAINING'])
        self.unmodified = (response.status == 304)
        if (self.rate_limit_remaining % 100) == 0:
            logger.debug('Rate limit remaining: %s', self.rate_limit_remaining)

    @property
    def headers(self):
        return self.response.headers


class CachedResponse:
    def __init__(self, timestamp, json):
        self.timestamp = timestamp
        self.json = json


def make_session():
    params = {}
    if access_token is not None:
        params['access_token'] = access_token

    system_proxy = os.environ.get('http_proxy', None)
    if system_proxy is not None:
        conn = aiohttp.ProxyConnector(proxy=system_proxy)
        return aiohttp.ClientSession(connector=conn), params

    return aiohttp.ClientSession(), params


async def _get_json_response(url, *, user_params=None, session=None,
                             user_headers=None, etag=None):
    params = {}
    headers = {}

    if session is None:
        session, session_params = make_session()
        own_session = True
        params.update(session_params)
    else:
        own_session = False

    if user_params is not None:
        params.update(user_params)

    if user_headers is not None:
        headers.update(user_headers)

    if etag is not None:
        headers['If-None-Match'] = etag

    try:
        async with session.get('https://api.github.com/{}'.format(url),
                               params=params, headers=headers) as resp:
            if etag is not None and resp.status == 304:
                json = {}
            else:
                json = await resp.json()
    except Exception:
        raise
    finally:
        if own_session:
            session.close()

    return GitResponse(resp, json)


async def get_cacheable_response(key, url, cache):
    resp = await _get_json_response(url, etag=cache.tags.get(key, None))
    if not resp.unmodified:
        cache.set_with_tag(key, tag=resp.etag, value=resp.json)

    return resp, cache[key]


async def get_user_repos(user):
    resp = await get_cacheable_response(user,
                                        'users/{}/repos'.format(user),
                                        cache=caches['user-repo'])
    return resp


async def get_org_repos(org):
    resp = await get_cacheable_response(org,
                                        'orgs/{}/repos'.format(org),
                                        cache=caches['org-repo'])
    return resp


async def get_if_newer_than_cache(url, cache, *, max_age=60 * 10, key=None):
    if key is None:
        key = url

    t0 = time.time()
    cached_time = cache.tags.get(key, 0.0)
    cache_age = t0 - cached_time
    if cache_age < max_age:
        resp = CachedResponse(cache.tags[key], cache[key])
    else:
        resp = await _get_json_response(url)
        cache.set_with_tag(key, tag=t0, value=resp.json)

    return resp


async def get_tags(owner, repo, **kwargs):
    url = 'repos/{owner}/{repo}/tags'.format(owner=owner, repo=repo)
    resp = await get_if_newer_than_cache(url, cache=caches['tags'], **kwargs)
    return resp, resp.json


async def get_branches(owner, repo, **kwargs):
    url = 'repos/{owner}/{repo}/branches'.format(owner=owner, repo=repo)
    resp = await get_if_newer_than_cache(url, cache=caches['branches'],
                                         **kwargs)
    return resp, resp.json


async def get_branch_info(owner, repo, branch, **kwargs):
    url = ('repos/{owner}/{repo}/branches/{branch}'
           ''.format(owner=owner, repo=repo, branch=branch))

    resp = await get_if_newer_than_cache(url, cache=caches['branches'],
                                         **kwargs)
    return resp, resp.json


async def get_commit_info(owner, repo, sha1, **kwargs):
    url = ('repos/{owner}/{repo}/git/commits/{sha1}'
           ''.format(owner=owner, repo=repo, sha1=sha1))

    resp = await get_if_newer_than_cache(url, cache=caches['commits'],
                                         **kwargs)
    return resp, resp.json



if __name__ == '__main__':
    for loggername in ('gitfuse', '__main__'):
        logging.getLogger(loggername).setLevel(logging.DEBUG)
    logging.basicConfig()

    loop = asyncio.get_event_loop()
    logger.debug('Get user repos')
    kresp, klauer_repos = loop.run_until_complete(get_user_repos('klauer'))
    logger.debug('Get org repos')
    nresp, nsls2_repos = loop.run_until_complete(get_org_repos('nsls-ii'))
    logger.debug('Get tags')
    tresp, ophyd_tags = loop.run_until_complete(get_tags('nsls-ii', 'ophyd'))
    logger.debug('Get branches')
    bresp, ophyd_branches = loop.run_until_complete(get_branches('nsls-ii',
                                                                 'ophyd'))
    loop.close()
