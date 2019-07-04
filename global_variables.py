import chunkcache
import cache
import S3Connector
import user_session
import config

_s3connector = S3Connector.s3connector()
_chunkcache = chunkcache.chunkcache()
_hashcache = cache.hashcache()
_metadata_cache = cache.cache(100, 'metadata')
_user_session = user_session.user_session(config.sqlite3_db_path)
