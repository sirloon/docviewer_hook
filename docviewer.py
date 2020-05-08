from functools import partial
import types
from biothings.hub.databuild.backend import create_backend


def docfetcher(backend_url, _id=None, count=False, q={}, limit=100):
    """
    Returns documents by _id and using query q. Backend follows
    biothings.hub.databuild.backend.create_backend() format,
    see this function documentation for more
    """
    backend = create_backend(backend_url)
    if count:
        return backend.count()
    elif _id:
        return backend.get_from_id(_id)
    else:
        q = backend.query(q)
        if isinstance(q,types.GeneratorType):
            # ES backend, need fetch then limit...
            return [doc for doc in q][:limit]
        else:
            # mongo
            return [doc for doc in q.limit(limit)]


# first endpoint /buildviewer/(.*) exposing command via GET. In that case, backend_url is
# expected to be passed in the URL, and *must* matcha target collection
expose(
    endpoint_name="buildviewer",
    command_name="docfetcher",
    method="get",
)

# this second endpoint /docviewer, using POST method, can handle more complex backend_url
# to access datasource collection, ES index, etc... It's more generic.
# force_bodyargs=True is used to force "backend_url" param to be specificed in the body,
# instead of generating a /docviewer/(.*) endpoint
expose(
    endpoint_name="docviewer",
    command_name="docfetcher",
    method="post",
    force_bodyargs=True,
)

# query example:
# - query a build using GET endpoint
# $ curl localhost:7280/buildviewer/mvcgi_20200408_fxn2wori
# - same using POST endpoint
# $ curl -XPOST -d '{"backend_url":"mvcgi_20200408_fxn2wori"}' localhost:7280/docviewer
# - limit to 3 docs returned
# $ curl -XPOST -d '{"backend_url":"mvcgi_20200408_fxn2wori","limit":3}' localhost:7280/docviewer
# - fetch by _id
# $ curl 'localhost:7280/buildviewer/mvcgi_20200408_fxn2wori?_id=chr2:g.198267359C>G'
# - specify a query
# $ curl -XPOST -d '{"backend_url":"mvcgi_20200408_fxn2wori","q":{"cgi.association":"Resistant"}}' localhost:7280/docviewer
# - now accessing a single datasource collection (backend_url is a list)
# $ curl -XPOST -d '{"backend_url":["src","ensembl_genomic_pos_mm9"]}' localhost:7280/docviewer
# - now accessing an ES index, fetching one document
# $ curl -XPOST -d '{"backend_url":["localhost:9200","kaviar_20191112_7widj2n5","variant"],"_id":"chr12:g.102137772_102137775del"}' localhost:7280/docviewer
