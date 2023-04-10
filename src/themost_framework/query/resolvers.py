from ..common import expect, SyncSeriesEventEmitter, object

class MemberResolver(type):

    resolving_member = SyncSeriesEventEmitter()
    resolving_join_member = SyncSeriesEventEmitter()

class MethodResolver(type):

    resolving_method = SyncSeriesEventEmitter()
    