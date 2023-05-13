from pycentroid.common import SyncSeriesEventEmitter


class MemberResolver(type):

    resolving_member = SyncSeriesEventEmitter()
    resolving_join_member = SyncSeriesEventEmitter()


class MethodResolver(type):

    resolving_method = SyncSeriesEventEmitter()
