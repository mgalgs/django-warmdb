class WarmDBError(RuntimeError):
    pass


class WarmDBNotInitialized(WarmDBError):
    pass


class WarmDBSchemaChanged(WarmDBError):
    pass


class WarmDBNoReadyDB(WarmDBError):
    pass


class WarmDBUnsupported(WarmDBError):
    pass


class WarmDBSnapshotNotFound(WarmDBError):
    pass


class WarmDBSnapshotHasClones(WarmDBError):
    pass
