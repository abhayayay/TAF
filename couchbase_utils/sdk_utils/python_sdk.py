from datetime import timedelta, datetime

from couchbase.durability import DurabilityType, PersistTo, ReplicateTo, DurabilityLevel, PersistToExtended, \
    ServerDurability, ClientDurability
from couchbase.options import \
    RemoveOptions, \
    GetOptions, \
    InsertOptions, \
    ReplaceOptions, \
    TouchOptions, \
    UpsertOptions, \
    MutateInOptions
from couchbase.transcoder import Transcoder, JSONTranscoder, RawBinaryTranscoder, RawStringTranscoder

from BucketLib.bucket import Bucket
from constants.sdk_constants.sdk_client_constants import SDKConstants


class SDKOptions(object):

    @staticmethod
    def get_duration(time: int, time_unit: str) -> timedelta:
        ini_time_for_now = datetime.now()
        new_final_time = ini_time_for_now
        time_unit = time_unit.lower()
        if time_unit == SDKConstants.TimeUnit.MILLISECONDS:
            new_final_time += timedelta(milliseconds=time)
        elif time_unit == SDKConstants.TimeUnit.MINUTES:
            new_final_time += timedelta(minutes=time)
        elif time_unit == SDKConstants.TimeUnit.HOURS:
            new_final_time += timedelta(hours=time)
        elif time_unit == SDKConstants.TimeUnit.DAYS:
            new_final_time += timedelta(days=time)
        else:
            new_final_time += timedelta(seconds=time)
        return new_final_time - ini_time_for_now

    @staticmethod
    def get_insert_options(exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS, persist_to=None, replicate_to=None, timeout=5,
                           time_unit=SDKConstants.TimeUnit.SECONDS, durability=None, sdk_retry_strategy=None,
                           doc_type="json") -> InsertOptions:

        return InsertOptions(timeout=SDKOptions.get_duration(time=timeout, time_unit=time_unit),
                             expiry=SDKOptions.get_duration(time=exp, time_unit=time_unit),
                             durability=SDKOptions.getDurability(persist_to, replicate_to, durability),
                             transcoder=SDKOptions.getTranscoder(doc_type))

    @staticmethod
    def get_read_options(timeout, time_unit=SDKConstants.TimeUnit.SECONDS,
                         sdk_retry_strategy=None) -> GetOptions:

        return GetOptions(timeout=SDKOptions.get_duration(time=timeout, time_unit=time_unit))

    @staticmethod
    def get_upsert_options(exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                           persist_to=None, replicate_to=None,
                           timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                           durability=None, preserve_expiry=None,
                           sdk_retry_strategy=None, doc_type="json") -> UpsertOptions:
        return UpsertOptions(timeout=SDKOptions.get_duration(time=timeout, time_unit=time_unit),
                             expiry=SDKOptions.get_duration(time=exp, time_unit=time_unit),
                             preserve_expiry=preserve_expiry,
                             durability=SDKOptions.getDurability(persist_to=persist_to, replicate_to=replicate_to,
                                                                 durability=durability),
                             transcoder=SDKOptions.getTranscoder(doc_type=doc_type))

    @staticmethod
    def get_remove_options(persist_to=None, replicate_to=None,
                           timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                           durability=None, cas=0, sdk_retry_strategy=None) -> RemoveOptions:

        return RemoveOptions(timeout=SDKOptions.get_duration(time=timeout, time_unit=time_unit),
                             cas=cas,
                             durability=SDKOptions.getDurability(persist_to, replicate_to, durability))

    @staticmethod
    def get_replace_options(exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                            persist_to=None, replicate_to=None,
                            timeout=5,
                            time_unit=SDKConstants.TimeUnit.SECONDS,
                            durability=None, cas=0,
                            preserve_expiry=None, sdk_retry_strategy=None, doc_type="json") -> ReplaceOptions:

        return ReplaceOptions(timeout=SDKOptions.get_duration(time=timeout, time_unit=time_unit),
                              expiry=SDKOptions.get_duration(time=exp, time_unit=time_unit),
                              cas=cas,
                              preserve_expiry=preserve_expiry,
                              durability=SDKOptions.getDurability(persist_to=persist_to, replicate_to=replicate_to,
                                                                  durability=durability),
                              transcoder=SDKOptions.getTranscoder(doc_type=doc_type))

    @staticmethod
    def get_touch_options(timeout=5, time_unit=SDKConstants.TimeUnit.SECONDS,
                          sdk_retry_strategy=None) -> TouchOptions:

        return TouchOptions(timeout=SDKOptions.get_duration(time=timeout, time_unit=time_unit))

    @staticmethod
    def get_mutate_in_options(exp=0, exp_unit=SDKConstants.TimeUnit.SECONDS,
                              persist_to=None, replicate_to=None, timeout=5,
                              time_unit=SDKConstants.TimeUnit.SECONDS,
                              durability=None,
                              store_semantics=None,
                              preserve_expiry=None, sdk_retry_strategy=None,
                              access_deleted=False,
                              create_as_deleted=False, cas=None) -> MutateInOptions:

        return MutateInOptions(timeout=SDKOptions.get_duration(time=timeout, time_unit=time_unit),
                               cas=cas,
                               durability=SDKOptions.getDurability(persist_to=persist_to,
                                                                   replicate_to=replicate_to,
                                                                   durability=durability),
                               store_semantics=store_semantics,
                               access_deleted=access_deleted,
                               preserve_expiry=preserve_expiry)

    @staticmethod
    def getDurability(persist_to: int = None, replicate_to: int = None, durability: str = None) -> DurabilityType:

        if durability is not None:
            return ServerDurability(level=getattr(DurabilityLevel, durability))

        if persist_to is not None and replicate_to is not None:
            return ClientDurability(replicate_to=SDKOptions.get_replicate_to(replicate_to),
                                    persist_to=SDKOptions.get_persist_to(persist_to))

    @staticmethod
    def get_persist_to(persist_to=None):
        try:
            persist_list = [PersistTo.NONE, PersistTo.ONE, PersistTo.TWO,
                            PersistTo.THREE]
            return persist_list[persist_to]
        except IndexError:
            return PersistToExtended.ACTIVE

    @staticmethod
    def get_replicate_to(replicate_to=None):
        try:
            replicate_list = [ReplicateTo.NONE, ReplicateTo.ONE,
                              ReplicateTo.TWO, ReplicateTo.THREE]
            return replicate_list[replicate_to]
        except IndexError:
            return ReplicateTo.NONE

    @staticmethod
    def getTranscoder(doc_type: str) -> Transcoder:
        if doc_type == "binary":
            return RawBinaryTranscoder()
        elif doc_type == "string":
            return RawStringTranscoder()
        else:
            return JSONTranscoder()
