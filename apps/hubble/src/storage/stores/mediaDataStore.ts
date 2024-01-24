/**
 * Generates a unique key index used to store a MediaDataAdd message key in the MediaDataAdd index
*/
import {
  MediaAddMessage,
  MediaDataId,
  MediaRemoveMessage,
  HubAsyncResult,
  HubError,
  MessageType,
  bytesCompare,
  getDefaultStoreLimit,
  StoreType,
  isMediaAddMessage,
  isMediaRemoveMessage,
} from "@farcaster/hub-nodejs";

import {
  getMessagesPageByPrefix,
  makeUserKey,
  makeMediaDataIdKey,
  makeFidKey,
  makeMessagePrimaryKey,
  makeTsHash,
} from "../db/message.js";
import { Transaction } from "../db/rocksdb.js";
import { RootPrefix, TRUE_VALUE, UserMessagePostfix, UserPostfix } from "../db/types.js";
import { err, ok, ResultAsync } from "neverthrow";
import { Store } from "./store.js";
import { MessagesPage, PageOptions } from "./types.js";


/**
 * Generates unique keys used to store or fetch MediaDataAdd messages in the adds set index
 *
 * @param fid farMediaDataer id of the user who created the MediaData
 * @param hash hash of the MediaData
 * @returns RocksDB key of the form <root_prefix>:<fid>:<user_postfix>:<tsHash?>
 */
const makeMediaDataAddsKey = (fid: number, hash?: Uint8Array): Buffer => {
    return Buffer.concat([makeUserKey(fid), Buffer.from([UserPostfix.MediaDataAdds]), Buffer.from(hash ?? "")]);
}

// TODO: make parentFid and parentHash fixed size
/**
 * Generates unique keys used to store or fetch MediaDataAdd messages in the byParentKey index
 *
 * @param parentFid the fid of the user who created the parent MediaData
 * @param parentTsHash the timestamp hash of the parent message
 * @param fid the fid of the user who created the MediaData
 * @param tsHash the timestamp hash of the MediaData message
 * @returns RocksDB index key of the form <root_prefix>:<parentFid>:<parentTsHash>:<tsHash?>:<fid?>
 */
const makeMediaDataByParentKey = (parent: MediaDataId | string, fid?: number, tsHash?: Uint8Array): Buffer => {
    const parentKey = typeof parent === "string" ? Buffer.from(parent) : makeMediaDataIdKey(parent);
    return Buffer.concat([
      Buffer.from([RootPrefix.MediaData]),
      parentKey,
      Buffer.from(tsHash ?? ""),
      fid ? makeFidKey(fid) : Buffer.from(""),
    ]);
  };
  
  /**
   * Generates unique keys used to store or fetch MediaDataAdd messages in the removes set index
   *
   * @param fid farMediaDataer id of the user who created the MediaData
   * @param hash hash of the MediaData
   * @returns RocksDB key of the form <root_prefix>:<fid>:<user_postfix>:<tsHash?>
   */
  const makeMediaDataRemovesKey = (fid: number, hash?: Uint8Array): Buffer => {
    return Buffer.concat([makeUserKey(fid), Buffer.from([UserPostfix.MediaDataMessage]), Buffer.from(hash ?? "")]);
  };

  /**
   * Generates unique keys used to store or fetch MediaDataAdd messages in the byParentKey index
   *
   * @param mentionFid the fid of the user who was mentioned in the media data
   * @param fid the fid of the user who created the media data
   * @param tsHash the timestamp hash of the MediaData message
   * @returns RocksDB index key of the form <root_prefix>:<mentionFid>:<tsHash?>:<fid?>
   */
  const makeMediaDataByMentionKey = (mentionFid: number, fid?: number, tsHash?: Uint8Array): Buffer => {
    return Buffer.concat([
      Buffer.from([RootPrefix.MediaData]),
      makeFidKey(mentionFid),
      Buffer.from(tsHash ?? ""),
      fid ? makeFidKey(fid) : Buffer.from(""),
    ]);
  };
  
class MediaDataStore extends Store<MediaAddMessage, MediaRemoveMessage> {
    override _postfix: UserMessagePostfix = UserPostfix.MediaDataMessage;
  override makeAddKey(msg: MediaAddMessage) {
    return makeMediaDataAddsKey(
      msg.data.fid,
      msg.data.mediaDataAddBody || !msg.data.mediaDataRemoveBody ? msg.hash : msg.data.mediaDataRemoveBody.targetHash,
    ) as Buffer;
  }

  override makeRemoveKey(msg: MediaRemoveMessage) {
    return makeMediaDataRemovesKey(
      msg.data.fid,
      msg.data.mediaDataAddBody || !msg.data.mediaDataRemoveBody ? msg.hash : msg.data.mediaDataRemoveBody.targetHash,
    );
  }

  override _isAddType = isMediaAddMessage;
  override _isRemoveType = isMediaRemoveMessage;
  override _addMessageType = MessageType.MEDIA_DATA_ADD;
  override _removeMessageType = MessageType.MEDIA_DATA_REMOVE;

  protected override get PRUNE_SIZE_LIMIT_DEFAULT() {
    return getDefaultStoreLimit(StoreType.MEDIA_DATA);
  }

  protected override get PRUNE_TIME_LIMIT_DEFAULT() {
    return 60 * 60 * 24 * 365; // 1 year
  }

  override async buildSecondaryIndices(txn: Transaction, message: MediaAddMessage): HubAsyncResult<void> {
    const tsHash = makeTsHash(message.data.timestamp, message.hash);

    if (tsHash.isErr()) {
      return err(tsHash.error);
    }

    // Puts the message key into the ByParent index
    const parent = message.data.MediaDataAddBody.parentMediaDataId ?? message.data.MediaDataAddBody.parentUrl;
    if (parent) {
      // biome-ignore lint/style/noParameterAssign: legacy code, avoid using ignore for new code
      txn = txn.put(makeMediaDataByParentKey(parent, message.data.fid, tsHash.value), TRUE_VALUE);
    }

    // Puts the message key into the ByMentions index
    for (const mentionFid of message.data.MediaDataAddBody.mentions) {
      // biome-ignore lint/style/noParameterAssign: legacy code, avoid using ignore for new code
      txn = txn.put(makeMediaDataByMentionKey(mentionFid, message.data.fid, tsHash.value), TRUE_VALUE);
    }

    return ok(undefined);
  }

  override async deleteSecondaryIndices(txn: Transaction, message: MediaAddMessage): HubAsyncResult<void> {
    const tsHash = makeTsHash(message.data.timestamp, message.hash);

    if (tsHash.isErr()) {
      return err(tsHash.error);
    }

    // Delete the message key from the ByMentions index
    for (const mentionFid of message.data.MediaDataAddBody.mentions) {
      // biome-ignore lint/style/noParameterAssign: legacy code, avoid using ignore for new code
      txn = txn.del(makeMediaDataByMentionKey(mentionFid, message.data.fid, tsHash.value));
    }

    // Delete the message key from the ByParent index
    const parent = message.data.MediaDataAddBody.parentMediaDataId ?? message.data.MediaDataAddBody.parentUrl;
    if (parent) {
      // biome-ignore lint/style/noParameterAssign: legacy code, avoid using ignore for new code
      txn = txn.del(makeMediaDataByParentKey(parent, message.data.fid, tsHash.value));
    }

    return ok(undefined);
  }

  override async findMergeAddConflicts(message: MediaAddMessage): HubAsyncResult<void> {
    // Look up the remove tsHash for this MediaData
    const MediaDataRemoveTsHash = await ResultAsync.fromPromise(
      this._db.get(makeMediaDataRemovesKey(message.data.fid, message.hash)),
      () => undefined,
    );

    // If remove tsHash exists, fail because this MediaData has already been removed
    if (MediaDataRemoveTsHash.isOk()) {
      throw new HubError("bad_request.conflict", "message conflicts with a MediaDataRemove");
    }

    // Look up the add tsHash for this MediaData
    const MediaDataAddTsHash = await ResultAsync.fromPromise(
      this._db.get(makeMediaDataAddsKey(message.data.fid, message.hash)),
      () => undefined,
    );

    // If add tsHash exists, no-op because this MediaData has already been added
    if (MediaDataAddTsHash.isOk()) {
      throw new HubError("bad_request.duplicate", "message has already been merged");
    }

    return ok(undefined);
  }

  override async findMergeRemoveConflicts(_message: MediaRemoveMessage): HubAsyncResult<void> {
    return ok(undefined);
  }

  // RemoveWins + LWW, instead of default
  override messageCompare(aType: MessageType, aTsHash: Uint8Array, bType: MessageType, bTsHash: Uint8Array): number {
    // Compare message types to enforce that RemoveWins in case of LWW ties.
    if (aType === this._removeMessageType && bType === this._addMessageType) {
      return 1;
    } else if (aType === this._addMessageType && bType === this._removeMessageType) {
      return -1;
    }

    // Compare timestamps (first 4 bytes of tsHash) to enforce Last-Write-Wins
    const timestampOrder = bytesCompare(aTsHash.subarray(0, 4), bTsHash.subarray(0, 4));
    if (timestampOrder !== 0) {
      return timestampOrder;
    }

    // Compare hashes (last 4 bytes of tsHash) to break ties between messages of the same type and timestamp
    return bytesCompare(aTsHash.subarray(4), bTsHash.subarray(4));
  }

  /** Looks up MediaDataAdd message by MediaData tsHash */
  async getMediaDataAdd(fid: number, hash: Uint8Array): Promise<MediaAddMessage> {
    return await this.getAdd({ data: { fid }, hash });
  }

  /** Looks up MediaDataRemove message by MediaData tsHash */
  async getMediaDataRemove(fid: number, hash: Uint8Array): Promise<MediaRemoveMessage> {
    return await this.getRemove({ data: { fid }, hash });
  }

  /** Gets all MediaDataAdd messages for an fid */
  async getMediaDataAddsByFid(fid: number, pageOptions: PageOptions = {}): Promise<MessagesPage<MediaAddMessage>> {
    return await this.getAddsByFid({ data: { fid } }, pageOptions);
  }

  /** Gets all MediaDataRemove messages for an fid */
  async getMediaDataRemovesByFid(fid: number, pageOptions: PageOptions = {}): Promise<MessagesPage<MediaRemoveMessage>> {
    const MediaDataMessagesPrefix = makeMessagePrimaryKey(fid, UserPostfix.MediaDataMessage);
    return getMessagesPageByPrefix(this._db, MediaDataMessagesPrefix, isMediaRemoveMessage, pageOptions);
  }

  async getAllMediaDataMessagesByFid(
    fid: number,
    pageOptions: PageOptions = {},
  ): Promise<MessagesPage<MediaAddMessage | MediaRemoveMessage>> {
    return await this.getAllMessagesByFid(fid, pageOptions);
  }

  /** Gets all MediaDataAdd messages for a parent MediaData (fid and tsHash) */
  async getMediaDatasByParent(
    parent: MediaDataId | string,
    pageOptions: PageOptions = {},
  ): Promise<MessagesPage<MediaAddMessage>> {
    return await this.getBySecondaryIndex(makeMediaDataByParentKey(parent), pageOptions);
  }

  /** Gets all MediaDataAdd messages for a mention (fid) */
  async getMediaDatasByMention(mentionFid: number, pageOptions: PageOptions = {}): Promise<MessagesPage<MediaAddMessage>> {
    return await this.getBySecondaryIndex(makeMediaDataByParentKey(mentionFid), pageOptions);
  }
}

export default MediaDataStore