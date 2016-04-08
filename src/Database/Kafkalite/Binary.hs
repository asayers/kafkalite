{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Database.Kafkalite.Binary
    ( getMessageEntry, putMessageEntry
    , getIndex, putIndex
    ) where

import Control.Applicative
import Control.Monad
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Digest.CRC32
import Data.Int
import qualified Data.Vector.Unboxed as VU

import Database.Kafkalite.Types

-- References:
-- [0]: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets
-- [1]: https://stackoverflow.com/questions/19394669/why-index-file-exists-in-kafka-log-directory

-------------------------------------------------------------------------------
-- Message sets

-- One structure common to both the produce and fetch requests is the
-- message set format. A message in kafka is a key-value pair with a small
-- amount of associated metadata. A message set is just a sequence of
-- messages with offset and size information. This format happens to be
-- used both for the on-disk storage on the broker and the on-the-wire
-- format.
--
-- A message set is also the unit of compression in Kafka, and we allow
-- messages to recursively contain compressed message sets to allow batch
-- compression.
--
-- N.B., MessageSets are not preceded by an int32 like other array elements
-- in the protocol.

-- MessageSet => [Offset MessageSize Message]
--   Offset => int64
--   MessageSize => int32

getMessageEntry :: Get MessageEntry
getMessageEntry = do
    offset <- getOffset
    size <- fromIntegral <$> getInt32be
    message <- getMessage
    return MessageEntry{..}

putMessageEntry :: MessageEntry -> Put
putMessageEntry MessageEntry{..} = do
    putOffset offset
    putInt32be (fromIntegral size)
    putMessage message

getMessage :: Get Message
getMessage = msum
    [ MV0 <$> getMessageV0
    , MV1 <$> getMessageV1
    ]

putMessage :: Message -> Put
putMessage = \case
    MV0 x -> putMessageV0 x
    MV1 x -> putMessageV1 x

-------------------------------------------------------------------------------
-- Message v0

-- Message => Crc MagicByte Attributes Key Value
--   Crc => int32
--   MagicByte => int8
--   Attributes => int8
--   Key => bytes
--   Value => bytes

getMessageV0 :: Get MessageV0
getMessageV0 =
    checkingCRC32 $ do
    requireInt8 0
    mv0Attributes <- getAttributes
    mv0Key <- getKafkaBytes
    mv0Value <- getKafkaBytes   -- TODO: make sure these are being evaluated strictly
    return MessageV0{..}

putMessageV0 :: MessageV0 -> Put
putMessageV0 MessageV0{..} =
    writingCRC32 $ do
    putInt8 0
    putAttributes mv0Attributes
    putKafkaBytes mv0Key
    putKafkaBytes mv0Value

-------------------------------------------------------------------------------
-- Message v1 (supported since 0.10.0)

-- Message => Crc MagicByte Attributes Key Value
--   Crc => int32
--   MagicByte => int8
--   Attributes => int8
--   Timestamp => int64
--   Key => bytes
--   Value => bytes

getMessageV1 :: Get MessageV1
getMessageV1 =
    checkingCRC32 $ do
    requireInt8 1
    mv1Attributes <- getAttributes
    mv1Timestamp <- getTimestamp
    mv1Key <- getKafkaBytes
    mv1Value <- getKafkaBytes   -- TODO: make sure these are being evaluated strictly
    return MessageV1{..}

putMessageV1 :: MessageV1 -> Put
putMessageV1 MessageV1{..} =
    writingCRC32 $ do
    putInt8 1
    putAttributes mv1Attributes
    putTimestamp  mv1Timestamp
    putKafkaBytes mv1Key
    putKafkaBytes mv1Value

-------------------------------------------------------------------------------
-- Offset

-- This is the offset used in kafka as the log sequence number. When the
-- producer is sending non compressed messages, it can set the offsets to
-- anything. When the producer is sending compressed messages, to avoid
-- server side recompression, each compressed message should have offset
-- starting from 0 and increasing by one for each inner message in the
-- compressed message. (see more details about compressed messages in Kafka
-- below)

getOffset :: Get Offset
getOffset = Offset <$> getInt64be

putOffset :: Offset -> Put
putOffset = putInt64be . unOffset

-------------------------------------------------------------------------------
-- CRC

-- The CRC is the CRC32 of the remainder of the message bytes. This is used
-- to check the integrity of the message on the broker and consumer.

-- | Write the given bytes prepended by their CRC32 checksum
writingCRC32 :: Put -> Put
writingCRC32 encoding = do
    let bs = runPut encoding
    putWord32be (crc32 bs)
    putLazyByteString bs

-- | Run the given decoder, checking that the consumed bytes match
-- a preceding CRC32 checksum.
checkingCRC32 :: Get a -> Get a
checkingCRC32 decoder = do
    crc <- getWord32be
    (msg, bs) <- withConsumedBytes decoder
    guard $ crc == crc32 bs
    return msg

-------------------------------------------------------------------------------
-- MagicByte

-- This is a version id used to allow backwards compatible evolution of the
-- message binary format. The current value is 1.

-- | Fail if the next byte is not the given one.
{-# INLINE requireInt8 #-}
requireInt8 :: Int8 -> Get ()
requireInt8 val =
    guard . (== val) =<< getInt8

-------------------------------------------------------------------------------
-- Attributes

-- This byte holds metadata attributes about the message. The lowest 3 bits
-- contain the compression codec used for the message. The fourth lowest
-- bit represents the timestamp type. 0 stands for CreateTime and 1 stands
-- for LogAppendTime. The producer should always set this bit to 0 (since
-- 0.10.0). All other bits should be set to 0.

getAttributes :: Get Attributes
getAttributes = do
    attrs <- getInt8
    let compBits = (attrs `shift` 0) .&. 0x7  -- lower 3 bits
    let tsBits   = (attrs `shift` 3) .&. 0x1  -- 4th lowest bit
    compression <- case compBits of
        0 -> return None
        1 -> return GZip
        2 -> return Snappy
        _ -> fail "Unknown compression codec"
    timestampType <- case tsBits of
        0 -> return CreateTime
        1 -> return LogAppendTime
        _ -> fail "Unknown timestamp type"
    return Attributes{..}

putAttributes :: Attributes -> Put
putAttributes Attributes{..} = do
    let compBits = case compression of
          None   -> 0
          GZip   -> 1
          Snappy -> 2
    let timeBits = case timestampType of
          CreateTime    -> 0
          LogAppendTime -> 1
    putInt8 $
        compBits `shift` 0 .&.
        timeBits `shift` 3


-------------------------------------------------------------------------------
-- Timestamp

-- This is the timestamp of the message. The timestamp type is indicated in
-- the attributes. Unit is milliseconds since beginning of the epoch
-- (midnight Jan 1, 1970 (UTC)).

getTimestamp :: Get Timestamp
getTimestamp = Timestamp <$> getInt64be

putTimestamp :: Timestamp -> Put
putTimestamp = putInt64be . unTimestamp

-------------------------------------------------------------------------------
-- Indices

getIndex :: Get Index
getIndex = VU.fromList <$> many getIndexEntry

putIndex :: Index -> Put
putIndex = VU.mapM_ putIndexEntry

getIndexEntry :: Get IndexEntry
getIndexEntry = do
    relativeOffset <- RelativeOffset <$> getInt32be
    filePosition <- FilePosition <$> getInt32be
    guard $ relativeOffset /= RelativeOffset 0  -- Otherwise we're at the end
    return IndexEntry{..}

putIndexEntry :: IndexEntry -> Put
putIndexEntry IndexEntry{..} = do
    putInt32be $ unRelativeOffset relativeOffset
    putInt32be $ unFilePosition filePosition

-------------------------------------------------------------------------------
-- Kafka protocol primitive types

-- Fixed Width Primitives: int8, int16, int32, int64 - Signed integers
-- with the given precision (in bits) stored in big endian order.

-- Variable Length Primitives: bytes, string - These types consist of
-- a signed integer giving a length N followed by N bytes of content.
-- A length of -1 indicates null. string uses an int16 for its size, and
-- bytes uses an int32.

{-# INLINE getKafkaBytes #-}
getKafkaBytes :: Get (Maybe ByteString)
getKafkaBytes = do
    len <- getInt32be
    if len == -1
      then return Nothing
      else Just <$> getByteString (fromIntegral len)

{-# INLINE putKafkaBytes #-}
putKafkaBytes :: Maybe ByteString -> Put
putKafkaBytes Nothing = putInt32be (-1)
putKafkaBytes (Just bs) = do
    putInt32be (fromIntegral $ BS.length bs)
    putByteString bs

-------------------------------------------------------------------------------
-- Utilities

-- | Runs the given decoder, returning both its result and the bytes which
-- it consumed. If the given decoder fails, this function also fails
-- without consuming anything.
{-# INLINE withConsumedBytes #-}
withConsumedBytes :: Get a -> Get (a, ByteString)
withConsumedBytes decoder = do
    (val, len) <- lookAhead $ do
      before <- bytesRead
      val <- decoder
      after <- bytesRead
      return (val, after - before)
    bs <- getByteString (fromIntegral len)
    return (val, bs)
